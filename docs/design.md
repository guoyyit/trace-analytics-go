# 设计文档：数据模型、查询语义与资源限制

## 1. 数据模型

### Span

| 字段 | 含义 |
| --- | --- |
| `traceId` / `spanId` | 轨迹/span 标识，均必填，非空字符串 |
| `parentSpanId` | 父 span；根 span 为空；允许指向尚未到达的父节点 |
| `service` / `operation` | 服务名与操作名，必填 |
| `startUnixNano` / `durationNano` | 起始时间（Unix 纳秒）与耗时，均非负 |
| `status` | `STATUS_OK` / `STATUS_ERROR` / `STATUS_UNSPECIFIED` |
| `attributes` | 字符串标签 map，数量与值长度受限 |

### Trace（组装视图）

同一 `traceId` 的 span 在读取时组装为一条轨迹：

- 按 `(startUnixNano, spanId)` 稳定排序；
- `Start/End` 取所有 span 的最小开始与最大结束；
- `HasError`：存在任一 error span；
- `MissingParents`：`parentSpanId` 非空但在本轨迹内找不到父 span 的数量；
- `Services`：轨迹内出现过的服务集合。

### 到达语义（乱序 / 重复 / 迟到 / 缺父）

- **乱序、迟到**：span 按 `(traceId, spanId)` 增量并入对应轨迹，到达顺序无关；
  后到的更早 span 会把轨迹起点前移。
- **重复**：同一 `(traceId, spanId)` 且内容完全相同 → 隔离为 `DUPLICATE`，幂等。
- **冲突**：同一 `(traceId, spanId)` 但内容不同 → 隔离为 `CONFLICT`，保留首次提交。
- **缺失父节点**：照常入库并组装，仅在 `MissingParents` 中计数；父节点迟到后
  计数自然归零。
- **畸形数据**：结构校验失败（缺字段、负耗时、自己是自己父节点、未知状态等）
  → 隔离为 `MALFORMED`，绝不进入正常索引；通过 `/api/v1/quarantine` 单独查询。

## 2. 持久化与并发一致性

### WAL

每个已提交批次序列化为一行 JSON（`seq` 单调递增）追加到 `data/wal.log`，
默认追加后 `fsync`（`SyncWAL`）。行帧设计保证：

- 崩溃时最多丢失最后一行（未完成批次自然不存在“半行可见”的状态）；
- 启动重放逐行解析，任何损坏/截断行被跳过并计数（日志记录
  `skipped N corrupt WAL record(s)`），不影响其余数据，也不会阻止启动；
- 启动后立即把当前状态压缩重写 WAL（每条轨迹一行），避免死记录无限增长。

### 不可变快照（MVCC）

- 每次提交在锁内构造新快照：旧快照只读不动，新提交只写 overlay，
  因此查询与写入真正并发、互不阻塞。
- 每 `SnapshotHistory` 次提交把 overlay 链物化为全量 base（发布前完成，
  读者无竞争），链长度因此有界。
- 任何查询解析且只解析一个快照：**它看到的状态必定对应某个完整已提交批次**，
  不会出现半条轨迹或统计跳变。

## 3. 查询语义

### 组合过滤

`TraceFilter` 的字段在 **span 谓词层面做 AND**：

- 列表字段（`services`、`operations`）列表内部为 OR；
- 标签 `attributes`：所有给定 key 都要在**同一个 span** 上以相同 value 命中；
- `services / operations / attributes / span 耗时 / 错误状态`
  必须由轨迹中的**至少一个 span** 同时满足（存在量词）；
- 时间窗口 `start/end` 作用于轨迹边界：轨迹区间与窗口有交集才命中；
- 轨迹级耗时过滤作用于轨迹总跨度 `End - Start`。

因此 `service=db&attr.env=prod` 只匹配“存在一个 span 既属于 db 又带
`env=prod`”的轨迹；跨两个 span 的条件不算同时满足。

### 稳定分页

- 排序键：`(startUnixNano, traceId)`，定宽字符串编码，字典序与时间序一致；
  `traceId` 作为 tie-breaker 保证唯一、稳定。
- 首页把 `snapshotSeq` 与最后一个 key 编进不透明游标；后续页在该 seq 快照上
  继续 key 大于游标的记录。**翻页期间写入只影响更新的快照，不影响钉住的页**，
  因而不会重复或遗漏。
- 快照按 `SnapshotHistory` 保留；过期游标返回 `410 cursor expired`（可解释的
  显式错误，调用方重新发起首页即可）。

### 聚合统计

- 维度：`service × operation × 时间桶`；桶起点
  `floor(startUnixNano / bucketNanos) * bucketNanos`，桶大小必须为正。
- 指标：`count`、`errorCount`、`errorRate = error/count`、
  `avgDurationNano`、以及请求的分位数。`quantiles` 是以分位数字符串为键
  （如 `"0.5"`、`"0.99"`，JSON 对象键只能是字符串）的纳秒延迟 map。
- 分位数：每个分组维护一个 t-digest（质心数 `O(compression)`），
  Add/Merge 均为流式、内存有界；高基数下不会退化为全量排序。
  digest 支持 gob 序列化（为后续跨重启合并预留）。精度随 compression 提高，
  典型 100 时均匀分布 p99 误差约 1% 量级（见 `tdigest` 测试）。

### 依赖分析

遍历每条轨迹内的父子 span 对：父 span 的服务 → 子 span 的服务形成一条边；
根 span 与缺失父的 span 不产生边；同服务内部调用不产生跨服务边。
边按时间/服务/操作过滤聚合 `count / errorCount / errorRate / avgDuration`。

### 下钻路径

聚合 → 轨迹（`SearchTraces` 列表 → `GetTrace`）→ span（`GetSpan`）三级，
均在明确快照/最新提交上读取。

### 关键路径

把轨迹视为以 span 为节点、父子为边的 DAG。节点独占工作量
`exclusive = spanDuration - 覆盖时长(子 span 时间区间的并集)`；
关键路径 = 从根开始 `exclusive(自己) + best(子树)` 取最大的那条链。
并行分支只计入最慢的一条，串行段相加，结果即决定总耗时的瓶颈 span 链。
缺失父节点的 span 作为额外根参与分析。

## 4. 资源限制与降级

| 配置 | 行为 |
| --- | --- |
| `MaxSpans` | 全局 span 上限；越界的新 span 隔离为 `LIMIT_EXCEEDED`，不影响已提交数据 |
| `MaxSpansPerTrace` | 超长轨迹的额外 span 被隔离，查询/组装工作量随之有界 |
| `MaxAttributesPerSpan` | 标签按 key 排序后截断，防止高基数标签撑爆内存 |
| `MaxAttributeValueLen` | 标签值按字节截断 |
| `MaxQuarantineRecords` | 隔离记录用有界环形缓冲，防止坏数据无限占用内存 |
| `MaxPageLimit` / `DefaultPageLimit` | 限制单次返回规模，超限返回 `413 limit exceeded` |
| `QueryTimeout` | 每个查询带 context 截止时间，超时返回 `504 query timeout` |
| `SnapshotHistory` | 限制 MVCC 保留窗口，从而限制旧游标占用的内存 |

所有超限均以显式、可解释的错误或独立隔离结果返回，不会静默截断正常结果。

## 5. 崩溃恢复矩阵

| 场景 | 行为 |
| --- | --- |
| 已 `fsync` 的批次后崩溃 | 重启完整可见（`store` 崩溃恢复测试覆盖） |
| 最后一行写坏/截断 | 跳过该行并计数，其余数据正常加载，服务照常启动 |
| 未完成批次 | 从未发布快照，天然不可见；重启无需特殊回滚 |
| 压缩过程中崩溃 | 先写 `wal.log.compact` 再 `rename`，rename 原子替换；失败保留原 WAL |
