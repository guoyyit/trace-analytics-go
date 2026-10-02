# trace-analytics-go

本地分布式追踪数据查询与下钻分析服务。接收带追踪标识的 span，完成乱序组装、持久化、
轨迹检索、聚合统计、依赖分析、关键路径与逐级下钻；重启后已提交数据完整可用。

## 架构总览

```
HTTP (server)
   │
service ── 快照隔离门面（当前不可变 View + 钉选旧 View 环）
   │
storage ── 不可变 View / 增量构建器 / WAL + 快照
   │
ingest（校验、去重、轨迹组装）  query（过滤、游标分页）  analytics（t-digest、统计、依赖、关键路径）
```

核心并发模型：每个已提交批次产生一个**不可变 View**。写入时先追加 WAL，再原子切换
`current` 指针；查询只读取某一个 View，因此永远对应某个完整已提交批次，不会看到半条
轨迹或统计跳变。旧 View 以有界环保留（`MaxPinnedViews`），用于继续服务绑定在旧版本
上的翻页游标。

## 数据模型

- `Span`（`model/model.go`）：`traceId`、`spanId`、`parentSpanId`（根为空）、
  `serviceName`、`operationName`、`startTime`、`duration`、`tags(map[string]string)`、
  `status(0 未指定 / 1 OK / 2 ERROR)`。
- `Trace`：同一 `traceId` 的全部 span 组装结果，含有序 `spans`、树 `roots`、
  `complete`、`missingParentIds`。轨迹时间窗 = 最早 `startTime` 到最晚
  `startTime+duration`。
- `Batch`：一次原子提交，带单调递增 `Seq`。一次 `POST /v1/spans` 即一个批次。
- `RejectedSpan`：被隔离的畸形/冲突输入，携带输入下标与拒绝原因，与正常结果分离存储，
  可通过 `/v1/rejected` 查询，并计入 `total`（保留条数有界）。

## 写入与组装语义（需求 1）

- **乱序 / 迟到**：span 按 `traceId` 分组、按 `spanId` 累积；晚到的父节点到达后，轨迹
  会重新组装并从 `complete=false` 变为 `complete=true`。缺父期间轨迹仍可查询（
  `missingParentIds` 标出缺失父 id），默认查询隐藏未完成轨迹，可显式包含。
- **重复**：同一 `(traceId, spanId)` 且内容完全相同 → 幂等忽略，计入 `duplicates`，
  不重复存储、不改变结果。
- **冲突重复**：同一 id 但内容不同 → 隔离为 `conflicting duplicate span`。
- **畸形数据**：结构校验失败（缺 traceId/spanId/service/operation/startTime、负耗时、
  非法 status、自己是自己父节点、id/名称/tag 超长、空 tag key 等）→ 隔离到
  rejected，绝不进入正常轨迹。畸形 JSON 请求体直接返回 `400`。
- **父子环**：检测到 parent 环时切断回边、将节点提升为根、记录缺失父 id，轨迹标记为
  `complete=false`，且不会造成无限递归。
- **原子性**：一个批次内被接受的 span 全部一起可见；校验失败的不影响其余 span。

## 查询过滤与组合语义（需求 2）

`GET /v1/traces` 参数（所有条件之间为 **逻辑 AND**）：

| 参数 | 语义 |
| --- | --- |
| `service` | 轨迹中**任一 span** 的 serviceName 精确匹配 |
| `operation` | 轨迹中**任一 span** 的 operationName 精确匹配（与 service 对同一 span 取 AND） |
| `tagKey` / `tagValue` | 任一 span 含该 tag；给了 value 则值也必须相等 |
| `error=true` | 任一 span `status=ERROR` |
| `minDuration` / `maxDuration` | 作用于**整条轨迹**总耗时（end-start），Go duration 字符串 |
| `start` / `end` | 轨迹时间窗与区间相交（轨迹 end ≥ start 且轨迹 start ≤ end），RFC3339 |
| `includeIncomplete=true` | 是否包含缺父/有环的未完成轨迹，默认不含 |

统计/依赖接口复用相同字段，但时间、耗时条件作用在**单个 span** 上（聚合语义）。

### 稳定游标分页

- 排序键固定为 `(轨迹开始时间升序, traceId 升序)`。
- 游标是 base64(JSON)：`{viewSeq, lastKey}`。首页不带游标，返回 `nextCursor` 与
  `hasMore`。
- 翻页时服务按 `viewSeq` 钉选对应旧 View，因此**翻页期间持续写入不会造成重复或遗漏**：
  整轮翻页始终反映首页时刻的快照。
- 游标损坏返回 `410 Gone`；旧 View 已被淘汰（超过 `MaxPinnedViews` 次提交）也返回
  `410`，客户端应以空游标重新开始。
- 全新查询（不带游标）总是读取最新 View。

## 统计、依赖、下钻（需求 3、4）

- `GET /v1/stats?bucket=1m&groupBy=operation&quantiles=0.5,0.9,0.95,0.99`
  - 按对齐时间桶 + service（可选 operation）分组，输出 `count`、`errorCount`、
    `errorRate` 与请求的延迟分位数（基于 span duration）。
  - 时间桶粒度可配（任何 Go duration）。组数受 `MaxGroups`（默认 10000）有界保护。
  - 分位数使用**合并式 t-digest**（压缩系数 100）：内存约为 O(压缩系数) 个质心，
    质心可序列化、可合并，尾部相对精度高；**绝不保存或全量排序原始值**。
- `GET /v1/dependencies`：用父子 span 关系聚合服务间调用边
  `(parentService -> childService)`，输出调用次数、错误次数/错误率、总耗时；
  同服务内调用不计为依赖边。
- 下钻路径：
  1. `GET /v1/traces?...` 聚合结果对应的轨迹列表（过滤条件可直接复用）；
  2. `GET /v1/traces/{traceId}` 单条轨迹（含完整 span 树）；
  3. `GET /v1/traces/{traceId}/spans/{spanId}` 单个 span 明细。
- `GET /v1/traces/{traceId}/critical-path`：关键路径。从“子树结束最晚”的根开始，
  每层选择“子树结束最晚”的子节点，输出决定总耗时的瓶颈 span 链（含服务、操作、
  状态、耗时）。

## 持久化与崩溃恢复（需求 5、6）

- **WAL**（`data/wal.log`）：帧格式 `magic(1)|type(1)|len(4)|crc32(4)|payload`，
  payload 为 batch 或 rejected 记录的 JSON。默认每帧 `fsync`（`SyncWAL`）。
- 提交流程严格为 **WAL 追加成功 → 内存 View 切换**。WAL 追加失败则内存绝不前进，
  因此查询永远只能读到已提交状态。
- 重启恢复：读取 `snapshot.json`（若有）后，重放所有 `Seq > snapshot.lastSeq` 的
  WAL 批次与 rejected 帧，重建最终 View。
- **未完成批次**：崩溃时只写了一半的帧（长度截断、CRC 不符、未知类型、坏 JSON）会在
  打开 WAL 时被识别并在最后一个完整帧处**截断丢弃**。
- **损坏快照**：快照 JSON 解析失败时忽略快照、仅依赖 WAL 重放，不阻止启动。
- **快照轮转**：每 `SnapshotAfterBatches` 个批次原子写快照（tmp + rename + fsync
  目录），随后用临时文件 + rename 重写 WAL。崩溃发生在中间也安全：旧 WAL/快照至少
  有一份完整可用，重放按 Seq 去重。

## 资源限制与降级（需求 7）

所有限制集中在 `config.Config`（可用 flag/代码覆盖）：

| 配置 | 默认 | 作用 |
| --- | --- | --- |
| `MaxSpans` | 2,000,000 | 全局保留 span 硬上限，超限批次中的 span 被拒绝并给出 `global span limit exceeded` |
| `MaxSpansPerTrace` | 10,000 | 单轨迹上限，防止超长轨迹失控，拒绝原因 `trace span limit exceeded` |
| `MaxTagIndexKeys` | 512 | 纳入索引/统计的不同 tag key 上限；饱和后新 key 不再扩张集合（`tagSaturated`），防止高基数标签内存无界 |
| `MaxRejectedRetained` | 10,000 | 保留的畸形记录明细上限，`total` 仍持续累计 |
| `MaxPinnedViews` | 16 | 为稳定游标保留的旧 View 数（每个 View 仅增量持有变化轨迹，未变轨迹结构共享） |
| `MaxGroups`（统计 API） | 10,000 | 单次聚合分组上限 |
| `QueryTimeout` | 5s | 单次查询上限；扫描周期性检查超时，超时返回 `504`（`model.ErrQueryTimeout`） |
| 分页 `limit` | 50，上限 1000 | 超限返回 `400` |
| `SnapshotAfterBatches` | 128 | 快照/WAL 压缩频率 |

超限不静默丢数据：写入超限的 span 进入 rejected 并返回明确原因；查询超时返回明确错误；
游标过期返回 `410` 并提示重新开始（可解释的降级）。

## HTTP API 摘要

| 方法/路径 | 说明 |
| --- | --- |
| `POST /v1/spans` | body `{"spans":[...]}`，返回 seq/accepted/duplicates/rejected |
| `GET /v1/traces` | 组合过滤 + `limit` + `cursor` 分页 |
| `GET /v1/traces/{traceId}` | 轨迹与 span 树 |
| `GET /v1/traces/{traceId}/spans/{spanId}` | 单 span 明细 |
| `GET /v1/traces/{traceId}/critical-path` | 关键路径 |
| `GET /v1/stats` | 时间桶 + service/operation 统计与分位数 |
| `GET /v1/dependencies` | 服务依赖图 |
| `GET /v1/rejected` | 被隔离的畸形记录与累计总数 |
| `GET /healthz` | 健康检查 |

错误码：`400` 参数/JSON 错误、`404` 不存在、`410` 游标无效或过期、`413` 存储/轨迹
超限、`504` 查询超时。

## 运行

```bash
go run ./cmd/traced -addr :8080 -data ./data
# flags: -addr -data -query-timeout -max-spans
```

## 测试

```bash
go test ./...
go test -race ./...
```

覆盖场景（需求 8）：

- 乱序/重复/缺父（晚到父节点补全）/父子环/畸形隔离（`ingest`、`service` 测试）
- 并发读写下的快照一致性与最终计数（含 `-race`）
- WAL 截断恢复、提交批次恢复、快照损坏启动、快照轮转后恢复（`storage` 测试）
- t-digest 均匀/偏态分布分位数精度、合并与快照往返（`analytics` 测试）
- 组合过滤语义与翻页期间写入不重不漏（`query` 测试）
- HTTP 端到端下钻、统计、依赖与错误码（`server` 测试）
