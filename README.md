# trace-analytics-go

本地分布式追踪数据查询与下钻分析服务。接收带追踪标识的 span 并持久化，提供轨迹检索、
聚合统计、依赖分析、关键路径与逐级下钻。写入采用行帧 WAL + 不可变快照，重启后数据可用。

## 快速开始

```bash
go build ./...
go test ./...
go run . -addr :8080 -data-dir ./data
```

主要启动参数（同名大写环境变量也可配置，如 `MAX_SPANS`、`QUERY_TIMEOUT`）：

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `-addr` | `:8080` | HTTP 监听地址 |
| `-data-dir` | `data` | WAL 与数据目录 |
| `-max-spans` | `1000000` | 全局持久化 span 上限，`0` 不限 |
| `-max-spans-per-trace` | `10000` | 单条轨迹 span 上限 |
| `-query-timeout` | `5s` | 单次查询超时 |
| `-snapshot-history` | `32` | 为稳定翻页保留的已提交快照数 |

完整语义、数据模型与资源限制见 [`docs/design.md`](docs/design.md)。

## HTTP API

所有时间字段均为 Unix 纳秒；枚举状态为 `STATUS_OK` / `STATUS_ERROR` /
`STATUS_UNSPECIFIED`。

### 写入

`POST /api/v1/batches`

```json
{
  "batchId": "b-001",
  "spans": [
    {
      "traceId": "t1", "spanId": "s1", "parentSpanId": "",
      "service": "api", "operation": "GET /users",
      "startUnixNano": 100, "durationNano": 250,
      "status": "STATUS_ERROR",
      "attributes": {"http.method": "GET", "env": "prod"}
    }
  ]
}
```

响应把“正常接受”和“隔离数据”分开报告：

```json
{"batchId":"b-001","accepted":1,"duplicate":0,
 "rejected":[{"span":{...},"reason":"MALFORMED","detail":"spanId is required"}],
 "committedSeq": 7}
```

`reason` 取值：`MALFORMED`（结构非法）、`DUPLICATE`（完全重复）、
`CONFLICT`（同 spanId 但内容不同）、`LIMIT_EXCEEDED`（越过资源上限）。

### 检索（稳定游标分页）

`GET /api/v1/traces`

| 参数 | 语义 |
| --- | --- |
| `service` | 逗号分隔，命中其一即可（列表内 OR） |
| `operation` | 逗号分隔 OR |
| `attr.<key>=<value>` | 标签精确匹配，多个 `attr.*` 之间 AND |
| `minDurationNano` / `maxDurationNano` | span 耗时区间 |
| `error` | `ANY`(默认) / `ERRORS_ONLY` / `OK_ONLY` |
| `startUnixNano` / `endUnixNano` | 时间窗口 |
| `limit` / `cursor` | 分页大小与不透明游标 |

首页响应在 `nextCursor` 中钉住当前快照序号；后续页始终读取同一不可变快照，
翻页期间持续写入不会重复或遗漏。若该快照已被裁剪，返回 `410 Gone`
（`cursor expired`），调用方应重新开始翻页。

### 下钻

- `GET /api/v1/traces/{traceId}` — 聚合结果 → 轨迹全量 span
- `GET /api/v1/traces/{traceId}/spans/{spanId}` — 轨迹 → 单 span 明细
- `GET /api/v1/traces/{traceId}/critical-path` — 决定总耗时的瓶颈 span 链

### 统计与依赖

- `GET /api/v1/stats?bucketNanos=60000000000&quantiles=0.5,0.9,0.99`
  返回按 `service + operation + 时间桶` 聚合的调用次数、错误率、平均延迟与分位数。
  分位数由可合并 t-digest 估算，内存按质心数有界，高基数下不做全量排序。
- `GET /api/v1/dependencies` 返回父子 span 推导出的服务依赖边
  （调用次数、错误率、平均耗时）。

### 隔离数据与健康检查

- `GET /api/v1/quarantine?limit=100` — 最近被隔离的 span
- `GET /healthz`

## 主要包

- `model`：数据模型与哨兵错误
- `config`：资源限制与超时配置
- `tdigest`：有界内存、可合并、可序列化的 t-digest
- `store`：WAL 持久化、启动重放/压缩、不可变快照链
- `ingest`：结构校验与批量隔离
- `query`：检索、分页、统计、依赖、关键路径
- `service`：HTTP API
