# Idempotent Event Pipeline (Java / Spring Boot)

一条可在本地完整运行的异步事件投递链路：以本地 JSONL 文件模拟消息代理的持久化语义，
在**重复提交、消费失败、进程重启**等情况下提供稳定且可解释的投递语义；
下游故障修复后，死信事件可通过**幂等重投**（单条/批量、带配额上限）重新进入投递链路。

## 架构

```
POST /api/events ──> EventService ──> FileEventStore (data/events.jsonl)
                                          │  内存索引 + 追加快照持久化
                                          ▼
                     EventDispatcher <── 轮询到期 PENDING 事件
                          │  按 orderKey 串行、跨键并行（虚拟线程）
                          ▼
                     EventConsumer（业务回调，默认实现可模拟失败）
                          │
              成功 → DELIVERED / 失败 → 退避重试 / 超限 → DEAD_LETTER
                                                              │
                              POST /api/dead-letters/{id}/replay（或批量重投）
                                                              ▼
                                              重新入队 PENDING（保留原顺序位，重投次数+1）
```

## 投递语义

### 幂等提交
- 每个事件携带业务**幂等键**（`idempotencyKey`），存储层对其施加唯一约束。
- 重复提交（含并发）只会有一个请求被受理（HTTP 201, `outcome=ACCEPTED`），
  其余请求复用首次创建的事件（HTTP 200, `outcome=DUPLICATE`），**不会产生多次投递**。
- 两种结果可通过响应体 `outcome` 字段明确区分。

### 状态机
```
PENDING --claim--> IN_FLIGHT --成功--> DELIVERED (终态)
                     │--失败且 attempts < maxAttempts--> PENDING (退避后重试)
                     │--失败且 attempts >= maxAttempts--> DEAD_LETTER (保留 lastError)
                                                          │--重投生效--> PENDING (replayCount+1, 保留原 createdAt)
重启恢复: 启动时所有残留 IN_FLIGHT --> PENDING (立即重新调度)
```
- 状态迁移基于**乐观版本号**（`version`），认领失败即放弃，避免并发重复投递。
- 重启后未完成事件继续处理：处理中的事件被重置为待投递（不静默丢弃），
  已完成/死信事件为终态，不会被重复完成。

### 顺序性
- 相同 `orderKey` 的事件同一时刻最多投递一条，严格按提交先后保持相对顺序。
- **队首阻塞**：同一顺序键内，只要最早的一条还没成功也没进死信（包括正在退避等待重试），
  后续事件一律不投出去，绝不会出现后提交的先完成。
- 死信重投后事件**保留原 `createdAt`**，按原来的先后位置重新参与同一顺序键的排队。
- 不同 `orderKey` 之间完全并行，单键阻塞（如消费端卡住）不影响其他键。

### 重试与死信
- 消费回调抛异常即视为投递失败，记录 `lastError`（异常类型 + 消息）。
- 退避策略：指数退避 `backoffBaseMs * 2^(attempt-1)`，封顶 `backoffMaxMs`。
- 达到 `maxAttempts` 后进入死信（`DEAD_LETTER`），保留最后一次失败原因，不再被调度。

### 死信重投
- 仅 `DEAD_LETTER` 状态的事件可重投；已成功（`DELIVERED`）或仍在正常处理中
  （`PENDING`/`IN_FLIGHT`）的事件发起重投会被拒绝（HTTP 409, `outcome=NOT_DEAD_LETTER`）并说明原因。
- **重投幂等**：重投生效是存储层的一次原子状态迁移（`DEAD_LETTER -> PENDING`），
  同一条死信被重复或并发重投时只有第一个请求生效（`outcome=REPLAYED`），
  其余请求得到可区分的拒绝结果（事件已不在死信态）。
- **重投配额**：每条事件最多重投 `maxReplays` 次（提交时从配置快照到事件上，
  老数据回退到当前配置值）。超过上限的重投被拒绝（`outcome=QUOTA_EXCEEDED`），
  拒绝原因持久化在事件的 `lastReplayRejectReason` 字段，重启后仍可查。
- **批量重投**：`POST /api/dead-letters/replay` 按创建时间从早到晚重投，
  单次条数受 `replayBatchMaxLimit` 截断，避免一次重投过多挤占正常的新事件提交与投递。
- 重投生效后 `attempts` 归零、开启新一轮退避重试；`replayCount`、`lastReplayAt`
  随状态快照持久化，重启后不丢，未完成的重投事件继续投递，已成功的不重复投递。
- 单条事件可通过查询接口看到 `replayCount`（已重投次数）、`lastReplayAt`（最近重投时间）、
  `remainingReplays`（剩余可重投次数）；`/api/stats` 额外汇总
  `totalReplays`（累计生效重投次数）与 `eventsReplayed`（被重投过的事件数）。

## 可配置项（`application.properties`，前缀 `pipeline.`）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `pipeline.store-path` | `data/events.jsonl` | 本地事件存储文件路径 |
| `pipeline.max-attempts` | `3` | 单条事件最大尝试次数，超限进死信 |
| `pipeline.backoff-base-ms` | `200` | 指数退避基数（毫秒） |
| `pipeline.backoff-max-ms` | `5000` | 退避上限（毫秒） |
| `pipeline.poll-interval-ms` | `100` | 调度器轮询间隔（毫秒） |
| `pipeline.max-replays` | `3` | 单条死信事件最大重投次数，超限拒绝重投 |
| `pipeline.replay-batch-max-limit` | `100` | 批量重投单次条数上限（请求超限按此截断） |

## REST API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/events` | 幂等提交：`{"idempotencyKey","orderKey","payload"}`；201=受理，200=重复复用 |
| GET | `/api/events/{id}` | 按事件 ID 查询当前状态 |
| GET | `/api/events/by-key/{idempotencyKey}` | 按幂等键查询 |
| GET | `/api/stats` | 各状态事件数量 + 重投汇总（`totalReplays`/`eventsReplayed`） |
| GET | `/api/dead-letters` | 死信列表（含失败原因、尝试次数与重投信息） |
| POST | `/api/dead-letters/{id}/replay` | 单条死信重投；200=生效（`REPLAYED`），404=事件不存在，409=非死信或超配额（附原因） |
| POST | `/api/dead-letters/replay` | 批量死信重投：`{"limit":N}`，缺省或超限按 `replayBatchMaxLimit` 截断 |

## 本地验证

```bash
# 运行全部测试（并发重复提交、失败重试、死信、死信重投、重启恢复、API 端到端）
mvn test

# 启动应用
mvn spring-boot:run

# 提交事件（首次 201 ACCEPTED，重复 200 DUPLICATE）
curl -i -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"order-123","orderKey":"user-1","payload":"hello"}'

# 模拟消费失败（payload 含 FAIL 会触发默认消费者抛异常），观察重试与死信日志
curl -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"bad-1","orderKey":"user-2","payload":"FAIL me"}'

# 查询状态 / 统计 / 死信
curl localhost:8080/api/events/by-key/bad-1
curl localhost:8080/api/stats
curl localhost:8080/api/dead-letters

# 死信重投（下游修复后）：单条重投，重复执行第二次会看到 409 NOT_DEAD_LETTER
curl -i -X POST localhost:8080/api/dead-letters/<事件ID>/replay

# 批量重投（limit 可选，超过 pipeline.replay-batch-max-limit 会被截断）
curl -X POST localhost:8080/api/dead-letters/replay -H 'Content-Type: application/json' -d '{"limit":10}'

# 查看重投次数 / 最近重投时间 / 剩余可重投次数
curl localhost:8080/api/events/by-key/bad-1   # replayCount / lastReplayAt / remainingReplays

# 验证重启恢复：kill 进程后重新启动，未完成的 PENDING/IN_FLIGHT 事件（含已重投的）会继续投递，
# 已用掉的重投次数（replayCount）不丢，已成功的事件不会重复投递
```

日志中会打印幂等键、尝试次数与关键状态迁移，例如：

```
事件状态迁移: id=... idempotencyKey=bad-1 attempts=2 IN_FLIGHT -> PENDING lastError=IllegalStateException: 模拟消费失败...
投递失败，进入死信: id=... idempotencyKey=bad-1 attempt=3 reason=IllegalStateException: ...
死信重投生效: id=... idempotencyKey=bad-1 orderKey=user-2 replayCount=1/3 DEAD_LETTER -> PENDING
重投被拒绝（超限）: id=... idempotencyKey=bad-1 replayCount=3 maxReplays=3
重启恢复: 1 条 IN_FLIGHT 事件已重置为 PENDING
```

## 测试覆盖

| 测试类 | 场景 |
|---|---|
| `FileEventStoreTest` | 幂等键唯一约束、IN_FLIGHT 重启恢复、失败原因跨重试/死信保留 |
| `ConcurrentSubmitTest` | 32 线程并发提交同一键：恰好 1 个 ACCEPTED，其余 DUPLICATE 且复用同一事件 |
| `EventDispatcherTest` | 退避重试后成功、超限进死信、同键串行/跨键不阻塞、重启后继续投递 |
| `EventApiTest` | REST 端到端：提交去重、状态查询、统计、死信接口 |
| `DeadLetterReplayTest` | 16 线程并发重投仅 1 次生效、超配额被拒且原因跨重启保留、批量上限与配置截断、队首退避等待时同键不越序、重投后保持原顺序位、重投后重启恢复且不重复投递 |
| `ReplayApiTest` | REST 端到端：单条/批量重投、配额耗尽 409、非死信 409、统计中的重投汇总 |

## 与原有语义的兼容关系

- 重投只是新增一条 `DEAD_LETTER -> PENDING` 的状态迁移，不改变原有提交幂等、
  退避重试、死信与重启恢复语义；未使用重投功能时行为与之前完全一致。
- 重投后事件保留原 `createdAt`，按原顺序位参与排队，不抢占新提交事件的调度公平性；
  批量重投有上限截断，不会长时间占用存储锁或挤占正常投递。
- 存储格式向后兼容：老数据缺少重投字段时按 `replayCount=0`、`maxReplays` 回退当前配置处理；
  新增派生字段 `remainingReplays` 不参与反序列化。
- `/api/stats` 在原有各状态计数基础上**新增** `totalReplays`/`eventsReplayed` 两个键，原有键不变。
