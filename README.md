# Idempotent Event Pipeline (Java / Spring Boot)

一条可在本地完整运行的异步事件投递链路：以本地 JSONL 文件模拟消息代理的持久化语义，
在**重复提交、消费失败、进程重启**等情况下提供稳定且可解释的投递语义。

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
                     │--失败且 attempts >= maxAttempts--> DEAD_LETTER (终态, 保留 lastError)
重启恢复: 启动时所有残留 IN_FLIGHT --> PENDING (立即重新调度)
```
- 状态迁移基于**乐观版本号**（`version`），认领失败即放弃，避免并发重复投递。
- 重启后未完成事件继续处理：处理中的事件被重置为待投递（不静默丢弃），
  已完成/死信事件为终态，不会被重复完成。

### 顺序性
- 相同 `orderKey` 的事件同一时刻最多投递一条，严格按提交先后保持相对顺序。
- 不同 `orderKey` 之间完全并行，单键阻塞（如消费端卡住）不影响其他键。

### 重试与死信
- 消费回调抛异常即视为投递失败，记录 `lastError`（异常类型 + 消息）。
- 退避策略：指数退避 `backoffBaseMs * 2^(attempt-1)`，封顶 `backoffMaxMs`。
- 达到 `maxAttempts` 后进入死信（`DEAD_LETTER`），保留最后一次失败原因，不再被调度。

## 可配置项（`application.properties`，前缀 `pipeline.`）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `pipeline.store-path` | `data/events.jsonl` | 本地事件存储文件路径 |
| `pipeline.max-attempts` | `3` | 单条事件最大尝试次数，超限进死信 |
| `pipeline.backoff-base-ms` | `200` | 指数退避基数（毫秒） |
| `pipeline.backoff-max-ms` | `5000` | 退避上限（毫秒） |
| `pipeline.poll-interval-ms` | `100` | 调度器轮询间隔（毫秒） |

## REST API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/events` | 幂等提交：`{"idempotencyKey","orderKey","payload"}`；201=受理，200=重复复用 |
| GET | `/api/events/{id}` | 按事件 ID 查询当前状态 |
| GET | `/api/events/by-key/{idempotencyKey}` | 按幂等键查询 |
| GET | `/api/stats` | 各状态事件数量统计 |
| GET | `/api/dead-letters` | 死信列表（含失败原因与尝试次数） |

## 本地验证

```bash
# 运行全部测试（并发重复提交、失败重试、死信、重启恢复、API 端到端）
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

# 验证重启恢复：kill 进程后重新启动，未完成的 PENDING/IN_FLIGHT 事件会继续投递
```

日志中会打印幂等键、尝试次数与关键状态迁移，例如：

```
事件状态迁移: id=... idempotencyKey=bad-1 attempts=2 IN_FLIGHT -> PENDING lastError=IllegalStateException: 模拟消费失败...
投递失败，进入死信: id=... idempotencyKey=bad-1 attempt=3 reason=IllegalStateException: ...
重启恢复: 1 条 IN_FLIGHT 事件已重置为 PENDING
```

## 测试覆盖

| 测试类 | 场景 |
|---|---|
| `FileEventStoreTest` | 幂等键唯一约束、IN_FLIGHT 重启恢复、失败原因跨重试/死信保留 |
| `ConcurrentSubmitTest` | 32 线程并发提交同一键：恰好 1 个 ACCEPTED，其余 DUPLICATE 且复用同一事件 |
| `EventDispatcherTest` | 退避重试后成功、超限进死信、同键串行/跨键不阻塞、重启后继续投递 |
| `EventApiTest` | REST 端到端：提交去重、状态查询、统计、死信接口 |
