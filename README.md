# 幂等事件异步投递链路（本地存储模拟消息代理）

一条可在本地完整运行的异步事件投递链路。用 **本地 JSON 文件存储**模拟消息代理，
提供事件接收、幂等提交、异步投递、退避重试、死信、顺序保证、重启恢复与运行观测能力。

- Java 21 + Spring Boot 3.4，无外部中间件依赖
- 存储：`data/event-store.json`（每次变更原子重写快照：临时文件 + atomic move）
- 投递：单线程轮询领取 + 每个顺序键一个单线程执行器

---

## 1. 投递语义

### 1.1 幂等提交

- 每个事件必须携带 **业务幂等键 `idempotencyKey`**，全库唯一。
- 同一幂等键的重复请求**绝不会产生多次投递**。
- 同一幂等键的并发请求在“每键提交锁”上排队，只有**第一个请求生效**。
- 提交结果通过 `outcome`（以及 HTTP 状态码）明确区分：

| outcome | HTTP | 含义 |
|---|---|---|
| `ACCEPTED` | 201 | 首次提交，事件已创建并入队 |
| `REJECTED_CONCURRENT` | 409 | 相同幂等键有另一个请求**同时并发**到达，本次未生效 |
| `DUPLICATE_INFLIGHT` | 409 | 相同幂等键的事件此前已提交、尚在处理中，本次为顺序重复，未生效 |
| `REUSED_DELIVERED` | 200 | 事件此前已投递成功，本次直接**幂等复用**已有结果 |
| `REUSED_DEAD_LETTER` | 200 | 事件此前已进入死信，本次复用该终态结果 |

> 判定“并发”与“普通顺序重复”的依据：第二个请求进入提交临界区时，首个请求是否仍在临界区内。
> 终态（成功/死信）的重复提交一律是幂等复用，不会再次投递。

### 1.2 投递保证：at-least-once + 幂等收敛

- 事件在三个关键点原子落盘：**领取（PROCESSING）→ 成功（DELIVERED）/ 失败（PENDING 或 DEAD_LETTER）**。
- 正常路径下每个事件恰好被下游处理一次。
- 崩溃可能发生在“下游已处理成功、但成功确认未落盘”之间，因此投递保证是 **at-least-once**：
  重启后该事件会被重新投递一次。下游消费方应保持业务幂等（用幂等键去重）。
- 已落盘为 `DELIVERED` 的事件**永远不会被重新投递**；未成功的事件不会被静默丢弃。

### 1.3 顺序保证

- 事件携带 `orderKey`（缺省等于 `idempotencyKey`）。
- **相同 `orderKey` 的事件严格按提交顺序（自增 seq）串行投递（FIFO）**：
  - 存储层只允许领取每个顺序键的**队首**事件；
  - 同键已有在途（PROCESSING）事件时，后续事件不领取；
  - 队首处于退避等待时，同键后续事件也不得越过队首。
- **不同 `orderKey` 之间互不阻塞**：每个顺序键绑定独立的单线程执行器，可并行投递。

### 1.4 状态机

```
                 ┌──────────────── 退避到期重新领取 ───────────────┐
                 ▼                                                   │
 提交 ──► PENDING ──领取(attempt+1)──► PROCESSING ──成功──► DELIVERED（终态）
           ▲                              │
           │                              │ 失败
           └────────退避(nextAttemptAt)────┤
                                          │ 且 attempt >= maxAttempts
                                          ▼
                                      DEAD_LETTER（终态，保留最后失败原因）
```

---

## 2. 重试与死信规则

- 下游处理器抛出任意异常都视为本次投递失败，失败原因记录为 `异常类名: message`。
  - `TransientDeliveryException`：临时性失败（如超时、网络抖动）
  - `PermanentDeliveryException`：永久性失败（如校验不通过）
  - 两类失败目前都走同一套“重试到上限 → 死信”的规则，但**保留的异常类型名可区分原因**。
- 第 n 次尝试失败后的退避（指数退避，封顶）：

  ```
  backoff(n) = min(backoffMaxMs, backoffInitialMs * backoffMultiplier^(n-1))
  ```

  默认：500ms × 2^n，封顶 30s → 第 1、2 次失败后分别等待约 500ms、1000ms。
- 尝试次数 `attempt` 在**领取**时 +1（第 1 次领取 attempt=1）。
- 当 `attempt >= pipeline.max-attempts`（默认 3）仍失败，事件进入 **DEAD_LETTER**：
  - 不再投递、不阻塞同键后续事件；
  - 保留**最后一次失败原因**与尝试次数，可通过查询接口查看；
  - 对死信事件重复提交返回 `REUSED_DEAD_LETTER`，不会重新触发投递。
- **重启会重置在途退避**：进程重启代表一次新的投递机会，未完成事件（含崩溃在途与退避等待中）
  会被立即重新投递，而不是继续等待旧进程安排的退避时间。

### 崩溃/重启恢复

worker 启动时执行恢复：

1. 残留的 `PROCESSING`（上次进程 kill -9 / 崩溃时正在投递）租约作废，退回 `PENDING`；
2. 所有未完成 `PENDING` 事件尚未到期的退避时间重置为“立即可领取”；
3. `DELIVERED` / `DEAD_LETTER` 终态绝不改动。

worker 通过唯一 `workerId` 持有租约；成功/失败确认都带租约 CAS 校验，错误的持有者无法完成事件。

---

## 3. 可配置项（application.properties，前缀 `pipeline`）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `pipeline.data-dir` | `data` | 本地存储数据目录 |
| `pipeline.store-file` | `event-store.json` | 快照文件名 |
| `pipeline.poll-interval-ms` | `200` | worker 轮询可投递事件的间隔 |
| `pipeline.max-attempts` | `3` | 单事件最大投递尝试次数，达到后进死信 |
| `pipeline.backoff-initial-ms` | `500` | 首次失败后退避时长 |
| `pipeline.backoff-multiplier` | `2.0` | 退避倍率 |
| `pipeline.backoff-max-ms` | `30000` | 退避上限 |
| `pipeline.claim-batch-size` | `64` | 每轮最多领取并分发的事件数 |
| `server.port` | `8080` | HTTP 端口（测试中为 0 随机端口）|

---

## 4. HTTP 接口

### 提交事件
```bash
curl -X POST http://localhost:8080/api/events \
  -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"order-123","orderKey":"user-7","payload":"{\"amount\":100}"}'
```
响应：
```json
{ "outcome": "ACCEPTED",
  "event": { "seq":1, "idempotencyKey":"order-123", "orderKey":"user-7",
             "status":"PENDING", "attempt":0, "nextAttemptAt":"...",
             "lastFailureReason": null } }
```

### 查询单条事件状态
```bash
curl http://localhost:8080/api/events/order-123
```

### 整体投递统计
```bash
curl http://localhost:8080/api/events/stats
# {"total":3,"pending":0,"processing":0,"delivered":2,"deadLetter":1}
```

### 本地验证用管理接口（控制模拟下游）
```bash
# 让某幂等键前 2 次投递以临时错误失败（permanent=true 则为永久错误）
curl -X POST http://localhost:8080/api/events/admin/fail-rules \
  -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"order-123","failTimes":2,"permanent":false}'
```
模拟规则只保存在进程内存中（模拟下游自身状态），进程重启后清空；事件状态仍从文件恢复。

---

## 5. 本地运行

需要 JDK 21+、Maven 3.9+。

```bash
# 启动（默认 8080，存储在 ./data/event-store.json）
mvn spring-boot:run
```

按第 4 节的 curl 操作即可观察：
`PENDING → PROCESSING → DELIVERED`，失败时的退避重试，以及耗尽后的 `DEAD_LETTER`。

验证“重启恢复”：
1. 为某键配置失败规则并提交，让其处于投递中或退避中；
2. 直接 `kill -9 <java pid>`（或 Ctrl+C）终止进程；
3. 重新 `mvn spring-boot:run`，观察日志 `恢复中断事件 ... PROCESSING->PENDING`
   或 `重启重置退避 ...`，未完成事件随后被继续处理，已完成事件不重复投递。

---

## 6. 测试

```bash
mvn test
```

测试清单（14 个用例）：

| 测试类 | 覆盖场景 |
|---|---|
| `FileEventStoreTest` | 8 线程并发同键提交（仅 1 个 ACCEPTED，其余 REJECTED_CONCURRENT）；顺序重复 vs 并发拒绝 vs 终态复用；同键 FIFO、队首退避阻塞同键、不同键不阻塞；重试到上限进死信并保留可区分原因；租约 CAS；PROCESSING 重启恢复；PENDING 退避重启重置（共 7 个用例） |
| `ConcurrentSubmitHttpTest` | 真实 HTTP 端口 6 路并发同键提交：恰好 1 个 201，其余 409 |
| `RetryDeadLetterIntegrationTest` | 临时失败退避重试后成功；永久失败耗尽尝试进死信、保留原因、死信后不再投递 |
| `OrderingIntegrationTest` | 同键 FIFO（队首阻塞时队尾不得开始）；不同键并行不阻塞 |
| `RestartRecoveryIntegrationTest` | 退避中 PENDING 事件重启后续投成功、已成功事件不重复；投递中 PROCESSING 被强杀后恢复重投（at-least-once，attempt 累加） |

日志（控制台与 surefire 输出）会打印每个关键节点的**幂等键、尝试次数 attempt 与状态变化**，例如：

```
接受新事件 idempotencyKey=k1 orderKey=o1 seq=1
领取事件 idempotencyKey=k1 orderKey=o1 attempt=1 lease=...
投递失败待重试 idempotencyKey=k1 seq=1 attempt=1 下次尝试=500ms 后 原因=TransientDeliveryException: ...
事件进入死信 idempotencyKey=k3 seq=3 attempt=3 最后失败原因=PermanentDeliveryException: ...
投递成功 idempotencyKey=k1 seq=1 attempt=3
恢复中断事件 idempotencyKey=k4 seq=4 已完成尝试次数=1 PROCESSING->PENDING
重复提交 idempotencyKey=k1 状态=DELIVERED 并发到达=false 结论=REUSED_DELIVERED
```

---

## 7. 代码结构

```
domain/    EventStatus、SubmitOutcome、EventRecord（事件记录）
config/    PipelineProperties（pipeline.* 配置）
store/     EventStore 接口 + FileEventStore（JSON 快照、锁、CAS、FIFO 领取、恢复）
handler/   DeliveryHandler、模拟下游 SimulatedDeliveryHandler、临时/永久异常
pipeline/  PipelineWorker（轮询、按顺序键串行投递、退避/死信、生命周期、重启恢复）
service/   EventService（提交）、ObservabilityService（查询/统计）
web/       EventController 与 DTO（提交/查询/统计/管理接口）
```
