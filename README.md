# 任务租约调度（Task Lease Scheduler）

在本地共享状态上模拟多实例竞争的任务调度能力：同一任务任意时刻只能有一个有效持有者；
租约到期/续约失败/被撤销后归属确定；结果提交满足幂等与所有权约束；
执行超时、中断与下游暂时不可用后进入可恢复重试链路，已完成步骤不重复执行。

- 无外部服务：进程内 `ConcurrentHashMap` 模拟多实例共享存储。
- 可控时间源：`MutableClock`，测试中推进时钟即可，不做真实等待。
- 全部用例可离线、可重复通过（`mvn -o test`）。

## 核心概念

- **租约（`Lease`）**：`(taskId, holderId, version, acquiredAt, expiresAt)`。
  `version` 是单调递增、**永不回退**的 fencing token（即使任务被释放/回收后重新获取也继续递增），
  是并发抢占与所有权判定的唯一依据。
- **共享状态（`LeaseStore`）**：所有“判定 + 状态迁移”都在记录级监视器锁（`synchronized(record)`）内原子完成，
  保证并发抢占得到唯一且可判定的结果。
- **检查点（`CheckpointStore`）**：记录每个任务已完成的步骤，恢复执行时跳过，避免重复副作用。

## 租约状态流转

```
                         acquire（无租约/租约到期/退避结束）
        PENDING ───────────────────────────────────────────────► LEASED
          ▲                                                         │
          │ release（持有者主动释放）                                 │ renew（持有者续约，version 不变）
          │                                                         │
          │ reclaimExpired / dueTasks（租约到期被回收）                │
          └─────────────────────────────────────────────────────────┤
                                                                    │
   reportFailure(RETRYABLE) 且次数未耗尽                              │ submitResult（所有权有效且未过期）
   ┌──────────────────────────────────────────┐                      ▼
   │                                          ├─────────────────► SUCCEEDED（终态）
   ▼                                          │                  结果幂等：同业务键重复提交=duplicate
RECOVERABLE ──(nextRetryAt 到达)──────────────┘
   │
   ├─ reportFailure(RETRYABLE) 且 attemptCount >= maxAttempts ──► DEAD（终态，不再调度）
   ├─ reportFailure(NON_RETRYABLE) ─────────────────────────────► FAILED（终态，不再调度）
   └─ cancel（PENDING/LEASED/RECOVERABLE 均可取消）──────────────► CANCELLED（终态，不再调度）
```

- `PENDING`：初始或释放/回收后，立即可调度。
- `LEASED`：存在唯一有效持有者；其他实例获取失败。
- `RECOVERABLE`：可重试失败，退避到 `nextRetryAt` 后才可重新获取（带指数退避 + 上限）。
- `SUCCEEDED / FAILED / DEAD / CANCELLED`：终态，不再被调度或获取。

## 幂等与所有权约束（结果提交）

`submitResult(lease, businessKey, result)` 的判定顺序：

1. `lease/businessKey/result` 缺失 → `INVALID_ARGUMENT`；
2. 任务已 `SUCCEEDED`：
   - 业务键与首次一致 → **`duplicate`**，返回首次结果，**不产生重复副作用**；
   - 业务键不同 → `INVALID_ARGUMENT`（拒绝，不覆盖）；
3. 任务处于其他终态（FAILED/DEAD/CANCELLED）→ `TERMINAL_TASK`；
4. 当前无租约或状态非 LEASED → `NOT_HOLDER`；
5. 当前租约 `version` **大于**提交凭据的 version → `OWNERSHIP_SUPERSEDED`
   （所有权已被新持有者取代）；
6. version 更小或 holder 不匹配 → `NOT_HOLDER`；
7. version/holder 匹配但已过期（`now >= expiresAt`）→ `LEASE_EXPIRED`；
8. 以上均通过才写入结果并迁移到 `SUCCEEDED`。

任何拒绝路径都不修改共享状态，因此迟到的旧持有者提交**永远不会静默覆盖**新持有者的结果。

续约 `renew` 与释放 `release` 同样要求 `(holderId, version)` 匹配；过期续约失败，
旧版本释放失败。失败上报 `reportFailure` 也校验所有权，过期持有者的迟到上报被忽略。

## 错误分类

| 情况 | 分类 | 结果 |
| --- | --- | --- |
| 执行超时（`executionTimeout`） | RETRYABLE | RECOVERABLE，指数退避后重试 |
| 执行中断（InterruptedException） | RETRYABLE | RECOVERABLE |
| 下游暂时不可用（未分类异常，如 RuntimeException） | RETRYABLE | RECOVERABLE |
| 显式抛出 `TaskFailureException(NON_RETRYABLE, …)` | NON_RETRYABLE | FAILED（终态） |
| 重试次数达到 `RetryPolicy.maxAttempts` | — | DEAD（终态） |
| 任务被 `cancel` | — | CANCELLED（终态） |

退避：`baseBackoff * 2^(attempt-1)`，以 `maxBackoff` 封顶（默认 100ms 起、5s 封顶、最多 3 次）。

## 运行状态查询

`TaskView` 可按任务查询：状态、当前持有者、租约 version、租约剩余时间、
已尝试次数、最近失败原因与分类（也可据此按持有者/剩余时间/失败原因筛选）。
`GET /api/leases/tasks` 返回全部任务视图。

## 本地验证

### 1. 运行全部测试（离线、无真实等待）

```bash
mvn -o test
```

- `ConcurrentAcquireTest`：32 线程并发抢占同一任务，唯一获胜者，日志打印租约 version 与判定依据。
- `LeaseExpiryTest`：租约到期回收、过期/被取代持有者提交被拒并区分原因。
- `IdempotentSubmitTest`：同业务键重复/并发重复提交，恰好一次 accept，其余 duplicate。
- `RetryRecoveryTest`：可重试退避、次数耗尽 DEAD、不可重试 FAILED、取消、迟到上报忽略、状态查询。
- `SchedulerRecoveryTest`：端到端成功、超时恢复（已完成步骤不重复执行）、不可重试、暂时性故障重试到成功。

### 2. 启动服务并用 REST 手动验证

```bash
mvn -o spring-boot:run -Dspring-boot.run.arguments=--server.port=18099

# 注册任务
curl -X POST "http://localhost:18099/api/leases/tasks/task-1?businessKey=biz-1"
# 获取租约（并发调用只有一个 acquired=true）
curl -X POST "http://localhost:18099/api/leases/tasks/task-1/acquire?holderId=local-1&ttl=PT30S"
# 查询状态（持有者、version、剩余时间、失败原因）
curl http://localhost:18099/api/leases/tasks/task-1
# 提交结果
curl -X POST -H 'Content-Type: application/json' \
  -d '{"lease":{"taskId":"task-1","holderId":"local-1","version":1,"acquiredAt":"...","expiresAt":"..."},"businessKey":"biz-1","result":"r1"}' \
  http://localhost:18099/api/leases/submit
# 到期回收
curl -X POST http://localhost:18099/api/leases/reclaim
```

> 注意：本应用默认使用 `MutableClock` Bean，REST 场景默认从真实 `Instant.now()` 起算，
> 其时间需通过获取的 `MutableClock` Bean 推进（测试中直接注入并 `advance`）。

## 主要代码结构

```
lease/
├── time/MutableClock.java          # 可控时间源
├── model/                          # TaskStatus / Lease / RetryPolicy / SubmitOutcome / TaskView …
├── store/LeaseStore.java           # 本地共享状态 + 记录级原子迁移
├── store/CheckpointStore.java      # 已完成步骤检查点
├── LeaseManager.java               # 获取/续约/释放/回收/提交/失败登记/查询
├── TaskScheduler.java              # 到期拉取 → 限时执行 → 幂等提交 → 恢复
├── TaskHandler.java                # 任务执行体接口
└── web/LeaseController.java        # 本地验证用 REST 入口
```
