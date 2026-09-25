# task-lease-scheduler-java

本地可运行的任务租约调度示例：以本地共享内存状态模拟多实例竞争，保证同一任务在并发抢占、
租约失效与失败重试下的执行结论唯一、确定且可解释。无外网、无外部服务依赖。

## 核心概念

### 租约与 fencing token

- 每次成功获取租约都会分配一个**单调递增的版本号**（fencing token），即使租约被释放或
  到期回收，版本序列也不会重置。
- 任意时刻同一任务最多一个有效持有者；所有判定在 per-task 监视器锁内完成，
  并发抢占得到唯一结果：胜者拿到 `LeaseDecision.acquired=true`，
  其余竞争者得到 `LEASE_HELD_BY_OTHER` 及当前持有者与版本信息。

### 租约状态流转

```
                acquire                acquire(到期回收后)
 PENDING  ───────────────►  LEASED  ──────────────────────►  LEASED(新持有者, 版本+1)
   ▲                         │  │
   │ release                 │  ├── submitResult ──► SUCCEEDED (终态)
   │                         │  ├── reportFailure(RETRYABLE, 未超上限) ──► RETRY_WAITING
   │                         │  ├── reportFailure(NON_RETRYABLE 或超上限) ─► DEAD (终态)
   │                         │  └── 租约到期 reclaimExpired ──► PENDING
   │                         │
   └─────────────────────────┘
 RETRY_WAITING ──退避到期──► PENDING(可重新调度)
 任意非终态 ──cancel──► CANCELLED (终态，不再被调度)
```

- **获取 acquire**：PENDING / 退避到期的 RETRY_WAITING / 租约已过期的 LEASED 可获取；
  否则按 `TASK_FINISHED` / `RETRY_BACKOFF_NOT_ELAPSED` / `LEASE_HELD_BY_OTHER` 拒绝。
- **续约 renew**：仅当前持有者（owner + version 匹配且未过期）可续，版本不变、过期时间顺延。
- **释放 release**：仅当前持有者可释放，任务回到 PENDING。
- **到期回收 reclaimExpired**：过期租约被回收为 PENDING；`acquire`/`schedulable` 也会惰性回收。

### 幂等与所有权约束（结果提交）

`submitResult(taskId, ownerId, version, businessKey, payload)` 按以下顺序判定：

1. **幂等优先**：`businessKey` 已应用过 → `DUPLICATE`，不产生任何副作用，
   同一业务键的重复提交/重试安全。
2. **终态拒绝**：任务已 SUCCEEDED/DEAD/CANCELLED 且业务键未见过 → `TASK_FINISHED`。
3. **所有权校验**（可区分原因，绝不静默覆盖新持有者）：
   - 非当前持有者 → `NOT_LEASE_HOLDER`
   - 版本落后于当前租约（fencing token 被取代）→ `OWNERSHIP_SUPERSEDED`
   - 租约已过期 → `LEASE_EXPIRED`
4. 全部通过 → `ACCEPTED`：记录业务键、写入结果、任务进入 SUCCEEDED。

参数非法（空 businessKey、空 ownerId、非法 TTL）→ `INVALID_ARGUMENT`；
任务不存在 → `TASK_NOT_FOUND`。

### 错误分类与重试

- `ErrorKind.RETRYABLE`：执行超时、中断、下游暂时不可用。任务进入 `RETRY_WAITING`，
  按指数退避（`baseBackoff << (attempt-1)`，封顶 `maxBackoff`）等待后重新调度；
  已提交的业务键不会重复执行（幂等保证）。
- `ErrorKind.NON_RETRYABLE`：参数非法、业务校验失败等，直接进入 `DEAD` 终态。
- 重试次数达到 `maxAttempts` 后进入 `DEAD`；`CANCELLED`/`DEAD`/`SUCCEEDED` 均不再被调度。
- `RetryPolicy.defaults()`：3 次尝试、100ms 基础退避、5s 上限（测试可自定义）。

### 运行状态查询

`LeaseManager` / `GET /api/tasks` 提供：

- `snapshot(taskId)`：单任务状态、持有者、租约版本与剩余时间、失败原因；
- `schedulable()`：当前可调度任务；
- `byOwner(ownerId)`：按持有者过滤；
- `byLeaseRemainingWithin(millis)`：按租约剩余时间过滤；
- `byFailureReason(reason)`：按失败原因过滤。

## 本地验证

```bash
# 全部测试（并发抢占 / 租约到期 / 幂等重复提交 / 失败恢复，使用 MutableClock 可控时间源，无真实等待）
mvn test

# 并发用例日志会打印每次判定的租约版本与依据，例如：
# [concurrent-acquire] acquired=true  version=1 detail=acquired
# [concurrent-acquire] acquired=false reason=LEASE_HELD_BY_OTHER detail=held by owner-15 version=1 remainingMs=10000

# 本地启动 REST 服务（随机端口）
mvn spring-boot:run
```

## 代码结构

```
lease/Clock/MutableClock/SystemClock   可控时间源
lease/Lease                            租约（含 fencing token 版本）
lease/LeaseManager                     共享状态：获取/续约/释放/回收/提交/失败上报/查询
lease/TaskScheduler                    调度器：抢占 -> 执行 -> 提交或上报失败
lease/RetryPolicy                      重试次数与指数退避上限
lease/TaskRecord/TaskSnapshot          内部状态与只读视图
lease/RejectReason/ErrorKind/TaskStatus 可解释的判定枚举
web/TaskController                     本地验证 REST 接口
```
