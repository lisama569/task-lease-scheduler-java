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
   │                         │  ├── 执行超时(checkpoint)/被中断(可恢复) ──► RETRY_WAITING
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

### 执行时限与打断规则

一次执行有明确的时间上限（`executionTimeout`，默认 25s，短于默认 30s 租约 TTL），
不再依赖租约被动到期来收场：

- **超时**：执行器通过 `ExecutionContext.checkpoint()` 做协作式检查（建议在每个步骤前后调用）。
  一旦达到时限，检查点立即抛出 `ExecutionTimeoutException`，本次执行被确定性中止，
  原持有者按一次**可恢复失败**上报（`EXECUTION_TIMEOUT`），任务进入退避等待，
  退避到期即可被重新调度——不必干等租约自然到期。
  除时钟软时限外，还有 wall-clock 硬超时（`executionTimeout + hardTimeoutSlack`）兜底，
  防止不响应检查点的执行器永久挂住。
- **外部打断**：执行线程被中断（线程中断、实例要求停机）会得出确定结论
  `EXECUTION_INTERRUPTED`，同样按可恢复失败上报；**未跑完的执行绝不会被当成成功提交**。
- 构造 `TaskScheduler(leaseManager, clock, leaseTtl, executionTimeout, hardTimeoutSlack)`
  可注入可控时钟，测试中推进 `MutableClock` 即在下个检查点立即触发超时，无需真实等待。

### 超时/被接管后旧持有者的提交判定

旧持有者在失去所有权后回来提交，会被精确区分、一律拒绝，且盖不掉新持有者的结果：

| 情形 | 拒绝原因 |
| --- | --- |
| 自己是当前租约归属人、版本也对，但租约已过期且尚无人接管 | `LEASE_EXPIRED` |
| 版本落后于当前租约（持有者已换人，或同一人已用新版本重新抢占） | `OWNERSHIP_SUPERSEDED` |
| 当前无租约，或提交者压根不是该版本的归属人（冒用当前版本） | `NOT_LEASE_HOLDER` |

响应 `detail` 中带 `requester / version / current(owner@version)`，可直接复现每一步。

### 步骤级进度（可选）与所有权隔离

真实任务常是一串步骤，前几步可能已产生外部副作用。需要续跑的执行器可走
`TaskScheduler.runSteps(taskId, ownerId, businessKey, StepExecutor, resultMapper)`：

- 执行器拿到 `ExecutionContext`，用 `isStepDone(step)` / `markStepDone(step)` 记录进度；
  **重试或恢复时只做没做过的步骤，已完成步骤即使重试也不会再执行**。
- 进度可通过 `LeaseManager.stepProgress(taskId)` 或 `GET /api/tasks/{id}/steps` 查询，
  每条进度都带完成时的租约版本 `ownerVersion`。
- **进度跟租约的持有者走**：只有“合作式 `reportFailure` → 退避到期”的同一条重试血缘才续用
  已有进度；一旦租约过期被回收、被新的人抢占（含 `acquire` 惰性接管）、被主动 `release`
  或 `cancel`，旧持有者血缘的进度会被**整体清空**，新持有者拿到一个干净、确定的起点，
  旧进度绝不会算到新持有者头上。
- 进度写入同样经过所有权校验：过期/被取代/非持有者调用 `markStepDone` 会被拒
  （对应 `LEASE_EXPIRED` / `OWNERSHIP_SUPERSEDED` / `NOT_LEASE_HOLDER`）。

**兼容性**：这套机制对调用方是可选的。原来的
`runOnce(taskId, ownerId, businessKey, Function<String,String>)` 签名与行为完全保留，
不关心中间步骤的任务无需任何改动；它同样享受新的执行时限与中断保护。

### 运行状态查询

`LeaseManager` / `GET /api/tasks` 提供：

- `snapshot(taskId)`：单任务状态、持有者、租约版本与剩余时间、失败原因、步骤进度；
- `schedulable()`：当前可调度任务；
- `byOwner(ownerId)`：按持有者过滤；
- `byLeaseRemainingWithin(millis)`：按租约剩余时间过滤；
- `byFailureReason(reason)`：按失败原因过滤；
- `stepProgress(taskId)`：当前持有者血缘的步骤进度。

## 本地把整条链路跑一遍

服务启动后，在同一个本地进程里即可通过接口完成 注册 → 抢占 → 执行/步骤 →
提交或上报失败 → 查询：

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=18080
B=http://localhost:18080/api/tasks

# 注册 & 抢占（并发多次调用只有一个赢家，输家返回 LEASE_HELD_BY_OTHER 及赢家版本）
curl -X POST $B/job
curl -X POST "$B/job/acquire?owner=node-A&ttlSeconds=300"

# 步骤级进度（可选）
curl -X POST "$B/job/steps/step-1?owner=node-A&version=1"
curl -s $B/job/steps

# 提交结果（同一 businessKey 重复提交返回 DUPLICATE；旧持有者提交分别得到
# LEASE_EXPIRED / OWNERSHIP_SUPERSEDED / NOT_LEASE_HOLDER，且不覆盖新持有者）
curl -X POST "$B/job/submit?owner=node-A&version=1&businessKey=k1&payload=result-1"

# 上报失败进入退避重试（kind=RETRYABLE / NON_RETRYABLE）
curl -X POST "$B/job/failure?owner=node-A&version=1&kind=RETRYABLE&reason=demo-timeout"

# 续约 / 释放 / 取消 / 查询
curl -X POST "$B/job/renew?owner=node-A&version=1&ttlSeconds=300"
curl -X POST "$B/job/release?owner=node-A&version=1"
curl -X POST "$B/job/cancel"
curl -s $B/job
```

在本机实测可观察到的结论：

- **并发只有一个赢家**：并发 `acquire` 仅一个 `acquired=true`，其余为
  `LEASE_HELD_BY_OTHER`，`detail` 带赢家 `owner`、`version`、`remainingMs`；
- **超时之后进入重试**：执行超过 `executionTimeout`（或用短 TTL 拖到租约过期）后，
  任务进入 `RETRY_WAITING`，退避到期可被重新抢占，新版本号递增；
- **重复提交被认成重复**：同 `businessKey` 第二次提交返回 `DUPLICATE`，无副作用；
- 每个判定响应都带**租约版本与拒绝原因**，方便逐步复现与解释。

## 本地验证（自动化测试）

```bash
# 全部测试：并发抢占唯一赢家 / 执行超时恢复且已完成步骤不重复 / 中断确定结论 /
# 超时或被接管旧持有者提交被拒并区分原因 / 旧进度不串到新持有者 / 幂等提交。
# 全部使用 MutableClock 可控时间源，无真实等待。
mvn test

# 并发用例日志会打印每次判定的租约版本与依据，例如：
# [acquire-verdict] acquired=true  ownerVersion=1 reason=null detail=acquired
# [acquire-verdict] acquired=false ownerVersion=-1 reason=LEASE_HELD_BY_OTHER
#                   detail=held by node-0 version=1 remainingMs=15000
# [execution-timeout] kind=REJECTED reason=EXECUTION_TIMEOUT detail=... task=t version=1
# [takeover] oldVersion=1 newVersion=2 stepsAfterTakeover=0
```

## 代码结构

```
lease/Clock/MutableClock/SystemClock   可控时间源
lease/Lease                            租约（含 fencing token 版本）
lease/LeaseManager                     共享状态：获取/续约/释放/回收/提交/失败上报/步骤进度/查询
lease/TaskScheduler                    调度器：抢占 -> 时限内执行（检查点/中断/步骤续跑）-> 提交或上报失败
lease/ExecutionContext                 执行上下文：checkpoint 超时/中断检查 + 步骤进度
lease/StepExecutor                     需要步骤续跑的执行器（可选能力）
lease/ExecutionTimeoutException        执行超时（按可恢复失败处理）
lease/ExecutionInterruptedException    执行被外部中断（确定为失败，绝不当成功）
lease/OwnershipLostException           执行途中所有权失效（进度写入被拒）即中止
lease/RetryPolicy                      重试次数与指数退避上限
lease/TaskRecord/TaskSnapshot          内部状态（含步骤进度）与只读视图
lease/StepProgress                     单步骤进度（带 ownerVersion）
lease/RejectReason/ErrorKind/TaskStatus 可解释的判定枚举
web/TaskController                     本地验证 REST 接口（含抢占/提交/失败/步骤全链路）
```
