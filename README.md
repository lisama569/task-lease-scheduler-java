# task-lease-scheduler-java

本地可运行的任务租约调度示例：以本地共享内存状态模拟多实例竞争，保证同一任务在并发抢占、
租约失效、执行超时/打断与失败重试下的执行结论唯一、确定且可解释。无外网、无外部服务依赖。

## 核心概念

### 租约与 fencing token

- 每次成功获取租约都会分配一个**单调递增的版本号**（fencing token），即使租约被释放或
  到期回收，版本序列也不会重置。
- 任意时刻同一任务最多一个有效持有者；所有判定在 per-task 监视器锁内完成，
  并发抢占得到唯一结果：胜者拿到 `LeaseDecision.acquired=true`，
  其余竞争者得到 `LEASE_HELD_BY_OTHER` 及当前持有者与版本信息。
- 所有判定结果（`LeaseDecision` / `SubmitOutcome` / `ProgressView`）都携带
  **租约版本与拒绝原因**，便于复现和解释每一步。

### 租约状态流转

```
                acquire                acquire(到期回收后)
 PENDING  ───────────────►  LEASED  ──────────────────────►  LEASED(新持有者, 版本+1)
   ▲                         │  │
   │ release                 │  ├── submitResult ──► SUCCEEDED (终态)
   │                         │  ├── reportFailure(RETRYABLE, 未超上限) ──► RETRY_WAITING
   │                         │  ├── reportFailure(NON_RETRYABLE 或超上限) ─► DEAD (终态)
   │                         │  ├── 执行超时/被打断 ──► reportFailure(RETRYABLE) ─► RETRY_WAITING
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

### 执行时限与打断（TaskScheduler）

每次执行都有明确的**时间上限**，不再依赖租约被动到期来兜底：

- `runOnce(taskId, ownerId, businessKey, executionTimeout, executor)`：
  执行在受控线程中运行，调度线程以 `executionTimeout` 为上限等待结果。
- **超时**：取消执行线程（中断），按可恢复失败上报（`RETRY_WAITING` + 退避），
  返回 `EXECUTION_TIMEOUT`。任务立刻回到可调度状态，而不是干等租约到期。
  **执行时限允许等于租约时长**（不显式传时限的旧调用方式默认就是相等）：
  即使这次执行真的卡满整段上限、租约刚好过期，只要租约版本尚未被取代，
  失败上报依然被接受（见下文 `reportFailure` 的判定），超时原因被记录，
  任务立刻进入退避重试，而不是保持“被别人占着”直到租约被动回收。
- **外部打断**（调度线程被中断、实例停机）：取消执行、恢复中断标记、按可恢复失败上报，
  返回 `EXECUTION_INTERRUPTED`；执行期间若观察到中断信号，即使 executor 已返回结果
  也**不会提交**——没跑完/可能不完整的执行绝不会被当成成功。
- **异常**：executor 抛异常按可重试失败上报，返回 `EXECUTION_FAILED`。
- 超时/打断后租约已结束，原持有者再提交结果或补记进度会被拒绝。
  系统会记住每个执行者最近一次持有过的租约（owner → 最近一次租约），
  因此三种情形各自给出**稳定且互相区分**的结论，判定结果都带当前租约版本便于核对：
  - 曾持有该版本、租约已过期/被回收/已随失败上报结束，且尚无人接手 → `LEASE_EXPIRED`
  - 曾持有该版本、但任务已被别人接手（或同一 owner 的租约版本已轮换）
    → `OWNERSHIP_SUPERSEDED`
  - 从未持有过该版本租约（压根不是持有者）→ `NOT_LEASE_HOLDER`
  无论哪种情况，旧持有者都**无法覆盖**新持有者的结果或进度。

### 步骤级进度与续跑（可选 API）

真实任务常是一串步骤，前面步骤可能已产生外部副作用，重试时不能整段重跑：

- `LeaseManager.recordStep(taskId, ownerId, version, stepKey, note)`：
  仅当前持有者可记录；同一 `stepKey` 重复记录幂等。
- `LeaseManager.progress(taskId, ownerId, version)`：
  仅当前持有者可查询自己的已完成步骤（按记录顺序）。
- `TaskScheduler.runSteps(taskId, ownerId, businessKey, executionTimeout, List<TaskStep>)`：
  执行前查询进度并**跳过已完成步骤**，每完成一步即记录；重试/恢复时只跑没做过的部分。
  执行途中若租约丢失（被回收/取代），立即中止并以对应原因拒绝，不提交结果。

**进度所有权隔离**：

- 进度跟着租约持有者走。租约过期、被回收或被别人抢走后，旧持有者留下的进度
  **不会算到新持有者头上**：不同 owner 获取租约时旧进度被清除，新持有者拿到
  干净、确定的起点（空进度）。
- 同一 owner 因重试/恢复重新获得租约时，继承自己之前记录的进度，已完成步骤不重复执行。
- 任务进入终态（SUCCEEDED/DEAD/CANCELLED）时进度被清除。
- 这套机制完全可选：不关心中间进度的任务继续只用 `runOnce`，无需任何改动。

### 幂等与所有权约束（结果提交）

`submitResult(taskId, ownerId, version, businessKey, payload)` 按以下顺序判定：

1. **幂等优先**：`businessKey` 已应用过 → `DUPLICATE`，不产生任何副作用，
   同一业务键的重复提交/重试安全。
2. **终态拒绝**：任务已 SUCCEEDED/DEAD/CANCELLED 且业务键未见过 → `TASK_FINISHED`。
3. **所有权校验**（三种拒绝稳定可区分，绝不静默覆盖新持有者）：
   - 曾持有该版本但任务已被别人接手 / 版本已轮换 → `OWNERSHIP_SUPERSEDED`
   - 曾持有该版本但租约已过期、被回收或已随失败上报结束 → `LEASE_EXPIRED`
   - 从未持有过该版本租约 → `NOT_LEASE_HOLDER`
4. 全部通过 → `ACCEPTED`：记录业务键、写入结果、任务进入 SUCCEEDED。

`reportFailure` 的所有权校验与上面略有不同：**租约已过期但版本未被取代**
（仍是自己那一版、无人接手）时依然接受上报。这是“执行时限 == 租约时长”场景的
关键：卡满上限的执行在租约刚好过期后才上报，也必须得到确定结论并立刻进入重试。
安全性不受影响——只要 fencing token 未被取代，其他执行者就不可能已经拿到该任务；
一旦别人接手（版本变大），旧持有者的失败上报会被 `OWNERSHIP_SUPERSEDED` 拒绝，
无法写坏新持有者的状态。

参数非法（空 businessKey、空 ownerId、非法 TTL）→ `INVALID_ARGUMENT`；
任务不存在 → `TASK_NOT_FOUND`。

### 错误分类与重试

- `ErrorKind.RETRYABLE`：执行超时、中断、下游暂时不可用。任务进入 `RETRY_WAITING`，
  按指数退避（`baseBackoff << (attempt-1)`，封顶 `maxBackoff`）等待后重新调度；
  已提交的业务键不会重复执行（幂等保证），已记录的步骤不会被新持有者继承（进度隔离）。
- `ErrorKind.NON_RETRYABLE`：参数非法、业务校验失败等，直接进入 `DEAD` 终态。
- 重试次数达到 `maxAttempts` 后进入 `DEAD`；`CANCELLED`/`DEAD`/`SUCCEEDED` 均不再被调度。
- `RetryPolicy.defaults()`：3 次尝试、100ms 基础退避、5s 上限（测试可自定义）。

### 运行状态查询

`LeaseManager` / `GET /api/tasks` 提供：

- `snapshot(taskId)`：单任务状态、持有者、租约版本与剩余时间、失败原因；
- `schedulable()`：当前可调度任务；
- `byOwner(ownerId)`：按持有者过滤；
- `byLeaseRemainingWithin(millis)`：按租约剩余时间过滤；
- `byFailureReason(reason)`：按失败原因过滤；
- `progress(taskId, ownerId, version)`：当前持有者的步骤进度。

## 本地验证

### 自动化测试

```bash
# 全部测试：并发抢占 / 租约到期 / 幂等重复提交 / 失败恢复 / 执行超时与打断 / 步骤进度隔离 /
# 执行时限==租约时长的超时恢复 / 三种拒绝情形的区分
# 租约与退避时间使用 MutableClock 可控时间源，无真实等待；
# 执行超时本身为百毫秒级的极短真实等待（线程超时无法用时钟模拟）。
mvn test

# 用例日志会打印每次判定的租约版本与依据，例如：
# [concurrent-acquire] acquired=false reason=LEASE_HELD_BY_OTHER detail=held by owner-0 version=1 remainingMs=10000
# [late-submit-2] reason=OWNERSHIP_SUPERSEDED currentVersion=2 detail=requester=A version=1 current=B@v2
# [late-submit-5-after-reclaim] reason=LEASE_EXPIRED detail=requester=A version=3 current=none
# [stranger-submit] reason=NOT_LEASE_HOLDER detail=requester=stranger version=1 current=none
# [after-timeout-eq-ttl] status=RETRY_WAITING lastError=RETRYABLE reason=execution-timeout after 200ms
# [steps-recovered] kind=ACCEPTED leaseVersion=3 step1Runs=1 step2Runs=2 step3Runs=1
```

### 三种拒绝情形的本地复现

对同一任务 `t`（租约版本从 1 开始）：

1. **`LEASE_EXPIRED`**：A 拿到 v1 后不续约，等租约过期（测试里用 `MutableClock.advance`，
   或 REST 下把 `ttlMillis` 设小并稍等），A 再以 v1 提交/记进度 → `LEASE_EXPIRED`。
   过期后被 `reclaimExpired` 回收再提交，结论仍是 `LEASE_EXPIRED`。
2. **`OWNERSHIP_SUPERSEDED`**：A 的 v1 过期后 B 抢到 v2，A 再以 v1 提交 →
   `OWNERSHIP_SUPERSEDED`，判定里带 `currentVersion=2`。
3. **`NOT_LEASE_HOLDER`**：从未拿过租约的执行者直接以任意版本提交 → `NOT_LEASE_HOLDER`。

对应自动化用例：`ExecutionTimeoutTest.oldHolderSubmitRejected_afterTimeout_distinguishableReasons`、
`LeaseExpiryTest.reportFailure_*`、`StepProgressTest.progressDoesNotLeakToNewHolder_afterExpiryAndTakeover`。

### 执行时限 == 租约时长的本地复现

- 自动化：`ExecutionTimeoutTest.timeoutEqualToLeaseTtl_deterministicFailure_andImmediateRetry`
  用 `MutableClock` 把业务时间推进满整个租约时长来模拟“卡满上限”，断言任务立刻进入
  `RETRY_WAITING` 且 `lastFailureReason` 已记录，而不是保持 `LEASED` 等被动回收。
- REST：`POST /api/tasks/{id}/execute` 让 `timeoutMillis` 与调度器租约 TTL 相等且
  `workMillis` 更大，随后 `GET /api/tasks/{id}` 应看到 `status=RETRY_WAITING` 与
  `lastFailureReason=execution-timeout...`。

### REST 全链路验证

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8080
B=http://localhost:8080/api/tasks

# 1) 注册任务
curl -X POST $B/job-1

# 2) 并发抢占：并行发两个请求，只有一个赢家（另一个得到 LEASE_HELD_BY_OTHER + 当前版本）
curl -X POST $B/job-1/acquire -H 'Content-Type: application/json' -d '{"ownerId":"inst-A","ttlMillis":30000}' &
curl -X POST $B/job-1/acquire -H 'Content-Type: application/json' -d '{"ownerId":"inst-B","ttlMillis":30000}' &
wait

# 3) 记录/查询步骤进度（持有者可查，非持有者被拒）
curl -X POST $B/job-1/steps -H 'Content-Type: application/json' \
  -d '{"ownerId":"inst-B","version":1,"stepKey":"extract","note":"rows=100"}'
curl "$B/job-1/progress?ownerId=inst-B&version=1"

# 4) 提交结果；同一 businessKey 再提交一次 -> DUPLICATE（重复提交被认成重复）
curl -X POST $B/job-1/submit -H 'Content-Type: application/json' \
  -d '{"ownerId":"inst-B","version":1,"businessKey":"bk-1","payload":"done"}'

# 5) 演示执行：workMillis > timeoutMillis -> EXECUTION_TIMEOUT，任务进入 RETRY_WAITING
curl -X POST $B/job-2
curl -X POST $B/job-2/execute -H 'Content-Type: application/json' \
  -d '{"ownerId":"inst-C","businessKey":"bk-2","timeoutMillis":300,"workMillis":5000,"fail":false}'
curl $B/job-2   # status=RETRY_WAITING, lastFailureReason=execution-timeout...

# 6) 上报失败（可重试进入退避；NON_RETRYABLE 直接 DEAD）
curl -X POST $B/job-3/failure -H 'Content-Type: application/json' \
  -d '{"ownerId":"inst-D","version":1,"kind":"RETRYABLE","reason":"downstream-503"}'

# 其它：续约 / 释放 / 查询 / 取消
curl -X POST $B/job-1/renew  -H 'Content-Type: application/json' -d '{"ownerId":"inst-B","version":1,"ttlMillis":30000}'
curl -X POST $B/job-1/release -H 'Content-Type: application/json' -d '{"ownerId":"inst-B","version":1}'
curl $B                      # 当前可调度任务
curl -X POST $B/job-1/cancel
```

## 兼容范围

- **原有调用方式照常可用**：`LeaseManager` 的 acquire/renew/release/submitResult/reportFailure/
  cancel 及各查询方法签名与基本语义不变；`TaskScheduler.runOnce(taskId, ownerId, businessKey, executor)`
  旧签名保留，行为仅新增“执行不得超过默认时限（= 租约 TTL）”的兜底保护——
  即使卡满该上限也能得到确定的超时失败结论并立刻进入重试。
- 步骤级进度（recordStep/progress/runSteps）为**可选新增**，不引入任何对既有调用方的强制改动。
- `SubmitOutcome` 带 `leaseVersion`、`LeaseDecision` 带 `currentVersion`、
  `ProgressView` 带 `leaseVersion`（判定时的当前租约版本，无租约为 null），
  既有 `kind()/reason()/detail()`、`acquired()/lease()` 等访问方式不变；
  `LeaseDecision` 保留旧的四参构造。
- 拒绝原因语义更细化（行为变更，属本次修复目标）：曾持有租约但已过期/被回收的提交
  现在得到 `LEASE_EXPIRED`、被别人接手得到 `OWNERSHIP_SUPERSEDED`，
  只有从未持有过该版本的执行者才得到 `NOT_LEASE_HOLDER`。

## 代码结构

```
lease/Clock/MutableClock/SystemClock   可控时间源
lease/Lease                            租约（含 fencing token 版本）
lease/LeaseManager                     共享状态：获取/续约/释放/回收/提交/失败上报/步骤进度/查询
lease/TaskScheduler                    调度器：抢占 -> 限时执行 -> 提交或上报失败；步骤级续跑
lease/TaskStep/ProgressView            步骤定义与进度查询视图
lease/RetryPolicy                      重试次数与指数退避上限
lease/TaskRecord/TaskSnapshot          内部状态与只读视图
lease/RejectReason/ErrorKind/TaskStatus 可解释的判定枚举
web/TaskController                     本地验证 REST 接口（注册/查询/抢占/提交/失败/步骤/演示执行）
```
