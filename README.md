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
  时限判定与租约到期判定**共用同一个 `Clock`**（生产为墙钟，测试为可控时钟）。
- **超时**：取消执行线程（中断），通过 `LeaseManager.finishAttempt(...)` 按可恢复失败落地
  （`RETRY_WAITING` + 退避），返回 `EXECUTION_TIMEOUT`。任务**立刻**回到可调度状态，
  而不是干等租约到期。
- **时间上限 = 租约时长也成立**：即使执行恰好卡满整段上限、租约在同一刻到期，
  `finishAttempt` 只做 fencing 校验（owner + version 必须是当前这一段租约）、
  **不要求租约尚未过期**，因此超时失败原因仍会立即记录、任务立即进入退避重试，
  绝不会停留在 `LEASED` 等待租约“再被动回收一次”。
- **外部打断**（调度线程被中断、实例停机）：取消执行、恢复中断标记、按可恢复失败上报，
  返回 `EXECUTION_INTERRUPTED`；执行期间若观察到中断信号，即使 executor 已返回结果
  也**不会提交**——没跑完/可能不完整的执行绝不会被当成成功。
- **异常**：executor 抛异常按可重试失败上报，返回 `EXECUTION_FAILED`。
- 超时/打断结束时若租约已经被更新的持有者接手，`finishAttempt` 的 fencing 校验失败，
  调度器会**原样透传**管理器给出的精确拒绝原因（旧执行者不能把自己的超时/异常
  当成最终结论，更不能写坏新持有者状态）。

超时/丢失租约后，旧执行者再回来提交结果或补记步骤进度，会得到以下**稳定、互相区分**
的结论（按判定优先级）：

| 结论 | 含义 | 典型场景 |
| --- | --- | --- |
| `LEASE_EXPIRED` | 它持有的就是**当前（最近一段）租约**，但已到期，且还没有更新的持有者接手 | 租约自然过期、或被 `reclaimExpired` 回收后，旧持有者回来提交 |
| `OWNERSHIP_SUPERSEDED` | 它的版本**确实发放过**，但当前持有者已是别人，或已发放过更新版本（fencing token 被取代） | 任务因超时/重启丢失后**被另一个执行者接手**；同一 owner 用旧版本提交 |
| `NOT_LEASE_HOLDER` | 它拿的版本**从未发放给它**（压根不是持有者），或它的租约是经释放/失败提交等方式**正常结束**（而非过期） | 伪造版本；执行超时/中断被正常收尾后旧持有者立刻回来提交 |

> 注意区分：**“执行超时”本身**由调度器给出 `EXECUTION_TIMEOUT`（本次执行的确定性结论）；
> 而**超时之后旧持有者的迟到提交**走上面的所有权判定——因为超时是正常收尾
> （非过期），拿到的是 `NOT_LEASE_HOLDER`；一旦任务被别人接手，再提交就变成
> `OWNERSHIP_SUPERSEDED`。这与“压根没拿到过租约”的 `NOT_LEASE_HOLDER` 虽同枚举，
> 但 `detail` 会给出该任务最近一段租约的 owner/version 与结束方式（`expired`/`closed`），
> 可据日志核对。

所有拒绝结论（`LeaseDecision` / `SubmitOutcome` / `ProgressView`）都带
**`leaseVersion`（判定时当前或最近一段租约的 fencing token）**，`detail` 形如
`requester=A version=1 current=B@v2(remainingMs=30000)` 或
`requester=A version=1 current=none(last=A@v1,closed)`，对着日志即可复现每一步。

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
3. **所有权校验**（可区分原因，绝不静默覆盖新持有者，判定见上表）：
   - 持有的就是当前/最近一段租约但已过期、尚无接手者 → `LEASE_EXPIRED`
   - 请求版本曾发放给请求者，但已被别人或更新版本接手 → `OWNERSHIP_SUPERSEDED`
   - 版本从未发放给请求者，或租约系正常结束 → `NOT_LEASE_HOLDER`
4. 全部通过 → `ACCEPTED`：记录业务键、写入结果、任务进入 SUCCEEDED。

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
# 全部测试（35 个，毫秒级跑完）：
#   并发抢占只有一个赢家 / 旧新持有者交错提交的 fencing /
#   执行超时立即恢复且已完成步骤不重复 / 时间上限=租约时长边界立即进入重试 /
#   三种拒绝结论稳定可区分（过期 / 被别人接手 / 不是持有者）/
#   旧持有者进度不串到新持有者 / 幂等提交 / 失败退避 / 打断不提交 / 旧调用兼容。
# 租约、退避、执行超时全部使用 MutableClock 可控时间源：
# 超时由“工作线程声明已开始后，主线程把时钟拨过上限”确定性触发，
# 没有用真实等待或 Thread.sleep 去撑超时（CountDownLatch.await 仅用于线程交接）。
mvn test

# 只跑某一组
mvn test -Dtest='ExecutionTimeoutTest,OwnershipFencingTest'
```

用例日志会打印每一步的租约版本、判定依据与三种业务结论，例如：

```
[finish-attempt] task=t owner=A version=1 leaseExpired=true kind=RETRYABLE reason=execution-timeout after 30000ms
[late-submit-1] reason=NOT_LEASE_HOLDER leaseVersion=1 detail=requester=A version=1 current=none(last=A@v1,closed)
[late-submit-2] reason=OWNERSHIP_SUPERSEDED currentVersion=2 detail=requester=A version=1 current=B@v2(remainingMs=30000)
[late-submit-3] reason=LEASE_EXPIRED leaseVersion=2 detail=requester=B version=2 current=B@v2(remainingMs=0)
[late-submit-4] reason=OWNERSHIP_SUPERSEDED currentVersion=3 detail=requester=B version=2 current=C@v3(remainingMs=30000)
[ttl-boundary] kind=REJECTED reason=EXECUTION_TIMEOUT status=RETRY_WAITING lastFailureReason=execution-timeout after 30000ms
[steps-recovered] kind=ACCEPTED leaseVersion=3 step1Runs=1 step2Runs=2 step3Runs=1
```

### 三种拒绝情形的本地复现

最直接的方式是看 `ExecutionTimeoutTest.oldHolderSubmitRejected_threeStableDistinguishableReasons`
与 `OwnershipFencingTest.threeReasons_areStableAndDistinguishable_withVersions`（全程可控时钟）。
用 REST 手动复现（注意 REST 用墙钟，需要真实等到租约过期）：

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8080
B=http://localhost:8080/api/tasks
curl -X POST $B/job-x

# 1) A 拿到 v1
curl -X POST $B/job-x/acquire -H 'Content-Type: application/json' \
  -d '{"ownerId":"A","ttlMillis":1000}'
# 2) 等租约过期（>1s）后立刻用 A/v1 提交 -> LEASE_EXPIRED（尚无新持有者）
sleep 1.2
curl -X POST $B/job-x/submit -H 'Content-Type: application/json' \
  -d '{"ownerId":"A","version":1,"businessKey":"bk-a","payload":"late"}'
# 3) B 抢到 v2 后，再用 A/v1 提交 -> OWNERSHIP_SUPERSEDED（已被别人接手）
curl -X POST $B/job-x/acquire -H 'Content-Type: application/json' \
  -d '{"ownerId":"B","ttlMillis":30000}'
curl -X POST $B/job-x/submit -H 'Content-Type: application/json' \
  -d '{"ownerId":"A","version":1,"businessKey":"bk-a2","payload":"late"}'
# 4) 从没拿过租约的 C 用伪造版本 -> NOT_LEASE_HOLDER
curl -X POST $B/job-x/submit -H 'Content-Type: application/json' \
  -d '{"ownerId":"C","version":999,"businessKey":"bk-c","payload":"x"}'
```

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
#    特别地，令 timeoutMillis 与租约 TTL 相同（默认 TTL=30000ms）并让 work 卡满，
#    任务依然立刻进入退避重试，而不是停在 LEASED 等租约自然过期。
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
  旧签名保留，行为仅新增“执行不得超过默认时限（= 租约 TTL）”的兜底保护；不传执行时限、
  不关心中间进度的调用方无需任何改动。
- 步骤级进度（recordStep/progress/runSteps）为**可选能力**，不引入对既有调用方的强制改动。
- `SubmitOutcome` / `LeaseDecision` / `ProgressView` 均带 `leaseVersion` 字段（判定时的当前
  或最近一段租约版本，无历史租约为 null），既有 `kind()/reason()/detail()/acquired()`
  访问方式不变。
- 拒绝结论枚举 `LEASE_EXPIRED` / `OWNERSHIP_SUPERSEDED` / `NOT_LEASE_HOLDER` 的**含义被
  收窄并稳定化**（见上表）：这是本次修复的核心语义变化——过去“过期被回收”“被别人接手”
  “压根没持有”会混为同一类，现在三者各自独立、可对着版本复现。
- 新增 `LeaseManager.finishAttempt(...)`（供 TaskScheduler 在执行结束时做 fencing 收尾，
  不要求租约未过期）；对外的 `reportFailure(...)` 仍保留原有“仅当前未过期持有者可上报”
  的严格语义。
- `TaskScheduler` 新增一个构造器参数用于注入时钟与轮询粒度；既有构造器（含 Spring
  自动装配的单参构造器）签名不变，默认复用 `LeaseManager` 的时钟、2~5ms 轮询粒度。

## 代码结构

```
lease/Clock/MutableClock/SystemClock   可控时间源（调度器与租约判定同源）
lease/Lease                            租约（含 fencing token 版本）
lease/TaskRecord                       共享状态：版本->owner 归属历史、最近租约及其结束方式
lease/LeaseManager                     获取/续约/释放/回收/提交/失败上报/finishAttempt/步骤/查询
lease/TaskScheduler                    抢占 -> 时钟驱动限时执行 -> 提交或 fencing 收尾；步骤续跑
lease/TaskStep/ProgressView            步骤定义与进度查询视图
lease/RetryPolicy                      重试次数与指数退避上限
lease/TaskSnapshot                    只读视图
lease/RejectReason/ErrorKind/TaskStatus 可解释的判定枚举
web/TaskController                     本地验证 REST 接口（注册/查询/抢占/提交/失败/步骤/演示执行）
tests/LeaseConcurrencyTest             并发抢占只有一个赢家
tests/OwnershipFencingTest             旧/新持有者交错提交：三种拒绝区分、结果与进度不被写坏
tests/ExecutionTimeoutTest             时钟驱动超时、TTL 边界立即重试、步骤不重复、打断、旧调用兼容
tests/StepProgressTest                 步骤进度记录/幂等/所有权隔离/同 owner 恢复续跑
tests/LeaseExpiryTest                  到期/续约/释放/回收/被取代判定
tests/RetryRecoveryTest / IdempotentSubmitTest / ApplicationTests
```
