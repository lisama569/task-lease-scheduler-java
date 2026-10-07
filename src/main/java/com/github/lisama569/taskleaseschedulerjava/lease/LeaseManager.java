package com.github.lisama569.taskleaseschedulerjava.lease;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地共享租约管理器：所有实例竞争同一内存状态。
 * 每个 TaskRecord 一把监视器锁，保证同一任务任意时刻只有一个有效持有者，
 * 并发抢占得到唯一且可判定的结果；租约版本（fencing token）单调递增。
 *
 * <p>旧持有者（超时/实例重启丢失租约后）再回来提交或补记进度时，结论稳定且互相区分：
 * <ul>
 *   <li>{@link RejectReason#LEASE_EXPIRED}：它持有的就是当前（最近一段）租约，且已过期，
 *       还没有更新的持有者接手；</li>
 *   <li>{@link RejectReason#OWNERSHIP_SUPERSEDED}：它的版本确实发放过，但当前持有者已是
 *       另一个 owner，或已发放过更新版本（fencing token 被取代）——任务已被别人接手；</li>
 *   <li>{@link RejectReason#NOT_LEASE_HOLDER}：它拿的版本从未发放给它（压根不是持有者），
 *       或它的租约是通过释放/失败提交等方式正常结束的。</li>
 * </ul>
 * 每条结论的 detail 都带请求版本、当前/最近租约的 owner 与版本，方便对着日志核对。
 */
@Component
public class LeaseManager {

    private static final Logger log = LoggerFactory.getLogger(LeaseManager.class);

    private final Clock clock;
    private final RetryPolicy retryPolicy;
    private final Map<String, TaskRecord> tasks = new ConcurrentHashMap<>();

    public LeaseManager() {
        this(new SystemClock(), RetryPolicy.defaults());
    }

    public LeaseManager(Clock clock, RetryPolicy retryPolicy) {
        this.clock = clock;
        this.retryPolicy = retryPolicy;
    }

    /** 时间源（供 TaskScheduler 等复用同一可控时钟）。 */
    public Clock clock() {
        return clock;
    }

    /** 注册任务（幂等）。 */
    public void registerTask(String taskId) {
        tasks.computeIfAbsent(taskId, id -> new TaskRecord(id, retryPolicy.maxAttempts()));
    }

    /** 尝试获取租约；到期租约先被回收再判定，结果唯一。 */
    public LeaseDecision acquire(String taskId, String ownerId, Duration ttl) {
        TaskRecord r = tasks.get(taskId);
        if (r == null) {
            return LeaseDecision.rejected(RejectReason.TASK_NOT_FOUND, "no such task: " + taskId);
        }
        if (ownerId == null || ownerId.isBlank() || ttl == null || ttl.isNegative() || ttl.isZero()) {
            return LeaseDecision.rejected(RejectReason.INVALID_ARGUMENT, "ownerId/ttl invalid");
        }
        synchronized (r) {
            Instant now = clock.now();
            if (r.status() == TaskStatus.SUCCEEDED || r.status() == TaskStatus.DEAD
                    || r.status() == TaskStatus.CANCELLED) {
                return LeaseDecision.rejected(RejectReason.TASK_FINISHED,
                        "task " + taskId + " is " + r.status(), referenceVersion(r));
            }
            if (r.status() == TaskStatus.RETRY_WAITING
                    && r.nextRetryAt() != null && now.isBefore(r.nextRetryAt())) {
                return LeaseDecision.rejected(RejectReason.RETRY_BACKOFF_NOT_ELAPSED,
                        "retry at " + r.nextRetryAt(), referenceVersion(r));
            }
            if (r.status() == TaskStatus.LEASED) {
                Lease cur = r.lease();
                if (cur != null && !cur.isExpired(now) && !cur.ownerId().equals(ownerId)) {
                    return LeaseDecision.rejected(RejectReason.LEASE_HELD_BY_OTHER,
                            "held by " + cur.ownerId() + " version=" + cur.version()
                                    + " remainingMs=" + cur.remainingMillis(now),
                            cur.version());
                }
            }
            // PENDING / 退避到期的 RETRY_WAITING / 租约已过期的 LEASED：发放新租约。
            Lease old = r.lease();
            boolean oldExpired = old != null && old.isExpired(now);
            // 步骤进度所有权隔离：持有者变更（不同 owner）时清除旧进度，
            // 新持有者拿到干净起点；同一 owner 重新获得租约视为恢复，保留进度。
            if (old != null && !old.ownerId().equals(ownerId)) {
                r.clearSteps();
            } else if (r.stepsOwner() != null && !r.stepsOwner().equals(ownerId)) {
                r.clearSteps();
            }
            long newVersion = r.nextVersion();
            Lease lease = new Lease(taskId, ownerId, newVersion, now, now.plus(ttl));
            r.rememberVersionOwner(newVersion, ownerId);
            r.replaceWithLease(lease, oldExpired);
            r.status(TaskStatus.LEASED);
            r.incrementAttempts();
            r.nextRetryAt(null);
            log.info("[acquire] task={} owner={} version={} oldLease={} oldExpired={}",
                    taskId, ownerId, newVersion,
                    old == null ? "none" : old.ownerId() + "@v" + old.version(), oldExpired);
            return LeaseDecision.ok(lease);
        }
    }

    /** 续约：仅当前持有者可续，版本不变、过期时间顺延。 */
    public LeaseDecision renew(String taskId, String ownerId, long version, Duration ttl) {
        TaskRecord r = tasks.get(taskId);
        if (r == null) {
            return LeaseDecision.rejected(RejectReason.TASK_NOT_FOUND, "no such task: " + taskId);
        }
        synchronized (r) {
            Lease cur = r.lease();
            RejectReason invalid = checkHolder(r, cur, ownerId, version, clock.now());
            if (invalid != null) {
                return LeaseDecision.rejected(invalid, describe(cur, ownerId, version, r, clock.now()),
                        referenceVersion(r));
            }
            Lease renewed = new Lease(taskId, ownerId, version, cur.acquiredAt(),
                    clock.now().plus(ttl));
            r.lease(renewed);
            return LeaseDecision.ok(renewed);
        }
    }

    /** 主动释放：仅当前持有者可释放，任务回到 PENDING。 */
    public LeaseDecision release(String taskId, String ownerId, long version) {
        TaskRecord r = tasks.get(taskId);
        if (r == null) {
            return LeaseDecision.rejected(RejectReason.TASK_NOT_FOUND, "no such task: " + taskId);
        }
        synchronized (r) {
            Lease cur = r.lease();
            RejectReason invalid = checkHolder(r, cur, ownerId, version, clock.now());
            if (invalid != null) {
                return LeaseDecision.rejected(invalid, describe(cur, ownerId, version, r, clock.now()),
                        referenceVersion(r));
            }
            r.closeCurrentLease();
            r.status(TaskStatus.PENDING);
            return LeaseDecision.okNoLease();
        }
    }

    /**
     * 提交执行结果：先幂等判定（同一业务键不重复产生副作用），再所有权校验。
     * 过期或被取代的持有者提交一律拒绝，绝不覆盖新持有者状态。
     */
    public SubmitOutcome submitResult(String taskId, String ownerId, long version,
                                      String businessKey, String payload) {
        if (businessKey == null || businessKey.isBlank()) {
            return SubmitOutcome.rejected(RejectReason.INVALID_ARGUMENT, "businessKey required");
        }
        TaskRecord r = tasks.get(taskId);
        if (r == null) {
            return SubmitOutcome.rejected(RejectReason.TASK_NOT_FOUND, "no such task: " + taskId);
        }
        synchronized (r) {
            Long curVersion = referenceVersion(r);
            if (r.hasApplied(businessKey)) {
                return SubmitOutcome.duplicate(curVersion);
            }
            if (r.status() == TaskStatus.SUCCEEDED || r.status() == TaskStatus.DEAD
                    || r.status() == TaskStatus.CANCELLED) {
                return SubmitOutcome.rejected(RejectReason.TASK_FINISHED,
                        "task is " + r.status() + ", businessKey not applied: " + businessKey,
                        curVersion);
            }
            Lease cur = r.lease();
            RejectReason invalid = checkHolder(r, cur, ownerId, version, clock.now());
            if (invalid != null) {
                log.info("[submit-rejected] task={} requester={} version={} reason={} refVersion={}",
                        taskId, ownerId, version, invalid, curVersion);
                return SubmitOutcome.rejected(invalid, describe(cur, ownerId, version, r, clock.now()),
                        curVersion);
            }
            r.markApplied(businessKey);
            r.result(payload);
            r.closeCurrentLease();
            r.clearSteps();
            r.status(TaskStatus.SUCCEEDED);
            log.info("[submit-accepted] task={} owner={} version={} businessKey={}",
                    taskId, ownerId, version, businessKey);
            return SubmitOutcome.accepted(version);
        }
    }

    /** 上报失败：可重试错误进入退避等待，不可重试或超上限进入 DEAD 终态。 */
    public LeaseDecision reportFailure(String taskId, String ownerId, long version,
                                       ErrorKind kind, String reason) {
        TaskRecord r = tasks.get(taskId);
        if (r == null) {
            return LeaseDecision.rejected(RejectReason.TASK_NOT_FOUND, "no such task: " + taskId);
        }
        synchronized (r) {
            Lease cur = r.lease();
            RejectReason invalid = checkHolder(r, cur, ownerId, version, clock.now());
            if (invalid != null) {
                return LeaseDecision.rejected(invalid, describe(cur, ownerId, version, r, clock.now()),
                        referenceVersion(r));
            }
            applyFailureOutcome(r, kind, reason);
            return r.status() == TaskStatus.DEAD
                    ? LeaseDecision.rejected(RejectReason.TASK_FINISHED,
                            "task DEAD: kind=" + kind + " attempts=" + r.attempts(),
                            referenceVersion(r))
                    : LeaseDecision.okNoLease();
        }
    }

    /**
     * 结束一次执行尝试（执行超时 / 被打断 / 抛异常时由 TaskScheduler 调用）。
     *
     * <p>与 {@link #reportFailure} 的区别：这里只做 fencing 校验（版本必须是当前这一段
     * 租约、owner 必须匹配），<b>不要求租约尚未过期</b>。这样即使执行时间上限被设成和
     * 租约时长一样长、执行恰好卡满整段上限（租约同时到期），超时结论也能立刻落地：
     * 记录失败原因并让任务回到 RETRY_WAITING/PENDING，而不是靠租约被动回收。
     *
     * <p>若当前持有者已经是别人或版本已被取代，则拒绝（旧执行者不得写坏新持有者状态），
     * 调用方应原样透传该拒绝原因，不能把自己的超时/异常当成最终结论。
     */
    public LeaseDecision finishAttempt(String taskId, String ownerId, long version,
                                       ErrorKind kind, String reason) {
        TaskRecord r = tasks.get(taskId);
        if (r == null) {
            return LeaseDecision.rejected(RejectReason.TASK_NOT_FOUND, "no such task: " + taskId);
        }
        synchronized (r) {
            if (r.status() == TaskStatus.SUCCEEDED || r.status() == TaskStatus.DEAD
                    || r.status() == TaskStatus.CANCELLED) {
                return LeaseDecision.rejected(RejectReason.TASK_FINISHED,
                        "task " + taskId + " is " + r.status(), referenceVersion(r));
            }
            Lease cur = r.lease();
            // fencing：owner+version 必须与当前这一段租约一致；不检查是否已过期。
            if (cur == null
                    || !cur.ownerId().equals(ownerId)
                    || cur.version() != version) {
                RejectReason reason2 = checkHolder(r, cur, ownerId, version, clock.now());
                log.info("[finish-attempt-rejected] task={} requester={} version={} reason={}",
                        taskId, ownerId, version, reason2);
                return LeaseDecision.rejected(reason2, describe(cur, ownerId, version, r, clock.now()),
                        referenceVersion(r));
            }
            boolean expired = cur.isExpired(clock.now());
            log.info("[finish-attempt] task={} owner={} version={} leaseExpired={} kind={} reason={}",
                    taskId, ownerId, version, expired, kind, reason);
            applyFailureOutcome(r, kind, reason);
            return r.status() == TaskStatus.DEAD
                    ? LeaseDecision.rejected(RejectReason.TASK_FINISHED,
                            "task DEAD: kind=" + kind + " attempts=" + r.attempts(),
                            referenceVersion(r))
                    : LeaseDecision.okNoLease();
        }
    }

    /** 在已通过持有者校验后落地失败状态：记录原因、释放租约、进入重试或终态。 */
    private void applyFailureOutcome(TaskRecord r, ErrorKind kind, String reason) {
        r.lastErrorKind(kind);
        r.lastFailureReason(reason);
        r.closeCurrentLease();
        if (kind == ErrorKind.NON_RETRYABLE || r.attempts() >= r.maxAttempts()) {
            r.status(TaskStatus.DEAD);
            r.clearSteps();
            r.nextRetryAt(null);
            return;
        }
        r.status(TaskStatus.RETRY_WAITING);
        r.nextRetryAt(clock.now().plus(retryPolicy.backoffFor(r.attempts())));
    }

    /** 取消任务：终态，不再被调度；已终态则返回 false。 */
    public boolean cancel(String taskId) {
        TaskRecord r = tasks.get(taskId);
        if (r == null) {
            return false;
        }
        synchronized (r) {
            if (r.status() == TaskStatus.SUCCEEDED || r.status() == TaskStatus.DEAD
                    || r.status() == TaskStatus.CANCELLED) {
                return false;
            }
            r.closeCurrentLease();
            r.clearSteps();
            r.status(TaskStatus.CANCELLED);
            return true;
        }
    }

    /**
     * 记录一个已完成步骤（可选的步骤级进度 API）。
     * 仅当前持有者（owner + version 匹配且未过期）可记录；同一 stepKey 重复记录幂等。
     */
    public LeaseDecision recordStep(String taskId, String ownerId, long version,
                                    String stepKey, String note) {
        if (stepKey == null || stepKey.isBlank()) {
            return LeaseDecision.rejected(RejectReason.INVALID_ARGUMENT, "stepKey required");
        }
        TaskRecord r = tasks.get(taskId);
        if (r == null) {
            return LeaseDecision.rejected(RejectReason.TASK_NOT_FOUND, "no such task: " + taskId);
        }
        synchronized (r) {
            Lease cur = r.lease();
            RejectReason invalid = checkHolder(r, cur, ownerId, version, clock.now());
            if (invalid != null) {
                log.info("[record-step-rejected] task={} requester={} version={} step={} reason={}",
                        taskId, ownerId, version, stepKey, invalid);
                return LeaseDecision.rejected(invalid, describe(cur, ownerId, version, r, clock.now()),
                        referenceVersion(r));
            }
            r.recordStep(ownerId, stepKey, note);
            return LeaseDecision.ok(cur);
        }
    }

    /**
     * 查询当前持有者的步骤进度。仅当前持有者可查，且返回的进度必然属于查询者：
     * 持有者一旦变更，旧持有者的进度已在 acquire 时被清除。
     */
    public ProgressView progress(String taskId, String ownerId, long version) {
        TaskRecord r = tasks.get(taskId);
        if (r == null) {
            return ProgressView.rejected(RejectReason.TASK_NOT_FOUND, "no such task: " + taskId);
        }
        synchronized (r) {
            Lease cur = r.lease();
            RejectReason invalid = checkHolder(r, cur, ownerId, version, clock.now());
            if (invalid != null) {
                return ProgressView.rejected(invalid, describe(cur, ownerId, version, r, clock.now()),
                        referenceVersion(r));
            }
            return ProgressView.of(cur.version(), r.completedStepKeys());
        }
    }

    /** 回收到期租约，使其可被重新抢占；返回回收数量。 */
    public int reclaimExpired() {
        int reclaimed = 0;
        Instant now = clock.now();
        for (TaskRecord r : tasks.values()) {
            synchronized (r) {
                if (r.status() == TaskStatus.LEASED && r.lease() != null
                        && r.lease().isExpired(now)) {
                    Lease old = r.lease();
                    r.expireCurrentLease();
                    r.status(TaskStatus.PENDING);
                    reclaimed++;
                    log.info("[reclaim-expired] task={} owner={} version={} expiredAt={}",
                            r.taskId(), old.ownerId(), old.version(), old.expiresAt());
                }
            }
        }
        return reclaimed;
    }

    /** 查询当前可调度任务（PENDING 或退避到期的 RETRY_WAITING）。 */
    public List<TaskSnapshot> schedulable() {
        reclaimExpired();
        Instant now = clock.now();
        List<TaskSnapshot> out = new ArrayList<>();
        for (TaskRecord r : tasks.values()) {
            synchronized (r) {
                boolean ready = r.status() == TaskStatus.PENDING
                        || (r.status() == TaskStatus.RETRY_WAITING
                            && r.nextRetryAt() != null && !now.isBefore(r.nextRetryAt()));
                if (ready) {
                    out.add(TaskSnapshot.of(r, now));
                }
            }
        }
        return out;
    }

    public Optional<TaskSnapshot> snapshot(String taskId) {
        TaskRecord r = tasks.get(taskId);
        if (r == null) {
            return Optional.empty();
        }
        synchronized (r) {
            return Optional.of(TaskSnapshot.of(r, clock.now()));
        }
    }

    /** 按持有者过滤。 */
    public List<TaskSnapshot> byOwner(String ownerId) {
        return filter(r -> r.lease() != null && ownerId.equals(r.lease().ownerId()));
    }

    /** 按失败原因过滤。 */
    public List<TaskSnapshot> byFailureReason(String reason) {
        return filter(r -> reason.equals(r.lastFailureReason()));
    }

    /** 按租约剩余时间过滤（毫秒以内）。 */
    public List<TaskSnapshot> byLeaseRemainingWithin(long millis) {
        Instant now = clock.now();
        return filter(r -> r.lease() != null && r.lease().remainingMillis(now) <= millis);
    }

    private List<TaskSnapshot> filter(java.util.function.Predicate<TaskRecord> p) {
        Instant now = clock.now();
        List<TaskSnapshot> out = new ArrayList<>();
        for (TaskRecord r : tasks.values()) {
            synchronized (r) {
                if (p.test(r)) {
                    out.add(TaskSnapshot.of(r, now));
                }
            }
        }
        return out;
    }

    /** 判定时可对外核对的租约版本：优先当前租约，其次最近一段历史租约。 */
    private static Long referenceVersion(TaskRecord r) {
        if (r.lease() != null) {
            return r.lease().version();
        }
        if (r.previousLease() != null) {
            return r.previousLease().version();
        }
        return r.lastIssuedVersion() > 0 ? r.lastIssuedVersion() : null;
    }

    /**
     * 持有者校验，结论按稳定优先级给出，且互相区分：
     * <ol>
     *   <li>任务已终态 → TASK_FINISHED；</li>
     *   <li>请求者持有的就是当前（最近一段）租约但已过期、尚无更新持有者 → LEASE_EXPIRED；</li>
     *   <li>请求版本曾发放给请求者，但当前 owner 是别人或已存在更新版本
     *       → OWNERSHIP_SUPERSEDED（任务已被别人接手 / fencing token 被取代）；</li>
     *   <li>其余（版本从未发放给请求者，或其租约经正常结束而非过期）→ NOT_LEASE_HOLDER。</li>
     * </ol>
     */
    private static RejectReason checkHolder(TaskRecord r, Lease cur, String ownerId,
                                            long version, Instant now) {
        if (r.status() == TaskStatus.CANCELLED || r.status() == TaskStatus.DEAD
                || r.status() == TaskStatus.SUCCEEDED) {
            return RejectReason.TASK_FINISHED;
        }
        Lease recent = cur != null ? cur : r.previousLease();
        boolean expired = cur != null && cur.isExpired(now);
        // 1) 请求者持有的就是当前（最近一段）租约：有效则放行，过期则明确为 LEASE_EXPIRED。
        if (recent != null && recent.version() == version
                && ownerId.equals(recent.ownerId())) {
            if (cur == null) {
                // 最近一段租约已不在位：过期回收 -> LEASE_EXPIRED；正常结束 -> NOT_LEASE_HOLDER。
                return r.lastLeaseExpired()
                        ? RejectReason.LEASE_EXPIRED : RejectReason.NOT_LEASE_HOLDER;
            }
            return expired ? RejectReason.LEASE_EXPIRED : null;
        }
        // 2) 请求版本确实曾发放给请求者，但当前已被别人/新版本接手 -> OWNERSHIP_SUPERSEDED。
        if (r.wasEverLeased(version) && ownerId.equals(r.ownerOfVersion(version))) {
            return RejectReason.OWNERSHIP_SUPERSEDED;
        }
        // 3) 压根不是持有者（伪造版本 / 别人的版本）。
        return RejectReason.NOT_LEASE_HOLDER;
    }

    /** 描述一次被拒绝的判定，带请求版本与当前/最近租约的 owner、版本，便于复现。 */
    private static String describe(Lease cur, String ownerId, long version, TaskRecord r,
                                   Instant now) {
        String current;
        if (cur != null) {
            current = cur.ownerId() + "@v" + cur.version()
                    + "(remainingMs=" + cur.remainingMillis(now) + ")";
        } else if (r.previousLease() != null) {
            current = "none(last=" + r.previousLease().ownerId() + "@v"
                    + r.previousLease().version()
                    + (r.lastLeaseExpired() ? ",expired" : ",closed") + ")";
        } else {
            current = "none";
        }
        return "requester=" + ownerId + " version=" + version + " current=" + current;
    }
}
