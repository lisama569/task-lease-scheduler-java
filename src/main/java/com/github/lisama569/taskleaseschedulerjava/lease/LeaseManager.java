package com.github.lisama569.taskleaseschedulerjava.lease;

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
 */
@Component
public class LeaseManager {

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
            switch (r.status()) {
                case SUCCEEDED, DEAD, CANCELLED -> {
                    return LeaseDecision.rejected(RejectReason.TASK_FINISHED,
                            "task " + taskId + " is " + r.status());
                }
                case RETRY_WAITING -> {
                    if (r.nextRetryAt() != null && now.isBefore(r.nextRetryAt())) {
                        return LeaseDecision.rejected(RejectReason.RETRY_BACKOFF_NOT_ELAPSED,
                                "retry at " + r.nextRetryAt());
                    }
                }
                case LEASED -> {
                    Lease cur = r.lease();
                    if (cur != null && !cur.isExpired(now) && !cur.ownerId().equals(ownerId)) {
                        return LeaseDecision.rejected(RejectReason.LEASE_HELD_BY_OTHER,
                                "held by " + cur.ownerId() + " version=" + cur.version()
                                        + " remainingMs=" + cur.remainingMillis(now));
                    }
                }
                default -> { /* PENDING: fall through */ }
            }
            long newVersion = r.nextVersion();
            Lease lease = new Lease(taskId, ownerId, newVersion, now, now.plus(ttl));
            // 步骤进度所有权隔离：持有者变更（租约过期/被回收/被抢走后被他人获取）
            // 时清除旧进度，新持有者拿到干净起点；同一 owner 重新获得租约视为恢复，保留进度。
            if (r.stepsOwner() != null && !r.stepsOwner().equals(ownerId)) {
                r.clearSteps();
            }
            r.lease(lease);
            r.noteHeldLease(lease);
            r.status(TaskStatus.LEASED);
            r.incrementAttempts();
            r.nextRetryAt(null);
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
                return LeaseDecision.rejected(invalid, describe(cur, ownerId, version),
                        cur == null ? null : cur.version());
            }
            Lease renewed = new Lease(taskId, ownerId, version, cur.acquiredAt(),
                    clock.now().plus(ttl));
            r.lease(renewed);
            r.noteHeldLease(renewed);
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
                return LeaseDecision.rejected(invalid, describe(cur, ownerId, version),
                        cur == null ? null : cur.version());
            }
            r.lease(null);
            r.status(TaskStatus.PENDING);
            return LeaseDecision.ok(null);
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
            Long curVersion = r.lease() == null ? null : r.lease().version();
            if (r.hasApplied(businessKey)) {
                return SubmitOutcome.duplicate(curVersion);
            }
            if (r.status() == TaskStatus.SUCCEEDED || r.status() == TaskStatus.DEAD
                    || r.status() == TaskStatus.CANCELLED) {
                return SubmitOutcome.rejected(RejectReason.TASK_FINISHED,
                        "task is " + r.status() + ", businessKey not applied: " + businessKey,
                        curVersion);
            }
            RejectReason invalid = checkHolder(r, r.lease(), ownerId, version, clock.now());
            if (invalid != null) {
                return SubmitOutcome.rejected(invalid, describe(r.lease(), ownerId, version),
                        curVersion);
            }
            r.markApplied(businessKey);
            r.result(payload);
            r.lease(null);
            r.clearSteps();
            r.status(TaskStatus.SUCCEEDED);
            return SubmitOutcome.accepted(version);
        }
    }

    /**
     * 上报失败：可重试错误进入退避等待，不可重试或超上限进入 DEAD 终态。
     * 租约已过期但版本未被取代（仍是自己那一版）时也接受上报——
     * 执行时限与租约时长相同、卡满上限的场景依赖这一点得到确定结论。
     */
    public LeaseDecision reportFailure(String taskId, String ownerId, long version,
                                       ErrorKind kind, String reason) {
        TaskRecord r = tasks.get(taskId);
        if (r == null) {
            return LeaseDecision.rejected(RejectReason.TASK_NOT_FOUND, "no such task: " + taskId);
        }
        synchronized (r) {
            RejectReason invalid = checkHolderForFailureReport(r, r.lease(), ownerId, version);
            if (invalid != null) {
                return LeaseDecision.rejected(invalid, describe(r.lease(), ownerId, version),
                        r.lease() == null ? null : r.lease().version());
            }
            r.lastErrorKind(kind);
            r.lastFailureReason(reason);
            r.lease(null);
            if (kind == ErrorKind.NON_RETRYABLE || r.attempts() >= r.maxAttempts()) {
                r.status(TaskStatus.DEAD);
                r.clearSteps();
                return LeaseDecision.rejected(RejectReason.TASK_FINISHED,
                        "task DEAD: kind=" + kind + " attempts=" + r.attempts());
            }
            r.status(TaskStatus.RETRY_WAITING);
            r.nextRetryAt(clock.now().plus(retryPolicy.backoffFor(r.attempts())));
            return LeaseDecision.ok(null);
        }
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
            r.lease(null);
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
                return LeaseDecision.rejected(invalid, describe(cur, ownerId, version),
                        cur == null ? null : cur.version());
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
                return ProgressView.rejected(invalid, describe(cur, ownerId, version),
                        cur == null ? null : cur.version());
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
                    r.lease(null);
                    r.status(TaskStatus.PENDING);
                    reclaimed++;
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

    /**
     * 持有者校验（写操作/进度/续约用）：通过则返回 null；拒绝时三种情形互相区分——
     * OWNERSHIP_SUPERSEDED：请求者曾持有该版本租约，但任务已被别人（或同 owner 的新版本）接手；
     * LEASE_EXPIRED：请求者持有的租约已过期/被回收，且尚无人接手；
     * NOT_LEASE_HOLDER：请求者从未持有过该版本租约（压根不是持有者）。
     */
    private static RejectReason checkHolder(TaskRecord r, Lease cur, String ownerId,
                                            long version, Instant now) {
        if (r.status() == TaskStatus.CANCELLED || r.status() == TaskStatus.DEAD
                || r.status() == TaskStatus.SUCCEEDED) {
            return RejectReason.TASK_FINISHED;
        }
        if (cur != null && cur.ownerId().equals(ownerId)) {
            if (cur.version() != version) {
                return RejectReason.OWNERSHIP_SUPERSEDED; // 同一 owner 的版本已轮换
            }
            return cur.isExpired(now) ? RejectReason.LEASE_EXPIRED : null;
        }
        // 当前租约不在请求者手里（或已无租约）：回看请求者是否持有过该版本
        Lease prev = r.lastLeaseOf(ownerId);
        boolean wasHolderOfVersion = prev != null && prev.version() == version;
        if (cur != null) {
            // 任务已被别人接手
            return wasHolderOfVersion ? RejectReason.OWNERSHIP_SUPERSEDED
                    : RejectReason.NOT_LEASE_HOLDER;
        }
        // 当前无租约：曾持有该版本说明租约已过期/被回收/已随失败上报结束
        return wasHolderOfVersion ? RejectReason.LEASE_EXPIRED
                : RejectReason.NOT_LEASE_HOLDER;
    }

    /**
     * 失败上报专用的持有者校验：租约已过期但版本未被取代（仍是自己那一版、无人接手）
     * 时仍允许上报。这样执行时限 == 租约时长时，卡满上限的执行也能把失败落定，
     * 让任务立刻进入重试，而不是干等租约被动回收。安全性：只要版本未被取代，
     * 其他执行者就不可能已经拿到该任务，状态转换仍只有当前持有者能触发。
     */
    private static RejectReason checkHolderForFailureReport(TaskRecord r, Lease cur,
                                                            String ownerId, long version) {
        if (r.status() == TaskStatus.CANCELLED || r.status() == TaskStatus.DEAD
                || r.status() == TaskStatus.SUCCEEDED) {
            return RejectReason.TASK_FINISHED;
        }
        if (cur != null && cur.ownerId().equals(ownerId) && cur.version() == version) {
            return null; // 仍是自己那一版：即使已过期也允许把失败落定
        }
        return checkHolder(r, cur, ownerId, version, Instant.MAX);
    }

    private static String describe(Lease cur, String ownerId, long version) {
        return "requester=" + ownerId + " version=" + version + " current="
                + (cur == null ? "none" : cur.ownerId() + "@v" + cur.version());
    }
}
