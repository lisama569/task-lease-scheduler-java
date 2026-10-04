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
            // 只有“合作式失败后退避到期”的重试血缘才续用已有步骤进度；
            // 首次抢占、租约过期后的接管一律从干净起点开始，旧持有者进度不得串过来。
            boolean retryLineage = r.status() == TaskStatus.RETRY_WAITING;
            long newVersion = r.nextVersion();
            Lease lease = new Lease(taskId, ownerId, newVersion, now, now.plus(ttl));
            if (!retryLineage) {
                r.clearSteps();
            }
            r.lease(lease);
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
                return LeaseDecision.rejected(invalid, describe(cur, ownerId, version));
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
                return LeaseDecision.rejected(invalid, describe(cur, ownerId, version));
            }
            r.lease(null);
            r.status(TaskStatus.PENDING);
            r.clearSteps();
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
            if (r.hasApplied(businessKey)) {
                return SubmitOutcome.duplicate();
            }
            if (r.status() == TaskStatus.SUCCEEDED || r.status() == TaskStatus.DEAD
                    || r.status() == TaskStatus.CANCELLED) {
                return SubmitOutcome.rejected(RejectReason.TASK_FINISHED,
                        "task is " + r.status() + ", businessKey not applied: " + businessKey);
            }
            RejectReason invalid = checkHolder(r, r.lease(), ownerId, version, clock.now());
            if (invalid != null) {
                return SubmitOutcome.rejected(invalid, describe(r.lease(), ownerId, version));
            }
            r.markApplied(businessKey);
            r.result(payload);
            r.lease(null);
            r.status(TaskStatus.SUCCEEDED);
            return SubmitOutcome.accepted();
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
            RejectReason invalid = checkHolder(r, r.lease(), ownerId, version, clock.now());
            if (invalid != null) {
                return LeaseDecision.rejected(invalid, describe(r.lease(), ownerId, version));
            }
            r.lastErrorKind(kind);
            r.lastFailureReason(reason);
            r.lease(null);
            if (kind == ErrorKind.NON_RETRYABLE || r.attempts() >= r.maxAttempts()) {
                r.status(TaskStatus.DEAD);
                return LeaseDecision.rejected(RejectReason.TASK_FINISHED,
                        "task DEAD: kind=" + kind + " attempts=" + r.attempts());
            }
            r.status(TaskStatus.RETRY_WAITING);
            r.nextRetryAt(clock.now().plus(retryPolicy.backoffFor(r.attempts())));
            return LeaseDecision.ok(null);
        }
    }

    /** 查询当前持有者血缘已完成的步骤（进度跟租约走，过期/接管后已清空）。 */
    public java.util.List<StepProgress> stepProgress(String taskId) {
        TaskRecord r = tasks.get(taskId);
        if (r == null) {
            return java.util.List.of();
        }
        synchronized (r) {
            return r.steps();
        }
    }

    /**
     * 标记一个步骤完成：仅当前持有者（owner+version 匹配且未过期）可写。
     * 已完成的步骤重复标记无副作用（步骤级幂等）。
     */
    public LeaseDecision markStepDone(String taskId, String ownerId, long version, String step) {
        if (step == null || step.isBlank()) {
            return LeaseDecision.rejected(RejectReason.INVALID_ARGUMENT, "step required");
        }
        TaskRecord r = tasks.get(taskId);
        if (r == null) {
            return LeaseDecision.rejected(RejectReason.TASK_NOT_FOUND, "no such task: " + taskId);
        }
        synchronized (r) {
            Lease cur = r.lease();
            RejectReason invalid = checkHolder(r, cur, ownerId, version, clock.now());
            if (invalid != null) {
                return LeaseDecision.rejected(invalid, describe(cur, ownerId, version));
            }
            if (!r.isStepDone(step)) {
                r.markStepDone(step, version, clock.now());
            }
            return LeaseDecision.ok(cur);
        }
    }

    /** 查询某步骤在当前持有者血缘中是否已完成（不存在任务视为 false）。 */
    public boolean isStepDone(String taskId, String step) {
        TaskRecord r = tasks.get(taskId);
        if (r == null) {
            return false;
        }
        synchronized (r) {
            return r.isStepDone(step);
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
            r.status(TaskStatus.CANCELLED);
            r.clearSteps();
            return true;
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
                    r.clearSteps();
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
     * 持有者校验，精确区分三类拒绝：
     * <ul>
     *   <li>owner/version 都是当前租约，仅时间到期 -> LEASE_EXPIRED（还没人接管）；</li>
     *   <li>版本对不上当前租约（持有者已换人，或同一人已用新版本重新抢占）
     *       -> OWNERSHIP_SUPERSEDED（任务已被新持有者接管）；</li>
     *   <li>当前无租约，或提交者根本不是该版本的归属人 -> NOT_LEASE_HOLDER。</li>
     * </ul>
     */
    private static RejectReason checkHolder(TaskRecord r, Lease cur, String ownerId,
                                            long version, Instant now) {
        if (r.status() == TaskStatus.CANCELLED || r.status() == TaskStatus.DEAD
                || r.status() == TaskStatus.SUCCEEDED) {
            return RejectReason.TASK_FINISHED;
        }
        if (cur == null) {
            return RejectReason.NOT_LEASE_HOLDER;
        }
        if (cur.ownerId().equals(ownerId)) {
            if (cur.version() != version) {
                return RejectReason.OWNERSHIP_SUPERSEDED;
            }
            if (cur.isExpired(now)) {
                return RejectReason.LEASE_EXPIRED;
            }
            return null;
        }
        // 当前持有者是别人：拿着当前版本来的是冒充者；拿着旧版本来的是被接管的旧持有者。
        return cur.version() == version
                ? RejectReason.NOT_LEASE_HOLDER
                : RejectReason.OWNERSHIP_SUPERSEDED;
    }

    private static String describe(Lease cur, String ownerId, long version) {
        return "requester=" + ownerId + " version=" + version + " current="
                + (cur == null ? "none" : cur.ownerId() + "@v" + cur.version());
    }
}
