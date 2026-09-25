package com.github.lisama569.taskleaseschedulerjava.lease;

import com.github.lisama569.taskleaseschedulerjava.lease.model.FailureKind;
import com.github.lisama569.taskleaseschedulerjava.lease.model.Lease;
import com.github.lisama569.taskleaseschedulerjava.lease.model.RetryPolicy;
import com.github.lisama569.taskleaseschedulerjava.lease.model.SubmitOutcome;
import com.github.lisama569.taskleaseschedulerjava.lease.model.TaskRecord;
import com.github.lisama569.taskleaseschedulerjava.lease.model.TaskStatus;
import com.github.lisama569.taskleaseschedulerjava.lease.model.TaskView;
import com.github.lisama569.taskleaseschedulerjava.lease.store.LeaseStore;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 租约管理：获取/续约/释放/到期回收/结果提交/失败登记。
 * 所有状态迁移经 LeaseStore.mutate 在记录锁内原子完成，
 * 租约 version 单调递增，作为并发抢占与所有权判定的 fencing token。
 */
public class LeaseManager {

    protected final LeaseStore store;
    protected final Clock clock;
    protected final RetryPolicy retryPolicy;

    public LeaseManager(LeaseStore store, Clock clock, RetryPolicy retryPolicy) {
        this.store = store;
        this.clock = clock;
        this.retryPolicy = retryPolicy;
    }

    public void registerTask(String taskId, String businessKey) {
        store.createIfAbsent(taskId, businessKey);
    }

    /**
     * 尝试获取租约。仅当任务处于 PENDING、退避到期的 RECOVERABLE，
     * 或当前租约已到期（视为持有者失联）时成功；否则返回 empty。
     */
    public Optional<Lease> tryAcquire(String taskId, String holderId, Duration ttl) {
        if (taskId == null || holderId == null || holderId.isBlank()
                || ttl == null || ttl.isNegative() || ttl.isZero()) {
            return Optional.empty();
        }
        Instant now = clock.instant();
        return Optional.ofNullable(store.mutate(taskId, record -> {
            boolean acquirable = switch (record.status()) {
                case PENDING -> true;
                case RECOVERABLE -> record.nextRetryAt() == null || !record.nextRetryAt().isAfter(now);
                case LEASED -> record.lease() != null && !record.lease().isValidAt(now);
                default -> false; // SUCCEEDED / FAILED / DEAD / CANCELLED
            };
            if (!acquirable) {
                return null;
            }
            long nextVersion = record.lastLeaseVersion() + 1;
            Lease lease = new Lease(taskId, holderId, nextVersion, now, now.plus(ttl));
            record.lease(lease);
            record.lastLeaseVersion(nextVersion);
            record.status(TaskStatus.LEASED);
            record.attemptCount(record.attemptCount() + 1);
            record.nextRetryAt(null);
            return lease;
        }));
    }

    /** 续约：仅当前持有者（holder+version 匹配）且租约仍有效时成功，version 不变。 */
    public Optional<Lease> renew(Lease lease, Duration ttl) {
        if (lease == null || ttl == null || ttl.isNegative() || ttl.isZero()) {
            return Optional.empty();
        }
        Instant now = clock.instant();
        return Optional.ofNullable(store.mutate(lease.taskId(), record -> {
            if (!isCurrentValidHolder(record, lease, now)) {
                return null;
            }
            Lease renewed = new Lease(lease.taskId(), lease.holderId(), lease.version(),
                    record.lease().acquiredAt(), now.plus(ttl));
            record.lease(renewed);
            return renewed;
        }));
    }

    /** 释放：持有者主动归还，任务回到 PENDING 可立即被重新调度。 */
    public boolean release(Lease lease) {
        if (lease == null) {
            return false;
        }
        Boolean ok = store.mutate(lease.taskId(), record -> {
            if (record.status() != TaskStatus.LEASED || record.lease() == null
                    || record.lease().version() != lease.version()
                    || !record.lease().holderId().equals(lease.holderId())) {
                return false;
            }
            record.lease(null);
            record.status(TaskStatus.PENDING);
            return true;
        });
        return Boolean.TRUE.equals(ok);
    }

    /**
     * 幂等提交结果：
     * - 同一业务键重复提交 -> duplicate，返回首次结果，无副作用；
     * - 过期持有者 -> LEASE_EXPIRED；被新持有者取代 -> OWNERSHIP_SUPERSEDED；
     * - 参数非法 -> INVALID_ARGUMENT；终态任务 -> TERMINAL_TASK。
     * 任何拒绝都不会覆盖已有状态。
     */
    public SubmitOutcome submitResult(Lease lease, String businessKey, String result) {
        if (lease == null || businessKey == null || businessKey.isBlank()
                || result == null) {
            return SubmitOutcome.rejected(com.github.lisama569.taskleaseschedulerjava.lease.model.RejectReason.INVALID_ARGUMENT);
        }
        Instant now = clock.instant();
        SubmitOutcome outcome = store.mutate(lease.taskId(), record -> {
            // 幂等命中：任务已成功且业务键一致
            if (record.status() == TaskStatus.SUCCEEDED) {
                if (businessKey.equals(record.resultBusinessKey())) {
                    return SubmitOutcome.duplicate(record.result());
                }
                return SubmitOutcome.rejected(com.github.lisama569.taskleaseschedulerjava.lease.model.RejectReason.INVALID_ARGUMENT);
            }
            if (record.status() == TaskStatus.FAILED || record.status() == TaskStatus.DEAD
                    || record.status() == TaskStatus.CANCELLED) {
                return SubmitOutcome.rejected(com.github.lisama569.taskleaseschedulerjava.lease.model.RejectReason.TERMINAL_TASK);
            }
            Lease current = record.lease();
            if (current == null || record.status() != TaskStatus.LEASED) {
                return SubmitOutcome.rejected(com.github.lisama569.taskleaseschedulerjava.lease.model.RejectReason.NOT_HOLDER);
            }
            if (current.version() > lease.version()) {
                return SubmitOutcome.rejected(com.github.lisama569.taskleaseschedulerjava.lease.model.RejectReason.OWNERSHIP_SUPERSEDED);
            }
            if (current.version() < lease.version()
                    || !current.holderId().equals(lease.holderId())) {
                return SubmitOutcome.rejected(com.github.lisama569.taskleaseschedulerjava.lease.model.RejectReason.NOT_HOLDER);
            }
            if (!current.isValidAt(now)) {
                return SubmitOutcome.rejected(com.github.lisama569.taskleaseschedulerjava.lease.model.RejectReason.LEASE_EXPIRED);
            }
            record.status(TaskStatus.SUCCEEDED);
            record.result(result);
            record.resultBusinessKey(businessKey);
            record.lease(null);
            return SubmitOutcome.accepted(result);
        });
        return outcome == null
                ? SubmitOutcome.rejected(com.github.lisama569.taskleaseschedulerjava.lease.model.RejectReason.INVALID_ARGUMENT)
                : outcome;
    }

    /**
     * 登记失败：仅当前持有者有效。可重试失败进入 RECOVERABLE（带退避），
     * 次数耗尽进入 DEAD；不可重试直接进入 FAILED。过期/失效持有者的上报被忽略。
     */
    public void reportFailure(Lease lease, FailureKind kind, String reason) {
        if (lease == null || kind == null) {
            return;
        }
        Instant now = clock.instant();
        store.mutate(lease.taskId(), record -> {
            if (record.status() != TaskStatus.LEASED || record.lease() == null
                    || record.lease().version() != lease.version()
                    || !record.lease().holderId().equals(lease.holderId())) {
                return null; //  stale 持有者上报，忽略
            }
            record.lastFailure(reason);
            record.lastFailureKind(kind);
            record.lease(null);
            if (kind == FailureKind.NON_RETRYABLE) {
                record.status(TaskStatus.FAILED);
                record.nextRetryAt(null);
            } else if (retryPolicy.exhausted(record.attemptCount())) {
                record.status(TaskStatus.DEAD);
                record.nextRetryAt(null);
            } else {
                record.status(TaskStatus.RECOVERABLE);
                record.nextRetryAt(now.plus(retryPolicy.backoffFor(record.attemptCount())));
            }
            return null;
        });
    }

    /** 取消：非终态任务进入 CANCELLED，不再被调度。 */
    public boolean cancel(String taskId) {
        Boolean ok = store.mutate(taskId, record -> {
            if (record.status() == TaskStatus.SUCCEEDED || record.status() == TaskStatus.FAILED
                    || record.status() == TaskStatus.DEAD || record.status() == TaskStatus.CANCELLED) {
                return false;
            }
            record.lease(null);
            record.status(TaskStatus.CANCELLED);
            record.nextRetryAt(null);
            return true;
        });
        return Boolean.TRUE.equals(ok);
    }

    /** 到期回收：把租约已过期的任务回收为 PENDING，返回回收数量。 */
    public int reclaimExpired() {
        Instant now = clock.instant();
        int reclaimed = 0;
        for (TaskRecord record : store.all()) {
            Boolean done = store.mutate(record.taskId(), r -> {
                if (r.status() == TaskStatus.LEASED && r.lease() != null
                        && !r.lease().isValidAt(now)) {
                    r.lease(null);
                    r.status(TaskStatus.PENDING);
                    return true;
                }
                return false;
            });
            if (Boolean.TRUE.equals(done)) {
                reclaimed++;
            }
        }
        return reclaimed;
    }

    /** 当前可被调度的任务：PENDING、退避到期的 RECOVERABLE、租约已过期的 LEASED。 */
    public List<String> dueTasks() {
        Instant now = clock.instant();
        List<String> due = new ArrayList<>();
        for (TaskRecord record : store.all()) {
            switch (record.status()) {
                case PENDING -> due.add(record.taskId());
                case RECOVERABLE -> {
                    if (record.nextRetryAt() == null || !record.nextRetryAt().isAfter(now)) {
                        due.add(record.taskId());
                    }
                }
                case LEASED -> {
                    if (record.lease() != null && !record.lease().isValidAt(now)) {
                        due.add(record.taskId());
                    }
                }
                default -> { }
            }
        }
        return due;
    }

    /** 任务注册时的业务键（幂等提交使用）。 */
    public Optional<String> businessKey(String taskId) {
        return store.find(taskId).map(TaskRecord::businessKey);
    }

    public Optional<TaskView> view(String taskId) {
        return store.find(taskId).map(this::toView);
    }

    public List<TaskView> views() {
        return store.all().stream().map(this::toView).toList();
    }

    private TaskView toView(TaskRecord record) {
        Lease lease = record.lease();
        Duration remaining = null;
        if (lease != null) {
            Duration r = lease.remaining(clock.instant());
            remaining = r.isNegative() ? Duration.ZERO : r;
        }
        return new TaskView(record.taskId(), record.status(),
                lease == null ? null : lease.holderId(),
                lease == null ? null : lease.version(),
                remaining, record.attemptCount(),
                record.lastFailure(), record.lastFailureKind());
    }

    private boolean isCurrentValidHolder(TaskRecord record, Lease lease, Instant now) {
        return record.status() == TaskStatus.LEASED
                && record.lease() != null
                && record.lease().version() == lease.version()
                && record.lease().holderId().equals(lease.holderId())
                && record.lease().isValidAt(now);
    }
}
