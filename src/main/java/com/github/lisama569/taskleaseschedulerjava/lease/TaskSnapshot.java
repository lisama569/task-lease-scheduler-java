package com.github.lisama569.taskleaseschedulerjava.lease;

import java.time.Instant;
import java.util.List;

/** 对外查询用的只读视图。 */
public record TaskSnapshot(String taskId, TaskStatus status, String ownerId,
                           Long leaseVersion, Long leaseRemainingMillis,
                           int attempts, int maxAttempts, String lastFailureReason,
                           ErrorKind lastErrorKind, String result, Instant nextRetryAt,
                           List<StepProgress> steps) {

    public static TaskSnapshot of(TaskRecord r, Instant now) {
        Lease l = r.lease();
        return new TaskSnapshot(
                r.taskId(), r.status(),
                l == null ? null : l.ownerId(),
                l == null ? null : l.version(),
                l == null ? null : l.remainingMillis(now),
                r.attempts(), r.maxAttempts(),
                r.lastFailureReason(), r.lastErrorKind(), r.result(), r.nextRetryAt(),
                r.steps());
    }
}
