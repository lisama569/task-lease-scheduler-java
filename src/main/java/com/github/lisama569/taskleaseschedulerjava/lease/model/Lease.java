package com.github.lisama569.taskleaseschedulerjava.lease.model;

import java.time.Duration;
import java.time.Instant;

/**
 * 租约凭据（fencing token）。version 单调递增，是并发抢占与所有权判定的唯一依据。
 */
public record Lease(String taskId, String holderId, long version,
                    Instant acquiredAt, Instant expiresAt) {

    public boolean isValidAt(Instant now) {
        return now.isBefore(expiresAt);
    }

    public Duration remaining(Instant now) {
        return Duration.between(now, expiresAt);
    }
}
