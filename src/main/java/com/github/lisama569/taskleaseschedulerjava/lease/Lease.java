package com.github.lisama569.taskleaseschedulerjava.lease;

import java.time.Instant;

/**
 * 租约。version 为 fencing token：每次所有权变更单调递增，
 * 用于判定提交者是否为最新持有者。
 */
public record Lease(String taskId, String ownerId, long version,
                    Instant acquiredAt, Instant expiresAt) {

    public boolean isExpired(Instant now) {
        return !now.isBefore(expiresAt);
    }

    public long remainingMillis(Instant now) {
        return Math.max(0L, expiresAt.toEpochMilli() - now.toEpochMilli());
    }
}
