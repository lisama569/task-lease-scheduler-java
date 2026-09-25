package com.github.lisama569.taskleaseschedulerjava.lease.model;

import java.time.Duration;

/** 运行状态查询视图：持有者与租约剩余时间、失败原因均可读。 */
public record TaskView(String taskId, TaskStatus status, String holderId,
                       Long leaseVersion, Duration leaseRemaining,
                       int attemptCount, String lastFailure,
                       FailureKind lastFailureKind) {
}
