package com.github.lisama569.taskleaseschedulerjava.lease;

import java.util.List;

/**
 * 步骤级进度查询结果：仅当前持有者可查；返回的进度必然属于查询者自己
 * （持有者变更时旧进度已被清除，见 LeaseManager）。
 */
public record ProgressView(boolean ok, RejectReason reason, String detail,
                           Long leaseVersion, List<String> completedSteps) {

    public static ProgressView of(long leaseVersion, List<String> completedSteps) {
        return new ProgressView(true, null, "ok", leaseVersion, List.copyOf(completedSteps));
    }

    public static ProgressView rejected(RejectReason reason, String detail) {
        return new ProgressView(false, reason, detail, null, List.of());
    }
}
