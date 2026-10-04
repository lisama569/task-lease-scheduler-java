package com.github.lisama569.taskleaseschedulerjava.lease;

import java.time.Instant;

/**
 * 单个步骤的执行进度（只读视图）。
 * ownerVersion 为完成该步骤时的租约版本（fencing token）：
 * 进度只属于当时的持有者，租约失效/被接管后旧进度会被清空，不会串到新持有者。
 */
public record StepProgress(String step, boolean completed, long ownerVersion, Instant completedAt) {

    public static StepProgress completed(String step, long ownerVersion, Instant at) {
        return new StepProgress(step, true, ownerVersion, at);
    }
}
