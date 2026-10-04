package com.github.lisama569.taskleaseschedulerjava.lease;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * 具名执行步骤：key 用于进度记录与续跑去重，action 为步骤本体（可产生外部副作用）。
 * 仅供关心中间进度的调用方使用，属于可选 API。
 */
public record TaskStep(String key, Supplier<String> action) {

    public TaskStep {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("step key required");
        }
        Objects.requireNonNull(action, "step action required");
    }
}
