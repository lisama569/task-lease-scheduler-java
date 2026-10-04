package com.github.lisama569.taskleaseschedulerjava.lease;

/**
 * 需要步骤级续跑的执行器（可选能力）：不使用这套机制的任务继续用原有 Function 调用方式。
 */
@FunctionalInterface
public interface StepExecutor<T> {
    T execute(ExecutionContext ctx) throws Exception;
}
