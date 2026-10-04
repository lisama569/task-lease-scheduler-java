package com.github.lisama569.taskleaseschedulerjava.lease;

/** 执行超过时限：在检查点被确定性中止，按一次可恢复失败处理。 */
public class ExecutionTimeoutException extends RuntimeException {
    public ExecutionTimeoutException(String message) {
        super(message);
    }
}
