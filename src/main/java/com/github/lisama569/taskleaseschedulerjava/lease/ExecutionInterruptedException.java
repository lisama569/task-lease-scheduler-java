package com.github.lisama569.taskleaseschedulerjava.lease;

/** 执行被外部中断：结论确定为失败，绝不能当作成功提交。 */
public class ExecutionInterruptedException extends RuntimeException {
    public ExecutionInterruptedException(String message) {
        super(message);
    }

    public ExecutionInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
