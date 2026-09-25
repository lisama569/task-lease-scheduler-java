package com.github.lisama569.taskleaseschedulerjava.lease.model;

/** 执行体显式声明失败分类；未声明的异常一律按 RETRYABLE 处理（如下游暂时不可用）。 */
public class TaskFailureException extends RuntimeException {

    private final FailureKind kind;

    public TaskFailureException(FailureKind kind, String message) {
        super(message);
        this.kind = kind;
    }

    public TaskFailureException(FailureKind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public FailureKind kind() {
        return kind;
    }
}
