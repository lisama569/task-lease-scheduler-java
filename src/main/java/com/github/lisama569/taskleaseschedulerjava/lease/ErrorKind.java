package com.github.lisama569.taskleaseschedulerjava.lease;

/** 失败错误分类：决定是否进入重试链路。 */
public enum ErrorKind {
    /** 暂时性故障（下游不可用、超时、中断），可重试。 */
    RETRYABLE,
    /** 参数非法、业务校验失败等，不可重试。 */
    NON_RETRYABLE
}
