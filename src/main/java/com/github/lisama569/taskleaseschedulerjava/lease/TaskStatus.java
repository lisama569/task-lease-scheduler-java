package com.github.lisama569.taskleaseschedulerjava.lease;

/** 任务生命周期状态。 */
public enum TaskStatus {
    /** 待调度，可被抢占。 */
    PENDING,
    /** 已被某持有者租出。 */
    LEASED,
    /** 执行成功，终态。 */
    SUCCEEDED,
    /** 失败但可重试，等待退避到期后重新调度。 */
    RETRY_WAITING,
    /** 不可重试错误或超过重试上限，终态。 */
    DEAD,
    /** 被显式取消，终态，不再被调度。 */
    CANCELLED
}
