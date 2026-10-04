package com.github.lisama569.taskleaseschedulerjava.lease;

/** 租约操作或结果提交被拒绝的原因，必须可区分、可解释。 */
public enum RejectReason {
    TASK_NOT_FOUND,
    /** 任务已被取消或已到终态，不再接受操作。 */
    TASK_FINISHED,
    /** 租约已被其他持有者占用且未过期。 */
    LEASE_HELD_BY_OTHER,
    /** 持有者持有的租约已过期。 */
    LEASE_EXPIRED,
    /** 租约版本已被更新的持有者取代（fencing）。 */
    OWNERSHIP_SUPERSEDED,
    /** 提交者不是当前持有者。 */
    NOT_LEASE_HOLDER,
    /** 失败退避未到期，暂不可重新调度。 */
    RETRY_BACKOFF_NOT_ELAPSED,
    /** 执行失败（已上报并进入重试链路）。 */
    EXECUTION_FAILED,
    /** 执行超过时限被中止：已按一次可恢复失败上报，任务进入重试链路。 */
    EXECUTION_TIMEOUT,
    /** 执行被外部中断（线程中断/实例停机）：结论确定为失败，绝不当成功提交。 */
    EXECUTION_INTERRUPTED,
    /** 参数非法。 */
    INVALID_ARGUMENT
}
