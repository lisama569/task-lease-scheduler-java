package com.github.lisama569.taskleaseschedulerjava.lease.model;

/** 任务/租约状态机。 */
public enum TaskStatus {
    PENDING,        // 初始或释放后，可被调度
    LEASED,         // 已有持有者
    RECOVERABLE,    // 可重试失败，等待退避后重新调度
    SUCCEEDED,      // 终态：成功
    FAILED,         // 终态：不可重试失败
    DEAD,           // 终态：重试次数耗尽
    CANCELLED       // 终态：被取消
}
