package com.github.lisama569.taskleaseschedulerjava.lease.model;

/** 提交/续约被拒绝的可判定原因。 */
public enum RejectReason {
    INVALID_ARGUMENT,
    LEASE_EXPIRED,
    OWNERSHIP_SUPERSEDED,
    TERMINAL_TASK,
    NOT_HOLDER
}
