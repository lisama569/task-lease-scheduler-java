package com.github.lisama569.taskleaseschedulerjava.lease.model;

/** 所有权相关操作被拒绝时抛出，携带明确原因。 */
public class LeaseException extends RuntimeException {

    private final RejectReason reason;

    public LeaseException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
