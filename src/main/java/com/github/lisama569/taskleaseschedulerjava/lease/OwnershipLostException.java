package com.github.lisama569.taskleaseschedulerjava.lease;

/**
 * 执行途中发现所有权已失效（步骤进度写入被租约校验拒绝）。
 * 立即中止执行；随后的失败上报同样会被拒，旧持有者无法覆盖新持有者。
 */
public class OwnershipLostException extends RuntimeException {
    private final RejectReason reason;

    public OwnershipLostException(RejectReason reason, String detail) {
        super("ownership lost: " + reason + " (" + detail + ")");
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
