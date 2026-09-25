package com.github.lisama569.taskleaseschedulerjava.lease;

/** 租约获取/续约的判定结果：唯一且可解释。 */
public record LeaseDecision(boolean acquired, Lease lease, RejectReason reason, String detail) {

    public static LeaseDecision ok(Lease lease) {
        return new LeaseDecision(true, lease, null, "acquired");
    }

    public static LeaseDecision rejected(RejectReason reason, String detail) {
        return new LeaseDecision(false, null, reason, detail);
    }
}
