package com.github.lisama569.taskleaseschedulerjava.lease;

/**
 * 租约获取/续约的判定结果：唯一且可解释。
 * leaseVersion 为判定时任务当前（或最近一段）租约的 fencing token，
 * 无任何历史租约时为 null，用于对着日志核对每一步。
 */
public record LeaseDecision(boolean acquired, Lease lease, RejectReason reason, String detail,
                            Long leaseVersion) {

    public static LeaseDecision ok(Lease lease) {
        return new LeaseDecision(true, lease, null, "acquired", lease.version());
    }

    /** 操作成功但不产生新租约（如释放、失败上报后进入重试）。 */
    public static LeaseDecision okNoLease() {
        return new LeaseDecision(true, null, null, "accepted", null);
    }

    public static LeaseDecision rejected(RejectReason reason, String detail) {
        return new LeaseDecision(false, null, reason, detail, null);
    }

    public static LeaseDecision rejected(RejectReason reason, String detail, Long leaseVersion) {
        return new LeaseDecision(false, null, reason, detail, leaseVersion);
    }
}
