package com.github.lisama569.taskleaseschedulerjava.lease;

/**
 * 租约获取/续约的判定结果：唯一且可解释。
 * currentVersion 为判定时任务当前租约的 fencing token（无租约时为 null），
 * 用于对照日志复现每一次判定。
 */
public record LeaseDecision(boolean acquired, Lease lease, RejectReason reason, String detail,
                            Long currentVersion) {

    /** 兼容旧构造：不显式给 currentVersion 时取所携带租约的版本。 */
    public LeaseDecision(boolean acquired, Lease lease, RejectReason reason, String detail) {
        this(acquired, lease, reason, detail, lease == null ? null : lease.version());
    }

    public static LeaseDecision ok(Lease lease) {
        return new LeaseDecision(true, lease, null, "acquired");
    }

    public static LeaseDecision rejected(RejectReason reason, String detail) {
        return new LeaseDecision(false, null, reason, detail, null);
    }

    public static LeaseDecision rejected(RejectReason reason, String detail,
                                         Long currentVersion) {
        return new LeaseDecision(false, null, reason, detail, currentVersion);
    }
}
