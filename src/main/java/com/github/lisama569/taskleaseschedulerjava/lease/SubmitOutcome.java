package com.github.lisama569.taskleaseschedulerjava.lease;

/**
 * 结果提交的判定结果。
 * leaseVersion 为判定时任务当前租约的 fencing token（无租约时为 null），
 * 用于复现与解释每一次判定。
 */
public record SubmitOutcome(Kind kind, RejectReason reason, String detail, Long leaseVersion) {

    public enum Kind {
        /** 首次接受，产生副作用。 */
        ACCEPTED,
        /** 幂等命中：同一业务键已提交过，无副作用。 */
        DUPLICATE,
        /** 被拒绝，附带可区分原因。 */
        REJECTED
    }

    public static SubmitOutcome accepted(Long leaseVersion) {
        return new SubmitOutcome(Kind.ACCEPTED, null, "result applied", leaseVersion);
    }

    public static SubmitOutcome duplicate(Long leaseVersion) {
        return new SubmitOutcome(Kind.DUPLICATE, null, "idempotent hit, no side effect",
                leaseVersion);
    }

    public static SubmitOutcome rejected(RejectReason reason, String detail) {
        return new SubmitOutcome(Kind.REJECTED, reason, detail, null);
    }

    public static SubmitOutcome rejected(RejectReason reason, String detail, Long leaseVersion) {
        return new SubmitOutcome(Kind.REJECTED, reason, detail, leaseVersion);
    }
}
