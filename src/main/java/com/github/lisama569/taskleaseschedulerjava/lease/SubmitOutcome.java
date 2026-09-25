package com.github.lisama569.taskleaseschedulerjava.lease;

/** 结果提交的判定结果。 */
public record SubmitOutcome(Kind kind, RejectReason reason, String detail) {

    public enum Kind {
        /** 首次接受，产生副作用。 */
        ACCEPTED,
        /** 幂等命中：同一业务键已提交过，无副作用。 */
        DUPLICATE,
        /** 被拒绝，附带可区分原因。 */
        REJECTED
    }

    public static SubmitOutcome accepted() {
        return new SubmitOutcome(Kind.ACCEPTED, null, "result applied");
    }

    public static SubmitOutcome duplicate() {
        return new SubmitOutcome(Kind.DUPLICATE, null, "idempotent hit, no side effect");
    }

    public static SubmitOutcome rejected(RejectReason reason, String detail) {
        return new SubmitOutcome(Kind.REJECTED, reason, detail);
    }
}
