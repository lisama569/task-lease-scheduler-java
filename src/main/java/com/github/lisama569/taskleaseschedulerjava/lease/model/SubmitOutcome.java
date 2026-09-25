package com.github.lisama569.taskleaseschedulerjava.lease.model;

/** 结果提交结论：accepted 新写入；duplicate 幂等命中（无重复副作用）；rejected 携带原因。 */
public record SubmitOutcome(boolean accepted, boolean duplicate,
                            RejectReason rejectReason, String result) {

    public static SubmitOutcome accepted(String result) {
        return new SubmitOutcome(true, false, null, result);
    }

    public static SubmitOutcome duplicate(String result) {
        return new SubmitOutcome(false, true, null, result);
    }

    public static SubmitOutcome rejected(RejectReason reason) {
        return new SubmitOutcome(false, false, reason, null);
    }
}
