package com.github.lisama569.taskleaseschedulerjava.lease;

import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** 任务的本地共享状态（多实例通过同一 LeaseManager 竞争它）。 */
public final class TaskRecord {
    private final String taskId;
    private TaskStatus status = TaskStatus.PENDING;
    private Lease lease;
    /** fencing token 序列：单调递增，不随租约释放/回收而重置。 */
    private long versionSeq;
    private int attempts;
    private final int maxAttempts;
    private Instant nextRetryAt;
    private String lastFailureReason;
    private ErrorKind lastErrorKind;
    private String result;
    /** 已应用结果的业务键，保证幂等。 */
    private final Set<String> appliedBusinessKeys = new HashSet<>();
    /** 当前持有者记录的已完成步骤（stepKey -> 备注），按记录顺序排列。 */
    private final Map<String, String> completedSteps = new LinkedHashMap<>();
    /** 步骤进度的归属者：只有同一 owner 重新获得租约时才继承进度，否则清空。 */
    private String stepsOwner;

    public TaskRecord(String taskId, int maxAttempts) {
        this.taskId = taskId;
        this.maxAttempts = maxAttempts;
    }

    public String taskId() { return taskId; }
    public TaskStatus status() { return status; }
    public void status(TaskStatus s) { this.status = s; }
    public Lease lease() { return lease; }
    public void lease(Lease l) { this.lease = l; }
    public long nextVersion() { return ++versionSeq; }
    public int attempts() { return attempts; }
    public void incrementAttempts() { this.attempts++; }
    public int maxAttempts() { return maxAttempts; }
    public Instant nextRetryAt() { return nextRetryAt; }
    public void nextRetryAt(Instant t) { this.nextRetryAt = t; }
    public String lastFailureReason() { return lastFailureReason; }
    public void lastFailureReason(String r) { this.lastFailureReason = r; }
    public ErrorKind lastErrorKind() { return lastErrorKind; }
    public void lastErrorKind(ErrorKind k) { this.lastErrorKind = k; }
    public String result() { return result; }
    public void result(String r) { this.result = r; }
    public boolean hasApplied(String businessKey) { return appliedBusinessKeys.contains(businessKey); }
    public void markApplied(String businessKey) { appliedBusinessKeys.add(businessKey); }

    /** 记录一个已完成步骤（同一 stepKey 重复记录幂等，保留首次顺序）。 */
    public void recordStep(String ownerId, String stepKey, String note) {
        if (completedSteps.isEmpty()) {
            stepsOwner = ownerId;
        }
        completedSteps.putIfAbsent(stepKey, note);
    }

    public List<String> completedStepKeys() { return List.copyOf(completedSteps.keySet()); }
    public String stepsOwner() { return stepsOwner; }

    /** 清空步骤进度：持有者变更或任务进入终态时调用。 */
    public void clearSteps() {
        completedSteps.clear();
        stepsOwner = null;
    }
}
