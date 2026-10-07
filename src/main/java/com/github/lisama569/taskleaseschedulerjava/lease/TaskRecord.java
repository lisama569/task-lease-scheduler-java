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
    /**
     * 最近一段不再生效的租约（被释放/被回收/被新租约取代时保留），
     * 用于在没有当前租约时仍然给出可核对的版本与结论。
     */
    private Lease previousLease;
    /** 最近一段租约是否因为过期而结束（而非主动释放/被正常提交/被新持有者取代）。 */
    private boolean lastLeaseExpired;
    /** fencing token 序列：单调递增，不随租约释放/回收而重置。 */
    private long versionSeq;
    /** 每个 fencing token 曾分配给哪个 owner，用于判定提交者是否曾经持有该版本。 */
    private final Map<Long, String> ownerByVersion = new LinkedHashMap<>();
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
    public Lease previousLease() { return previousLease; }
    public boolean lastLeaseExpired() { return lastLeaseExpired; }
    /** 记录版本归属：每次成功获取租约时登记 fencing token -> owner。 */
    public void rememberVersionOwner(long version, String ownerId) {
        ownerByVersion.put(version, ownerId);
    }
    /** 该版本是否曾经作为租约发放过，以及是否发放给请求者本人。 */
    public String ownerOfVersion(long version) { return ownerByVersion.get(version); }
    public boolean wasEverLeased(long version) { return ownerByVersion.containsKey(version); }

    /** 当前租约因过期被回收。 */
    public void expireCurrentLease() {
        this.previousLease = lease;
        this.lastLeaseExpired = true;
        this.lease = null;
    }

    /** 当前租约随终态/主动释放/显式失败而正常结束（非过期）。 */
    public void closeCurrentLease() {
        this.previousLease = lease;
        this.lastLeaseExpired = false;
        this.lease = null;
    }

    /** 发放新租约：旧租约让位，是否过期按旧租约实际情况记录。 */
    public void replaceWithLease(Lease newLease, boolean oldLeaseExpired) {
        if (lease != null) {
            this.previousLease = lease;
        }
        this.lastLeaseExpired = oldLeaseExpired;
        this.lease = newLease;
    }

    public long nextVersion() { return ++versionSeq; }
    /** 最近一次发放的 fencing token（未发放过为 0）。 */
    public long lastIssuedVersion() { return versionSeq; }
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
