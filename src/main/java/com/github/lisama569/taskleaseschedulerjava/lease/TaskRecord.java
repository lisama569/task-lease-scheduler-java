package com.github.lisama569.taskleaseschedulerjava.lease;

import java.time.Instant;
import java.util.ArrayList;
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
    /**
     * 步骤级进度：step -> 完成标记（含完成时的租约版本）。
     * 只属于当前这条“持有者/重试血缘”；租约过期、被回收、被接管或主动释放时整体清空，
     * 新持有者从干净起点开始，旧进度绝不会串到新持有者头上。
     */
    private final Map<String, StepProgress> stepProgress = new LinkedHashMap<>();

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

    /** 某步骤是否已在当前持有者血缘中完成。 */
    public boolean isStepDone(String step) {
        StepProgress p = stepProgress.get(step);
        return p != null && p.completed();
    }

    /** 记录步骤完成，归属到取得该结果时的租约版本。 */
    public void markStepDone(String step, long ownerVersion, Instant at) {
        stepProgress.put(step, StepProgress.completed(step, ownerVersion, at));
    }

    /** 当前血缘的步骤进度快照（按完成顺序）。 */
    public List<StepProgress> steps() {
        return new ArrayList<>(stepProgress.values());
    }

    /** 清空旧持有者血缘的步骤进度：新持有者必须拿到干净、确定的起点。 */
    public void clearSteps() {
        stepProgress.clear();
    }
}
