package com.github.lisama569.taskleaseschedulerjava.lease.model;

import java.time.Instant;

/** 任务的共享状态记录（模拟多实例共享的存储行）。 */
public final class TaskRecord {

    private final String taskId;
    private final String businessKey;
    private TaskStatus status;
    private Lease lease;              // null 表示当前无持有者
    private long lastLeaseVersion;    // fencing token：单调递增，永不回退
    private int attemptCount;
    private String lastFailure;
    private FailureKind lastFailureKind;
    private Instant nextRetryAt;      // RECOVERABLE 状态下可重新调度的最早时间
    private String result;            // 已提交的幂等结果
    private String resultBusinessKey; // 结果对应的业务键（幂等判定依据）

    public TaskRecord(String taskId, String businessKey) {
        this.taskId = taskId;
        this.businessKey = businessKey;
        this.status = TaskStatus.PENDING;
        this.attemptCount = 0;
    }

    public String taskId() { return taskId; }
    public String businessKey() { return businessKey; }
    public TaskStatus status() { return status; }
    public Lease lease() { return lease; }
    public long lastLeaseVersion() { return lastLeaseVersion; }
    public int attemptCount() { return attemptCount; }
    public String lastFailure() { return lastFailure; }
    public FailureKind lastFailureKind() { return lastFailureKind; }
    public Instant nextRetryAt() { return nextRetryAt; }
    public String result() { return result; }
    public String resultBusinessKey() { return resultBusinessKey; }

    public void status(TaskStatus s) { this.status = s; }
    public void lease(Lease l) { this.lease = l; }
    public void lastLeaseVersion(long v) { this.lastLeaseVersion = v; }
    public void attemptCount(int n) { this.attemptCount = n; }
    public void lastFailure(String f) { this.lastFailure = f; }
    public void lastFailureKind(FailureKind k) { this.lastFailureKind = k; }
    public void nextRetryAt(Instant t) { this.nextRetryAt = t; }
    public void result(String r) { this.result = r; }
    public void resultBusinessKey(String k) { this.resultBusinessKey = k; }
}
