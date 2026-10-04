package com.github.lisama569.taskleaseschedulerjava.web;

import com.github.lisama569.taskleaseschedulerjava.lease.ErrorKind;
import com.github.lisama569.taskleaseschedulerjava.lease.LeaseDecision;
import com.github.lisama569.taskleaseschedulerjava.lease.LeaseManager;
import com.github.lisama569.taskleaseschedulerjava.lease.ProgressView;
import com.github.lisama569.taskleaseschedulerjava.lease.SubmitOutcome;
import com.github.lisama569.taskleaseschedulerjava.lease.TaskScheduler;
import com.github.lisama569.taskleaseschedulerjava.lease.TaskSnapshot;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 本地验证用 REST 接口：注册、查询、取消，以及完整执行链路——
 * 抢占 / 续约 / 释放 / 提交结果 / 上报失败 / 步骤进度 / 演示执行。
 * 所有判定结果都带租约版本（fencing token）与拒绝原因，便于复现每一步。
 */
@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final LeaseManager leaseManager;
    private final TaskScheduler scheduler;

    public TaskController(LeaseManager leaseManager, TaskScheduler scheduler) {
        this.leaseManager = leaseManager;
        this.scheduler = scheduler;
    }

    public record AcquireRequest(String ownerId, long ttlMillis) {}
    public record RenewRequest(String ownerId, long version, long ttlMillis) {}
    public record ReleaseRequest(String ownerId, long version) {}
    public record SubmitRequest(String ownerId, long version, String businessKey,
                                String payload) {}
    public record FailureRequest(String ownerId, long version, ErrorKind kind, String reason) {}
    public record StepRequest(String ownerId, long version, String stepKey, String note) {}
    /** 演示执行请求：workMillis 模拟执行耗时，fail=true 模拟执行抛异常。 */
    public record ExecuteRequest(String ownerId, String businessKey, long timeoutMillis,
                                 long workMillis, boolean fail) {}

    @PostMapping("/{taskId}")
    public void register(@PathVariable String taskId) {
        leaseManager.registerTask(taskId);
    }

    @PostMapping("/{taskId}/acquire")
    public LeaseDecision acquire(@PathVariable String taskId,
                                 @RequestBody AcquireRequest req) {
        return leaseManager.acquire(taskId, req.ownerId(), Duration.ofMillis(req.ttlMillis()));
    }

    @PostMapping("/{taskId}/renew")
    public LeaseDecision renew(@PathVariable String taskId, @RequestBody RenewRequest req) {
        return leaseManager.renew(taskId, req.ownerId(), req.version(),
                Duration.ofMillis(req.ttlMillis()));
    }

    @PostMapping("/{taskId}/release")
    public LeaseDecision release(@PathVariable String taskId, @RequestBody ReleaseRequest req) {
        return leaseManager.release(taskId, req.ownerId(), req.version());
    }

    @PostMapping("/{taskId}/submit")
    public SubmitOutcome submit(@PathVariable String taskId, @RequestBody SubmitRequest req) {
        return leaseManager.submitResult(taskId, req.ownerId(), req.version(),
                req.businessKey(), req.payload());
    }

    @PostMapping("/{taskId}/failure")
    public LeaseDecision failure(@PathVariable String taskId, @RequestBody FailureRequest req) {
        return leaseManager.reportFailure(taskId, req.ownerId(), req.version(),
                req.kind(), req.reason());
    }

    @PostMapping("/{taskId}/steps")
    public LeaseDecision recordStep(@PathVariable String taskId, @RequestBody StepRequest req) {
        return leaseManager.recordStep(taskId, req.ownerId(), req.version(),
                req.stepKey(), req.note());
    }

    @GetMapping("/{taskId}/progress")
    public ProgressView progress(@PathVariable String taskId, @RequestParam String ownerId,
                                 @RequestParam long version) {
        return leaseManager.progress(taskId, ownerId, version);
    }

    /**
     * 演示执行：从接口发起“抢占 -> 限时执行 -> 提交或失败上报”的完整链路。
     * workMillis > timeoutMillis 可复现“超时之后进入重试”；
     * 重复提交同一 businessKey 可复现“重复提交被认成重复”。
     */
    @PostMapping("/{taskId}/execute")
    public SubmitOutcome execute(@PathVariable String taskId, @RequestBody ExecuteRequest req) {
        return scheduler.runOnce(taskId, req.ownerId(), req.businessKey(),
                Duration.ofMillis(req.timeoutMillis()), id -> {
                    CountDownLatch done = new CountDownLatch(1);
                    try {
                        if (!done.await(req.workMillis(), TimeUnit.MILLISECONDS)) {
                            // 模拟耗时到达，正常完成
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("execution interrupted");
                    }
                    if (req.fail()) {
                        throw new IllegalStateException("simulated failure");
                    }
                    return "done-by-" + req.ownerId();
                });
    }

    @GetMapping("/{taskId}")
    public TaskSnapshot snapshot(@PathVariable String taskId) {
        return leaseManager.snapshot(taskId).orElseThrow();
    }

    @GetMapping
    public List<TaskSnapshot> schedulable() {
        return leaseManager.schedulable();
    }

    @GetMapping("/by-owner/{ownerId}")
    public List<TaskSnapshot> byOwner(@PathVariable String ownerId) {
        return leaseManager.byOwner(ownerId);
    }

    @GetMapping("/by-failure/{reason}")
    public List<TaskSnapshot> byFailure(@PathVariable String reason) {
        return leaseManager.byFailureReason(reason);
    }

    @PostMapping("/{taskId}/cancel")
    public boolean cancel(@PathVariable String taskId) {
        return leaseManager.cancel(taskId);
    }
}
