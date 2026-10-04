package com.github.lisama569.taskleaseschedulerjava.web;

import com.github.lisama569.taskleaseschedulerjava.lease.ErrorKind;
import com.github.lisama569.taskleaseschedulerjava.lease.LeaseDecision;
import com.github.lisama569.taskleaseschedulerjava.lease.LeaseManager;
import com.github.lisama569.taskleaseschedulerjava.lease.SubmitOutcome;
import com.github.lisama569.taskleaseschedulerjava.lease.TaskSnapshot;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.List;

/**
 * 本地验证用 REST 接口。
 * 在同一个本地进程内即可走完整条链路：注册 -> 抢占 ->（步骤进度）-> 提交/上报失败 -> 查询。
 * 所有判定响应都带租约版本与拒绝原因，便于复现与解释每一步。
 */
@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final LeaseManager leaseManager;

    public TaskController(LeaseManager leaseManager) {
        this.leaseManager = leaseManager;
    }

    // ---------- 注册 / 查询 / 取消 ----------

    @PostMapping("/{taskId}")
    public TaskSnapshot register(@PathVariable String taskId) {
        leaseManager.registerTask(taskId);
        return leaseManager.snapshot(taskId).orElseThrow();
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

    // ---------- 租约生命周期：抢占 / 续约 / 释放 ----------

    /** 抢占租约；ttlSeconds 可选，默认 30s。 */
    @PostMapping("/{taskId}/acquire")
    public LeaseDecision acquire(@PathVariable String taskId,
                                 @RequestParam String owner,
                                 @RequestParam(defaultValue = "30") long ttlSeconds) {
        return leaseManager.acquire(taskId, owner, Duration.ofSeconds(ttlSeconds));
    }

    @PostMapping("/{taskId}/renew")
    public LeaseDecision renew(@PathVariable String taskId,
                               @RequestParam String owner,
                               @RequestParam long version,
                               @RequestParam(defaultValue = "30") long ttlSeconds) {
        return leaseManager.renew(taskId, owner, version, Duration.ofSeconds(ttlSeconds));
    }

    @PostMapping("/{taskId}/release")
    public LeaseDecision release(@PathVariable String taskId,
                                 @RequestParam String owner,
                                 @RequestParam long version) {
        return leaseManager.release(taskId, owner, version);
    }

    // ---------- 结果提交（幂等 + 所有权 fencing） ----------

    @PostMapping("/{taskId}/submit")
    public SubmitOutcome submit(@PathVariable String taskId,
                                @RequestParam String owner,
                                @RequestParam long version,
                                @RequestParam String businessKey,
                                @RequestParam String payload) {
        return leaseManager.submitResult(taskId, owner, version, businessKey, payload);
    }

    // ---------- 失败上报 ----------

    @PostMapping("/{taskId}/failure")
    public LeaseDecision failure(@PathVariable String taskId,
                                 @RequestParam String owner,
                                 @RequestParam long version,
                                 @RequestParam(defaultValue = "RETRYABLE") ErrorKind kind,
                                 @RequestParam(defaultValue = "reported") String reason) {
        return leaseManager.reportFailure(taskId, owner, version, kind, reason);
    }

    // ---------- 步骤级进度（可选） ----------

    @GetMapping("/{taskId}/steps")
    public Object steps(@PathVariable String taskId) {
        return leaseManager.stepProgress(taskId);
    }

    @PostMapping("/{taskId}/steps/{step}")
    public LeaseDecision markStep(@PathVariable String taskId,
                                  @PathVariable String step,
                                  @RequestParam String owner,
                                  @RequestParam long version) {
        return leaseManager.markStepDone(taskId, owner, version, step);
    }
}
