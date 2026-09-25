package com.github.lisama569.taskleaseschedulerjava.lease.web;

import com.github.lisama569.taskleaseschedulerjava.lease.LeaseManager;
import com.github.lisama569.taskleaseschedulerjava.lease.model.Lease;
import com.github.lisama569.taskleaseschedulerjava.lease.model.SubmitOutcome;
import com.github.lisama569.taskleaseschedulerjava.lease.model.TaskView;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.List;

/** 本地运行验证用 REST 入口。 */
@RestController
@RequestMapping("/api/leases")
public class LeaseController {

    private final LeaseManager leaseManager;

    public LeaseController(LeaseManager leaseManager) {
        this.leaseManager = leaseManager;
    }

    public record AcquireResponse(boolean acquired, Lease lease) {}
    public record SubmitRequest(Lease lease, String businessKey, String result) {}

    @PostMapping("/tasks/{taskId}")
    public void register(@PathVariable String taskId, @RequestParam String businessKey) {
        leaseManager.registerTask(taskId, businessKey);
    }

    @PostMapping("/tasks/{taskId}/acquire")
    public AcquireResponse acquire(@PathVariable String taskId,
                                   @RequestParam String holderId,
                                   @RequestParam(defaultValue = "PT30S") Duration ttl) {
        return leaseManager.tryAcquire(taskId, holderId, ttl)
                .map(l -> new AcquireResponse(true, l))
                .orElse(new AcquireResponse(false, null));
    }

    @PostMapping("/acquire/{taskId}/renew")
    public Lease renew(@PathVariable String taskId, @RequestBody Lease lease,
                       @RequestParam(defaultValue = "PT30S") Duration ttl) {
        return leaseManager.renew(lease, ttl).orElse(null);
    }

    @PostMapping("/release")
    public boolean release(@RequestBody Lease lease) {
        return leaseManager.release(lease);
    }

    @PostMapping("/submit")
    public SubmitOutcome submit(@RequestBody SubmitRequest body) {
        return leaseManager.submitResult(body.lease(), body.businessKey(), body.result());
    }

    @PostMapping("/reclaim")
    public int reclaim() {
        return leaseManager.reclaimExpired();
    }

    @GetMapping("/tasks")
    public List<TaskView> tasks() {
        return leaseManager.views();
    }

    @GetMapping("/tasks/{taskId}")
    public TaskView task(@PathVariable String taskId) {
        return leaseManager.view(taskId).orElse(null);
    }
}
