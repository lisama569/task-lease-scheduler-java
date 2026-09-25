package com.github.lisama569.taskleaseschedulerjava.web;

import com.github.lisama569.taskleaseschedulerjava.lease.LeaseManager;
import com.github.lisama569.taskleaseschedulerjava.lease.TaskSnapshot;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/** 本地验证用 REST 接口：注册、查询、取消。 */
@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final LeaseManager leaseManager;

    public TaskController(LeaseManager leaseManager) {
        this.leaseManager = leaseManager;
    }

    @PostMapping("/{taskId}")
    public void register(@PathVariable String taskId) {
        leaseManager.registerTask(taskId);
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
