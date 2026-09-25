package com.github.lisama569.taskleaseschedulerjava.lease;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.function.Function;

/**
 * 调度器：抢占租约 -> 执行任务 -> 提交结果或上报失败。
 * 多实例共享同一 LeaseManager，天然形成竞争；
 * 执行异常被归类为可重试错误进入恢复链路，已完成部分（已提交的业务键）不会重复执行。
 */
@Service
public class TaskScheduler {

    private final LeaseManager leaseManager;
    private final Duration leaseTtl;

    @Autowired
    public TaskScheduler(LeaseManager leaseManager) {
        this(leaseManager, Duration.ofSeconds(30));
    }

    public TaskScheduler(LeaseManager leaseManager, Duration leaseTtl) {
        this.leaseManager = leaseManager;
        this.leaseTtl = leaseTtl;
    }

    /**
     * 以 ownerId 身份尝试执行一次任务。
     * executor 仅在持有有效租约时被调用；其返回值作为结果按 businessKey 幂等提交。
     * executor 抛异常视为可重试失败（超时/中断/下游不可用），上报后进入退避。
     */
    public SubmitOutcome runOnce(String taskId, String ownerId, String businessKey,
                                 Function<String, String> executor) {
        LeaseDecision decision = leaseManager.acquire(taskId, ownerId, leaseTtl);
        if (!decision.acquired()) {
            return SubmitOutcome.rejected(decision.reason(), decision.detail());
        }
        Lease lease = decision.lease();
        String payload;
        try {
            payload = executor.apply(taskId);
        } catch (RuntimeException e) {
            leaseManager.reportFailure(taskId, ownerId, lease.version(),
                    ErrorKind.RETRYABLE, e.getClass().getSimpleName() + ": " + e.getMessage());
            return SubmitOutcome.rejected(RejectReason.EXECUTION_FAILED,
                    "execution failed, scheduled for retry: " + e.getMessage());
        }
        return leaseManager.submitResult(taskId, ownerId, lease.version(), businessKey, payload);
    }
}
