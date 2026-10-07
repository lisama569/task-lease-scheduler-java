package com.github.lisama569.taskleaseschedulerjava.lease;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 调度器：抢占租约 -> 在受控线程中限时执行 -> 提交结果或上报失败。
 *
 * 执行时限与打断规则：
 * - 每次执行有明确时间上限（executionTimeout），超时即取消执行线程（中断），
 *   并把本次执行按可恢复失败落地，任务立刻进入退避重试，而不是干等租约到期。
 *   即使时间上限被设成与租约时长完全相等、执行恰好卡满整段上限（租约同时到期），
 *   结论也通过 fencing 校验（finishAttempt，不要求租约未过期）立即落地。
 * - 时限判定基于与 {@link LeaseManager} 相同的 {@link Clock}：测试可用可控时钟把时间
 *   精确拨到“时间上限 = 租约时长”的边界，无需真实等待或线程睡眠撑出超时。
 * - 调度线程被外部打断（停机）或执行期间收到中断信号时，绝不提交结果，
 *   同样按可恢复失败上报，结论确定。
 * - 超时/打断后旧持有者再提交会被拒绝，LEASE_EXPIRED / OWNERSHIP_SUPERSEDED /
 *   NOT_LEASE_HOLDER 三种结论稳定可区分（见 LeaseManager.checkHolder）。
 *
 * 步骤级续跑（可选）：runSteps 在执行前查询当前持有者的已完成步骤并跳过，
 * 每完成一步记录一次；调用方不关心中间进度时可继续只用 runOnce，无需改动。
 */
@Service
public class TaskScheduler {

    private final LeaseManager leaseManager;
    private final Duration leaseTtl;
    private final Duration defaultExecutionTimeout;
    private final ExecutorService executorService;
    private final Clock clock;
    /** 时钟驱动等待时的检查粒度（仅影响超时判定的及时性，不影响正确性）。 */
    private final long tickMillis;

    @Autowired
    public TaskScheduler(LeaseManager leaseManager) {
        this(leaseManager, Duration.ofSeconds(30));
    }

    public TaskScheduler(LeaseManager leaseManager, Duration leaseTtl) {
        this(leaseManager, leaseTtl, leaseTtl, defaultExecutor(), leaseManager.clock(), 5L);
    }

    public TaskScheduler(LeaseManager leaseManager, Duration leaseTtl,
                         Duration defaultExecutionTimeout, ExecutorService executorService) {
        this(leaseManager, leaseTtl, defaultExecutionTimeout, executorService,
                leaseManager.clock(), 5L);
    }

    public TaskScheduler(LeaseManager leaseManager, Duration leaseTtl,
                         Duration defaultExecutionTimeout, ExecutorService executorService,
                         Clock clock, long tickMillis) {
        this.leaseManager = leaseManager;
        this.leaseTtl = leaseTtl;
        this.defaultExecutionTimeout = defaultExecutionTimeout;
        this.executorService = executorService;
        this.clock = clock;
        this.tickMillis = Math.max(1L, tickMillis);
    }

    private static ExecutorService defaultExecutor() {
        return Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "task-executor");
            t.setDaemon(true);
            return t;
        });
    }

    @PreDestroy
    public void shutdown() {
        executorService.shutdownNow();
    }

    /**
     * 兼容旧调用：以默认执行时限（= 租约 TTL）执行一次。
     * 签名与语义保持不变，仅新增“执行不得无限期拖延”的上限保护。
     */
    public SubmitOutcome runOnce(String taskId, String ownerId, String businessKey,
                                 Function<String, String> executor) {
        return runOnce(taskId, ownerId, businessKey, defaultExecutionTimeout, executor);
    }

    /**
     * 以 ownerId 身份限时执行一次任务。
     * executor 仅在持有有效租约时被调用；其返回值作为结果按 businessKey 幂等提交。
     * 超时 / 中断 / 异常一律得出确定结论：上报可重试失败并拒绝本次提交，
     * 绝不会把没跑完的执行当成成功。
     */
    public SubmitOutcome runOnce(String taskId, String ownerId, String businessKey,
                                 Duration executionTimeout,
                                 Function<String, String> executor) {
        return runOnceWithLease(taskId, ownerId, businessKey, executionTimeout,
                (id, lease) -> executor.apply(id));
    }

    /**
     * 步骤级续跑（可选 API）：按顺序执行 steps，跳过当前持有者已记录完成的步骤，
     * 每完成一步即记录进度；重试/恢复时已完成步骤不会重复执行。
     * 进度归当前持有者所有：租约易主后新持有者从干净起点开始。
     */
    public SubmitOutcome runSteps(String taskId, String ownerId, String businessKey,
                                  Duration executionTimeout, List<TaskStep> steps) {
        return runOnceWithLease(taskId, ownerId, businessKey, executionTimeout, (id, lease) -> {
            ProgressView view = leaseManager.progress(id, ownerId, lease.version());
            if (!view.ok()) {
                throw new LeaseLostException(view.reason(), view.detail());
            }
            Set<String> done = new HashSet<>(view.completedSteps());
            int executed = 0;
            for (TaskStep step : steps) {
                if (done.contains(step.key())) {
                    continue; // 已完成步骤：重试/恢复时不再执行，避免重复副作用
                }
                String note = step.action().get();
                LeaseDecision rec = leaseManager.recordStep(id, ownerId, lease.version(),
                        step.key(), note);
                if (!rec.acquired()) {
                    throw new LeaseLostException(rec.reason(), rec.detail());
                }
                executed++;
            }
            return "steps=" + steps.size() + ",executedThisRun=" + executed;
        });
    }

    /** 内部入口：executor 可拿到当前租约（含 fencing token），用于步骤级进度。 */
    private SubmitOutcome runOnceWithLease(String taskId, String ownerId, String businessKey,
                                           Duration executionTimeout,
                                           BiFunction<String, Lease, String> executor) {
        LeaseDecision decision = leaseManager.acquire(taskId, ownerId, leaseTtl);
        if (!decision.acquired()) {
            return SubmitOutcome.rejected(decision.reason(), decision.detail(),
                    decision.leaseVersion());
        }
        Lease lease = decision.lease();
        Future<String> future = executorService.submit(() -> executor.apply(taskId, lease));
        String payload;
        try {
            payload = awaitWithin(future, executionTimeout);
        } catch (ExecutionTimedOutException e) {
            return onAttemptEnded(taskId, ownerId, lease, future,
                    ErrorKind.RETRYABLE,
                    "execution-timeout after " + executionTimeout.toMillis() + "ms",
                    RejectReason.EXECUTION_TIMEOUT,
                    "execution exceeded " + executionTimeout.toMillis()
                            + "ms, cancelled and scheduled for retry");
        } catch (InterruptedException e) {
            return onAttemptEnded(taskId, ownerId, lease, future,
                    ErrorKind.RETRYABLE, "scheduler-interrupted",
                    RejectReason.EXECUTION_INTERRUPTED,
                    "interrupted while executing, result not submitted");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof LeaseLostException lost) {
                // 执行途中失去租约：结论已确定（拒绝），无需重复上报。
                return SubmitOutcome.rejected(lost.reason(),
                        "lease lost during execution: " + lost.getMessage(),
                        lease.version());
            }
            return onAttemptEnded(taskId, ownerId, lease, future,
                    ErrorKind.RETRYABLE,
                    cause.getClass().getSimpleName() + ": " + cause.getMessage(),
                    RejectReason.EXECUTION_FAILED,
                    "execution failed, scheduled for retry: " + cause.getMessage());
        }
        if (Thread.currentThread().isInterrupted()) {
            // 执行期间收到停机/打断信号：结果可能不完整，不得当作成功提交。
            return onAttemptEnded(taskId, ownerId, lease, future,
                    ErrorKind.RETRYABLE, "interrupted-before-submit",
                    RejectReason.EXECUTION_INTERRUPTED,
                    "interrupt observed before submit, result discarded");
        }
        return leaseManager.submitResult(taskId, ownerId, lease.version(), businessKey, payload);
    }

    /**
     * 取消执行线程并通过 finishAttempt 落地本次执行的失败结论。
     * finishAttempt 只做 fencing 校验、不要求租约未过期：即使执行时间上限 == 租约时长、
     * 执行卡满整段（租约同时到期），失败原因也会立即记录、任务立刻进入重试，
     * 而不是停留在 LEASED 等待被动回收。若租约已被新持有者接手，则透传管理器的
     * 精确拒绝原因（旧执行者无法写坏新持有者状态）。
     */
    private SubmitOutcome onAttemptEnded(String taskId, String ownerId, Lease lease,
                                         Future<String> future, ErrorKind kind,
                                         String failureReason, RejectReason localReason,
                                         String localDetail) {
        future.cancel(true);
        if (localReason == RejectReason.EXECUTION_INTERRUPTED) {
            Thread.currentThread().interrupt();
        }
        LeaseDecision ended = leaseManager.finishAttempt(taskId, ownerId, lease.version(),
                kind, failureReason);
        if (!ended.acquired()) {
            return SubmitOutcome.rejected(ended.reason(), ended.detail(), ended.leaseVersion());
        }
        return SubmitOutcome.rejected(localReason, localDetail, lease.version());
    }

    /**
     * 以受控时钟为基准等待执行结果，超过 executionTimeout 即判定超时。
     * 截止时间来自 {@link #clock}（与租约到期判定同源）：测试把时钟拨到边界即可
     * 确定性地制造“超时且租约同时到期”，无需真实等待；生产环境 SystemClock 即墙钟。
     * 等待以 tickMillis 粒度轮询 Future（可中断），既不占用 CPU，也能及时返回。
     */
    private String awaitWithin(Future<String> future, Duration executionTimeout)
            throws InterruptedException, ExecutionException, ExecutionTimedOutException {
        Instant deadline = clock.now().plus(executionTimeout);
        while (true) {
            if (!clock.now().isBefore(deadline)) {
                throw new ExecutionTimedOutException();
            }
            long remaining = Duration.between(clock.now(), deadline).toMillis();
            try {
                // 最多阻塞一个 tick；时钟在其它线程（测试）被拨快时下次循环即见分晓。
                return future.get(Math.min(tickMillis, remaining), TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.TimeoutException tick) {
                // 单 tick 未完成：回到循环按受控时钟重新判定是否超时。
            }
        }
    }

    /** 时钟判定的执行超时（区别于 Future 单 tick 的临时 TimeoutException）。 */
    private static final class ExecutionTimedOutException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    /** 执行途中发现租约已不属于自己（被回收/被取代/已过期）。 */
    private static final class LeaseLostException extends RuntimeException {
        private final RejectReason reason;

        LeaseLostException(RejectReason reason, String detail) {
            super(detail);
            this.reason = reason;
        }

        RejectReason reason() {
            return reason;
        }
    }
}
