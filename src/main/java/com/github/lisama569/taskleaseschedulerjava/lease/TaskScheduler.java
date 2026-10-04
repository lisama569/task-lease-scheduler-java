package com.github.lisama569.taskleaseschedulerjava.lease;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * 调度器：抢占租约 -> 在明确时限内执行 -> 提交结果或上报失败。
 *
 * <p>执行语义：</p>
 * <ul>
 *   <li>每次执行有明确的 {@code executionTimeout} 上限。执行器通过
 *       {@link ExecutionContext#checkpoint()} 协作检查时限；超时后本次执行被中止，
 *       原持有者按一次可恢复失败上报，任务尽快回到可重新调度状态，而不是干等租约到期；</li>
 *   <li>执行线程被中断（实例停机等）同样得出确定结论：按可恢复失败上报，绝不把未完成的
 *       执行当成功提交；</li>
 *   <li>步骤级续跑是可选能力（{@link #runSteps}）：已完成步骤跨重试跳过；进度跟租约走，
 *       旧持有者过期/被接管后其进度由 LeaseManager 清空，新持有者拿到干净起点。
 *       不关心进度的任务继续使用原 {@link #runOnce}，调用方式不变。</li>
 * </ul>
 */
@Service
public class TaskScheduler {

    private static final Logger log = LoggerFactory.getLogger(TaskScheduler.class);

    private final LeaseManager leaseManager;
    private final Clock clock;
    private final Duration leaseTtl;
    private final Duration executionTimeout;
    /** 软时限（时钟判定）之外的硬性兜底，防止不响应检查点的执行器永久挂住。 */
    private final Duration hardTimeoutSlack;
    private final ExecutorService worker;
    private final AtomicLong executionSeq = new AtomicLong();

    @Autowired
    public TaskScheduler(LeaseManager leaseManager) {
        this(leaseManager, new SystemClock(), Duration.ofSeconds(30), Duration.ofSeconds(25),
                Duration.ofSeconds(2));
    }

    public TaskScheduler(LeaseManager leaseManager, Duration leaseTtl) {
        this(leaseManager, new SystemClock(), leaseTtl,
                leaseTtl.minusSeconds(5).isNegative() ? leaseTtl : leaseTtl.minusSeconds(5),
                Duration.ofSeconds(2));
    }

    public TaskScheduler(LeaseManager leaseManager, Clock clock, Duration leaseTtl,
                         Duration executionTimeout, Duration hardTimeoutSlack) {
        this.leaseManager = leaseManager;
        this.clock = clock;
        this.leaseTtl = leaseTtl;
        this.executionTimeout = executionTimeout;
        this.hardTimeoutSlack = hardTimeoutSlack;
        this.worker = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "task-exec-" + executionSeq.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 原有调用方式（完全兼容）：以 ownerId 抢占并执行，结果按 businessKey 幂等提交。
     * 执行同样受 executionTimeout 约束，超时/中断按可恢复失败上报。
     */
    public SubmitOutcome runOnce(String taskId, String ownerId, String businessKey,
                                 Function<String, String> executor) {
        LeaseDecision decision = leaseManager.acquire(taskId, ownerId, leaseTtl);
        if (!decision.acquired()) {
            return SubmitOutcome.rejected(decision.reason(), decision.detail());
        }
        Lease lease = decision.lease();
        Instant deadline = clock.now().plus(executionTimeout);
        String payload;
        try {
            payload = runWithDeadline(taskId, lease, deadline,
                    ctx -> executor.apply(taskId));
        } catch (ExecutionTimeoutException e) {
            return reportExecutionEnd(taskId, lease, ErrorKind.RETRYABLE,
                    RejectReason.EXECUTION_TIMEOUT, "execution timed out after "
                            + executionTimeout + ": " + e.getMessage(), e);
        } catch (ExecutionInterruptedException e) {
            return reportExecutionEnd(taskId, lease, ErrorKind.RETRYABLE,
                    RejectReason.EXECUTION_INTERRUPTED, "execution interrupted: " + e.getMessage(), e);
        } catch (RuntimeException e) {
            return reportExecutionEnd(taskId, lease, ErrorKind.RETRYABLE,
                    RejectReason.EXECUTION_FAILED,
                    "execution failed, scheduled for retry: " + e.getMessage(), e);
        }
        return leaseManager.submitResult(taskId, ownerId, lease.version(), businessKey, payload);
    }

    /**
     * 可选的步骤级续跑执行：已在当前重试血缘完成的步骤不会重复执行。
     * executor 通过 ctx.isStepDone/markStepDone/checkpoint 与调度器协作。
     */
    public <T> SubmitOutcome runSteps(String taskId, String ownerId, String businessKey,
                                      StepExecutor<T> executor,
                                      Function<T, String> resultMapper) {
        LeaseDecision decision = leaseManager.acquire(taskId, ownerId, leaseTtl);
        if (!decision.acquired()) {
            return SubmitOutcome.rejected(decision.reason(), decision.detail());
        }
        Lease lease = decision.lease();
        Instant deadline = clock.now().plus(executionTimeout);
        T result;
        try {
            result = runWithDeadline(taskId, lease, deadline, executor::execute);
        } catch (ExecutionTimeoutException e) {
            return reportExecutionEnd(taskId, lease, ErrorKind.RETRYABLE,
                    RejectReason.EXECUTION_TIMEOUT, "execution timed out after "
                            + executionTimeout + ": " + e.getMessage(), e);
        } catch (ExecutionInterruptedException e) {
            return reportExecutionEnd(taskId, lease, ErrorKind.RETRYABLE,
                    RejectReason.EXECUTION_INTERRUPTED, "execution interrupted: " + e.getMessage(), e);
        } catch (RuntimeException e) {
            return reportExecutionEnd(taskId, lease, ErrorKind.RETRYABLE,
                    RejectReason.EXECUTION_FAILED,
                    "execution failed, scheduled for retry: " + e.getMessage(), e);
        }
        String payload = resultMapper.apply(result);
        return leaseManager.submitResult(taskId, ownerId, lease.version(), businessKey, payload);
    }

    /**
     * 在独立 worker 线程运行执行器，主线程按可控时钟检查软时限、按 wall-clock 硬超时兜底，
     * 并把执行线程被中断（实例停机）转成确定的 {@link ExecutionInterruptedException}。
     */
    private <T> T runWithDeadline(String taskId, Lease lease, Instant deadline,
                                  ThrowingSupplier<T> body) {
        DefaultExecutionContext ctx = new DefaultExecutionContext(lease, deadline);
        Future<T> future = worker.submit(() -> body.getWithCtx(ctx));

        long hardNanos = executionTimeout.plus(hardTimeoutSlack).toNanos();
        long waitNanos = Math.min(50_000_000L, hardNanos);
        long waitedNanos = 0L;
        try {
            while (true) {
                try {
                    return future.get(waitNanos, TimeUnit.NANOSECONDS);
                } catch (TimeoutException te) {
                    waitedNanos += waitNanos;
                    // 软时限：以注入时间源为准，测试中推进 MutableClock 即可立即触发
                    if (clock.now().isAfter(deadline) || clock.now().equals(deadline)) {
                        future.cancel(true);
                        throw new ExecutionTimeoutException("task=" + taskId
                                + " owner=" + lease.ownerId() + " version=" + lease.version()
                                + " deadline=" + deadline);
                    }
                    if (waitedNanos >= hardNanos) {
                        future.cancel(true);
                        throw new ExecutionTimeoutException("hard deadline breached, task=" + taskId
                                + " owner=" + lease.ownerId() + " version=" + lease.version());
                    }
                    waitNanos = Math.min(waitNanos, hardNanos - waitedNanos);
                }
            }
        } catch (InterruptedException ie) {
            // 调度线程（承载服务停机信号）被中断：打断 worker，失败是唯一确定结论
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new ExecutionInterruptedException("scheduler interrupted while executing task="
                    + taskId + " owner=" + lease.ownerId() + " version=" + lease.version(), ie);
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof ExecutionTimeoutException t) {
                throw t;
            }
            if (cause instanceof ExecutionInterruptedException i) {
                throw i;
            }
            if (cause instanceof InterruptedException) {
                future.cancel(true);
                throw new ExecutionInterruptedException("worker interrupted while executing task="
                        + taskId + " owner=" + lease.ownerId() + " version=" + lease.version());
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw new RuntimeException(cause);
        }
    }

    /** 执行异常结束：上报一次失败；若租约已失效则以上报拒绝原因为准（旧持有者盖不掉新持有者）。 */
    private SubmitOutcome reportExecutionEnd(String taskId, Lease lease, ErrorKind kind,
                                             RejectReason outcomeReason, String reason,
                                             Exception e) {
        log.warn("[execution-end] task={} owner={} version={} kind={} outcome={} reason={}",
                taskId, lease.ownerId(), lease.version(), kind, outcomeReason, reason);
        LeaseDecision reported = leaseManager.reportFailure(
                taskId, lease.ownerId(), lease.version(), kind, outcomeReason.name() + ": " + reason);
        if (!reported.acquired()) {
            return SubmitOutcome.rejected(reported.reason(),
                    outcomeReason + " then failure-report rejected: " + reported.detail());
        }
        return SubmitOutcome.rejected(outcomeReason, reason + " (" + e.getClass().getSimpleName() + ")");
    }

    @PreDestroy
    public void shutdown() {
        worker.shutdownNow();
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T getWithCtx(ExecutionContext ctx) throws Exception;
    }

    /** 执行上下文实现：检查点把超时/中断转成确定性异常。 */
    private final class DefaultExecutionContext implements ExecutionContext {
        private final Lease lease;
        private final Instant deadline;

        DefaultExecutionContext(Lease lease, Instant deadline) {
            this.lease = lease;
            this.deadline = deadline;
        }

        @Override public String taskId() { return lease.taskId(); }
        @Override public String ownerId() { return lease.ownerId(); }
        @Override public long version() { return lease.version(); }
        @Override public Instant deadline() { return deadline; }

        @Override
        public boolean isTimedOut() {
            Instant now = clock.now();
            return !now.isBefore(deadline);
        }

        @Override
        public boolean isInterrupted() {
            return Thread.currentThread().isInterrupted();
        }

        @Override
        public void checkpoint() {
            if (Thread.currentThread().isInterrupted()) {
                throw new ExecutionInterruptedException("interrupted at checkpoint, task="
                        + taskId() + " version=" + version());
            }
            if (isTimedOut()) {
                throw new ExecutionTimeoutException("checkpoint after deadline=" + deadline
                        + ", task=" + taskId() + " version=" + version());
            }
        }

        @Override
        public boolean isStepDone(String step) {
            return leaseManager.isStepDone(taskId(), step);
        }

        @Override
        public void markStepDone(String step) {
            LeaseDecision d = leaseManager.markStepDone(taskId(), ownerId(), version(), step);
            if (!d.acquired()) {
                // 进度写入都被拒，说明所有权已失效：立即得出确定结论，禁止继续执行/提交
                throw new OwnershipLostException(d.reason(), d.detail());
            }
        }
    }
}
