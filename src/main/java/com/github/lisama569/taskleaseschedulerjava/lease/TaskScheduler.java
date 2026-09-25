package com.github.lisama569.taskleaseschedulerjava.lease;

import com.github.lisama569.taskleaseschedulerjava.lease.model.FailureKind;
import com.github.lisama569.taskleaseschedulerjava.lease.model.Lease;
import com.github.lisama569.taskleaseschedulerjava.lease.model.SubmitOutcome;
import com.github.lisama569.taskleaseschedulerjava.lease.model.TaskFailureException;
import com.github.lisama569.taskleaseschedulerjava.lease.store.CheckpointStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * 调度执行器：拉取到期任务 -> 获取租约 -> 限时执行 -> 幂等提交。
 * 超时/中断/下游不可用按可重试失败进入恢复链路；
 * 执行体通过 CheckpointStore 跳过已完成步骤，保证恢复时不重复执行。
 */
public class TaskScheduler {

    private static final Logger log = LoggerFactory.getLogger(TaskScheduler.class);

    private final LeaseManager leaseManager;
    private final CheckpointStore checkpoints;
    private final Duration leaseTtl;
    private final Duration executionTimeout;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public TaskScheduler(LeaseManager leaseManager, CheckpointStore checkpoints,
                         Duration leaseTtl, Duration executionTimeout) {
        this.leaseManager = leaseManager;
        this.checkpoints = checkpoints;
        this.leaseTtl = leaseTtl;
        this.executionTimeout = executionTimeout;
    }

    /** 执行一轮调度：对当前到期任务逐个尝试执行，返回本轮实际获得租约并执行的任务数。 */
    public int runOnce(String holderId, Function<String, TaskHandler> handlers) {
        int executed = 0;
        for (String taskId : dueTasks()) {
            var acquired = leaseManager.tryAcquire(taskId, holderId, leaseTtl);
            if (acquired.isEmpty()) {
                continue;
            }
            executed++;
            executeOne(acquired.get(), handlers.apply(taskId));
        }
        return executed;
    }

    private void executeOne(Lease lease, TaskHandler handler) {
        String taskId = lease.taskId();
        if (handler == null) {
            leaseManager.reportFailure(lease, FailureKind.NON_RETRYABLE, "no handler registered");
            return;
        }
        Future<String> future = executor.submit(() -> handler.execute(lease, checkpoints));
        try {
            String result = future.get(executionTimeout.toMillis(), TimeUnit.MILLISECONDS);
            String businessKey = leaseManager.businessKey(taskId).orElse(taskId);
            SubmitOutcome outcome = leaseManager.submitResult(lease, businessKey, result);
            log.info("submit task={} version={} accepted={} duplicate={} reject={}",
                    taskId, lease.version(), outcome.accepted(), outcome.duplicate(), outcome.rejectReason());
        } catch (TimeoutException e) {
            future.cancel(true);
            leaseManager.reportFailure(lease, FailureKind.RETRYABLE, "execution timeout after " + executionTimeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            leaseManager.reportFailure(lease, FailureKind.RETRYABLE, "execution interrupted");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            FailureKind kind = cause instanceof TaskFailureException tfe
                    ? tfe.kind() : FailureKind.RETRYABLE;
            leaseManager.reportFailure(lease, kind, String.valueOf(cause.getMessage()));
        }
    }

    public List<String> dueTasks() {
        return leaseManager.dueTasks();
    }

    public void shutdown() {
        executor.shutdownNow();
    }
}
