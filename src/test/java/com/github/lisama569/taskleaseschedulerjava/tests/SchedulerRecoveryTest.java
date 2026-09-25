package com.github.lisama569.taskleaseschedulerjava.tests;

import com.github.lisama569.taskleaseschedulerjava.lease.LeaseManager;
import com.github.lisama569.taskleaseschedulerjava.lease.TaskHandler;
import com.github.lisama569.taskleaseschedulerjava.lease.TaskScheduler;
import com.github.lisama569.taskleaseschedulerjava.lease.model.FailureKind;
import com.github.lisama569.taskleaseschedulerjava.lease.model.RetryPolicy;
import com.github.lisama569.taskleaseschedulerjava.lease.model.TaskFailureException;
import com.github.lisama569.taskleaseschedulerjava.lease.model.TaskStatus;
import com.github.lisama569.taskleaseschedulerjava.lease.store.CheckpointStore;
import com.github.lisama569.taskleaseschedulerjava.lease.store.LeaseStore;
import com.github.lisama569.taskleaseschedulerjava.lease.time.MutableClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SchedulerRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(SchedulerRecoveryTest.class);

    private final MutableClock clock =
            new MutableClock(Instant.parse("2026-09-25T00:00:00Z"), ZoneOffset.UTC);
    private final LeaseManager manager = new LeaseManager(
            new LeaseStore(), clock,
            new RetryPolicy(5, Duration.ofMillis(100), Duration.ofSeconds(5)));
    private final CheckpointStore checkpoints = new CheckpointStore();
    private final TaskScheduler scheduler = new TaskScheduler(
            manager, checkpoints, Duration.ofSeconds(30), Duration.ofMillis(200));

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    private void register(String taskId) {
        manager.registerTask(taskId, "biz-" + taskId);
    }

    @Test
    void successfulExecutionSubmitsResultOnce() {
        register("t-ok");
        AtomicInteger executions = new AtomicInteger();
        TaskHandler handler = (lease, cp) -> {
            executions.incrementAndGet();
            return "done";
        };
        assertEquals(1, scheduler.runOnce("worker-1", id -> handler));
        assertEquals(TaskStatus.SUCCEEDED, manager.view("t-ok").orElseThrow().status());
        // 终态后再次调度不再执行
        assertEquals(0, scheduler.runOnce("worker-1", id -> handler));
        assertEquals(1, executions.get(), "成功任务不得重复执行");
    }

    @Test
    void timeoutLeadsToRecoverableAndCompletedStepsAreNotReexecuted() {
        register("t-timeout");
        AtomicInteger step1Runs = new AtomicInteger();
        AtomicInteger attempt = new AtomicInteger();

        TaskHandler handler = (lease, cp) -> {
            int n = attempt.incrementAndGet();
            if (!cp.isDone("t-timeout", "step-1")) {
                step1Runs.incrementAndGet();
                cp.markDone("t-timeout", "step-1");
            }
            if (n == 1) {
                // 首次执行在 step-1 完成后卡住，触发执行超时
                Thread.sleep(10_000);
            }
            return "recovered";
        };

        // 第一轮：超时 -> 可恢复
        assertEquals(1, scheduler.runOnce("worker-1", id -> handler));
        var view = manager.view("t-timeout").orElseThrow();
        log.info("[超时恢复] status={} attempts={} failure={} 判定依据=执行超过executionTimeout被中断并记为RETRYABLE",
                view.status(), view.attemptCount(), view.lastFailure());
        assertEquals(TaskStatus.RECOVERABLE, view.status());
        assertTrue(view.lastFailure().contains("timeout"));

        // 越过退避后第二轮：恢复成功，step-1 不重复执行
        clock.advance(Duration.ofSeconds(10));
        assertEquals(1, scheduler.runOnce("worker-1", id -> handler));
        assertEquals(TaskStatus.SUCCEEDED, manager.view("t-timeout").orElseThrow().status());
        assertEquals(1, step1Runs.get(), "已完成步骤在恢复后不得重复执行");
        assertEquals(2, attempt.get());
    }

    @Test
    void nonRetryableHandlerErrorGoesToFailed() {
        register("t-bad");
        TaskHandler handler = (lease, cp) -> {
            throw new TaskFailureException(FailureKind.NON_RETRYABLE, "illegal payload");
        };
        scheduler.runOnce("worker-1", id -> handler);
        var view = manager.view("t-bad").orElseThrow();
        assertEquals(TaskStatus.FAILED, view.status());
        assertEquals(FailureKind.NON_RETRYABLE, view.lastFailureKind());
        assertFalse(scheduler.dueTasks().contains("t-bad"));
    }

    @Test
    void transientDownstreamErrorIsRetriedUntilSuccess() {
        register("t-flaky");
        AtomicInteger calls = new AtomicInteger();
        TaskHandler handler = (lease, cp) -> {
            if (calls.incrementAndGet() < 3) {
                throw new RuntimeException("downstream temporarily unavailable");
            }
            return "ok-after-retry";
        };
        Map<String, TaskHandler> handlers = Map.of("t-flaky", handler);

        scheduler.runOnce("worker-1", handlers::get);
        assertEquals(TaskStatus.RECOVERABLE, manager.view("t-flaky").orElseThrow().status());
        clock.advance(Duration.ofSeconds(10));
        scheduler.runOnce("worker-1", handlers::get);
        assertEquals(TaskStatus.RECOVERABLE, manager.view("t-flaky").orElseThrow().status());
        clock.advance(Duration.ofSeconds(10));
        scheduler.runOnce("worker-1", handlers::get);

        var view = manager.view("t-flaky").orElseThrow();
        log.info("[暂时性故障恢复] status={} attempts={} 判定依据=未分类异常一律RETRYABLE并在退避后重试",
                view.status(), view.attemptCount());
        assertEquals(TaskStatus.SUCCEEDED, view.status());
        assertEquals(3, calls.get());
    }
}
