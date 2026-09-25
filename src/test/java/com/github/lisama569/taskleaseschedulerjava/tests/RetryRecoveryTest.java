package com.github.lisama569.taskleaseschedulerjava.tests;

import com.github.lisama569.taskleaseschedulerjava.lease.LeaseManager;
import com.github.lisama569.taskleaseschedulerjava.lease.model.FailureKind;
import com.github.lisama569.taskleaseschedulerjava.lease.model.Lease;
import com.github.lisama569.taskleaseschedulerjava.lease.model.RetryPolicy;
import com.github.lisama569.taskleaseschedulerjava.lease.model.TaskStatus;
import com.github.lisama569.taskleaseschedulerjava.lease.model.TaskView;
import com.github.lisama569.taskleaseschedulerjava.lease.store.LeaseStore;
import com.github.lisama569.taskleaseschedulerjava.lease.time.MutableClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

class RetryRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RetryRecoveryTest.class);

    // maxAttempts=3, base=100ms, max=5s
    private final MutableClock clock =
            new MutableClock(Instant.parse("2026-09-25T00:00:00Z"), ZoneOffset.UTC);
    private final LeaseManager manager = new LeaseManager(
            new LeaseStore(), clock,
            new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(5)));

    @BeforeEach
    void register() {
        manager.registerTask("task-1", "biz-1");
    }

    @Test
    void retryableFailureEntersRecoverableWithBackoff() {
        Lease lease = manager.tryAcquire("task-1", "h", Duration.ofSeconds(10)).orElseThrow();
        manager.reportFailure(lease, FailureKind.RETRYABLE, "downstream unavailable");

        TaskView view = manager.view("task-1").orElseThrow();
        log.info("[可重试失败] status={} attempts={} failure={} 判定依据=RETRYABLE且attempts<maxAttempts",
                view.status(), view.attemptCount(), view.lastFailure());
        assertEquals(TaskStatus.RECOVERABLE, view.status());
        assertEquals(1, view.attemptCount());
        assertEquals("downstream unavailable", view.lastFailure());
        assertEquals(FailureKind.RETRYABLE, view.lastFailureKind());

        // 退避窗口内不可调度
        assertFalse(manager.dueTasks().contains("task-1"));
        assertTrue(manager.tryAcquire("task-1", "h2", Duration.ofSeconds(10)).isEmpty());

        // 推进时钟越过退避（首次退避 100ms）后可重新调度
        clock.advance(Duration.ofMillis(101));
        assertTrue(manager.dueTasks().contains("task-1"));
        assertTrue(manager.tryAcquire("task-1", "h2", Duration.ofSeconds(10)).isPresent());
    }

    @Test
    void retriesExhaustedMovesToDeadAndNeverScheduledAgain() {
        for (int attempt = 1; attempt <= 3; attempt++) {
            Lease lease = manager.tryAcquire("task-1", "h", Duration.ofSeconds(10)).orElseThrow();
            manager.reportFailure(lease, FailureKind.RETRYABLE, "boom-" + attempt);
            TaskView view = manager.view("task-1").orElseThrow();
            log.info("[第{}次失败] status={} attempts={} backoffBoundaryHit={}",
                    attempt, view.status(), view.attemptCount(), attempt == 3);
            clock.advance(Duration.ofSeconds(10)); // 越过任意退避
        }
        TaskView view = manager.view("task-1").orElseThrow();
        assertEquals(TaskStatus.DEAD, view.status());
        assertEquals(3, view.attemptCount());
        assertFalse(manager.dueTasks().contains("task-1"), "DEAD 任务不再被调度");
        assertTrue(manager.tryAcquire("task-1", "h", Duration.ofSeconds(10)).isEmpty());
    }

    @Test
    void nonRetryableFailureGoesStraightToFailed() {
        Lease lease = manager.tryAcquire("task-1", "h", Duration.ofSeconds(10)).orElseThrow();
        manager.reportFailure(lease, FailureKind.NON_RETRYABLE, "bad payload");
        TaskView view = manager.view("task-1").orElseThrow();
        assertEquals(TaskStatus.FAILED, view.status());
        assertEquals(FailureKind.NON_RETRYABLE, view.lastFailureKind());
        assertFalse(manager.dueTasks().contains("task-1"));
    }

    @Test
    void cancelledTaskIsNotScheduledAndCannotBeAcquired() {
        manager.tryAcquire("task-1", "h", Duration.ofSeconds(10)).orElseThrow();
        assertTrue(manager.cancel("task-1"));
        TaskView view = manager.view("task-1").orElseThrow();
        assertEquals(TaskStatus.CANCELLED, view.status());
        assertNull(view.holderId());
        assertFalse(manager.dueTasks().contains("task-1"));
        assertTrue(manager.tryAcquire("task-1", "h2", Duration.ofSeconds(10)).isEmpty());
        assertFalse(manager.cancel("task-1"), "终态任务不可重复取消");
    }

    @Test
    void staleHolderFailureReportIsIgnored() {
        Lease old = manager.tryAcquire("task-1", "h1", Duration.ofSeconds(5)).orElseThrow();
        clock.advance(Duration.ofSeconds(6));
        manager.tryAcquire("task-1", "h2", Duration.ofSeconds(10)).orElseThrow();
        manager.reportFailure(old, FailureKind.RETRYABLE, "stale report");
        TaskView view = manager.view("task-1").orElseThrow();
        assertEquals(TaskStatus.LEASED, view.status(), "过期持有者的失败上报不得影响新持有者");
        assertEquals("h2", view.holderId());
    }

    @Test
    void statusQueryableByHolderRemainingAndFailure() {
        Lease lease = manager.tryAcquire("task-1", "holder-x", Duration.ofSeconds(30)).orElseThrow();
        clock.advance(Duration.ofSeconds(10));
        TaskView view = manager.view("task-1").orElseThrow();
        assertEquals("holder-x", view.holderId());
        assertEquals(lease.version(), view.leaseVersion());
        assertEquals(Duration.ofSeconds(20), view.leaseRemaining(), "租约剩余时间可查询");

        manager.reportFailure(lease, FailureKind.RETRYABLE, "timeout");
        view = manager.view("task-1").orElseThrow();
        assertNull(view.holderId());
        assertEquals("timeout", view.lastFailure());
        assertEquals(TaskStatus.RECOVERABLE, view.status());
    }
}
