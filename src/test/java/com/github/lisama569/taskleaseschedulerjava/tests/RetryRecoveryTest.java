package com.github.lisama569.taskleaseschedulerjava.tests;

import com.github.lisama569.taskleaseschedulerjava.lease.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** 失败恢复链路：可重试/不可重试分类、次数与退避上限、取消后不再调度、状态查询。 */
class RetryRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RetryRecoveryTest.class);

    private MutableClock clock;
    private LeaseManager manager;
    private final RetryPolicy policy = new RetryPolicy(3, Duration.ofMillis(100), Duration.ofMillis(250));

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-09-25T00:00:00Z"));
        manager = new LeaseManager(clock, policy);
        manager.registerTask("t");
    }

    @Test
    void retryableFailure_entersRetryWaiting_withBackoff() {
        Lease lease = manager.acquire("t", "A", Duration.ofSeconds(5)).lease();
        manager.reportFailure("t", "A", lease.version(), ErrorKind.RETRYABLE, "downstream-timeout");

        TaskSnapshot snap = manager.snapshot("t").orElseThrow();
        log.info("[retry-waiting] status={} attempts={} nextRetryAt={} reason={}",
                snap.status(), snap.attempts(), snap.nextRetryAt(), snap.lastFailureReason());
        assertEquals(TaskStatus.RETRY_WAITING, snap.status());
        assertEquals(ErrorKind.RETRYABLE, snap.lastErrorKind());
        assertEquals("downstream-timeout", snap.lastFailureReason());
        assertEquals(clock.now().plusMillis(100), snap.nextRetryAt(), "首次退避应为 baseBackoff");

        // 退避未到期不可调度
        assertTrue(manager.schedulable().isEmpty());
        LeaseDecision early = manager.acquire("t", "B", Duration.ofSeconds(5));
        assertEquals(RejectReason.RETRY_BACKOFF_NOT_ELAPSED, early.reason());

        // 退避到期后恢复调度，已完成部分（attempts）不重复
        clock.advance(Duration.ofMillis(100));
        assertEquals(1, manager.schedulable().size());
        LeaseDecision retry = manager.acquire("t", "B", Duration.ofSeconds(5));
        assertTrue(retry.acquired());
        assertEquals(2, manager.snapshot("t").orElseThrow().attempts());
    }

    @Test
    void backoffIsExponentialAndCapped() {
        assertEquals(Duration.ofMillis(100), policy.backoffFor(1));
        assertEquals(Duration.ofMillis(200), policy.backoffFor(2));
        assertEquals(Duration.ofMillis(250), policy.backoffFor(3), "退避必须封顶 maxBackoff");
        assertEquals(Duration.ofMillis(250), policy.backoffFor(10));
    }

    @Test
    void exceedingMaxAttempts_goesDead_andNeverSchedulable() {
        for (int i = 1; i <= 3; i++) {
            // 等待退避到期再抢占
            clock.advance(Duration.ofMillis(300));
            LeaseDecision d = manager.acquire("t", "A", Duration.ofSeconds(5));
            assertTrue(d.acquired(), "第 " + i + " 次尝试应能获取租约");
            LeaseDecision f = manager.reportFailure("t", "A", d.lease().version(),
                    ErrorKind.RETRYABLE, "boom-" + i);
            log.info("[attempt-{}] failureAccepted={} status={}", i, f.acquired(),
                    manager.snapshot("t").orElseThrow().status());
        }
        TaskSnapshot snap = manager.snapshot("t").orElseThrow();
        assertEquals(TaskStatus.DEAD, snap.status(), "超过重试上限必须进入 DEAD 终态");
        assertTrue(manager.schedulable().isEmpty());
        assertEquals(RejectReason.TASK_FINISHED,
                manager.acquire("t", "B", Duration.ofSeconds(5)).reason());
    }

    @Test
    void nonRetryableFailure_goesDeadImmediately() {
        Lease lease = manager.acquire("t", "A", Duration.ofSeconds(5)).lease();
        manager.reportFailure("t", "A", lease.version(), ErrorKind.NON_RETRYABLE, "invalid-payload");
        TaskSnapshot snap = manager.snapshot("t").orElseThrow();
        assertEquals(TaskStatus.DEAD, snap.status());
        assertEquals(ErrorKind.NON_RETRYABLE, snap.lastErrorKind());
        assertTrue(manager.schedulable().isEmpty());
    }

    @Test
    void cancelledTask_notSchedulable_andOperationsRejected() {
        manager.acquire("t", "A", Duration.ofSeconds(5));
        assertTrue(manager.cancel("t"));
        assertEquals(TaskStatus.CANCELLED, manager.snapshot("t").orElseThrow().status());
        assertTrue(manager.schedulable().isEmpty());
        assertEquals(RejectReason.TASK_FINISHED,
                manager.acquire("t", "B", Duration.ofSeconds(5)).reason());
        assertFalse(manager.cancel("t"), "已终态任务不能重复取消");
    }

    @Test
    void scheduler_retriesAfterTransientFailure_thenSucceeds() {
        TaskScheduler scheduler = new TaskScheduler(manager, Duration.ofSeconds(5));
        AtomicInteger executions = new AtomicInteger();
        // 第一次执行下游不可用，第二次恢复
        SubmitOutcome first = scheduler.runOnce("t", "inst-1", "bk-1", id -> {
            executions.incrementAndGet();
            throw new IllegalStateException("downstream unavailable");
        });
        assertEquals(RejectReason.EXECUTION_FAILED, first.reason());
        assertEquals(TaskStatus.RETRY_WAITING, manager.snapshot("t").orElseThrow().status());

        clock.advance(Duration.ofMillis(100));
        SubmitOutcome second = scheduler.runOnce("t", "inst-2", "bk-1", id -> {
            executions.incrementAndGet();
            return "ok";
        });
        assertEquals(SubmitOutcome.Kind.ACCEPTED, second.kind());
        assertEquals(2, executions.get(), "失败尝试 + 重试各执行一次，已完成部分不重复");
        assertEquals(TaskStatus.SUCCEEDED, manager.snapshot("t").orElseThrow().status());
    }

    @Test
    void queryByOwnerLeaseRemainingAndFailureReason() {
        manager.registerTask("t2");
        Lease l1 = manager.acquire("t", "A", Duration.ofSeconds(5)).lease();
        manager.acquire("t2", "B", Duration.ofSeconds(50));

        List<TaskSnapshot> byOwner = manager.byOwner("A");
        assertEquals(1, byOwner.size());
        assertEquals("t", byOwner.get(0).taskId());

        List<TaskSnapshot> soonExpiring = manager.byLeaseRemainingWithin(10_000);
        log.info("[query] soonExpiring={} versions={}", soonExpiring.size(),
                soonExpiring.stream().map(TaskSnapshot::leaseVersion).toList());
        assertEquals(1, soonExpiring.size());
        assertEquals(l1.version(), soonExpiring.get(0).leaseVersion());

        manager.reportFailure("t", "A", l1.version(), ErrorKind.RETRYABLE, "net-flap");
        List<TaskSnapshot> byReason = manager.byFailureReason("net-flap");
        assertEquals(1, byReason.size());
        assertEquals("t", byReason.get(0).taskId());
        assertEquals(TaskStatus.RETRY_WAITING, byReason.get(0).status());
    }
}
