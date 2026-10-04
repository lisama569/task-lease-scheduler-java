package com.github.lisama569.taskleaseschedulerjava.tests;

import com.github.lisama569.taskleaseschedulerjava.lease.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 执行时限与打断：超时/中断必须得出确定结论（按一次可恢复失败上报），
 * 任务尽快回到可重新调度状态，而不是干等租约自然到期。使用 MutableClock，不做真实等待。
 */
class ExecutionDeadlineTest {

    private static final Logger log = LoggerFactory.getLogger(ExecutionDeadlineTest.class);

    private MutableClock clock;
    private LeaseManager manager;
    private TaskScheduler scheduler;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-10-05T00:00:00Z"));
        manager = new LeaseManager(clock, new RetryPolicy(3, Duration.ofMillis(10), Duration.ofMillis(50)));
        // 租约 60s，执行时限 20s，硬兜底 1s；时钟可控，超时在检查点即时触发
        scheduler = new TaskScheduler(manager, clock, Duration.ofSeconds(60),
                Duration.ofSeconds(20), Duration.ofSeconds(1));
        manager.registerTask("t");
    }

    @Test
    void timedOutExecution_entersRetryWithoutWaitingForLeaseExpiry() {
        AtomicInteger entered = new AtomicInteger();
        SubmitOutcome out = scheduler.runSteps("t", "A", "bk-1", ctx -> {
            entered.incrementAndGet();
            clock.advance(Duration.ofSeconds(21)); // 执行途中超过执行时限（不是租约 TTL）
            ctx.checkpoint();                       // 下个检查点立即被确定性中止
            return "should-not-reach";
        }, r -> r);

        log.info("[execution-timeout] kind={} reason={} detail={}", out.kind(), out.reason(), out.detail());
        assertEquals(SubmitOutcome.Kind.REJECTED, out.kind());
        assertEquals(RejectReason.EXECUTION_TIMEOUT, out.reason(), "超时必须被归类为 EXECUTION_TIMEOUT");
        TaskSnapshot snap = manager.snapshot("t").orElseThrow();
        assertEquals(TaskStatus.RETRY_WAITING, snap.status(), "超时按可恢复失败处理，进入退避等待");
        assertEquals(ErrorKind.RETRYABLE, snap.lastErrorKind());
        assertEquals(1, entered.get());

        // 租约远未到期（TTL 60s，只过了 21s），任务也已经脱离旧持有者、等待重试
        clock.advance(Duration.ofMillis(10));
        assertTrue(manager.schedulable().stream().anyMatch(s -> s.taskId().equals("t")),
                "退避到期后任务应立即可重新调度，无需等租约到期");
    }

    @Test
    void timedOutOwner_submitAfterTimeout_isRejected_andNeverOverwrites() {
        SubmitOutcome out = scheduler.runSteps("t", "A", "bk-1", ctx -> {
            clock.advance(Duration.ofSeconds(21));
            ctx.checkpoint();
            return "x";
        }, r -> r);
        assertEquals(RejectReason.EXECUTION_TIMEOUT, out.reason());

        // 旧持有者拿着原版本回来提交：任务已不在其名下（在退避等待），必须拒绝
        SubmitOutcome late = manager.submitResult("t", "A", 1, "bk-late", "stale-success");
        log.info("[late-submit-after-timeout] kind={} reason={} detail={}",
                late.kind(), late.reason(), late.detail());
        assertEquals(SubmitOutcome.Kind.REJECTED, late.kind());
        assertNull(manager.snapshot("t").orElseThrow().result(), "超时持有者的迟到结果不得写入");
        assertNotEquals(TaskStatus.SUCCEEDED, manager.snapshot("t").orElseThrow().status());
    }

    @Test
    void interruptedExecution_reportedAsRetryable_neverAsSuccess() {
        SubmitOutcome out = scheduler.runSteps("t", "A", "bk-1", ctx -> {
            // 模拟执行线程在外部停机时被中断
            Thread.currentThread().interrupt();
            ctx.checkpoint();
            return "should-not-succeed";
        }, r -> r);

        log.info("[execution-interrupted] kind={} reason={} detail={}",
                out.kind(), out.reason(), out.detail());
        assertEquals(SubmitOutcome.Kind.REJECTED, out.kind());
        assertEquals(RejectReason.EXECUTION_INTERRUPTED, out.reason(),
                "被打断必须得出确定结论 EXECUTION_INTERRUPTED，绝不能当成功");
        assertEquals(TaskStatus.RETRY_WAITING, manager.snapshot("t").orElseThrow().status());
        assertNull(manager.snapshot("t").orElseThrow().result());
        assertFalse(Thread.currentThread().isInterrupted(), "检查点异常路径不应泄漏中断标记到测试线程");
    }

    @Test
    void timeoutThenRetry_completesWithinAttempts() {
        AtomicInteger rounds = new AtomicInteger();
        SubmitOutcome first = scheduler.runSteps("t", "A", "bk-1", ctx -> {
            rounds.incrementAndGet();
            clock.advance(Duration.ofSeconds(21));
            ctx.checkpoint();
            return "x";
        }, r -> r);
        assertEquals(RejectReason.EXECUTION_TIMEOUT, first.reason());

        clock.advance(Duration.ofMillis(10));
        SubmitOutcome second = scheduler.runSteps("t", "B", "bk-1", ctx -> {
            rounds.incrementAndGet();
            ctx.markStepDone("recover");
            return "recovered";
        }, r -> r);
        log.info("[retry-after-timeout] second={} rounds={} status={}",
                second.kind(), rounds.get(), manager.snapshot("t").orElseThrow().status());
        assertEquals(SubmitOutcome.Kind.ACCEPTED, second.kind());
        assertEquals(TaskStatus.SUCCEEDED, manager.snapshot("t").orElseThrow().status());
        assertEquals("recovered", manager.snapshot("t").orElseThrow().result());
    }
}
