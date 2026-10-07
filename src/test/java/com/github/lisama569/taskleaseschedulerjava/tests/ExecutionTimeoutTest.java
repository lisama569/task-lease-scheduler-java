package com.github.lisama569.taskleaseschedulerjava.tests;

import com.github.lisama569.taskleaseschedulerjava.lease.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 执行时限与打断：超时/中断必须得出确定结论（可恢复失败），任务快速回到可调度状态；
 * 超时后旧持有者的提交被可区分地拒绝；恢复时已完成的步骤不重复执行。
 * 租约/退避时间由 MutableClock 控制，执行超时为极短的真实等待（百毫秒级）。
 */
class ExecutionTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(ExecutionTimeoutTest.class);

    private static final Duration LEASE_TTL = Duration.ofSeconds(30);
    private static final Duration EXEC_TIMEOUT = Duration.ofMillis(150);

    private MutableClock clock;
    private LeaseManager manager;
    private TaskScheduler scheduler;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-09-25T00:00:00Z"));
        manager = new LeaseManager(clock, RetryPolicy.defaults());
        manager.registerTask("t");
        scheduler = new TaskScheduler(manager, LEASE_TTL);
    }

    /** 可中断的阻塞步骤：等待 unblock 置位；被中断（超时取消）时抛出异常退出。 */
    private static String interruptibleWait(AtomicBoolean unblock) {
        long deadline = System.currentTimeMillis() + 10_000;
        try {
            while (!unblock.get() && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("step interrupted");
        }
        return "step-done";
    }

    @Test
    void executionTimeout_recoverableFailure_andQuickRecovery() {
        AtomicBoolean unblock = new AtomicBoolean(false);
        SubmitOutcome timedOut = scheduler.runOnce("t", "inst-1", "bk-1", EXEC_TIMEOUT,
                id -> interruptibleWait(unblock));
        log.info("[timeout] kind={} reason={} leaseVersion={} detail={}",
                timedOut.kind(), timedOut.reason(), timedOut.leaseVersion(), timedOut.detail());

        assertEquals(SubmitOutcome.Kind.REJECTED, timedOut.kind());
        assertEquals(RejectReason.EXECUTION_TIMEOUT, timedOut.reason());

        // 确定结论：可恢复失败，任务立刻进入退避重试，而不是干等租约到期
        TaskSnapshot snap = manager.snapshot("t").orElseThrow();
        log.info("[after-timeout] status={} attempts={} lastError={} nextRetryAt={}",
                snap.status(), snap.attempts(), snap.lastErrorKind(), snap.nextRetryAt());
        assertEquals(TaskStatus.RETRY_WAITING, snap.status());
        assertEquals(ErrorKind.RETRYABLE, snap.lastErrorKind());
        assertTrue(snap.lastFailureReason().contains("execution-timeout"));
        assertNull(snap.result(), "超时的执行不得留下结果");

        // 退避到期后恢复执行并成功
        clock.advance(Duration.ofMillis(100));
        SubmitOutcome recovered = scheduler.runOnce("t", "inst-1", "bk-1", EXEC_TIMEOUT,
                id -> "ok");
        log.info("[recovered] kind={} leaseVersion={}", recovered.kind(), recovered.leaseVersion());
        assertEquals(SubmitOutcome.Kind.ACCEPTED, recovered.kind());
        assertEquals(TaskStatus.SUCCEEDED, manager.snapshot("t").orElseThrow().status());
    }

    @Test
    void timeoutRecovery_completedStepsNotReexecuted() {
        AtomicBoolean unblockStep2 = new AtomicBoolean(false);
        AtomicInteger step1Runs = new AtomicInteger();
        AtomicInteger step2Runs = new AtomicInteger();
        AtomicInteger step3Runs = new AtomicInteger();
        List<TaskStep> steps = List.of(
                new TaskStep("extract", () -> {
                    step1Runs.incrementAndGet();
                    return "rows=100";
                }),
                new TaskStep("transform", () -> {
                    step2Runs.incrementAndGet();
                    return interruptibleWait(unblockStep2);
                }),
                new TaskStep("load", () -> {
                    step3Runs.incrementAndGet();
                    return "loaded";
                }));

        // 第一次执行：step1 完成并记录，step2 卡住直到超时被取消
        SubmitOutcome first = scheduler.runSteps("t", "inst-1", "bk-1", EXEC_TIMEOUT, steps);
        log.info("[steps-timeout] kind={} reason={} detail={}", first.kind(), first.reason(),
                first.detail());
        assertEquals(RejectReason.EXECUTION_TIMEOUT, first.reason());
        assertEquals(1, step1Runs.get());
        assertEquals(1, step2Runs.get(), "step2 被执行过一次但未能完成");
        assertEquals(0, step3Runs.get());

        // 进度可查询：同一持有者恢复租约后能看到自己已完成的步骤
        clock.advance(Duration.ofMillis(100));
        LeaseDecision re = manager.acquire("t", "inst-1", LEASE_TTL);
        assertTrue(re.acquired());
        ProgressView view = manager.progress("t", "inst-1", re.lease().version());
        log.info("[resume-progress] version={} completedSteps={}",
                view.leaseVersion(), view.completedSteps());
        assertEquals(List.of("extract"), view.completedSteps());
        assertTrue(manager.release("t", "inst-1", re.lease().version()).acquired());

        // 恢复执行：step1 不重复执行，只跑未完成的部分
        unblockStep2.set(true);
        SubmitOutcome second = scheduler.runSteps("t", "inst-1", "bk-1", EXEC_TIMEOUT, steps);
        log.info("[steps-recovered] kind={} leaseVersion={} step1Runs={} step2Runs={} step3Runs={}",
                second.kind(), second.leaseVersion(), step1Runs.get(), step2Runs.get(),
                step3Runs.get());
        assertEquals(SubmitOutcome.Kind.ACCEPTED, second.kind());
        assertEquals(1, step1Runs.get(), "已完成步骤在重试时不得重复执行");
        assertEquals(2, step2Runs.get(), "未完成步骤需要重跑");
        assertEquals(1, step3Runs.get());
        assertEquals(TaskStatus.SUCCEEDED, manager.snapshot("t").orElseThrow().status());
    }

    @Test
    void oldHolderSubmitRejected_afterTimeout_distinguishableReasons() {
        AtomicBoolean unblock = new AtomicBoolean(false);
        SubmitOutcome timedOut = scheduler.runOnce("t", "A", "bk-1", EXEC_TIMEOUT,
                id -> interruptibleWait(unblock));
        assertEquals(RejectReason.EXECUTION_TIMEOUT, timedOut.reason());

        // 1) 超时后租约已随失败上报结束、尚无人接手：旧持有者曾持有该版本 -> 租约已失效
        SubmitOutcome late1 = manager.submitResult("t", "A", 1, "bk-late", "stale");
        log.info("[late-submit-1] reason={} currentVersion={} detail={}",
                late1.reason(), late1.leaseVersion(), late1.detail());
        assertEquals(RejectReason.LEASE_EXPIRED, late1.reason());

        // 1b) 从未持有过租约的执行者：与“曾持有但失效”可区分
        SubmitOutcome stranger = manager.submitResult("t", "stranger", 1, "bk-late", "stale");
        log.info("[stranger-submit] reason={} detail={}", stranger.reason(), stranger.detail());
        assertEquals(RejectReason.NOT_LEASE_HOLDER, stranger.reason());

        // 2) 任务被别人接管：旧持有者被识别为“所有权已被取代”，与 NOT_LEASE_HOLDER 区分
        clock.advance(Duration.ofMillis(100));
        Lease vB = manager.acquire("t", "B", LEASE_TTL).lease();
        SubmitOutcome late2 = manager.submitResult("t", "A", 1, "bk-late", "stale");
        log.info("[late-submit-2] reason={} currentVersion={} detail={}",
                late2.reason(), late2.leaseVersion(), late2.detail());
        assertEquals(RejectReason.OWNERSHIP_SUPERSEDED, late2.reason());
        assertEquals(vB.version(), late2.leaseVersion(), "判定必须带上当前租约版本便于核对");

        // 2b) 此时一个从未持有过的执行者仍是 NOT_LEASE_HOLDER，两种拒绝不混淆
        SubmitOutcome stranger2 = manager.submitResult("t", "stranger", 1, "bk-late", "stale");
        assertEquals(RejectReason.NOT_LEASE_HOLDER, stranger2.reason());

        // 3) 旧持有者重新抢到租约（版本+1）：旧版本提交被 fencing 判定取代
        assertTrue(manager.release("t", "B", vB.version()).acquired());
        Lease vA2 = manager.acquire("t", "A", LEASE_TTL).lease();
        SubmitOutcome late3 = manager.submitResult("t", "A", 1, "bk-late", "stale");
        log.info("[late-submit-3] reason={} staleVersion=1 currentVersion={} detail={}",
                late3.reason(), vA2.version(), late3.detail());
        assertEquals(RejectReason.OWNERSHIP_SUPERSEDED, late3.reason());

        // 4) 租约自然过期：过期持有者提交被识别为 LEASE_EXPIRED
        clock.advance(LEASE_TTL.plusSeconds(1));
        SubmitOutcome late4 = manager.submitResult("t", "A", vA2.version(), "bk-late", "stale");
        log.info("[late-submit-4] reason={} version={} detail={}",
                late4.reason(), late4.leaseVersion(), late4.detail());
        assertEquals(RejectReason.LEASE_EXPIRED, late4.reason());

        // 4b) 租约过期并被回收后，旧持有者回来提交仍是 LEASE_EXPIRED，而非 NOT_LEASE_HOLDER
        assertEquals(1, manager.reclaimExpired());
        SubmitOutcome late5 = manager.submitResult("t", "A", vA2.version(), "bk-late", "stale");
        log.info("[late-submit-5-after-reclaim] reason={} detail={}",
                late5.reason(), late5.detail());
        assertEquals(RejectReason.LEASE_EXPIRED, late5.reason());

        // 新持有者正常提交不受旧持有者影响
        Lease vC = manager.acquire("t", "C", LEASE_TTL).lease();
        SubmitOutcome ok = manager.submitResult("t", "C", vC.version(), "bk-c", "fresh");
        assertEquals(SubmitOutcome.Kind.ACCEPTED, ok.kind());
        assertEquals("fresh", manager.snapshot("t").orElseThrow().result());
    }

    @Test
    void interruptedExecution_definiteConclusion_neverSubmitted() throws Exception {
        AtomicBoolean unblock = new AtomicBoolean(false);
        AtomicReference<SubmitOutcome> outcomeRef = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);

        Thread runner = new Thread(() -> {
            SubmitOutcome out = scheduler.runOnce("t", "inst-1", "bk-1",
                    Duration.ofSeconds(30), id -> {
                        started.countDown();
                        return interruptibleWait(unblock);
                    });
            outcomeRef.set(out);
        });
        runner.start();
        assertTrue(started.await(5, TimeUnit.SECONDS));
        Thread.sleep(100); // 确认执行已进入等待
        runner.interrupt(); // 模拟实例被要求停机
        runner.join(10_000);

        SubmitOutcome out = outcomeRef.get();
        log.info("[interrupted] kind={} reason={} detail={}", out.kind(), out.reason(),
                out.detail());
        assertEquals(SubmitOutcome.Kind.REJECTED, out.kind());
        assertEquals(RejectReason.EXECUTION_INTERRUPTED, out.reason());

        // 确定结论：可恢复失败，未提交任何结果，任务可重新调度
        TaskSnapshot snap = manager.snapshot("t").orElseThrow();
        log.info("[after-interrupt] status={} result={} lastError={}",
                snap.status(), snap.result(), snap.lastErrorKind());
        assertEquals(TaskStatus.RETRY_WAITING, snap.status());
        assertNull(snap.result(), "被打断的执行绝不能被当成成功提交");
        assertNotEquals(TaskStatus.SUCCEEDED, snap.status());

        // 恢复后可正常完成
        clock.advance(Duration.ofMillis(100));
        SubmitOutcome recovered = scheduler.runOnce("t", "inst-2", "bk-1", EXEC_TIMEOUT,
                id -> "ok");
        assertEquals(SubmitOutcome.Kind.ACCEPTED, recovered.kind());
    }

    @Test
    void oldCallersUnaffected_runOnceWithoutTimeoutStillWorks() {
        // 兼容：旧签名（无执行时限、无步骤）照常可用
        SubmitOutcome out = scheduler.runOnce("t", "inst-1", "bk-1", id -> "legacy-ok");
        log.info("[legacy-runOnce] kind={} leaseVersion={}", out.kind(), out.leaseVersion());
        assertEquals(SubmitOutcome.Kind.ACCEPTED, out.kind());
        assertEquals("legacy-ok", manager.snapshot("t").orElseThrow().result());
    }

    /**
     * 执行时限 == 租约时长（含不显式传时限的旧调用方式）：卡满上限时必须得到确定失败结论，
     * 任务立刻回到可重试状态，而不是保持“被别人占着”直到租约被动回收。
     * 业务时间由 MutableClock 推进模拟“真的卡满整段上限”，不靠真实等待撑出租约过期。
     */
    @Test
    void timeoutEqualToLeaseTtl_deterministicFailure_andImmediateRetry() {
        Duration ttl = Duration.ofMillis(200);
        TaskScheduler tight = new TaskScheduler(manager, ttl); // 默认执行时限 == 租约 TTL
        AtomicBoolean unblock = new AtomicBoolean(false);

        // 旧调用方式：不显式传执行时限。执行体先把业务时钟推进满整个租约时长
        // （模拟这次执行真的卡满上限），再阻塞直到被超时取消。
        SubmitOutcome out = tight.runOnce("t", "A", "bk-1", id -> {
            clock.advance(ttl);
            return interruptibleWait(unblock);
        });
        log.info("[timeout-eq-ttl] kind={} reason={} leaseVersion={} detail={}",
                out.kind(), out.reason(), out.leaseVersion(), out.detail());
        assertEquals(SubmitOutcome.Kind.REJECTED, out.kind());
        assertEquals(RejectReason.EXECUTION_TIMEOUT, out.reason());

        // 确定结论：失败原因已记录，任务立刻进入退避重试，而不是仍显示被占着
        TaskSnapshot snap = manager.snapshot("t").orElseThrow();
        log.info("[after-timeout-eq-ttl] status={} lastError={} reason={} nextRetryAt={}",
                snap.status(), snap.lastErrorKind(), snap.lastFailureReason(),
                snap.nextRetryAt());
        assertEquals(TaskStatus.RETRY_WAITING, snap.status(),
                "卡满上限后任务必须立刻进入重试，而不是等租约被动回收");
        assertEquals(ErrorKind.RETRYABLE, snap.lastErrorKind());
        assertNotNull(snap.lastFailureReason(), "超时失败原因必须被记录");
        assertTrue(snap.lastFailureReason().contains("execution-timeout"));
        assertNull(snap.result());

        // 退避到期后立刻可重新调度并成功（不需要等租约剩余时间）
        clock.advance(Duration.ofMillis(100));
        assertEquals(1, manager.schedulable().size());
        SubmitOutcome recovered = tight.runOnce("t", "B", "bk-1", id -> "ok");
        log.info("[recovered-after-tight-timeout] kind={} leaseVersion={}",
                recovered.kind(), recovered.leaseVersion());
        assertEquals(SubmitOutcome.Kind.ACCEPTED, recovered.kind());
        assertEquals(TaskStatus.SUCCEEDED, manager.snapshot("t").orElseThrow().status());
    }

    /** 显式传与租约等长的执行时限，同样必须立即失败并回到可重试状态。 */
    @Test
    void explicitTimeoutEqualToLeaseTtl_alsoRecoversImmediately() {
        Duration ttl = Duration.ofMillis(200);
        TaskScheduler tight = new TaskScheduler(manager, ttl);
        AtomicBoolean unblock = new AtomicBoolean(false);

        SubmitOutcome out = tight.runOnce("t", "A", "bk-1", ttl, id -> {
            clock.advance(ttl);
            return interruptibleWait(unblock);
        });
        log.info("[explicit-timeout-eq-ttl] kind={} reason={} detail={}",
                out.kind(), out.reason(), out.detail());
        assertEquals(RejectReason.EXECUTION_TIMEOUT, out.reason());
        assertEquals(TaskStatus.RETRY_WAITING, manager.snapshot("t").orElseThrow().status());
        assertNotNull(manager.snapshot("t").orElseThrow().lastFailureReason());
    }
}
