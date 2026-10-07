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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 执行时限与打断：超时/中断必须得出确定结论（可恢复失败），任务立刻回到可调度状态；
 * 超时后旧持有者的提交被“过期 / 被别人接手 / 不是持有者”三种稳定且可区分的结论拒绝；
 * 恢复时已完成步骤不重复执行。
 *
 * <p><b>时间全部由 MutableClock 控制</b>：调度器与租约管理器共用同一时钟。
 * 超时由“卡住的工作线程声明已开始后，主线程把时钟拨过执行上限”确定性触发，
 * 不使用真实等待或线程睡眠撑超时；唯一的真实等待是 CountDownLatch.await，
 * 仅用于线程间交接（等待工作线程真正开始 / 等待调度线程观察到超时）。
 */
class ExecutionTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(ExecutionTimeoutTest.class);

    private static final Duration LEASE_TTL = Duration.ofSeconds(30);
    private static final Duration EXEC_TIMEOUT = Duration.ofSeconds(5);

    private MutableClock clock;
    private LeaseManager manager;
    private TaskScheduler scheduler;
    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-09-25T00:00:00Z"));
        manager = new LeaseManager(clock, RetryPolicy.defaults());
        manager.registerTask("t");
        pool = Executors.newCachedThreadPool();
        // tickMillis=2：调度线程以 2ms 粒度轮询受控时钟，超时判定及时且不空转
        scheduler = new TaskScheduler(manager, LEASE_TTL, LEASE_TTL, pool, clock, 2L);
    }

    /** 阻塞到被中断（超时取消）为止；只负责“已开始”握手，不推进时钟。 */
    private String stuckWork(CountDownLatch started, CountDownLatch release) {
        started.countDown();
        awaitInterruptibly(release);
        return "never";
    }

    private static void awaitInterruptibly(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("worker interrupted");
        }
    }

    /**
     * 在独立线程运行一次会超时的调度：工作线程声明开始后，由<b>主线程</b>把受控时钟
     * 拨过执行上限（可精确设成与租约 TTL 相同的边界），再等待调度线程给出超时结论。
     */
    private SubmitOutcome runExpectingTimeout(String owner, String businessKey,
                                              Duration executionTimeout,
                                              CountDownLatch started, CountDownLatch release)
            throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<SubmitOutcome> ref = new AtomicReference<>();
        pool.submit(() -> {
            try {
                ref.set(scheduler.runOnce("t", owner, businessKey, executionTimeout,
                        id -> stuckWork(started, release)));
            } finally {
                done.countDown();
            }
        });
        assertTrue(started.await(5, TimeUnit.SECONDS), "工作线程必须已开始执行");
        clock.advance(executionTimeout); // 受控时钟越过执行上限（确定性触发超时）
        assertTrue(done.await(5, TimeUnit.SECONDS), "超时必须在受控时钟驱动下确定发生");
        release.countDown(); // 让被取消的工作线程退出
        return ref.get();
    }

    @Test
    void executionTimeout_recoverableFailure_andQuickRecovery() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        SubmitOutcome timedOut = runExpectingTimeout("inst-1", "bk-1", EXEC_TIMEOUT,
                new CountDownLatch(1), release);
        log.info("[timeout] kind={} reason={} leaseVersion={} detail={}",
                timedOut.kind(), timedOut.reason(), timedOut.leaseVersion(), timedOut.detail());

        assertEquals(SubmitOutcome.Kind.REJECTED, timedOut.kind());
        assertEquals(RejectReason.EXECUTION_TIMEOUT, timedOut.reason());

        // 确定结论：可恢复失败，任务立刻进入退避重试，而不是干等租约到期
        TaskSnapshot snap = manager.snapshot("t").orElseThrow();
        log.info("[after-timeout] status={} attempts={} lastError={} reason={} nextRetryAt={}",
                snap.status(), snap.attempts(), snap.lastErrorKind(),
                snap.lastFailureReason(), snap.nextRetryAt());
        assertEquals(TaskStatus.RETRY_WAITING, snap.status());
        assertEquals(ErrorKind.RETRYABLE, snap.lastErrorKind());
        assertTrue(snap.lastFailureReason().contains("execution-timeout"));
        assertNull(snap.result(), "超时的执行不得留下结果");

        // 退避到期后恢复执行并成功（同一时钟，无需任何真实等待）
        clock.advance(Duration.ofMillis(100));
        SubmitOutcome recovered = scheduler.runOnce("t", "inst-1", "bk-1", EXEC_TIMEOUT,
                id -> "ok");
        log.info("[recovered] kind={} leaseVersion={}", recovered.kind(), recovered.leaseVersion());
        assertEquals(SubmitOutcome.Kind.ACCEPTED, recovered.kind());
        assertEquals(TaskStatus.SUCCEEDED, manager.snapshot("t").orElseThrow().status());
    }

    @Test
    void timeoutEqualsLeaseTtl_entersRetryImmediately_atExactBoundary() throws Exception {
        // 关键边界：执行时间上限 == 租约时长，执行卡满整段 -> 租约恰好同时到期，
        // 但失败结论必须立刻落地（finishAttempt 只 fencing，不要求租约未过期）。
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        SubmitOutcome out = runExpectingTimeout("A", "bk-1", LEASE_TTL, started, release);

        TaskSnapshot snap = manager.snapshot("t").orElseThrow();
        log.info("[ttl-boundary] kind={} reason={} status={} lastFailureReason={}",
                out.kind(), out.reason(), snap.status(), snap.lastFailureReason());
        assertEquals(RejectReason.EXECUTION_TIMEOUT, out.reason());
        assertEquals(TaskStatus.RETRY_WAITING, snap.status(),
                "即使租约同时到期，任务也必须立刻进入退避重试，而非停留在 LEASED");
        assertTrue(snap.lastFailureReason().contains("execution-timeout"),
                "本次超时失败原因必须被记录");

        // 立刻就可被重新调度（退避到期后），不需要等租约“再自然过期一次”
        clock.advance(Duration.ofMillis(100));
        assertEquals(1, manager.schedulable().size());
        SubmitOutcome retry = scheduler.runOnce("t", "B", "bk-2", LEASE_TTL, id -> "ok");
        assertEquals(SubmitOutcome.Kind.ACCEPTED, retry.kind());
    }

    @Test
    void timeoutRecovery_completedStepsNotReexecuted() throws Exception {
        AtomicInteger step1Runs = new AtomicInteger();
        AtomicInteger step2Runs = new AtomicInteger();
        AtomicInteger step3Runs = new AtomicInteger();
        CountDownLatch step2Started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<TaskStep> firstSteps = List.of(
                new TaskStep("extract", () -> {
                    step1Runs.incrementAndGet();
                    return "rows=100";
                }),
                new TaskStep("transform", () -> {
                    step2Runs.incrementAndGet();
                    step2Started.countDown();
                    awaitInterruptibly(release); // step2 卡住直到被超时取消
                    return "blocked";
                }),
                new TaskStep("load", () -> {
                    step3Runs.incrementAndGet();
                    return "loaded";
                }));

        // 第一次执行：step1 完成并记录，step2 卡住；主线程拨钟到超时
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<SubmitOutcome> firstRef = new AtomicReference<>();
        pool.submit(() -> {
            try {
                firstRef.set(scheduler.runSteps("t", "inst-1", "bk-1", EXEC_TIMEOUT, firstSteps));
            } finally {
                done.countDown();
            }
        });
        assertTrue(step2Started.await(5, TimeUnit.SECONDS));
        clock.advance(EXEC_TIMEOUT);
        assertTrue(done.await(5, TimeUnit.SECONDS));
        release.countDown();
        SubmitOutcome first = firstRef.get();
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
        SubmitOutcome second = scheduler.runSteps("t", "inst-1", "bk-1", EXEC_TIMEOUT,
                List.of(
                        new TaskStep("extract", () -> {
                            step1Runs.incrementAndGet();
                            return "rows=100";
                        }),
                        new TaskStep("transform", () -> {
                            step2Runs.incrementAndGet();
                            return "transformed";
                        }),
                        new TaskStep("load", () -> {
                            step3Runs.incrementAndGet();
                            return "loaded";
                        })));
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
    void oldHolderSubmitRejected_threeStableDistinguishableReasons() throws Exception {
        // —— 阶段 0：A 执行超时，租约随失败被正常结束（非过期），任务进入退避 ——
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        SubmitOutcome timedOut = runExpectingTimeout("A", "bk-1", EXEC_TIMEOUT,
                started, release);
        assertEquals(RejectReason.EXECUTION_TIMEOUT, timedOut.reason());

        // 1) A 的租约已随超时失败正常结束：A 再回来提交 -> NOT_LEASE_HOLDER
        SubmitOutcome late1 = manager.submitResult("t", "A", 1, "bk-late", "stale");
        log.info("[late-submit-1] reason={} leaseVersion={} detail={}",
                late1.reason(), late1.leaseVersion(), late1.detail());
        assertEquals(RejectReason.NOT_LEASE_HOLDER, late1.reason(),
                "超时/中断后租约已被正常释放，旧持有者不再是当前持有者");
        // 对照：从未持有过租约的执行者同样是 NOT_LEASE_HOLDER，但 detail 能核对历史版本
        SubmitOutcome stranger = manager.submitResult("t", "Z", 999L, "bk-x", "x");
        assertEquals(RejectReason.NOT_LEASE_HOLDER, stranger.reason());

        // 2) 任务被 B 接手：A 用旧版本提交 -> OWNERSHIP_SUPERSEDED（任务已被别人接手）
        clock.advance(Duration.ofMillis(100));
        Lease vB = manager.acquire("t", "B", LEASE_TTL).lease();
        SubmitOutcome late2 = manager.submitResult("t", "A", 1, "bk-late", "stale");
        log.info("[late-submit-2] reason={} currentVersion={} detail={}",
                late2.reason(), late2.leaseVersion(), late2.detail());
        assertEquals(RejectReason.OWNERSHIP_SUPERSEDED, late2.reason(),
                "任务已被别人接手时，旧持有者必须得到 OWNERSHIP_SUPERSEDED");
        assertEquals(vB.version(), late2.leaseVersion());
        // 旧持有者不能写坏新持有者的结果
        assertEquals(RejectReason.OWNERSHIP_SUPERSEDED,
                manager.submitResult("t", "A", 1, "bk-A", "A-result").reason());

        // 3) B 的租约自然过期、尚未被别人接手：B 用自己的当前版本提交 -> LEASE_EXPIRED
        clock.advance(LEASE_TTL.plusSeconds(1));
        SubmitOutcome late3 = manager.submitResult("t", "B", vB.version(), "bk-late", "stale");
        log.info("[late-submit-3] reason={} leaseVersion={} detail={}",
                late3.reason(), late3.leaseVersion(), late3.detail());
        assertEquals(RejectReason.LEASE_EXPIRED, late3.reason(),
                "租约到期被回收、尚无新持有者时，旧持有者得到 LEASE_EXPIRED");
        assertEquals(vB.version(), late3.leaseVersion());

        // 过期后 C 接手：B 的过期版本再提交 -> OWNERSHIP_SUPERSEDED（已被别人接手）
        Lease vC = manager.acquire("t", "C", LEASE_TTL).lease();
        SubmitOutcome late4 = manager.submitResult("t", "B", vB.version(), "bk-late", "stale");
        log.info("[late-submit-4] reason={} currentVersion={} detail={}",
                late4.reason(), late4.leaseVersion(), late4.detail());
        assertEquals(RejectReason.OWNERSHIP_SUPERSEDED, late4.reason());
        assertEquals(vC.version(), late4.leaseVersion());

        // 三种结论稳定互斥：这里收集断言便于日志集中展示
        log.info("[reasons-summary] endedNormally={} takenOver={} expiredThenTaken={}",
                late1.reason(), late2.reason(), late4.reason());

        // 新持有者正常提交不受任何旧持有者影响
        SubmitOutcome ok = manager.submitResult("t", "C", vC.version(), "bk-c", "fresh");
        assertEquals(SubmitOutcome.Kind.ACCEPTED, ok.kind());
        assertEquals("fresh", manager.snapshot("t").orElseThrow().result());
    }

    @Test
    void interruptedExecution_definiteConclusion_neverSubmitted() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<SubmitOutcome> outcomeRef = new AtomicReference<>();

        Thread runner = new Thread(() -> outcomeRef.set(scheduler.runOnce("t", "inst-1", "bk-1",
                Duration.ofSeconds(30), id -> {
                    started.countDown();
                    awaitInterruptibly(release); // 模拟执行直到被中断（停机）
                    return "never";
                })));
        runner.start();
        assertTrue(started.await(5, TimeUnit.SECONDS));
        runner.interrupt(); // 模拟实例被要求停机（真实中断信号，非用于制造超时）
        runner.join(10_000);
        release.countDown();

        SubmitOutcome out = outcomeRef.get();
        log.info("[interrupted] kind={} reason={} detail={}", out.kind(), out.reason(),
                out.detail());
        assertEquals(SubmitOutcome.Kind.REJECTED, out.kind());
        assertEquals(RejectReason.EXECUTION_INTERRUPTED, out.reason());

        // 确定结论：可恢复失败，未提交任何结果，任务可重新调度
        TaskSnapshot snap = manager.snapshot("t").orElseThrow();
        log.info("[after-interrupt] status={} result={} lastError={} reason={}",
                snap.status(), snap.result(), snap.lastErrorKind(), snap.lastFailureReason());
        assertEquals(TaskStatus.RETRY_WAITING, snap.status());
        assertNull(snap.result(), "被打断的执行绝不能被当成成功提交");
        assertTrue(snap.lastFailureReason().contains("scheduler-interrupted"));

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
}
