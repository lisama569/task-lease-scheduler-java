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

/**
 * 步骤级进度：重试只做没做过的步骤；进度跟租约持有者走，
 * 过期/回收/被接管后旧进度不得串到新持有者，新持有者拿到干净起点。
 */
class StepProgressTest {

    private static final Logger log = LoggerFactory.getLogger(StepProgressTest.class);

    private MutableClock clock;
    private LeaseManager manager;
    private TaskScheduler scheduler;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-10-05T00:00:00Z"));
        manager = new LeaseManager(clock, new RetryPolicy(3, Duration.ofMillis(10), Duration.ofMillis(50)));
        // 执行时限 20s 短于租约 60s：超时时租约仍有效，合作式失败上报会被接受
        scheduler = new TaskScheduler(manager, clock, Duration.ofSeconds(60),
                Duration.ofSeconds(20), Duration.ofSeconds(1));
        manager.registerTask("t");
    }

    /** 执行三步，在某一步上失败；已完成步骤即使重试也不再执行。 */
    @Test
    void retry_resumesFromUnfinishedStep_completedStepsNotRepeated() {
        AtomicInteger step1Runs = new AtomicInteger();
        AtomicInteger step2Runs = new AtomicInteger();
        AtomicInteger step3Runs = new AtomicInteger();

        SubmitOutcome first = scheduler.runSteps("t", "A", "bk-1", ctx -> {
            if (!ctx.isStepDone("step-1")) {
                step1Runs.incrementAndGet();
                ctx.markStepDone("step-1");
            }
            if (!ctx.isStepDone("step-2")) {
                step2Runs.incrementAndGet();
                ctx.markStepDone("step-2");
                throw new IllegalStateException("downstream flap at step-2");
            }
            if (!ctx.isStepDone("step-3")) {
                step3Runs.incrementAndGet();
                ctx.markStepDone("step-3");
            }
            return "done";
        }, r -> r);
        assertEquals(RejectReason.EXECUTION_FAILED, first.reason());

        TaskSnapshot mid = manager.snapshot("t").orElseThrow();
        log.info("[step-progress-after-fail] status={} steps={}", mid.status(),
                mid.steps().stream().map(s -> s.step() + "@v" + s.ownerVersion()).toList());
        assertEquals(List.of("step-1", "step-2"),
                mid.steps().stream().map(StepProgress::step).toList(), "失败后已完成步骤进度仍可查");

        clock.advance(Duration.ofMillis(10));
        SubmitOutcome second = scheduler.runSteps("t", "B", "bk-1", ctx -> {
            if (!ctx.isStepDone("step-1")) {
                step1Runs.incrementAndGet();
                ctx.markStepDone("step-1");
            }
            if (!ctx.isStepDone("step-2")) {
                step2Runs.incrementAndGet();
                ctx.markStepDone("step-2");
            }
            if (!ctx.isStepDone("step-3")) {
                step3Runs.incrementAndGet();
                ctx.markStepDone("step-3");
            }
            return "done";
        }, r -> r);

        log.info("[step-resume] outcome={} runs=[1:{}, 2:{}, 3:{}]",
                second.kind(), step1Runs.get(), step2Runs.get(), step3Runs.get());
        assertEquals(SubmitOutcome.Kind.ACCEPTED, second.kind());
        assertEquals(1, step1Runs.get(), "step-1 已完成，重试不得重复执行");
        assertEquals(1, step2Runs.get(), "step-2 已完成（副作用已产生），重试不得重复执行");
        assertEquals(1, step3Runs.get(), "step-3 未完成，重试应执行一次");
    }

    /** 租约到期后被新持有者接管：旧持有者血缘的进度必须整体清空。 */
    @Test
    void expiredThenTakeover_newOwnerStartsFromCleanSlate() {
        Lease v1 = manager.acquire("t", "A", Duration.ofSeconds(10)).lease();
        manager.markStepDone("t", "A", v1.version(), "step-1");
        manager.markStepDone("t", "A", v1.version(), "step-2");
        assertEquals(2, manager.snapshot("t").orElseThrow().steps().size());

        clock.advance(Duration.ofSeconds(11));
        manager.reclaimExpired();
        Lease v2 = manager.acquire("t", "B", Duration.ofSeconds(10)).lease();
        log.info("[takeover] oldVersion={} newVersion={} stepsAfterTakeover={}",
                v1.version(), v2.version(),
                manager.snapshot("t").orElseThrow().steps().size());

        assertEquals(v1.version() + 1, v2.version());
        assertTrue(manager.snapshot("t").orElseThrow().steps().isEmpty(),
                "旧持有者租约过期/被接管后，其进度必须清空，新持有者拿到干净起点");
        assertFalse(manager.isStepDone("t", "step-1"), "旧进度不能算到新持有者头上");
        assertFalse(manager.isStepDone("t", "step-2"));
    }

    /** 不经过回收，直接由新持有者在到期后抢占（惰性接管），进度同样清空。 */
    @Test
    void lazyTakeoverAfterExpiry_alsoWipesProgress() {
        Lease v1 = manager.acquire("t", "A", Duration.ofSeconds(10)).lease();
        manager.markStepDone("t", "A", v1.version(), "step-1");
        clock.advance(Duration.ofSeconds(11));
        Lease v2 = manager.acquire("t", "B", Duration.ofSeconds(10)).lease();
        assertEquals(v1.version() + 1, v2.version());
        assertTrue(manager.snapshot("t").orElseThrow().steps().isEmpty());
    }

    /** 超时（合作式失败上报）本身不清进度，但随后若租约被别人抢走，新持有者仍从干净起点。 */
    @Test
    void timeoutPreservesProgress_forCooperativeRetry_butTakeoverWipes() {
        SubmitOutcome out = scheduler.runSteps("t", "A", "bk-1", ctx -> {
            ctx.markStepDone("step-1");
            clock.advance(Duration.ofSeconds(21));
            ctx.checkpoint();
            return "x";
        }, r -> r);
        assertEquals(RejectReason.EXECUTION_TIMEOUT, out.reason());
        assertEquals(1, manager.snapshot("t").orElseThrow().steps().size(),
                "合作式超时上报进入同一重试血缘，进度保留以供续跑");

        // 退避未到期，别人抢不走；退避到期后由新持有者抢占 —— 这仍是同任务的重试血缘（合作式失败）
        clock.advance(Duration.ofMillis(10));
        Lease v2 = manager.acquire("t", "B", Duration.ofSeconds(60)).lease();
        assertEquals(1, manager.snapshot("t").orElseThrow().steps().size(),
                "合作式重试血缘允许新执行者续用进度");

        // 但该持有者若再次把租约拖到过期被接管，进度必须随所有权失效清空
        clock.advance(Duration.ofSeconds(61));
        Lease v3 = manager.acquire("t", "C", Duration.ofSeconds(60)).lease();
        log.info("[retry-then-expire] v2={} v3={} steps={}", v2.version(), v3.version(),
                manager.snapshot("t").orElseThrow().steps().size());
        assertTrue(manager.snapshot("t").orElseThrow().steps().isEmpty(),
                "非合作的租约失效（过期接管）必须清空进度");
    }

    /** 旧持有者在租约过期且尚未被接管时提交：LEASE_EXPIRED；被接管后提交：SUPERSEDED。 */
    @Test
    void staleOwnerSubmits_reasonsDistinguishable_expiredVsSupersededVsNotHolder() {
        Lease v1 = manager.acquire("t", "A", Duration.ofSeconds(10)).lease();
        manager.markStepDone("t", "A", v1.version(), "step-1");

        clock.advance(Duration.ofSeconds(11));
        SubmitOutcome expired = manager.submitResult("t", "A", v1.version(), "bk-A", "late");
        log.info("[stale-expired] reason={} detail={}", expired.reason(), expired.detail());
        assertEquals(RejectReason.LEASE_EXPIRED, expired.reason(), "租约过期且未被接管 -> LEASE_EXPIRED");

        Lease v2 = manager.acquire("t", "B", Duration.ofSeconds(10)).lease();
        SubmitOutcome superseded = manager.submitResult("t", "A", v1.version(), "bk-A", "late2");
        log.info("[stale-superseded] reason={} currentOwner=B version={} detail={}",
                superseded.reason(), v2.version(), superseded.detail());
        assertEquals(RejectReason.OWNERSHIP_SUPERSEDED, superseded.reason(),
                "任务已被新持有者接管 -> OWNERSHIP_SUPERSEDED");

        // 压根不是持有者，却冒用当前版本
        SubmitOutcome notHolder = manager.submitResult("t", "X", v2.version(), "bk-X", "fake");
        log.info("[not-holder] reason={} detail={}", notHolder.reason(), notHolder.detail());
        assertEquals(RejectReason.NOT_LEASE_HOLDER, notHolder.reason());

        // 新持有者结果绝不能被旧持有者盖掉
        SubmitOutcome ok = manager.submitResult("t", "B", v2.version(), "bk-B", "B-result");
        assertEquals(SubmitOutcome.Kind.ACCEPTED, ok.kind());
        assertEquals("B-result", manager.snapshot("t").orElseThrow().result());
    }

    /** 过期旧持有者写进度同样被拒。 */
    @Test
    void staleOwnerMarkingStep_isRejected() {
        Lease v1 = manager.acquire("t", "A", Duration.ofSeconds(10)).lease();
        clock.advance(Duration.ofSeconds(11));
        LeaseDecision d = manager.markStepDone("t", "A", v1.version(), "step-late");
        assertFalse(d.acquired());
        assertEquals(RejectReason.LEASE_EXPIRED, d.reason());
    }
}
