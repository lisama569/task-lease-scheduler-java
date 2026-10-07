package com.github.lisama569.taskleaseschedulerjava.tests;

import com.github.lisama569.taskleaseschedulerjava.lease.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 所有权 fencing：旧持有者与新持有者交错提交结果/补记进度时，
 * 任何旧执行者都不能写坏新持有者的状态；并发下结论唯一、只能有一个赢家。
 * 时间全部由 MutableClock 控制。
 */
class OwnershipFencingTest {

    private static final Logger log = LoggerFactory.getLogger(OwnershipFencingTest.class);

    private MutableClock clock;
    private LeaseManager manager;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-09-25T00:00:00Z"));
        manager = new LeaseManager(clock, RetryPolicy.defaults());
        manager.registerTask("t");
    }

    @Test
    void staleOwner_cannotOverwrite_newHolderResultOrProgress_interleaved() {
        // A 先持有（v1），过期后被 B 接手（v2）
        Lease vA = manager.acquire("t", "A", Duration.ofSeconds(10)).lease();
        manager.recordStep("t", "A", vA.version(), "A-step", "v1-note");
        clock.advance(Duration.ofSeconds(11));
        Lease vB = manager.acquire("t", "B", Duration.ofSeconds(30)).lease();
        assertEquals(vA.version() + 1, vB.version());

        // 交错：B 先补记自己的进度
        assertTrue(manager.recordStep("t", "B", vB.version(), "B-step", "v2-note").acquired());
        // A 以旧版本补记进度 -> 被识别为“任务已被别人接手”，写不进去
        LeaseDecision staleStep = manager.recordStep("t", "A", vA.version(), "A-late", "stale");
        log.info("[interleaved-stale-step] reason={} currentVersion={} detail={}",
                staleStep.reason(), staleStep.leaseVersion(), staleStep.detail());
        assertEquals(RejectReason.OWNERSHIP_SUPERSEDED, staleStep.reason());
        assertEquals(vB.version(), staleStep.leaseVersion());

        // B 正常提交成功
        SubmitOutcome bOk = manager.submitResult("t", "B", vB.version(), "bk-B", "B-result");
        assertEquals(SubmitOutcome.Kind.ACCEPTED, bOk.kind());

        // A 在 B 已成功后再用旧版本提交 -> 任务终态拒绝，结果绝不被覆盖
        SubmitOutcome aLate = manager.submitResult("t", "A", vA.version(), "bk-A", "A-result");
        log.info("[interleaved-stale-submit] kind={} reason={} detail={}",
                aLate.kind(), aLate.reason(), aLate.detail());
        assertEquals(SubmitOutcome.Kind.REJECTED, aLate.kind());
        assertEquals(RejectReason.TASK_FINISHED, aLate.reason());
        assertEquals("B-result", manager.snapshot("t").orElseThrow().result(),
                "旧执行者的迟到提交绝不能覆盖新持有者的结果");
        assertEquals(TaskStatus.SUCCEEDED, manager.snapshot("t").orElseThrow().status());
    }

    @Test
    void concurrentStaleSubmits_onlyNewHolderWins() throws Exception {
        Lease vA = manager.acquire("t", "A", Duration.ofSeconds(10)).lease();
        clock.advance(Duration.ofSeconds(11));
        Lease vB = manager.acquire("t", "B", Duration.ofSeconds(30)).lease();

        int staleThreads = 12;
        ExecutorService pool = Executors.newFixedThreadPool(staleThreads);
        CountDownLatch gate = new CountDownLatch(1);
        List<java.util.concurrent.Future<SubmitOutcome>> futures = new ArrayList<>();

        // 一堆旧持有者 A 用过期版本并发提交不同 businessKey（此时任务仍由 B 持有）
        for (int i = 0; i < staleThreads; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                gate.await();
                return manager.submitResult("t", "A", vA.version(), "bk-A-" + idx, "stale");
            }));
        }
        gate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        int superseded = 0;
        for (java.util.concurrent.Future<SubmitOutcome> f : futures) {
            SubmitOutcome o = f.get();
            log.info("[concurrent-stale-submit] kind={} reason={} leaseVersion={}",
                    o.kind(), o.reason(), o.leaseVersion());
            assertEquals(SubmitOutcome.Kind.REJECTED, o.kind());
            if (o.reason() == RejectReason.OWNERSHIP_SUPERSEDED) {
                superseded++;
            }
        }
        assertEquals(staleThreads, superseded,
                "每一个旧持有者的并发提交都必须得到 OWNERSHIP_SUPERSEDED");

        // 只有当前持有者 B 的提交能被接受（唯一赢家）
        SubmitOutcome bWins = manager.submitResult("t", "B", vB.version(), "bk-B", "B-result");
        log.info("[new-holder-wins] kind={} leaseVersion={}", bWins.kind(), bWins.leaseVersion());
        assertEquals(SubmitOutcome.Kind.ACCEPTED, bWins.kind());
        assertEquals("B-result", manager.snapshot("t").orElseThrow().result());
    }

    @Test
    void threeReasons_areStableAndDistinguishable_withVersions() {
        // NOT_LEASE_HOLDER：从未持有过（伪造版本/别人的版本且自己没历史）
        Lease vA = manager.acquire("t", "A", Duration.ofSeconds(10)).lease();
        SubmitOutcome stranger = manager.submitResult("t", "X", 555L, "bk-x", "x");
        log.info("[reason-stranger] reason={} leaseVersion={} detail={}",
                stranger.reason(), stranger.leaseVersion(), stranger.detail());
        assertEquals(RejectReason.NOT_LEASE_HOLDER, stranger.reason());
        assertEquals(vA.version(), stranger.leaseVersion(), "判定要带当前版本用于核对");

        // LEASE_EXPIRED：持有当前租约但已过期，尚无新持有者（注意不调用 reclaim，
        // submitResult 在锁内直接按时钟判定过期）
        clock.advance(Duration.ofSeconds(11));
        SubmitOutcome expired = manager.submitResult("t", "A", vA.version(), "bk-a", "late");
        log.info("[reason-expired] reason={} leaseVersion={} detail={}",
                expired.reason(), expired.leaseVersion(), expired.detail());
        assertEquals(RejectReason.LEASE_EXPIRED, expired.reason());
        assertEquals(vA.version(), expired.leaseVersion());

        // OWNERSHIP_SUPERSEDED：过期后被 B 接手，A 的旧版本提交被 fencing
        Lease vB = manager.acquire("t", "B", Duration.ofSeconds(30)).lease();
        SubmitOutcome taken = manager.submitResult("t", "A", vA.version(), "bk-a2", "late");
        log.info("[reason-taken] reason={} leaseVersion={} detail={}",
                taken.reason(), taken.leaseVersion(), taken.detail());
        assertEquals(RejectReason.OWNERSHIP_SUPERSEDED, taken.reason());
        assertEquals(vB.version(), taken.leaseVersion());

        // 三者两两不同
        assertNotEquals(stranger.reason(), expired.reason());
        assertNotEquals(expired.reason(), taken.reason());
        assertNotEquals(stranger.reason(), taken.reason());
    }
}
