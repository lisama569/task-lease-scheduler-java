package com.github.lisama569.taskleaseschedulerjava.tests;

import com.github.lisama569.taskleaseschedulerjava.lease.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 步骤级进度：记录、查询、幂等，以及进度归当前持有者所有的隔离语义。 */
class StepProgressTest {

    private static final Logger log = LoggerFactory.getLogger(StepProgressTest.class);

    private MutableClock clock;
    private LeaseManager manager;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-09-25T00:00:00Z"));
        manager = new LeaseManager(clock, RetryPolicy.defaults());
        manager.registerTask("t");
    }

    @Test
    void recordAndQuerySteps_onlyCurrentHolder() {
        Lease lease = manager.acquire("t", "A", Duration.ofSeconds(30)).lease();
        assertTrue(manager.recordStep("t", "A", lease.version(), "extract", "rows=100").acquired());
        assertTrue(manager.recordStep("t", "A", lease.version(), "transform", null).acquired());

        ProgressView view = manager.progress("t", "A", lease.version());
        log.info("[progress] holder=A version={} steps={}", view.leaseVersion(),
                view.completedSteps());
        assertTrue(view.ok());
        assertEquals(List.of("extract", "transform"), view.completedSteps());
        assertEquals(lease.version(), view.leaseVersion());

        // 非持有者查询/记录被拒，原因可区分
        ProgressView stranger = manager.progress("t", "B", lease.version());
        log.info("[progress-stranger] ok={} reason={} detail={}",
                stranger.ok(), stranger.reason(), stranger.detail());
        assertFalse(stranger.ok());
        assertEquals(RejectReason.NOT_LEASE_HOLDER, stranger.reason());
        assertEquals(RejectReason.NOT_LEASE_HOLDER,
                manager.recordStep("t", "B", lease.version(), "load", null).reason());
    }

    @Test
    void duplicateStepKey_idempotent() {
        Lease lease = manager.acquire("t", "A", Duration.ofSeconds(30)).lease();
        manager.recordStep("t", "A", lease.version(), "extract", "first");
        manager.recordStep("t", "A", lease.version(), "extract", "second");
        manager.recordStep("t", "A", lease.version(), "extract", "third");
        ProgressView view = manager.progress("t", "A", lease.version());
        log.info("[progress-idempotent] steps={}", view.completedSteps());
        assertEquals(List.of("extract"), view.completedSteps(),
                "同一 stepKey 重复记录必须幂等");
    }

    @Test
    void progressDoesNotLeakToNewHolder_afterExpiryAndTakeover() {
        Lease v1 = manager.acquire("t", "A", Duration.ofSeconds(5)).lease();
        manager.recordStep("t", "A", v1.version(), "extract", "rows=100");
        manager.recordStep("t", "A", v1.version(), "transform", "v1");

        clock.advance(Duration.ofSeconds(6)); // 租约过期
        assertEquals(1, manager.reclaimExpired());

        // 旧持有者查询/记录被拒：租约已过期被回收，与“从未持有过”可区分
        ProgressView stale = manager.progress("t", "A", v1.version());
        log.info("[stale-progress] ok={} reason={} detail={}", stale.ok(), stale.reason(),
                stale.detail());
        assertFalse(stale.ok());
        assertEquals(RejectReason.LEASE_EXPIRED, stale.reason(),
                "租约被回收后旧持有者的判定应是 LEASE_EXPIRED，而非笼统的 NOT_LEASE_HOLDER");

        // 新持有者拿到干净、确定的起点
        Lease v2 = manager.acquire("t", "B", Duration.ofSeconds(30)).lease();
        ProgressView fresh = manager.progress("t", "B", v2.version());
        log.info("[fresh-holder] owner=B version={} steps={} (oldVersion={})",
                fresh.leaseVersion(), fresh.completedSteps(), v1.version());
        assertTrue(fresh.ok());
        assertTrue(fresh.completedSteps().isEmpty(),
                "旧持有者留下的进度不得算到新持有者头上");
        assertEquals(v1.version() + 1, v2.version());

        // 新持有者自己的进度从空开始累积
        manager.recordStep("t", "B", v2.version(), "extract", "rows=100");
        assertEquals(List.of("extract"), manager.progress("t", "B", v2.version()).completedSteps());

        // 旧持有者以旧版本提交进度/结果：任务已被别人接手，与“不是持有者”可区分
        LeaseDecision late = manager.recordStep("t", "A", v1.version(), "load", null);
        log.info("[late-record] reason={} currentVersion={} detail={}",
                late.reason(), v2.version(), late.detail());
        assertEquals(RejectReason.OWNERSHIP_SUPERSEDED, late.reason());
        assertEquals(v2.version(), late.currentVersion(), "判定需带上当前租约版本便于核对");

        // 从未持有过的执行者记录进度：仍是 NOT_LEASE_HOLDER
        assertEquals(RejectReason.NOT_LEASE_HOLDER,
                manager.recordStep("t", "stranger", v1.version(), "load", null).reason());
    }

    @Test
    void sameOwnerReacquire_keepsProgress_forResume() {
        Lease v1 = manager.acquire("t", "A", Duration.ofSeconds(5)).lease();
        manager.recordStep("t", "A", v1.version(), "extract", "rows=100");
        manager.reportFailure("t", "A", v1.version(), ErrorKind.RETRYABLE, "flap");

        clock.advance(Duration.ofMillis(100)); // 退避到期
        Lease v2 = manager.acquire("t", "A", Duration.ofSeconds(5)).lease();
        ProgressView view = manager.progress("t", "A", v2.version());
        log.info("[resume] owner=A oldVersion={} newVersion={} inheritedSteps={}",
                v1.version(), v2.version(), view.completedSteps());
        assertEquals(List.of("extract"), view.completedSteps(),
                "同一持有者重试/恢复时应继承自己的进度，已完成步骤不重复执行");
    }

    @Test
    void progressClearedOnTerminalStates() {
        Lease lease = manager.acquire("t", "A", Duration.ofSeconds(30)).lease();
        manager.recordStep("t", "A", lease.version(), "extract", null);
        manager.submitResult("t", "A", lease.version(), "bk-1", "done");
        // 任务成功后进度被清除；再查询因任务终态被拒
        ProgressView view = manager.progress("t", "A", lease.version());
        log.info("[terminal-progress] ok={} reason={}", view.ok(), view.reason());
        assertFalse(view.ok());
        assertEquals(RejectReason.TASK_FINISHED, view.reason());
    }
}
