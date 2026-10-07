package com.github.lisama569.taskleaseschedulerjava.tests;

import com.github.lisama569.taskleaseschedulerjava.lease.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/** 租约到期、续约、释放与所有权取代的判定。 */
class LeaseExpiryTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseExpiryTest.class);

    private MutableClock clock;
    private LeaseManager manager;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-09-25T00:00:00Z"));
        manager = new LeaseManager(clock, RetryPolicy.defaults());
        manager.registerTask("t");
    }

    @Test
    void expiredHolder_submitRejected_leaseExpired() {
        Lease lease = manager.acquire("t", "A", Duration.ofSeconds(5)).lease();
        clock.advance(Duration.ofSeconds(6));
        SubmitOutcome out = manager.submitResult("t", "A", lease.version(), "bk-1", "done");
        log.info("[expired-submit] kind={} reason={} detail={}", out.kind(), out.reason(), out.detail());
        assertEquals(SubmitOutcome.Kind.REJECTED, out.kind());
        assertEquals(RejectReason.LEASE_EXPIRED, out.reason());
    }

    @Test
    void supersededOwner_submitRejected_ownershipSuperseded() {
        Lease v1 = manager.acquire("t", "A", Duration.ofSeconds(5)).lease();
        clock.advance(Duration.ofSeconds(6));
        manager.reclaimExpired();
        Lease v2 = manager.acquire("t", "A", Duration.ofSeconds(5)).lease();
        assertEquals(v1.version() + 1, v2.version());

        SubmitOutcome out = manager.submitResult("t", "A", v1.version(), "bk-1", "stale");
        log.info("[superseded-submit] kind={} reason={} currentVersion={} detail={}",
                out.kind(), out.reason(), v2.version(), out.detail());
        assertEquals(RejectReason.OWNERSHIP_SUPERSEDED, out.reason(),
                "旧版本提交必须被识别为所有权已取代");

        // 新持有者提交不受影响，不被旧持有者覆盖
        SubmitOutcome ok = manager.submitResult("t", "A", v2.version(), "bk-1", "fresh");
        assertEquals(SubmitOutcome.Kind.ACCEPTED, ok.kind());
        assertEquals("fresh", manager.snapshot("t").orElseThrow().result());
    }

    @Test
    void takenOverByOther_submitRejected_notLeaseHolder_andResultNotOverwritten() {
        Lease v1 = manager.acquire("t", "A", Duration.ofSeconds(5)).lease();
        clock.advance(Duration.ofSeconds(6));
        Lease v2 = manager.acquire("t", "B", Duration.ofSeconds(5)).lease();
        manager.submitResult("t", "B", v2.version(), "bk-B", "B-result");

        SubmitOutcome late = manager.submitResult("t", "A", v1.version(), "bk-A", "A-result");
        log.info("[late-submit] kind={} reason={} detail={}", late.kind(), late.reason(), late.detail());
        assertEquals(SubmitOutcome.Kind.REJECTED, late.kind());
        // 任务已由 B 完成，A 的迟到提交不得覆盖
        assertEquals("B-result", manager.snapshot("t").orElseThrow().result());
        assertEquals(TaskStatus.SUCCEEDED, manager.snapshot("t").orElseThrow().status());
    }

    @Test
    void renew_extendsExpiry_sameVersion() {
        Lease v1 = manager.acquire("t", "A", Duration.ofSeconds(5)).lease();
        clock.advance(Duration.ofSeconds(3));
        LeaseDecision renewed = manager.renew("t", "A", v1.version(), Duration.ofSeconds(5));
        assertTrue(renewed.acquired());
        assertEquals(v1.version(), renewed.lease().version(), "续约不得改变 fencing token");
        assertEquals(5000, renewed.lease().remainingMillis(clock.now()));

        clock.advance(Duration.ofSeconds(4)); // 原租约已过期，续约后仍有效
        SubmitOutcome out = manager.submitResult("t", "A", v1.version(), "bk-1", "done");
        assertEquals(SubmitOutcome.Kind.ACCEPTED, out.kind());
    }

    @Test
    void renew_rejectedForNonHolder() {
        Lease v1 = manager.acquire("t", "A", Duration.ofSeconds(5)).lease();
        LeaseDecision out = manager.renew("t", "B", v1.version(), Duration.ofSeconds(5));
        log.info("[renew-non-holder] reason={} detail={}", out.reason(), out.detail());
        assertFalse(out.acquired());
        assertEquals(RejectReason.NOT_LEASE_HOLDER, out.reason());
    }

    @Test
    void release_returnsTaskToPending_andReacquirable() {
        Lease v1 = manager.acquire("t", "A", Duration.ofSeconds(30)).lease();
        assertTrue(manager.release("t", "A", v1.version()).acquired());
        assertEquals(TaskStatus.PENDING, manager.snapshot("t").orElseThrow().status());

        LeaseDecision v2 = manager.acquire("t", "B", Duration.ofSeconds(30));
        assertTrue(v2.acquired());
        assertEquals(v1.version() + 1, v2.lease().version());
    }

    @Test
    void reclaimExpired_makesTaskSchedulableAgain() {
        manager.acquire("t", "A", Duration.ofSeconds(5));
        clock.advance(Duration.ofSeconds(6));
        assertEquals(1, manager.reclaimExpired());
        assertEquals(TaskStatus.PENDING, manager.snapshot("t").orElseThrow().status());
        assertEquals(1, manager.schedulable().size());
    }

    @Test
    void reportFailure_acceptedOnExpiredButOwnLease_immediateRetry() {
        // 执行时限 == 租约时长的核心场景：租约刚好过期，但版本未被取代，
        // 持有者上报失败必须被接受，任务立刻进入退避重试且原因被记录
        Lease lease = manager.acquire("t", "A", Duration.ofSeconds(5)).lease();
        clock.advance(Duration.ofSeconds(5)); // 恰好卡满整段租约
        LeaseDecision f = manager.reportFailure("t", "A", lease.version(),
                ErrorKind.RETRYABLE, "execution-timeout after 5000ms");
        log.info("[failure-on-expired-own-lease] accepted={} reason={} detail={}",
                f.acquired(), f.reason(), f.detail());
        assertTrue(f.acquired(), "租约过期但版本未被取代时，失败上报必须被接受");

        TaskSnapshot snap = manager.snapshot("t").orElseThrow();
        assertEquals(TaskStatus.RETRY_WAITING, snap.status());
        assertEquals("execution-timeout after 5000ms", snap.lastFailureReason());
        clock.advance(Duration.ofMillis(100)); // 退避到期
        assertEquals(1, manager.schedulable().size(), "不依赖租约被动回收即可重新调度");
    }

    @Test
    void reportFailure_rejectedAfterTakeover_cannotClobberNewHolder() {
        Lease v1 = manager.acquire("t", "A", Duration.ofSeconds(5)).lease();
        clock.advance(Duration.ofSeconds(6));
        Lease v2 = manager.acquire("t", "B", Duration.ofSeconds(30)).lease();

        // 旧持有者上报失败：被识别为“所有权已被取代”，新持有者的租约不受影响
        LeaseDecision stale = manager.reportFailure("t", "A", v1.version(),
                ErrorKind.RETRYABLE, "stale-timeout");
        log.info("[stale-failure-report] accepted={} reason={} currentVersion={} detail={}",
                stale.acquired(), stale.reason(), stale.currentVersion(), stale.detail());
        assertFalse(stale.acquired());
        assertEquals(RejectReason.OWNERSHIP_SUPERSEDED, stale.reason());
        assertEquals(v2.version(), stale.currentVersion());

        TaskSnapshot snap = manager.snapshot("t").orElseThrow();
        assertEquals(TaskStatus.LEASED, snap.status(), "旧持有者不得写坏新持有者的状态");
        assertEquals("B", snap.ownerId());
        assertNull(snap.lastFailureReason());

        // 从未持有过的执行者上报失败：NOT_LEASE_HOLDER，与“被取代”可区分
        LeaseDecision stranger = manager.reportFailure("t", "stranger", v1.version(),
                ErrorKind.RETRYABLE, "bogus");
        assertEquals(RejectReason.NOT_LEASE_HOLDER, stranger.reason());

        // 新持有者正常提交，结果不被旧持有者影响
        assertEquals(SubmitOutcome.Kind.ACCEPTED,
                manager.submitResult("t", "B", v2.version(), "bk-B", "B-done").kind());
        assertEquals("B-done", manager.snapshot("t").orElseThrow().result());
    }
}
