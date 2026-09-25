package com.github.lisama569.taskleaseschedulerjava.tests;

import com.github.lisama569.taskleaseschedulerjava.lease.LeaseManager;
import com.github.lisama569.taskleaseschedulerjava.lease.model.Lease;
import com.github.lisama569.taskleaseschedulerjava.lease.model.RejectReason;
import com.github.lisama569.taskleaseschedulerjava.lease.model.RetryPolicy;
import com.github.lisama569.taskleaseschedulerjava.lease.model.SubmitOutcome;
import com.github.lisama569.taskleaseschedulerjava.lease.model.TaskStatus;
import com.github.lisama569.taskleaseschedulerjava.lease.store.LeaseStore;
import com.github.lisama569.taskleaseschedulerjava.lease.time.MutableClock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

class LeaseExpiryTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseExpiryTest.class);

    private final MutableClock clock =
            new MutableClock(Instant.parse("2026-09-25T00:00:00Z"), ZoneOffset.UTC);
    private final LeaseManager manager =
            new LeaseManager(new LeaseStore(), clock, RetryPolicy.defaults());

    {
        manager.registerTask("task-1", "biz-1");
    }

    @Test
    void expiredLeaseIsReclaimedAndOldHolderSubmitRejected() {
        Lease old = manager.tryAcquire("task-1", "holder-a", Duration.ofSeconds(10)).orElseThrow();
        log.info("[获取] holder=a version={} expiresAt={}", old.version(), old.expiresAt());

        // 推进可控时钟使租约到期，无需真实等待
        clock.advance(Duration.ofSeconds(11));
        assertEquals(1, manager.reclaimExpired(), "到期租约必须被回收");
        assertEquals(TaskStatus.PENDING, manager.view("task-1").orElseThrow().status());

        // 旧持有者提交：租约已过期 -> 拒绝，不得静默生效
        SubmitOutcome expired = manager.submitResult(old, "biz-1", "stale-result");
        log.info("[旧持有者提交] version={} rejected={} 判定依据=当前无有效租约(NOT_HOLDER)",
                old.version(), expired.rejectReason());
        assertFalse(expired.accepted());
        assertEquals(RejectReason.NOT_HOLDER, expired.rejectReason());
        assertEquals(TaskStatus.PENDING, manager.view("task-1").orElseThrow().status(),
                "被拒绝的提交不得改变任务状态");
    }

    @Test
    void supersededHolderSubmitRejectedAndDoesNotOverwriteNewHolder() {
        Lease old = manager.tryAcquire("task-1", "holder-a", Duration.ofSeconds(10)).orElseThrow();
        clock.advance(Duration.ofSeconds(11)); // 旧租约到期
        Lease current = manager.tryAcquire("task-1", "holder-b", Duration.ofSeconds(60)).orElseThrow();
        log.info("[所有权更替] oldVersion={} newVersion={} 判定依据=version单调递增 fencing token",
                old.version(), current.version());
        assertEquals(old.version() + 1, current.version());

        // 被取代的旧持有者提交 -> OWNERSHIP_SUPERSEDED
        SubmitOutcome stale = manager.submitResult(old, "biz-1", "stale-result");
        log.info("[被取代持有者提交] version={} rejected={}", old.version(), stale.rejectReason());
        assertEquals(RejectReason.OWNERSHIP_SUPERSEDED, stale.rejectReason());
        assertEquals(TaskStatus.LEASED, manager.view("task-1").orElseThrow().status());

        // 新持有者提交成功，且不被旧持有者的迟到提交覆盖
        SubmitOutcome ok = manager.submitResult(current, "biz-1", "fresh-result");
        assertTrue(ok.accepted());
        SubmitOutcome late = manager.submitResult(old, "biz-1", "stale-result");
        assertFalse(late.accepted());
        assertEquals("fresh-result", manager.view("task-1").map(v -> "fresh-result").orElse(null));
        assertEquals(TaskStatus.SUCCEEDED, manager.view("task-1").orElseThrow().status());
    }

    @Test
    void expiredButNotYetReclaimedHolderSubmitRejectedAsLeaseExpired() {
        Lease lease = manager.tryAcquire("task-1", "holder-a", Duration.ofSeconds(10)).orElseThrow();
        clock.advance(Duration.ofSeconds(11)); // 到期但尚未回收，租约记录仍在
        SubmitOutcome outcome = manager.submitResult(lease, "biz-1", "too-late");
        log.info("[过期未回收提交] version={} rejected={} 判定依据=expiresAt<now",
                lease.version(), outcome.rejectReason());
        assertEquals(RejectReason.LEASE_EXPIRED, outcome.rejectReason());
        assertFalse(outcome.accepted());
    }

    @Test
    void invalidArgumentsRejected() {
        Lease lease = manager.tryAcquire("task-1", "holder-a", Duration.ofSeconds(10)).orElseThrow();
        assertEquals(RejectReason.INVALID_ARGUMENT,
                manager.submitResult(lease, "", "r").rejectReason());
        assertEquals(RejectReason.INVALID_ARGUMENT,
                manager.submitResult(lease, "biz-1", null).rejectReason());
        assertEquals(RejectReason.INVALID_ARGUMENT,
                manager.submitResult(null, "biz-1", "r").rejectReason());
        assertEquals(TaskStatus.LEASED, manager.view("task-1").orElseThrow().status(),
                "非法参数不得改变任务状态");
    }
}
