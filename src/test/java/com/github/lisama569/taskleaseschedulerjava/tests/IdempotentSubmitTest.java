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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** 结果提交：同一业务键幂等，重复/并发提交不产生重复副作用。 */
class IdempotentSubmitTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotentSubmitTest.class);

    private MutableClock clock;
    private LeaseManager manager;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-09-25T00:00:00Z"));
        manager = new LeaseManager(clock, RetryPolicy.defaults());
        manager.registerTask("t");
    }

    @Test
    void duplicateBusinessKey_noDuplicateSideEffect() {
        Lease lease = manager.acquire("t", "A", Duration.ofSeconds(30)).lease();
        AtomicInteger sideEffects = new AtomicInteger();

        SubmitOutcome first = submitWithSideEffect(lease, "bk-1", "r1", sideEffects);
        SubmitOutcome second = submitWithSideEffect(lease, "bk-1", "r1", sideEffects);
        SubmitOutcome third = submitWithSideEffect(lease, "bk-1", "r1", sideEffects);
        log.info("[idempotent] first={} second={} third={} sideEffects={}",
                first.kind(), second.kind(), third.kind(), sideEffects.get());

        assertEquals(SubmitOutcome.Kind.ACCEPTED, first.kind());
        assertEquals(SubmitOutcome.Kind.DUPLICATE, second.kind());
        assertEquals(SubmitOutcome.Kind.DUPLICATE, third.kind());
        assertEquals(1, sideEffects.get(), "同一业务键重复提交不得产生重复副作用");
    }

    @Test
    void concurrentDuplicateSubmit_exactlyOneSideEffect() throws Exception {
        Lease lease = manager.acquire("t", "A", Duration.ofSeconds(30)).lease();
        int threads = 12;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger sideEffects = new AtomicInteger();
        List<Future<SubmitOutcome>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                gate.await();
                return submitWithSideEffect(lease, "bk-hot", "payload", sideEffects);
            }));
        }
        gate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        int accepted = 0, duplicate = 0;
        for (Future<SubmitOutcome> f : futures) {
            SubmitOutcome o = f.get();
            log.info("[concurrent-submit] kind={} reason={}", o.kind(), o.reason());
            if (o.kind() == SubmitOutcome.Kind.ACCEPTED) accepted++;
            if (o.kind() == SubmitOutcome.Kind.DUPLICATE) duplicate++;
        }
        assertEquals(1, accepted, "并发下同业务键只能被接受一次");
        assertEquals(threads - 1, duplicate);
        assertEquals(1, sideEffects.get());
    }

    @Test
    void differentBusinessKeyAfterFinish_rejectedTaskFinished() {
        Lease lease = manager.acquire("t", "A", Duration.ofSeconds(30)).lease();
        manager.submitResult("t", "A", lease.version(), "bk-1", "r1");
        SubmitOutcome out = manager.submitResult("t", "A", lease.version(), "bk-2", "r2");
        log.info("[finished-submit] kind={} reason={} detail={}", out.kind(), out.reason(), out.detail());
        assertEquals(SubmitOutcome.Kind.REJECTED, out.kind());
        assertEquals(RejectReason.TASK_FINISHED, out.reason());
    }

    @Test
    void invalidArguments_rejected() {
        Lease lease = manager.acquire("t", "A", Duration.ofSeconds(30)).lease();
        SubmitOutcome blank = manager.submitResult("t", "A", lease.version(), " ", "x");
        assertEquals(RejectReason.INVALID_ARGUMENT, blank.reason());
        SubmitOutcome missing = manager.submitResult("no-such", "A", 1, "bk", "x");
        assertEquals(RejectReason.TASK_NOT_FOUND, missing.reason());
        LeaseDecision badAcquire = manager.acquire("t", " ", Duration.ofSeconds(1));
        assertEquals(RejectReason.INVALID_ARGUMENT, badAcquire.reason());
    }

    /** 模拟“先产生副作用再提交”的执行器：只有 ACCEPTED 时副作用才应生效。 */
    private SubmitOutcome submitWithSideEffect(Lease lease, String bk, String payload,
                                               AtomicInteger sideEffects) {
        SubmitOutcome out = manager.submitResult("t", lease.ownerId(), lease.version(), bk, payload);
        if (out.kind() == SubmitOutcome.Kind.ACCEPTED) {
            sideEffects.incrementAndGet();
        }
        return out;
    }
}
