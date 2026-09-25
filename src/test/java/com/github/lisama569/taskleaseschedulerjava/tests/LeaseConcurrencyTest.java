package com.github.lisama569.taskleaseschedulerjava.tests;

import com.github.lisama569.taskleaseschedulerjava.lease.*;
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

/** 并发抢占：同一任务任意时刻只能有一个有效持有者，结果唯一且可判定。 */
class LeaseConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseConcurrencyTest.class);

    @Test
    void concurrentAcquire_exactlyOneWinner() throws Exception {
        MutableClock clock = new MutableClock(Instant.parse("2026-09-25T00:00:00Z"));
        LeaseManager manager = new LeaseManager(clock, RetryPolicy.defaults());
        manager.registerTask("task-1");

        int contenders = 16;
        ExecutorService pool = Executors.newFixedThreadPool(contenders);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<LeaseDecision>> futures = new ArrayList<>();
        for (int i = 0; i < contenders; i++) {
            String owner = "owner-" + i;
            futures.add(pool.submit(() -> {
                gate.await();
                return manager.acquire("task-1", owner, Duration.ofSeconds(10));
            }));
        }
        gate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        List<LeaseDecision> decisions = new ArrayList<>();
        for (Future<LeaseDecision> f : futures) {
            decisions.add(f.get());
        }
        List<LeaseDecision> winners = decisions.stream().filter(LeaseDecision::acquired).toList();
        List<LeaseDecision> losers = decisions.stream().filter(d -> !d.acquired()).toList();

        for (LeaseDecision d : decisions) {
            log.info("[concurrent-acquire] acquired={} version={} reason={} detail={}",
                    d.acquired(),
                    d.lease() == null ? "-" : d.lease().version(),
                    d.reason(), d.detail());
        }

        assertEquals(1, winners.size(), "并发抢占必须有且仅有一个胜者");
        assertEquals(contenders - 1, losers.size());
        assertTrue(losers.stream().allMatch(d -> d.reason() == RejectReason.LEASE_HELD_BY_OTHER),
                "所有失败者必须得到 LEASE_HELD_BY_OTHER 判定");
        assertEquals(1L, winners.get(0).lease().version(), "首次租约版本应为 1");

        TaskSnapshot snap = manager.snapshot("task-1").orElseThrow();
        assertEquals(TaskStatus.LEASED, snap.status());
        assertEquals(winners.get(0).lease().ownerId(), snap.ownerId());
    }

    @Test
    void concurrentAcquireAfterExpiry_newOwnerGetsBumpedVersion() throws Exception {
        MutableClock clock = new MutableClock(Instant.parse("2026-09-25T00:00:00Z"));
        LeaseManager manager = new LeaseManager(clock, RetryPolicy.defaults());
        manager.registerTask("task-2");

        LeaseDecision first = manager.acquire("task-2", "owner-A", Duration.ofSeconds(5));
        assertTrue(first.acquired());
        clock.advance(Duration.ofSeconds(6)); // 租约到期
        assertEquals(1, manager.reclaimExpired());

        int contenders = 8;
        ExecutorService pool = Executors.newFixedThreadPool(contenders);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<LeaseDecision>> futures = new ArrayList<>();
        for (int i = 0; i < contenders; i++) {
            String owner = "owner-B" + i;
            futures.add(pool.submit(() -> {
                gate.await();
                return manager.acquire("task-2", owner, Duration.ofSeconds(5));
            }));
        }
        gate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        AtomicInteger winCount = new AtomicInteger();
        for (Future<LeaseDecision> f : futures) {
            LeaseDecision d = f.get();
            log.info("[reacquire-after-expiry] acquired={} version={} reason={} detail={}",
                    d.acquired(), d.lease() == null ? "-" : d.lease().version(),
                    d.reason(), d.detail());
            if (d.acquired()) {
                winCount.incrementAndGet();
                assertEquals(2L, d.lease().version(), "所有权变更后 fencing token 必须单调递增");
            }
        }
        assertEquals(1, winCount.get());
    }
}
