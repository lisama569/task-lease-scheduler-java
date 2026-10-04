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

/** 并发抢占只有一个赢家：日志中必须能看出每次判定的租约版本与依据。 */
class AcquireWinnerLoggingTest {

    private static final Logger log = LoggerFactory.getLogger(AcquireWinnerLoggingTest.class);

    @Test
    void onlyOneWinner_andEveryDecisionLoggedWithVersionAndReason() throws Exception {
        MutableClock clock = new MutableClock(Instant.parse("2026-10-05T00:00:00Z"));
        LeaseManager manager = new LeaseManager(clock, RetryPolicy.defaults());
        manager.registerTask("hot");

        int contenders = 24;
        ExecutorService pool = Executors.newFixedThreadPool(contenders);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<LeaseDecision>> futures = new ArrayList<>();
        for (int i = 0; i < contenders; i++) {
            String owner = "node-" + i;
            futures.add(pool.submit(() -> {
                gate.await();
                return manager.acquire("hot", owner, Duration.ofSeconds(15));
            }));
        }
        gate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        AtomicInteger winners = new AtomicInteger();
        for (Future<LeaseDecision> f : futures) {
            LeaseDecision d = f.get();
            long version = d.lease() == null ? -1L : d.lease().version();
            // 关键：每次并发判定都带租约版本与依据
            log.info("[acquire-verdict] acquired={} ownerVersion={} reason={} detail={}",
                    d.acquired(), version, d.reason(), d.detail());
            if (d.acquired()) {
                winners.incrementAndGet();
                assertEquals(1L, version, "唯一赢家拿到 v1");
            } else {
                assertEquals(RejectReason.LEASE_HELD_BY_OTHER, d.reason());
                assertNotNull(d.detail());
                assertTrue(d.detail().contains("version=1"), "失败者的依据必须指明当前赢家版本");
            }
        }
        assertEquals(1, winners.get(), "并发抢占只能有一个赢家");
        assertEquals(contenders - 1, futures.size() - winners.get());
    }
}
