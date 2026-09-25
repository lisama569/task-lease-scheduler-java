package com.github.lisama569.taskleaseschedulerjava.tests;

import com.github.lisama569.taskleaseschedulerjava.lease.LeaseManager;
import com.github.lisama569.taskleaseschedulerjava.lease.model.Lease;
import com.github.lisama569.taskleaseschedulerjava.lease.model.RetryPolicy;
import com.github.lisama569.taskleaseschedulerjava.lease.model.TaskStatus;
import com.github.lisama569.taskleaseschedulerjava.lease.store.LeaseStore;
import com.github.lisama569.taskleaseschedulerjava.lease.time.MutableClock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrentAcquireTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentAcquireTest.class);

    private LeaseManager newManager(MutableClock clock) {
        LeaseStore store = new LeaseStore();
        LeaseManager manager = new LeaseManager(store, clock, RetryPolicy.defaults());
        manager.registerTask("task-1", "biz-1");
        return manager;
    }

    @Test
    void onlyOneWinnerAmongConcurrentAcquirers() throws Exception {
        MutableClock clock = new MutableClock(Instant.parse("2026-09-25T00:00:00Z"),
                java.time.ZoneOffset.UTC);
        LeaseManager manager = newManager(clock);

        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger winners = new AtomicInteger();
        ConcurrentLinkedQueue<String> winnerHolders = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < threads; i++) {
            String holderId = "holder-" + i;
            pool.submit(() -> {
                try {
                    start.await();
                    Optional<Lease> lease = manager.tryAcquire("task-1", holderId, Duration.ofSeconds(30));
                    lease.ifPresent(l -> {
                        winners.incrementAndGet();
                        winnerHolders.add(l.holderId());
                        // 判定依据：唯一持有者以租约 version 与 holder 标识可判定
                        log.info("[抢占成功] holder={} task={} leaseVersion={} expiresAt={} 判定依据=记录锁内CAS状态PENDING->LEASED且version单调递增",
                                l.holderId(), l.taskId(), l.version(), l.expiresAt());
                    });
                    if (lease.isEmpty()) {
                        var view = manager.view("task-1").orElseThrow();
                        log.info("[抢占失败] holder={} 当前持有者={} 当前version={} 状态={} 判定依据=记录状态为LEASED且租约未到期",
                                holderId, view.holderId(), view.leaseVersion(), view.status());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();

        assertEquals(1, winners.get(), "并发抢占必须只有一个获胜者");
        assertEquals(1, winnerHolders.size());

        var view = manager.view("task-1").orElseThrow();
        assertEquals(TaskStatus.LEASED, view.status());
        assertEquals(1L, view.leaseVersion(), "首次获取的租约版本为1");
        assertEquals(winnerHolders.peek(), view.holderId());
    }

    @Test
    void renewKeepsOwnershipAndExpiredHolderCannotRenew() {
        MutableClock clock = new MutableClock(Instant.parse("2026-09-25T00:00:00Z"),
                java.time.ZoneOffset.UTC);
        LeaseManager manager = newManager(clock);

        Lease lease = manager.tryAcquire("task-1", "a", Duration.ofSeconds(10)).orElseThrow();
        clock.advance(Duration.ofSeconds(5));
        Lease renewed = manager.renew(lease, Duration.ofSeconds(20)).orElseThrow();
        assertEquals(lease.version(), renewed.version(), "续约不改变 version");
        assertEquals(20L, Duration.between(clock.instant(), renewed.expiresAt()).getSeconds());

        clock.advance(Duration.ofSeconds(31));
        assertTrue(manager.renew(renewed, Duration.ofSeconds(10)).isEmpty(),
                "过期租约续约必须失败");
        Lease other = manager.tryAcquire("task-1", "b", Duration.ofSeconds(10)).orElseThrow();
        assertEquals(2L, other.version(), "过期后新持有者的版本递增");

        // 旧持有者续约失败，状态归属可查询
        var view = manager.view("task-1").orElseThrow();
        assertEquals("b", view.holderId());
        assertEquals(2L, view.leaseVersion());
    }

    @Test
    void releaseReturnsTaskToPending() {
        MutableClock clock = new MutableClock(Instant.parse("2026-09-25T00:00:00Z"),
                java.time.ZoneOffset.UTC);
        LeaseManager manager = newManager(clock);

        Lease lease = manager.tryAcquire("task-1", "a", Duration.ofSeconds(10)).orElseThrow();
        assertTrue(manager.release(lease));
        assertEquals(TaskStatus.PENDING, manager.view("task-1").orElseThrow().status());
        Lease next = manager.tryAcquire("task-1", "b", Duration.ofSeconds(10)).orElseThrow();
        assertEquals(2L, next.version());
        assertFalse(manager.release(lease), "旧版本持有者释放必须被拒绝");
    }
}
