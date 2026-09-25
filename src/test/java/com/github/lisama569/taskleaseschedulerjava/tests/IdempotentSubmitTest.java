package com.github.lisama569.taskleaseschedulerjava.tests;

import com.github.lisama569.taskleaseschedulerjava.lease.LeaseManager;
import com.github.lisama569.taskleaseschedulerjava.lease.model.Lease;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class IdempotentSubmitTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotentSubmitTest.class);

    private final MutableClock clock =
            new MutableClock(Instant.parse("2026-09-25T00:00:00Z"), ZoneOffset.UTC);
    private final LeaseManager manager =
            new LeaseManager(new LeaseStore(), clock, RetryPolicy.defaults());

    {
        manager.registerTask("task-1", "biz-1");
    }

    @Test
    void duplicateSubmitWithSameBusinessKeyHasNoSideEffect() {
        Lease lease = manager.tryAcquire("task-1", "holder-a", Duration.ofSeconds(30)).orElseThrow();

        SubmitOutcome first = manager.submitResult(lease, "biz-1", "result-v1");
        assertTrue(first.accepted());
        assertEquals(TaskStatus.SUCCEEDED, manager.view("task-1").orElseThrow().status());

        // 同一业务键重试提交：幂等命中，返回首次结果，无重复副作用
        SubmitOutcome retry = manager.submitResult(lease, "biz-1", "result-v1");
        log.info("[重复提交] businessKey=biz-1 duplicate={} result={} 判定依据=任务已SUCCEEDED且resultBusinessKey一致",
                retry.duplicate(), retry.result());
        assertTrue(retry.duplicate());
        assertFalse(retry.accepted());
        assertEquals("result-v1", retry.result());

        // 业务键不同：拒绝而非覆盖
        SubmitOutcome conflict = manager.submitResult(lease, "biz-other", "result-v2");
        assertFalse(conflict.accepted());
        assertEquals(TaskStatus.SUCCEEDED, manager.view("task-1").orElseThrow().status());
    }

    @Test
    void concurrentDuplicateSubmitsProduceExactlyOneAccept() throws Exception {
        Lease lease = manager.tryAcquire("task-1", "holder-a", Duration.ofSeconds(30)).orElseThrow();

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    SubmitOutcome outcome = manager.submitResult(lease, "biz-1", "result-v1");
                    if (outcome.accepted()) {
                        accepted.incrementAndGet();
                        log.info("[并发提交-接受] version={} 判定依据=记录锁内状态LEASED->SUCCEEDED仅发生一次",
                                lease.version());
                    } else if (outcome.duplicate()) {
                        duplicates.incrementAndGet();
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

        log.info("[并发提交汇总] accepted={} duplicate={} 并发度={}", accepted.get(), duplicates.get(), threads);
        assertEquals(1, accepted.get(), "并发重复提交只能有一个被接受");
        assertEquals(threads - 1, duplicates.get(), "其余提交必须幂等命中");
        assertEquals(TaskStatus.SUCCEEDED, manager.view("task-1").orElseThrow().status());
    }
}
