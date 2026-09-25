package com.github.lisama569.taskleaseschedulerjava.lease.model;

import java.time.Duration;

/** 重试策略：次数上限 + 指数退避上限。 */
public record RetryPolicy(int maxAttempts, Duration baseBackoff, Duration maxBackoff) {

    public static RetryPolicy defaults() {
        return new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(5));
    }

    /** 第 attempt 次失败后的退避时长（attempt 从 1 开始），指数增长并以 maxBackoff 封顶。 */
    public Duration backoffFor(int attempt) {
        if (attempt < 1) {
            attempt = 1;
        }
        Duration backoff = baseBackoff;
        for (int i = 1; i < attempt; i++) {
            backoff = backoff.multipliedBy(2);
            if (backoff.compareTo(maxBackoff) >= 0) {
                return maxBackoff;
            }
        }
        return backoff.compareTo(maxBackoff) > 0 ? maxBackoff : backoff;
    }

    /** 失败次数是否已耗尽重试上限。 */
    public boolean exhausted(int attemptCount) {
        return attemptCount >= maxAttempts;
    }
}
