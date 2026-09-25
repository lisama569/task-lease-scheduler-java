package com.github.lisama569.taskleaseschedulerjava.lease;

import java.time.Duration;

/** 重试策略：次数上限 + 指数退避上限。 */
public record RetryPolicy(int maxAttempts, Duration baseBackoff, Duration maxBackoff) {

    public static RetryPolicy defaults() {
        return new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(5));
    }

    /** 第 attempt 次失败（从 1 计）后的退避时长，封顶 maxBackoff。 */
    public Duration backoffFor(int attempt) {
        long shift = Math.min(Math.max(attempt - 1, 0), 20);
        long millis = baseBackoff.toMillis() << shift;
        return Duration.ofMillis(Math.min(millis, maxBackoff.toMillis()));
    }
}
