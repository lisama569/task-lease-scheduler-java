package com.github.lisama569.taskleaseschedulerjava.lease;

import java.time.Instant;

/** 生产环境使用的真实时钟。 */
public final class SystemClock implements Clock {
    @Override
    public Instant now() {
        return Instant.now();
    }
}
