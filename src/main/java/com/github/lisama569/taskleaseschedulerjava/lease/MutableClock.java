package com.github.lisama569.taskleaseschedulerjava.lease;

import java.time.Duration;
import java.time.Instant;

/** 可控时间源：只允许向前推进，用于测试租约到期与退避。 */
public final class MutableClock implements Clock {
    private Instant now;

    public MutableClock(Instant start) {
        this.now = start;
    }

    @Override
    public synchronized Instant now() {
        return now;
    }

    public synchronized void advance(Duration d) {
        now = now.plus(d);
    }
}
