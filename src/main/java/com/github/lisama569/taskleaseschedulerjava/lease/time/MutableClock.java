package com.github.lisama569.taskleaseschedulerjava.lease.time;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** 可控时间源：测试中通过 advance 推进时间，避免真实等待。 */
public final class MutableClock extends Clock {

    private Instant now;
    private ZoneId zone;

    public MutableClock() {
        this(Instant.now(), ZoneOffset.UTC);
    }

    public MutableClock(Instant start, ZoneId zone) {
        this.now = start;
        this.zone = zone;
    }

    public void setInstant(Instant instant) {
        this.now = instant;
    }

    public void advance(Duration duration) {
        this.now = this.now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        this.zone = zone;
        return this;
    }

    @Override
    public Instant instant() {
        return now;
    }
}
