package com.github.lisama569.taskleaseschedulerjava.lease;

import java.time.Instant;

/** 可替换的时间源，测试中用可控实现避免真实等待。 */
public interface Clock {
    Instant now();
}
