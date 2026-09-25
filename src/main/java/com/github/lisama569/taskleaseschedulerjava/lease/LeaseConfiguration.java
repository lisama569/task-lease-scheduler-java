package com.github.lisama569.taskleaseschedulerjava.lease;

import com.github.lisama569.taskleaseschedulerjava.lease.model.RetryPolicy;
import com.github.lisama569.taskleaseschedulerjava.lease.store.CheckpointStore;
import com.github.lisama569.taskleaseschedulerjava.lease.store.LeaseStore;
import com.github.lisama569.taskleaseschedulerjava.lease.time.MutableClock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LeaseConfiguration {

    @Bean
    public MutableClock mutableClock() {
        return new MutableClock();
    }

    @Bean
    public LeaseStore leaseStore() {
        return new LeaseStore();
    }

    @Bean
    public CheckpointStore checkpointStore() {
        return new CheckpointStore();
    }

    @Bean
    public LeaseManager leaseManager(LeaseStore store, MutableClock clock) {
        return new LeaseManager(store, clock, RetryPolicy.defaults());
    }
}
