package com.github.lisama569.taskleaseschedulerjava.lease;

import com.github.lisama569.taskleaseschedulerjava.lease.model.Lease;
import com.github.lisama569.taskleaseschedulerjava.lease.store.CheckpointStore;

/** 任务执行体。实现内应通过 checkpoints 跳过已完成步骤，保证恢复时不重复执行。 */
@FunctionalInterface
public interface TaskHandler {

    /** @return 业务结果（将与业务键一起幂等提交） */
    String execute(Lease lease, CheckpointStore checkpoints) throws Exception;
}
