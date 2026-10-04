package com.github.lisama569.taskleaseschedulerjava.lease;

import java.time.Instant;

/**
 * 执行器在一次租约持有期间拿到的上下文。
 * <ul>
 *   <li>{@link #checkpoint()} 是协作式时限/打断检查点：执行器应在每个步骤前后调用，
 *       一旦超过执行时限或线程被中断，立即抛出确定性异常，绝不让任务“默默跑完”；</li>
 *   <li>{@link #isStepDone(String)} / {@link #markStepDone(String)} 提供可选的步骤级进度，
 *       进度带租约版本、跟持有者走。</li>
 * </ul>
 */
public interface ExecutionContext {

    String taskId();

    String ownerId();

    /** 当前租约版本（fencing token）。 */
    long version();

    /** 本次执行的时限终点。 */
    Instant deadline();

    /** 基于时间源判定是否已超过执行时限（测试可用可控时钟立即触发）。 */
    boolean isTimedOut();

    /** 执行线程是否已被外部中断（实例停机/抢占中止）。 */
    boolean isInterrupted();

    /** 协作检查点：超时抛 {@link ExecutionTimeoutException}，中断抛 {@link ExecutionInterruptedException}。 */
    void checkpoint();

    /** 当前持有者血缘中该步骤是否已完成（重试续跑时据此跳过）。 */
    boolean isStepDone(String step);

    /** 标记步骤完成（带所有权校验，旧持有者/过期持有者写入会被拒）。 */
    void markStepDone(String step);
}
