package com.github.lisama569.taskleaseschedulerjava.lease.store;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 执行进度检查点：记录任务已完成的步骤，恢复时已完成部分不得重复执行。
 */
public final class CheckpointStore {

    private final Map<String, Set<String>> doneSteps = new ConcurrentHashMap<>();

    public boolean isDone(String taskId, String step) {
        return doneSteps.getOrDefault(taskId, Set.of()).contains(step);
    }

    public void markDone(String taskId, String step) {
        doneSteps.computeIfAbsent(taskId, k -> ConcurrentHashMap.newKeySet()).add(step);
    }
}
