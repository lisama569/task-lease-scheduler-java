package com.github.lisama569.taskleaseschedulerjava.lease.store;

import com.github.lisama569.taskleaseschedulerjava.lease.model.TaskRecord;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 本地共享状态：以进程内 ConcurrentHashMap 模拟多实例共享的存储。
 * 所有对单条记录的原子判定+修改都通过 mutate 在记录级锁内完成，
 * 保证并发抢占得到唯一且可判定的结果。
 */
public final class LeaseStore {

    private final Map<String, TaskRecord> records = new ConcurrentHashMap<>();

    public TaskRecord createIfAbsent(String taskId, String businessKey) {
        return records.computeIfAbsent(taskId, id -> new TaskRecord(id, businessKey));
    }

    public Optional<TaskRecord> find(String taskId) {
        return Optional.ofNullable(records.get(taskId));
    }

    public Collection<TaskRecord> all() {
        return records.values();
    }

    /** 在记录级监视器锁内执行 判定+状态迁移，返回迁移结果。 */
    public <R> R mutate(String taskId, Function<TaskRecord, R> mutation) {
        TaskRecord record = records.get(taskId);
        if (record == null) {
            return null;
        }
        synchronized (record) {
            return mutation.apply(record);
        }
    }
}
