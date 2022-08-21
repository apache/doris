package org.apache.doris.scheduler;

import org.apache.doris.scheduler.metadata.TaskRecord;

import com.google.common.collect.Queues;

import java.util.Deque;

public class TaskHistory {
    private final Deque<TaskRecord> historyDeque = Queues.newLinkedBlockingDeque();

    public void addHistory(TaskRecord record) {
        historyDeque.addFirst(record);
    }

    public Deque<TaskRecord> getAllHistory() {
        return historyDeque;
    }
}
