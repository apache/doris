package org.apache.doris.scheduler;

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.metadata.ChangeTaskRecord;
import org.apache.doris.scheduler.metadata.TaskRecord;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class TaskManager {

    private static final Logger LOG = LogManager.getLogger(TaskManager.class);

    // taskId -> pending TaskRun Queue, for each Task only support 1 running taskRun currently,
    // so the map value is priority queue need to be sorted by priority from large to small
    private final Map<Long, PriorityBlockingQueue<Task>> pendingTaskRunMap = Maps.newConcurrentMap();

    // taskId -> running TaskRun, for each Task only support 1 running taskRun currently,
    // so the map value is not queue
    private final Map<Long, Task> runningTaskRunMap = Maps.newConcurrentMap();

    // include SUCCESS/FAILED/CANCEL taskRun
    private final TaskHistory taskRunHistory = new TaskHistory();

    // Use to execute actual TaskRun
    private final TaskExecutor taskExecutor = new TaskExecutor();

    private final ReentrantLock taskRunLock = new ReentrantLock(true);

    public SubmitResult submitTaskRun(Task task, ExecuteOption option) {
        // duplicate submit
        if (task.getRecord() != null) {
            return new SubmitResult(task.getRecord().getQueryId(), SubmitResult.SubmitStatus.FAILED);
        }

        int validPendingCount = 0;
        for (Long taskId : pendingTaskRunMap.keySet()) {
            if (!pendingTaskRunMap.get(taskId).isEmpty()) {
                validPendingCount++;
            }
        }

        if (validPendingCount >= 1000) {
            LOG.warn("pending TaskRun exceeds task_runs_queue_length:{}, reject the submit.", 1000);
            return new SubmitResult(null, SubmitResult.SubmitStatus.REJECTED);
        }

        String queryId = UUID.randomUUID().toString();
        TaskRecord record = task.initRecord(queryId, System.currentTimeMillis());
        record.setPriority(option.getPriority());
        record.setMergeRedundant(option.isMergeRedundant());
        // GlobalStateMgr.getCurrentState().getEditLog().logTaskRunCreateStatus(status);
        arrangeTaskRun(task, option.isMergeRedundant());
        return new SubmitResult(queryId, SubmitResult.SubmitStatus.SUBMITTED);
    }


    public boolean killTaskRun(Long taskId) {
        Task taskRun = runningTaskRunMap.get(taskId);
        if (taskRun == null) {
            return false;
        }
        ConnectContext runCtx = taskRun.getRunCtx();
        if (runCtx != null) {
            runCtx.kill(false);
            return true;
        }
        return false;
    }

    // At present, only the manual and automatic tasks of the materialized view have different priorities.
    // The manual priority is higher. For manual tasks, we do not merge operations.
    // For automatic tasks, we will compare the definition, and if they are the same,
    // we will perform the merge operation.
    public void arrangeTaskRun(Task task, boolean mergeRedundant) {
        if (!tryTaskRunLock()) {
            return;
        }
        try {
            long taskId = task.getTaskId();
            PriorityBlockingQueue<Task> taskRuns =
                    pendingTaskRunMap.computeIfAbsent(taskId, u -> Queues.newPriorityBlockingQueue());
            if (mergeRedundant) {
                Task oldTaskRun = getTaskRun(taskRuns, task);
                if (oldTaskRun != null) {
                    // The remove here is actually remove the old TaskRun.
                    // Note that the old TaskRun and new TaskRun may have the same definition,
                    // but other attributes may be different, such as priority, creation time.
                    // higher priority and create time will be result after merge is complete
                    // and queryId will be change.
                    boolean isRemove = taskRuns.remove(task);
                    if (!isRemove) {
                        LOG.warn("failed to remove TaskRun definition is [{}]", task.getRecord().getDefinition());
                    }
                    if (oldTaskRun.getRecord().getPriority() > task.getRecord().getPriority()) {
                        task.getRecord().setPriority(oldTaskRun.getRecord().getPriority());
                    }
                    if (oldTaskRun.getRecord().getCreateTime() > task.getRecord().getCreateTime()) {
                        task.getRecord().setCreateTime(oldTaskRun.getRecord().getCreateTime());
                    }
                }
            }
            taskRuns.offer(task);
        } finally {
            taskRunUnlock();
        }
    }

    // Because java PriorityQueue does not provide an interface for searching by element,
    // so find it by code O(n), which can be optimized later
    @Nullable
    private Task getTaskRun(PriorityBlockingQueue<Task> taskRuns, Task taskRun) {
        Task oldTaskRun = null;
        for (Task run : taskRuns) {
            if (run.equals(taskRun)) {
                oldTaskRun = run;
                break;
            }
        }
        return oldTaskRun;
    }

    // check if a running TaskRun is complete and remove it from running TaskRun map
    public void checkRunningTaskRun() {
        Iterator<Long> runningIterator = runningTaskRunMap.keySet().iterator();
        while (runningIterator.hasNext()) {
            Long taskId = runningIterator.next();
            Task taskRun = runningTaskRunMap.get(taskId);
            if (taskRun == null) {
                LOG.warn("failed to get running TaskRun by taskId:{}", taskId);
                runningIterator.remove();
                return;
            }
            Future<?> future = taskRun.getFuture();
            if (future.isDone()) {
                runningIterator.remove();
                taskRunHistory.addHistory(taskRun.getRecord());
                ChangeTaskRecord statusChange =
                        new ChangeTaskRecord(taskRun.getTaskId(), taskRun.getRecord(), Utils.TaskState.RUNNING,
                                taskRun.getRecord().getState());
                // GlobalStateMgr.getCurrentState().getEditLog().logUpdateTaskRun(statusChange);
            }
        }
    }

    // schedule the pending TaskRun that can be run into running TaskRun map
    public void scheduledPendingTaskRun() {
        int currentRunning = runningTaskRunMap.size();

        Iterator<Long> pendingIterator = pendingTaskRunMap.keySet().iterator();
        while (pendingIterator.hasNext()) {
            Long taskId = pendingIterator.next();
            Task runningTaskRun = runningTaskRunMap.get(taskId);
            if (runningTaskRun == null) {
                Queue<Task> taskRunQueue = pendingTaskRunMap.get(taskId);
                if (taskRunQueue.size() == 0) {
                    pendingIterator.remove();
                } else {
                    if (currentRunning >= 1000) {
                        break;
                    }
                    Task pendingTaskRun = taskRunQueue.poll();
                    taskExecutor.executeTaskRun(pendingTaskRun);
                    runningTaskRunMap.put(taskId, pendingTaskRun);
                    // RUNNING state persistence is for FE FOLLOWER update state
                    ChangeTaskRecord statusChange =
                            new ChangeTaskRecord(taskId, pendingTaskRun.getRecord(), Utils.TaskState.PENDING,
                                    Utils.TaskState.RUNNING);
                    // GlobalStateMgr.getCurrentState().getEditLog().logUpdateTaskRun(statusChange);
                    currentRunning++;
                }
            }
        }
    }

    public boolean tryTaskRunLock() {
        try {
            return taskRunLock.tryLock(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("got exception while getting task run lock", e);
            Thread.currentThread().interrupt();
        }
        return false;
    }


    public void taskRunUnlock() {
        this.taskRunLock.unlock();
    }

    public Map<Long, PriorityBlockingQueue<Task>> getPendingTaskRunMap() {
        return pendingTaskRunMap;
    }

    public Map<Long, Task> getRunningTaskRunMap() {
        return runningTaskRunMap;
    }

    public TaskHistory getTaskRunHistory() {
        return taskRunHistory;
    }
}
