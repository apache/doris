package org.apache.doris.scheduler;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.scheduler.metadata.Job;
import org.apache.doris.scheduler.metadata.TaskRecord;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Future;

public class Task implements Comparable<Task> {
    private static final Logger LOG = LogManager.getLogger(Task.class);

    private long taskId;

    private Map<String, String> properties;

    private Future<?> future;

    private Job job;

    public Job getJob() {
        return job;
    }

    public void setJob(Job job) {
        this.job = job;
    }

    private ConnectContext runCtx;

    private TaskProcessor processor;

    private TaskRecord record;

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Future<?> getFuture() {
        return future;
    }

    public void setFuture(Future<?> future) {
        this.future = future;
    }


    public TaskProcessor getProcessor() {
        return processor;
    }

    public void setProcessor(TaskProcessor processor) {
        this.processor = processor;
    }

    public boolean executeTaskRun() throws Exception {
        TaskContext taskRunContext = new TaskContext();
        taskRunContext.setDefinition(record.getDefinition());
        runCtx = new ConnectContext();
        runCtx.setDatabase(job.getDbName());
        runCtx.setQualifiedUser(record.getUser());
        runCtx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(job.getCreateUser(), "%"));
        runCtx.getState().reset();
        runCtx.setQueryId(Utils.genTUniqueId(UUID.fromString(record.getQueryId())));
        Map<String, String> taskRunContextProperties = Maps.newHashMap();

        taskRunContext.setCtx(runCtx);
        taskRunContext.setRemoteIp(runCtx.getMysqlChannel().getRemoteHostPortString());
        taskRunContext.setProperties(taskRunContextProperties);
        processor.process(taskRunContext);
        QueryState queryState = runCtx.getState();
        if (runCtx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            record.setErrorMessage(queryState.getErrorMessage());
            int errorCode = -1;
            if (queryState.getErrorCode() != null) {
                errorCode = queryState.getErrorCode().getCode();
            }
            record.setErrorCode(errorCode);
            return false;
        }
        return true;
    }

    public ConnectContext getRunCtx() {
        return runCtx;
    }

    public TaskRecord getRecord() {
        return record;
    }

    public TaskRecord initRecord(String queryId, Long createTime) {
        TaskRecord record = new TaskRecord();
        record.setQueryId(queryId);
        record.setTaskName(job.getName());
        if (createTime == null) {
            record.setCreateTime(System.currentTimeMillis());
        } else {
            record.setCreateTime(createTime);
        }
        record.setUser(job.getCreateUser());
        record.setDbName(job.getDbName());
        record.setDefinition(job.getDefinition());
        record.setExpireTime(System.currentTimeMillis() + 7 * 24 * 3600 * 1000L);
        this.record = record;
        return record;
    }

    @Override
    public int compareTo(@NotNull Task task) {
        if (this.getRecord().getPriority() != task.getRecord().getPriority()) {
            return task.getRecord().getPriority() - this.getRecord().getPriority();
        } else {
            return this.getRecord().getCreateTime() > task.getRecord().getCreateTime() ? 1 : -1;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Task task = (Task) o;
        return record.getDefinition().equals(task.getRecord().getDefinition());
    }

    @Override
    public int hashCode() {
        return Objects.hash(record);
    }
}
