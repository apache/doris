package org.apache.doris.stack.component;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.manager.common.domain.TaskResult;
import org.apache.doris.manager.common.domain.TaskState;
import org.apache.doris.stack.constants.ExecutionStatus;
import org.apache.doris.stack.constants.ProcessTypeEnum;
import org.apache.doris.stack.constants.TaskTypeEnum;
import org.apache.doris.stack.dao.TaskInstanceRepository;
import org.apache.doris.stack.entity.TaskInstanceEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TaskInstanceComponent {

    @Autowired
    private TaskInstanceRepository taskInstanceRepository;

    /**
     * The same host, the same tasktype, can only have one
     */
    public boolean checkTaskRunning(int processId, String host, ProcessTypeEnum processType, TaskTypeEnum taskType) {
        TaskInstanceEntity taskEntity = taskInstanceRepository.queryTask(processId, host, processType, taskType);
        if (taskEntity == null) {
            return false;
        } else if (taskEntity.getStatus().typeIsRunning()) {
            log.warn("task {} already running in host {}", taskType.name(), host);
            return true;
        } else {
            taskInstanceRepository.deleteById(taskEntity.getId());
            return false;
        }
    }

    /**
     * If the same task is already running on the host, skip it
     */
    public TaskInstanceEntity saveTask(int processId, String host, ProcessTypeEnum processType, TaskTypeEnum taskType, ExecutionStatus status) {
        if (!checkTaskRunning(processId, host, processType, taskType)) {
            return taskInstanceRepository.save(new TaskInstanceEntity(processId, host, processType, taskType, status));
        } else {
            return null;
        }
    }

    public void refreshTask(TaskInstanceEntity taskInstance, RResult result) {
        if (result == null || result.getData() == null) {
            taskInstance.setStatus(ExecutionStatus.FAILURE);
        } else {
            TaskResult taskResult = JSON.parseObject(JSON.toJSONString(result.getData()), TaskResult.class);
            taskInstance.setExecutorId(taskResult.getTaskId());
            if (TaskState.FINISHED.equals(taskResult.getTaskState())) {
                taskInstance.setStatus(ExecutionStatus.SUCCESS);
            } else {
                taskInstance.setStatus(ExecutionStatus.RUNNING);
            }
        }
        taskInstanceRepository.save(taskInstance);
    }
}
