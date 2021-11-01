package org.apache.doris.stack.component;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.manager.common.domain.TaskResult;
import org.apache.doris.manager.common.domain.TaskState;
import org.apache.doris.stack.constants.ExecutionStatus;
import org.apache.doris.stack.constants.Flag;
import org.apache.doris.stack.constants.ProcessTypeEnum;
import org.apache.doris.stack.constants.TaskTypeEnum;
import org.apache.doris.stack.dao.TaskInstanceRepository;
import org.apache.doris.stack.entity.TaskInstanceEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

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

    /**
     * refresh task status
     */
    public void refreshTask(TaskInstanceEntity taskInstance, RResult result) {
        if (result == null || result.getData() == null) {
            taskInstance.setStatus(ExecutionStatus.FAILURE);
        } else {
            TaskResult taskResult = JSON.parseObject(JSON.toJSONString(result.getData()), TaskResult.class);
            taskInstance.setExecutorId(taskResult.getTaskId());
            if (TaskState.RUNNING.equals(taskResult.getTaskState())) {
                taskInstance.setStatus(ExecutionStatus.RUNNING);
            } else if (taskResult.getRetCode() == 0) {
                taskInstance.setStatus(ExecutionStatus.SUCCESS);
                taskInstance.setFinish(Flag.YES);
            } else {
                taskInstance.setStatus(ExecutionStatus.FAILURE);
            }
        }
        taskInstanceRepository.save(taskInstance);
    }

    /**
     * Check whether the parent task is successful
     */
    public boolean checkParentTaskSuccess(int processId, ProcessTypeEnum processType) {
        ProcessTypeEnum parent = ProcessTypeEnum.findParent(processType);
        if (parent == null) {
            return true;
        }
        List<TaskInstanceEntity> taskInstanceEntities = taskInstanceRepository.queryTasksByProcessStep(processId, parent);
        for (TaskInstanceEntity task : taskInstanceEntities) {
            if (Flag.YES.equals(task.getFinish())) {
                log.info("task {} is unsuccess", task.getTaskType());
                return false;
            }
        }
        return true;
    }

    /**
     * query task by id
     */
    public TaskInstanceEntity queryTaskById(int taskId) {
        Optional<TaskInstanceEntity> optional = taskInstanceRepository.findById(taskId);
        if (optional.isPresent()) {
            return optional.get();
        }
        return null;
    }
}
