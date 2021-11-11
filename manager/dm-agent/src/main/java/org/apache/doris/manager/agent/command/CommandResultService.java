// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.manager.agent.command;

import org.apache.doris.manager.agent.common.AgentConstants;
import org.apache.doris.manager.agent.service.ServiceContext;
import org.apache.doris.manager.agent.task.LRU;
import org.apache.doris.manager.agent.task.Task;
import org.apache.doris.manager.agent.task.TaskContext;
import org.apache.doris.manager.common.domain.CommandResult;
import org.apache.doris.manager.common.domain.CommandType;
import org.apache.doris.manager.common.domain.ServiceRole;
import org.apache.doris.manager.common.domain.TaskResult;
import org.apache.doris.manager.common.domain.TaskState;

import java.util.Objects;

public class CommandResultService {
    private static LRU<String, Integer> taskFinalStatus = new LRU<>(AgentConstants.COMMAND_HISTORY_SAVE_MAX_COUNT);

    public static CommandResult commandResult(String taskId) {
        if (Objects.isNull(TaskContext.getTaskByTaskId(taskId))) {
            return null;
        }

        Task task = TaskContext.getTaskByTaskId(taskId);
        CommandResult commandResult = new CommandResult(task.getTaskResult());

        switch (CommandType.valueOf(task.getTaskDesc().getTaskName())) {
            case INSTALL_FE:
            case INSTALL_BE:
            case WRITE_BE_CONF:
            case WRITE_FE_CONF:
                return commandResult;
            case START_FE:
                return fetchCommandResult(task, CommandType.START_FE);
            case STOP_FE:
                return fetchCommandResult(task, CommandType.STOP_FE);
            case START_BE:
                return fetchCommandResult(task, CommandType.START_BE);
            case STOP_BE:
                return fetchCommandResult(task, CommandType.STOP_BE);
            default:
                return null;
        }
    }

    private static CommandResult fetchCommandResult(Task task, CommandType commandType) {
        if (Objects.isNull(task.getTaskResult().getRetCode()) || task.getTaskResult().getRetCode() != 0) {
            return new CommandResult(task.getTaskResult());
        }

        TaskResult tmpTaskResult = new TaskResult(task.getTaskResult());

        Integer finalStatus = taskFinalStatus.get(task.getTaskId());
        if (Objects.nonNull(finalStatus)) {
            tmpTaskResult.setTaskState(TaskState.FINISHED);
            tmpTaskResult.setRetCode(finalStatus);
            return new CommandResult(tmpTaskResult);
        }

        int retCode = AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE;
        Boolean health = false;
        TaskState taskState = TaskState.RUNNING;
        if (CommandType.START_FE == commandType) {
            health = ServiceContext.getServiceMap().get(ServiceRole.FE).isHealth();
            retCode = health ? AgentConstants.COMMAND_EXECUTE_SUCCESS_CODE : AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE;
            taskState = health ? TaskState.FINISHED : TaskState.RUNNING;
        } else if (CommandType.STOP_FE == commandType) {
            health = ServiceContext.getServiceMap().get(ServiceRole.FE).isHealth();
            retCode = !health ? AgentConstants.COMMAND_EXECUTE_SUCCESS_CODE : AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE;
            taskState = !health ? TaskState.FINISHED : TaskState.RUNNING;
        } else if (CommandType.START_BE == commandType) {
            health = ServiceContext.getServiceMap().get(ServiceRole.BE).isHealth();
            retCode = health ? AgentConstants.COMMAND_EXECUTE_SUCCESS_CODE : AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE;
            taskState = health ? TaskState.FINISHED : TaskState.RUNNING;
        } else if (CommandType.STOP_BE == commandType) {
            health = ServiceContext.getServiceMap().get(ServiceRole.BE).isHealth();
            retCode = !health ? AgentConstants.COMMAND_EXECUTE_SUCCESS_CODE : AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE;
            taskState = !health ? TaskState.FINISHED : TaskState.RUNNING;
        }

        if (health && (CommandType.START_FE == commandType || CommandType.START_BE == commandType)) {
            taskFinalStatus.put(task.getTaskId(), AgentConstants.COMMAND_EXECUTE_SUCCESS_CODE);
        }
        if (!health && (CommandType.STOP_FE == commandType || CommandType.STOP_BE == commandType)) {
            taskFinalStatus.put(task.getTaskId(), AgentConstants.COMMAND_EXECUTE_SUCCESS_CODE);
        }

        if (task.getTaskResult().getSubmitTime().getTime() + AgentConstants.COMMAND_EXECUTE_TIMEOUT_MILSECOND < System.currentTimeMillis()) {
            taskFinalStatus.put(task.getTaskId(), AgentConstants.COMMAND_EXECUTE_TIMEOUT_CODE);
            tmpTaskResult.setTaskState(TaskState.FINISHED);
        }

        tmpTaskResult.setRetCode(retCode);
        tmpTaskResult.setTaskState(taskState);
        return new CommandResult(tmpTaskResult);
    }
}
