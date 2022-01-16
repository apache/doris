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
            case WRITE_BROKER_CONF:
            case INSTALL_BROKER:
                return commandResult;
            case START_FE:
                return fetchCommandResult(task, CommandType.START_FE);
            case STOP_FE:
                return fetchCommandResult(task, CommandType.STOP_FE);
            case START_BE:
                return fetchCommandResult(task, CommandType.START_BE);
            case STOP_BE:
                return fetchCommandResult(task, CommandType.STOP_BE);
            case START_BROKER:
                return fetchCommandResult(task, CommandType.START_BROKER);
            case STOP_BROKER:
                return fetchCommandResult(task, CommandType.STOP_BROKER);
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

        if (Objects.isNull(task.getTaskResult().getStartTime())
                || task.getTaskResult().getStartTime().getTime() + AgentConstants.COMMAND_EXECUTE_DETECT_DURATION_MILSECOND > System.currentTimeMillis()) {
            tmpTaskResult.setTaskState(TaskState.RUNNING);
            tmpTaskResult.setRetCode(AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE);
            return new CommandResult(tmpTaskResult);
        }

        int retCode = AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE;
        Boolean health = false;
        if (CommandType.START_FE == commandType) {
            health = ServiceContext.getServiceMap().get(ServiceRole.FE).processExist();
            retCode = health ? AgentConstants.COMMAND_EXECUTE_SUCCESS_CODE : AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE;
        } else if (CommandType.STOP_FE == commandType) {
            health = ServiceContext.getServiceMap().get(ServiceRole.FE).processExist();
            retCode = !health ? AgentConstants.COMMAND_EXECUTE_SUCCESS_CODE : AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE;
        } else if (CommandType.START_BE == commandType) {
            health = ServiceContext.getServiceMap().get(ServiceRole.BE).processExist();
            retCode = health ? AgentConstants.COMMAND_EXECUTE_SUCCESS_CODE : AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE;
        } else if (CommandType.STOP_BE == commandType) {
            health = ServiceContext.getServiceMap().get(ServiceRole.BE).processExist();
            retCode = !health ? AgentConstants.COMMAND_EXECUTE_SUCCESS_CODE : AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE;
        } else if (CommandType.START_BROKER == commandType) {
            health = ServiceContext.getServiceMap().get(ServiceRole.BROKER).processExist();
            retCode = health ? AgentConstants.COMMAND_EXECUTE_SUCCESS_CODE : AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE;
        } else if (CommandType.STOP_BROKER == commandType) {
            health = ServiceContext.getServiceMap().get(ServiceRole.BROKER).processExist();
            retCode = !health ? AgentConstants.COMMAND_EXECUTE_SUCCESS_CODE : AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE;
        }

        if (health && (CommandType.START_FE == commandType || CommandType.START_BE == commandType || CommandType.START_BROKER == commandType)) {
            taskFinalStatus.put(task.getTaskId(), AgentConstants.COMMAND_EXECUTE_SUCCESS_CODE);
        }
        if (!health && (CommandType.STOP_FE == commandType || CommandType.STOP_BE == commandType || CommandType.STOP_BROKER == commandType)) {
            taskFinalStatus.put(task.getTaskId(), AgentConstants.COMMAND_EXECUTE_SUCCESS_CODE);
        }

        tmpTaskResult.setRetCode(retCode);
        tmpTaskResult.setTaskState(TaskState.FINISHED);
        return new CommandResult(tmpTaskResult);
    }
}
