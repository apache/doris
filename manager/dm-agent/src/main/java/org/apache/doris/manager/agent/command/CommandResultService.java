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
import org.apache.doris.manager.agent.register.BeState;
import org.apache.doris.manager.agent.register.FeState;
import org.apache.doris.manager.agent.task.Task;
import org.apache.doris.manager.agent.task.TaskContext;
import org.apache.doris.manager.agent.task.TaskResult;
import org.apache.doris.manager.common.domain.CommandType;

import java.util.Objects;

public class CommandResultService {
    public static CommandResult commandResult(String taskId) {
        if (Objects.isNull(TaskContext.getTaskByTaskId(taskId))) {
            return null;
        }

        Task task = TaskContext.getTaskByTaskId(taskId);
        CommandResult commandResult = new CommandResult(task.getTaskResult(),
                task.getTasklog().allStdLog(),
                task.getTasklog().allErrLog());

        switch (CommandType.valueOf(task.getTaskDesc().getTaskName())) {
            case INSTALL_FE:
            case INSTALL_BE:
                return commandResult;
            case START_FE:
                return feStateResult(task, false);
            case STOP_FE:
                return feStateResult(task, true);
            case START_BE:
                return beStateResult(task, false);
            case STOP_BE:
                return beStateResult(task, true);
            default:
                return null;
        }
    }

    private static CommandResult feStateResult(Task task, boolean isStop) {
        if (Objects.isNull(task.getTaskResult().getRetCode()) || task.getTaskResult().getRetCode() != 0) {
            return new CommandResult(task.getTaskResult(),
                    task.getTasklog().allStdLog(),
                    task.getTasklog().allErrLog());
        }

        // todo: save final status
        TaskResult tmpTaskResult = new TaskResult(task.getTaskResult());
        boolean health = FeState.isHealth();
        if (isStop) {
            tmpTaskResult.setRetCode(!health ? 0 : AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE);
        } else {
            tmpTaskResult.setRetCode(health ? 0 : AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE);
        }

        return new CommandResult(tmpTaskResult,
                task.getTasklog().allStdLog(),
                task.getTasklog().allErrLog());
    }

    private static CommandResult beStateResult(Task task, boolean isStop) {
        if (Objects.isNull(task.getTaskResult().getRetCode()) || task.getTaskResult().getRetCode() != 0) {
            return new CommandResult(task.getTaskResult(),
                    task.getTasklog().allStdLog(),
                    task.getTasklog().allErrLog());
        }

        // todo: save final status
        TaskResult tmpTaskResult = new TaskResult(task.getTaskResult());
        boolean health = BeState.isHealth();
        if (isStop) {
            tmpTaskResult.setRetCode(!health ? 0 : AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE);
        } else {
            tmpTaskResult.setRetCode(health ? 0 : AgentConstants.COMMAND_EXECUTE_UNHEALTH_CODE);
        }

        return new CommandResult(task.getTaskResult(),
                task.getTasklog().allStdLog(),
                task.getTasklog().allErrLog());
    }
}
