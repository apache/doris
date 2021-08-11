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
package org.apache.doris.manager.agent.api;

import org.apache.doris.manager.agent.command.CommandFactory;
import org.apache.doris.manager.agent.common.AgentConstants;
import org.apache.doris.manager.common.domain.CommandRequest;
import org.apache.doris.manager.agent.command.CommandResult;
import org.apache.doris.manager.agent.command.CommandResultService;
import org.apache.doris.manager.common.domain.CommandType;
import org.apache.doris.manager.agent.task.Task;
import org.apache.doris.manager.agent.task.TaskContext;
import org.apache.doris.manager.common.domain.RResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;

@RestController
@RequestMapping("/command")
public class CommandController {

    @PostMapping("/execute")
    public RResult execute(@RequestBody CommandRequest commandRequest) {
        if (Objects.isNull(CommandType.findByName(commandRequest.getCommandType()))) {
            return RResult.error("unkonwn command");
        }
        Task task = CommandFactory.get(commandRequest).setup().execute();
        CommandResult commandResult = new CommandResult(task.getTaskResult(),
                task.getTasklog().allStdLog(),
                task.getTasklog().allErrLog());
        return RResult.success(commandResult);
    }

    @GetMapping("/result")
    public RResult commandResult(@RequestParam String taskId) {
        return RResult.success(CommandResultService.commandResult(taskId));
    }

    @GetMapping("/stdlog")
    public RResult getTaskStdLog(@RequestParam String taskId, @RequestParam int offset) {
        return RResult.success(TaskContext.getTaskByTaskId(taskId) == null ? null : TaskContext.getTaskByTaskId(taskId).getTasklog().stdLog(offset, AgentConstants.COMMAND_LOG_PAGE_SIZE));
    }

    @GetMapping("/errlog")
    public RResult getTaskErrLog(@RequestParam String taskId, @RequestParam int offset) {
        return RResult.success(TaskContext.getTaskByTaskId(taskId) == null ? null : TaskContext.getTaskByTaskId(taskId).getTasklog().errLog(offset, AgentConstants.COMMAND_LOG_PAGE_SIZE));
    }
}
