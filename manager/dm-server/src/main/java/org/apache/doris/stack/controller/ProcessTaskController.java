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

package org.apache.doris.stack.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.stack.entity.TaskInstanceEntity;
import org.apache.doris.stack.model.response.CurrentProcessResp;
import org.apache.doris.stack.model.response.TaskInstanceResp;
import org.apache.doris.stack.service.ProcessTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

/**
 * process task controller
 **/
@Api(tags = "Process Task API")
@RestController
@RequestMapping("/api/process")
public class ProcessTaskController {

    @Autowired
    private ProcessTask processTask;

    /**
     * query user current installation progress
     */
    @ApiOperation(value = "query user current installation process")
    @RequestMapping(value = "/currentProcess", method = RequestMethod.GET)
    public RResult currentProcess(HttpServletRequest request, HttpServletResponse response) throws Exception {
        CurrentProcessResp process = processTask.currentProcess(request, response);
        return RResult.success(process);
    }

    /**
     * Installation progress of the current process
     */
    @ApiOperation(value = "query user history installation progress")
    @RequestMapping(value = "/{processId}/progress", method = RequestMethod.GET)
    public RResult processProgress(HttpServletRequest request, HttpServletResponse response,
                                   @PathVariable(value = "processId") int processId) {
        List<TaskInstanceEntity> tasks = processTask.processProgress(request, response, processId);
        return RResult.success(tasks);
    }

    /**
     * Query the installation status of tasks in the current installation process
     */
    @ApiOperation(value = "Query the installation status of tasks in the current installation process")
    @RequestMapping(value = "/{processId}/currentTasks", method = RequestMethod.GET)
    public RResult taskProgress(HttpServletRequest request, HttpServletResponse response,
                                @PathVariable(value = "processId") int processId) {
        List<TaskInstanceResp> tasks = processTask.taskProgress(request, response, processId);
        return RResult.success(tasks);
    }

    /**
     * After the installation is complete, call the interface
     */
    @ApiOperation(value = "After the installation is complete, call the interface")
    @RequestMapping(value = "/installComplete/{processId}", method = RequestMethod.POST)
    public RResult installComplete(HttpServletRequest request, HttpServletResponse response,
                                   @PathVariable(value = "processId") int processId) throws Exception {
        processTask.installComplete(request, response, processId);
        return RResult.success();
    }

    /**
     * cancel process
     */
    @ApiOperation(value = "cancel process")
    @RequestMapping(value = "/cancel/{processId}", method = RequestMethod.POST)
    public RResult cancelProcess(HttpServletRequest request, HttpServletResponse response,
                                 @PathVariable(value = "processId") int processId) throws Exception {
        processTask.cancelProcess(request, response, processId);
        return RResult.success();
    }

    /**
     * Skip task when task fails
     */
    @ApiOperation(value = "Skip task when task fails")
    @RequestMapping(value = "/task/skip/{taskId}", method = RequestMethod.POST)
    public RResult skipTask(@PathVariable(value = "taskId") int taskId) {
        processTask.skipTask(taskId);
        return RResult.success();
    }

    /**
     * retry task when task fails
     */
    @ApiOperation(value = "retry task when task fails")
    @RequestMapping(value = "/task/retry/{taskId}", method = RequestMethod.POST)
    public RResult retryTask(@PathVariable(value = "taskId") int taskId) {
        processTask.retryTask(taskId);
        return RResult.success();
    }

    /**
     * request task info
     */
    @RequestMapping(value = "/task/info/{taskId}", method = RequestMethod.GET)
    public RResult taskInfo(@PathVariable int taskId) {
        return RResult.success(processTask.taskInfo(taskId));
    }

    /**
     * request task stdout log
     */
    @RequestMapping(value = "/task/log/{taskId}", method = RequestMethod.GET)
    public RResult taskLog(@PathVariable int taskId) {
        return RResult.success(processTask.taskLog(taskId));
    }

    /**
     * Go back to the previous step
     */
    @RequestMapping(value = "/back/{processId}", method = RequestMethod.POST)
    public RResult backPrevious(@PathVariable int processId) {
        processTask.backPrevious(processId);
        return RResult.success();
    }
}
