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

package org.apache.doris.stack.service;

import org.apache.doris.stack.entity.TaskInstanceEntity;
import org.apache.doris.stack.model.response.CurrentProcessResp;
import org.apache.doris.stack.model.response.TaskInstanceResp;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

public interface ProcessTask {

    /**
     * query user history installation progress
     * null means that nothing currently being installed
     */
    CurrentProcessResp currentProcess(HttpServletRequest request, HttpServletResponse response) throws Exception;

    List<TaskInstanceEntity> processProgress(HttpServletRequest request, HttpServletResponse response, int processId);

    List<TaskInstanceResp> taskProgress(HttpServletRequest request, HttpServletResponse response, int processId);

    void installComplete(HttpServletRequest request, HttpServletResponse response, int processId) throws Exception;

    void cancelProcess(HttpServletRequest request, HttpServletResponse response, int processId) throws Exception;

    void skipTask(int taskId);

    void retryTask(int taskId);

    /**
     * Refresh the task status on the agent side again
     */
    void refreshAgentTaskStatus(int processId);

    /**
     * fetch task info
     */
    Object taskInfo(int taskId);

    /**
     * fetch log
     */
    Object taskLog(int taskId);

    void backPrevious(int processId);
}
