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

package org.apache.doris.stack.runner;

import com.google.common.util.concurrent.FutureCallback;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.stack.bean.SpringApplicationContext;
import org.apache.doris.stack.constants.ExecutionStatus;
import org.apache.doris.stack.constants.Flag;
import org.apache.doris.stack.dao.TaskInstanceRepository;
import org.apache.doris.stack.entity.TaskInstanceEntity;

import java.util.Date;

/**
 * task call back update status
 **/
@Slf4j
public class TaskExecCallback implements FutureCallback<Object> {

    private TaskContext taskContext;

    private TaskInstanceRepository taskInstanceRepository;

    public TaskExecCallback(TaskContext taskContext) {
        this.taskContext = taskContext;
        this.taskInstanceRepository = SpringApplicationContext.getBean(TaskInstanceRepository.class);
    }

    @Override
    public void onSuccess(Object result) {
        //change task status
        TaskInstanceEntity taskInstance = taskContext.getTaskInstance();
        taskInstance.setEndTime(new Date());
        taskInstance.setResult(String.valueOf(result));
        taskInstance.setStatus(ExecutionStatus.SUCCESS);
        taskInstance.setFinish(Flag.YES);
        taskInstanceRepository.save(taskInstance);
    }

    @Override
    public void onFailure(Throwable throwable) {
        TaskInstanceEntity taskInstance = taskContext.getTaskInstance();
        log.error("task {} in host {} execute error:", taskInstance.getTaskType().name(), taskInstance.getHost(), throwable);
        taskInstance.setEndTime(new Date());
        taskInstance.setResult(throwable.getMessage());
        taskInstance.setStatus(ExecutionStatus.FAILURE);
        taskInstanceRepository.save(taskInstance);
    }
}
