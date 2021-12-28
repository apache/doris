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

import lombok.extern.slf4j.Slf4j;
import org.apache.doris.stack.bean.SpringApplicationContext;
import org.apache.doris.stack.constants.ExecutionStatus;
import org.apache.doris.stack.constants.TaskTypeEnum;
import org.apache.doris.stack.dao.TaskInstanceRepository;
import org.apache.doris.stack.entity.TaskInstanceEntity;
import org.apache.doris.stack.task.AbstractTask;
import org.apache.doris.stack.task.InstallAgentTask;

import java.util.concurrent.Callable;

/**
 * task execute thread
 **/
@Slf4j
public class TaskExecuteThread implements Callable<Object> {

    private AbstractTask task;

    private TaskContext taskContext;

    private TaskInstanceRepository taskInstanceRepository;

    public TaskExecuteThread(TaskContext taskContext) {
        this.taskContext = taskContext;
        this.taskInstanceRepository = SpringApplicationContext.getBean(TaskInstanceRepository.class);
    }

    @Override
    public Object call() throws Exception {
        updateStatus();
        task = initTask();
        task.init();
        try {
            return task.handle();
        } finally {
            task.after();
        }
    }

    private void updateStatus() {
        TaskInstanceEntity taskInstance = taskContext.getTaskInstance();
        taskInstance.setStatus(ExecutionStatus.RUNNING);
        taskInstanceRepository.save(taskInstance);
    }

    public AbstractTask initTask() {
        TaskTypeEnum taskType = taskContext.getTaskType();
        switch (taskType) {
            case INSTALL_AGENT:
                return new InstallAgentTask(taskContext);
            default:
                log.error("not support task type: {}", taskType.name());
                throw new IllegalArgumentException("not support task type");
        }
    }
}
