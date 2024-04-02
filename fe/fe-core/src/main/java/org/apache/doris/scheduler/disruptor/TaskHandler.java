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

package org.apache.doris.scheduler.disruptor;

import org.apache.doris.catalog.Env;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;
import org.apache.doris.scheduler.manager.TransientTaskManager;

import com.lmax.disruptor.WorkHandler;
import lombok.extern.log4j.Log4j2;

/**
 * This class represents a work handler for processing event tasks consumed by a Disruptor.
 * The work handler retrieves the associated event job and executes it if it is running.
 * If the event job is not running, the work handler logs an error message.
 * If the event job execution fails, the work handler logs an error message and pauses the event job.
 * The work handler also handles system events by scheduling batch scheduler tasks.
 */
@Log4j2
public class TaskHandler implements WorkHandler<TaskEvent> {


    /**
     * Processes an event task by retrieving the associated event job and executing it if it is running.
     * If the event job is not running, it logs an error message.
     * If the event job execution fails, it logs an error message and pauses the event job.
     *
     * @param event The event task to be processed.
     */
    @Override
    public void onEvent(TaskEvent event) {
        switch (event.getTaskType()) {
            case TRANSIENT_TASK:
                onTransientTaskHandle(event);
                break;
            default:
                log.warn("unknown task type: {}", event.getTaskType());
                break;
        }
    }

    public void onTransientTaskHandle(TaskEvent taskEvent) {
        Long taskId = taskEvent.getId();
        TransientTaskManager transientTaskManager = Env.getCurrentEnv().getTransientTaskManager();
        TransientTaskExecutor taskExecutor = transientTaskManager.getMemoryTaskExecutor(taskId);
        if (taskExecutor == null) {
            log.info("Memory task executor is null, task id: {}", taskId);
            return;
        }

        try {
            taskExecutor.execute();
        } catch (JobException e) {
            log.warn("Memory task execute failed, taskId: {}, msg : {}", taskId, e.getMessage());
        }
    }

}
