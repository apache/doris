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

package org.apache.doris.scheduler;

import org.apache.doris.scheduler.metadata.Task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TaskExecutorPool {
    private static final Logger LOG = LogManager.getLogger(TaskExecutorPool.class);
    private final ExecutorService taskPool = Executors.newCachedThreadPool();

    public void executeTask(TaskExecutor taskExecutor) {
        if (taskExecutor == null) {
            return;
        }
        Task task = taskExecutor.getTask();
        if (task == null) {
            return;
        }
        if (task.getState() == Utils.TaskState.SUCCESS || task.getState() == Utils.TaskState.FAILED) {
            LOG.warn("TaskRun {} is in final status {} ", task.getTaskId(), task.getState());
            return;
        }

        Future<?> future = taskPool.submit(() -> {
            task.setState(Utils.TaskState.RUNNING);
            try {
                boolean isSuccess = taskExecutor.executeTask();
                if (isSuccess) {
                    task.setState(Utils.TaskState.SUCCESS);
                } else {
                    task.setState(Utils.TaskState.FAILED);
                }
            } catch (Exception ex) {
                LOG.warn("failed to execute TaskRun.", ex);
                task.setState(Utils.TaskState.FAILED);
                task.setErrorCode(-1);
                task.setErrorMessage(ex.toString());
            } finally {
                task.setFinishTime(System.currentTimeMillis());
            }
        });
        taskExecutor.setFuture(future);
    }
}
