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

package org.apache.doris.mtmv;

import org.apache.doris.mtmv.MTMVUtils.TaskState;
import org.apache.doris.mtmv.metadata.MTMVTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MTMVTaskExecutorPool {
    private static final Logger LOG = LogManager.getLogger(MTMVTaskExecutorPool.class);
    private final ExecutorService taskPool = Executors.newCachedThreadPool();

    public void executeTask(MTMVTaskExecutor taskExecutor) {
        if (taskExecutor == null) {
            return;
        }
        MTMVTask task = taskExecutor.getTask();
        if (task == null) {
            return;
        }
        if (task.getState() == TaskState.SUCCESS || task.getState() == TaskState.FAILED) {
            LOG.warn("Task {} is in final status {} ", task.getTaskId(), task.getState());
            return;
        }

        Future<?> future = taskPool.submit(() -> {
            task.setState(TaskState.RUNNING);
            int retryTimes = task.getRetryTimes();
            boolean isSuccess = false;
            String lastExceptionString = "";
            do {
                try {
                    isSuccess = taskExecutor.executeTask();
                    if (isSuccess) {
                        task.setState(TaskState.SUCCESS);
                    } else {
                        task.setState(TaskState.FAILED);
                    }
                } catch (Exception ex) {
                    LOG.warn("failed to execute task.", ex);
                    lastExceptionString = ex.toString();
                } finally {
                    task.setFinishTime(System.currentTimeMillis());
                }
                retryTimes--;
            } while (!isSuccess && retryTimes >= 0);
            if (!isSuccess) {
                task.setState(TaskState.FAILED);
                task.setErrorCode(-1);
                task.setErrorMessage(lastExceptionString);
            }
        });
        taskExecutor.setFuture(future);
    }
}
