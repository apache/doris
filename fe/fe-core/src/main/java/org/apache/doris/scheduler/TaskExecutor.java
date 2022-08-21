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

import org.apache.doris.scheduler.metadata.TaskRecord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TaskExecutor {
    private static final Logger LOG = LogManager.getLogger(TaskExecutor.class);
    private final ExecutorService taskRunPool = Executors.newCachedThreadPool();

    public void executeTaskRun(Task task) {
        if (task == null) {
            return;
        }
        TaskRecord taskRecord = task.getRecord();
        if (taskRecord == null) {
            return;
        }
        if (taskRecord.getState() == Utils.TaskState.SUCCESS || taskRecord.getState() == Utils.TaskState.FAILED) {
            LOG.warn("TaskRun {} is in final status {} ", taskRecord.getQueryId(), taskRecord.getState());
            return;
        }

        Future<?> future = taskRunPool.submit(() -> {
            taskRecord.setState(Utils.TaskState.RUNNING);
            try {
                boolean isSuccess = task.executeTaskRun();
                if (isSuccess) {
                    taskRecord.setState(Utils.TaskState.SUCCESS);
                } else {
                    taskRecord.setState(Utils.TaskState.FAILED);
                }
            } catch (Exception ex) {
                LOG.warn("failed to execute TaskRun.", ex);
                taskRecord.setState(Utils.TaskState.FAILED);
                taskRecord.setErrorCode(-1);
                taskRecord.setErrorMessage(ex.toString());
            } finally {
                taskRecord.setFinishTime(System.currentTimeMillis());
            }
        });
        task.setFuture(future);
    }

    public static class TaskManager {
    }
}
