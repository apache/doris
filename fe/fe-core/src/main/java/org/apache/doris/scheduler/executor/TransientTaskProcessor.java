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

package org.apache.doris.scheduler.executor;

import org.apache.doris.catalog.Env;
import org.apache.doris.job.executor.TaskProcessor;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.manager.TransientTaskManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;

public class TransientTaskProcessor extends TaskProcessor implements Closeable {
    private static final Logger LOG = LogManager.getLogger(TransientTaskProcessor.class);

    public TransientTaskProcessor(int numberOfThreads, int queueSize, ThreadFactory threadFactory) {
        super(numberOfThreads, queueSize, threadFactory);
    }

    private boolean isClosed = false;

    public void addTask(Long taskId) throws JobException {
        if (isClosed) {
            LOG.info("Add task failed, executor is closed, taskId: {}", taskId);
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    TransientTaskManager transientTaskManager = Env.getCurrentEnv().getTransientTaskManager();
                    TransientTaskExecutor taskExecutor = transientTaskManager.getMemoryTaskExecutor(taskId);
                    if (taskExecutor == null) {
                        LOG.info("Memory task executor is null, task id: {}", taskId);
                        return;
                    }
                    taskExecutor.execute();
                } catch (JobException e) {
                    LOG.warn("Memory task execute failed, taskId: {}, msg : {}", taskId, e.getMessage());
                } finally {
                    Env.getCurrentEnv().getTransientTaskManager().removeMemoryTask(taskId);
                }
            });
            LOG.info("Add task to executor, task id: {}", taskId);
        } catch (RejectedExecutionException e) {
            LOG.warn("Failed to add task to executor, task id: {}", taskId, e);
            throw new JobException("Failed to add task to executor, task id: {}", taskId, e);
        }
    }

    @Override
    public void close() {
        isClosed = true;
        shutdown();
    }
}
