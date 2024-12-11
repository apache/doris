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

package org.apache.doris.job.executor;

import org.apache.doris.job.task.AbstractTask;

import lombok.extern.log4j.Log4j2;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Log4j2
public class TaskProcessor {
    private ExecutorService executor;

    public  TaskProcessor(int numberOfThreads, int queueSize, ThreadFactory threadFactory) {
        this.executor = new ThreadPoolExecutor(
                numberOfThreads,
                numberOfThreads,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueSize),
                threadFactory,
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    public boolean addTask(AbstractTask task) {
        try {
            executor.execute(() -> runTask(task));
            log.info("Add task to executor, task id: {}", task.getTaskId());
            return true;
        } catch (RejectedExecutionException e) {
            log.warn("Failed to add task to executor, task id: {}", task.getTaskId(), e);
            return false;
        }
    }

    public void shutdown() {
        log.info("Shutting down executor service...");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("Executor service shut down successfully.");
    }

    private void runTask(AbstractTask task) {
        try {
            if (task == null) {
                log.warn("Task is null, ignore. Maybe it has been canceled.");
                return;
            }
            if (task.isCancelled()) {
                log.info("Task is canceled, ignore. Task id: {}", task.getTaskId());
                return;
            }
            log.info("Start to execute task, task id: {}", task.getTaskId());
            task.runTask();
        } catch (Exception e) {
            log.warn("Execute task error, task id: {}", task.getTaskId(), e);
        }
    }
}
