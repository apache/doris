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

import org.apache.doris.job.disruptor.ExecuteTaskEvent;
import org.apache.doris.job.task.AbstractTask;

import com.lmax.disruptor.WorkHandler;
import lombok.extern.log4j.Log4j2;

/**
 * DefaultTaskExecutor is an implementation of the TaskExecutor interface.
 * if you need to implement your own TaskExecutor, you could refer to this class. and need to register
 * it in the TaskExecutorFactory
 * It executes a given AbstractTask by acquiring a semaphore token from the TaskTokenManager
 * and releasing it after the task execution.
 */
@Log4j2
public class DefaultTaskExecutorHandler<T extends AbstractTask> implements WorkHandler<ExecuteTaskEvent<T>> {


    @Override
    public void onEvent(ExecuteTaskEvent<T> executeTaskEvent) {
        T task = executeTaskEvent.getTask();
        if (null == task) {
            log.warn("task is null, ignore,maybe task has been canceled");
            return;
        }
        if (task.isCancelled()) {
            log.info("task is canceled, ignore. task id is {}", task.getTaskId());
            return;
        }
        log.info("start to execute task, task id is {}", task.getTaskId());
        try {
            task.runTask();
        } catch (Exception e) {
            //if task.onFail() throw exception, we will catch it here
            log.warn("task before error, task id is {}", task.getTaskId(), e);
        }
        //todo we need discuss whether we need to use semaphore to control the concurrent task num
        /* Semaphore semaphore = null;
        // get token
        try {
            int maxConcurrentTaskNum = executeTaskEvent.getJobConfig().getMaxConcurrentTaskNum();
            semaphore = TaskTokenManager.tryAcquire(task.getJobId(), maxConcurrentTaskNum);
            task.runTask();
        } catch (Exception e) {
            task.onFail();
            log.error("execute task error, task id is {}", task.getTaskId(), e);
        } finally {
            if (null != semaphore) {
                semaphore.release();
            }*/
    }
}
