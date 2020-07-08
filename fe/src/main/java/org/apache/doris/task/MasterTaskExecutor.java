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

package org.apache.doris.task;

import com.google.common.collect.Maps;

import org.apache.doris.common.ThreadPoolManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MasterTaskExecutor {
    private static final Logger LOG = LogManager.getLogger(MasterTaskExecutor.class);

    private ScheduledExecutorService executor;
    private Map<Long, Future<?>> runningTasks;

    public MasterTaskExecutor(int threadNum) {
        executor = ThreadPoolManager.newScheduledThreadPool(threadNum, "Master-Task-Executor-Pool");
        runningTasks = Maps.newHashMap();
        executor.scheduleAtFixedRate(new TaskChecker(), 0L, 1000L, TimeUnit.MILLISECONDS);
    }
    
    /**
     * submit task to task executor
     * @param task
     * @return true if submit success 
     *         false if task exists
     */
    public boolean submit(MasterTask task) {
        long signature = task.getSignature();
        synchronized (runningTasks) {
            if (runningTasks.containsKey(signature)) {
                return false;
            }
            Future<?> future = executor.submit(task);
            runningTasks.put(signature, future);
            return true;
        }
    }
    
    public void close() {
        executor.shutdown();
        runningTasks.clear();
    }
    
    public int getTaskNum() {
        synchronized (runningTasks) {
            return runningTasks.size();
        }
    }
    
    private class TaskChecker implements Runnable {
        @Override
        public void run() {
            try {
                synchronized (runningTasks) {
                    Iterator<Entry<Long, Future<?>>> iterator = runningTasks.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Entry<Long, Future<?>> entry = iterator.next();
                        Future<?> future = entry.getValue();
                        if (future.isDone()) {
                            iterator.remove();
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("check task error", e);
            }
        }
    }
}
