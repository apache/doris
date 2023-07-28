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

package org.apache.doris.statistics;

import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.ThreadPoolManager.BlockedPolicy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AnalysisTaskExecutor extends Thread {

    private static final Logger LOG = LogManager.getLogger(AnalysisTaskExecutor.class);

    private final ThreadPoolExecutor executors = ThreadPoolManager.newDaemonThreadPool(
            Config.statistics_simultaneously_running_task_num,
            Config.statistics_simultaneously_running_task_num, 0,
            TimeUnit.DAYS, new LinkedBlockingQueue<>(),
            new BlockedPolicy("Analysis Job Executor", Integer.MAX_VALUE),
            "Analysis Job Executor", true);

    private final AnalysisTaskScheduler taskScheduler;

    private final BlockingQueue<AnalysisTaskWrapper> taskQueue =
            new PriorityBlockingQueue<AnalysisTaskWrapper>(20,
                    Comparator.comparingLong(AnalysisTaskWrapper::getStartTime));

    public AnalysisTaskExecutor(AnalysisTaskScheduler jobExecutor) {
        this.taskScheduler = jobExecutor;
    }

    @Override
    public void run() {
        fetchAndExecute();
        cancelExpiredTask();
    }

    private void cancelExpiredTask() {
        String name = "Expired Analysis Task Killer";
        Thread t = new Thread(this::doCancelExpiredJob, name);
        t.setDaemon(true);
        t.start();
    }

    private void doCancelExpiredJob() {
        for (;;) {
            try {
                AnalysisTaskWrapper taskWrapper = taskQueue.take();
                try {
                    long timeout = TimeUnit.HOURS.toMillis(Config.analyze_task_timeout_in_hours)
                            - (System.currentTimeMillis() - taskWrapper.getStartTime());
                    taskWrapper.get(timeout < 0 ? 0 : timeout, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    taskWrapper.cancel(e.getMessage());
                }
            } catch (Throwable throwable) {
                LOG.warn(throwable);
            }
        }
    }

    public void fetchAndExecute() {
        Thread t = new Thread(() -> {
            for (;;) {
                try {
                    doFetchAndExecute();
                } catch (Throwable throwable) {
                    LOG.warn(throwable);
                }
            }
        }, "Analysis Task Submitter");
        t.setDaemon(true);
        t.start();
    }

    private void doFetchAndExecute() {
        BaseAnalysisTask task = taskScheduler.getPendingTasks();
        AnalysisTaskWrapper taskWrapper = new AnalysisTaskWrapper(this, task);
        executors.submit(taskWrapper);
    }

    public void putJob(AnalysisTaskWrapper wrapper) throws Exception {
        taskQueue.put(wrapper);
    }
}
