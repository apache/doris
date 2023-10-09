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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.ThreadPoolManager.BlockedPolicy;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AnalysisTaskExecutor {

    private static final Logger LOG = LogManager.getLogger(AnalysisTaskExecutor.class);

    protected final ThreadPoolExecutor executors;

    private final BlockingQueue<BaseAnalysisTask> highPriorityAnalysisTasks = new LinkedBlockingQueue<>();

    private final BlockingQueue<BaseAnalysisTask> lowPriorityAnalysisTasks = new LinkedBlockingQueue<>();


    public AnalysisTaskExecutor(int simultaneouslyRunningTaskNum) {
        if (!Env.isCheckpointThread()) {
            executors = ThreadPoolManager.newDaemonThreadPool(
                    simultaneouslyRunningTaskNum,
                    simultaneouslyRunningTaskNum, 0,
                    TimeUnit.DAYS, new SynchronousQueue<>(),
                    new BlockedPolicy("Analysis Job Executor", Integer.MAX_VALUE),
                    "Analysis Job Executor", true);
            cancelExpiredTask();
            executors.submit(new Executor(highPriorityAnalysisTasks, lowPriorityAnalysisTasks));
        } else {
            executors = null;
        }
    }

    private void cancelExpiredTask() {
        String name = "Expired Analysis Task Killer";
        Thread t = new Thread(this::doCancelExpiredJob, name);
        t.setDaemon(true);
        t.start();
    }

    private void doCancelExpiredJob() {
        for (;;) {
            tryToCancel();
        }
    }

    protected void tryToCancel() {
        try {
            long now = System.currentTimeMillis();
            for (Runnable runnable : executors.getQueue()) {
                Executor executor = (Executor) runnable;
                synchronized (executor.lock) {
                    if (executor.cur != null && (now - executor.cur.startTime)
                            > TimeUnit.SECONDS.toMillis(StatisticsUtil.getAnalyzeTimeout())) {
                        executor.cur.cancel();
                    }
                }
            }
        } catch (Throwable throwable) {
            LOG.warn("cancel analysis task failed", throwable);
        }
    }

    public void submitTask(BaseAnalysisTask task) {
        if (task.priority == null) {
            lowPriorityAnalysisTasks.add(task);
        }
        else if (task.priority.ordinal() >= AnalyzePriority.NORMAL.ordinal()) {
            lowPriorityAnalysisTasks.add(task);
        } else {
            highPriorityAnalysisTasks.add(task);
        }
    }

    private static class Executor implements Runnable {
        public BaseAnalysisTask cur = null;

        public final Object lock = new Object();

        private final BlockingQueue<BaseAnalysisTask> highPriorityAnalysisTasks;

        public final BlockingQueue<BaseAnalysisTask> lowPriorityAnalysisTasks;

        public Executor(BlockingQueue<BaseAnalysisTask> highPriorityAnalysisTasks,
                BlockingQueue<BaseAnalysisTask> lowPriorityAnalysisTasks) {
            this.highPriorityAnalysisTasks = highPriorityAnalysisTasks;
            this.lowPriorityAnalysisTasks = lowPriorityAnalysisTasks;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    if (!highPriorityAnalysisTasks.isEmpty()) {
                        synchronized (lock) {
                            cur = highPriorityAnalysisTasks.take();
                        }
                        cur.execute();
                    } else if (!lowPriorityAnalysisTasks.isEmpty()) {
                        synchronized (lock) {
                            cur = lowPriorityAnalysisTasks.take();
                        }
                        cur.execute();
                    } else {
                        synchronized (lock) {
                            cur = null;
                        }
                        Thread.sleep(StatisticConstants.AUTO_TASK_CHECK_INTERVAL_IN_SEC);
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to execute task", e);
                }
            }
        }
    }

    public boolean highPriorityTaskEmpty() {
        return highPriorityAnalysisTasks.isEmpty();
    }

    public boolean lowPriorityTaskEmpty() {
        return lowPriorityAnalysisTasks.isEmpty();
    }

}
