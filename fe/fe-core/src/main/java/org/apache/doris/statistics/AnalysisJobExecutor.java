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
import org.apache.doris.persist.AnalysisJobScheduler;
import org.apache.doris.statistics.AnalysisJobInfo.JobState;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AnalysisJobExecutor extends Thread {

    private static final Logger LOG = LogManager.getLogger(AnalysisJobExecutor.class);

    private final ThreadPoolExecutor executors = ThreadPoolManager.newDaemonThreadPool(
            Config.statistics_simultaneously_running_job_num,
            Config.statistics_simultaneously_running_job_num, 0,
            TimeUnit.DAYS, new LinkedBlockingQueue<>(),
            new BlockedPolicy("Analysis Job Executor", Integer.MAX_VALUE),
            "Analysis Job Executor", true);

    private final AnalysisJobScheduler jobScheduler;

    private final BlockingCounter blockingCounter =
            new BlockingCounter(Config.statistics_simultaneously_running_job_num);

    private final BlockingQueue<AnalysisJobWrapper> jobQueue =
            new PriorityBlockingQueue<AnalysisJobWrapper>(20, Comparator.comparingLong(j -> j.startTime));

    public AnalysisJobExecutor(AnalysisJobScheduler jobExecutor) {
        this.jobScheduler = jobExecutor;
    }

    @Override
    public void run() {
        fetchAndExecute();
        cancelExpiredJob();
    }

    private void cancelExpiredJob() {
        String name = "Expired Analysis Job Killer";
        new Thread(() -> {
            for (;;) {
                try {
                    AnalysisJobWrapper jobWrapper = jobQueue.take();
                    try {
                        long timeout = TimeUnit.MINUTES.toMillis(5)
                                - (System.currentTimeMillis() - jobWrapper.startTime);
                        jobWrapper.get(timeout < 0 ? 0 : timeout, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        jobWrapper.cancel(true);
                    }
                } catch (InterruptedException e) {
                    LOG.warn(String.format("Ignored the interrupt for thread : %s", name), e);
                }
            }
        }, name).start();
    }

    public void fetchAndExecute() {
        new Thread(() -> {
            for (;;) {
                AnalysisJob job = jobScheduler.getPendingJobs();
                AnalysisJobWrapper jobWrapper = new AnalysisJobWrapper(job);
                blockingCounter.incr();
                jobScheduler.updateJobStatus(job.getJobId(), JobState.RUNNING, "", -1);
                executors.submit(jobWrapper);
            }
        }, "Analysis Job Submitter").start();
    }


    private class AnalysisJobWrapper extends FutureTask<Void> {

        private final AnalysisJob job;

        private long startTime;

        public AnalysisJobWrapper(AnalysisJob job) {
            super(() -> {
                job.execute();
                return null;
            });
            this.job = job;
        }

        @Override
        public void run() {
            startTime = System.currentTimeMillis();
            Exception except = null;
            try {
                jobQueue.put(this);
                super.run();
            } catch (Exception e) {
                except = e;
            } finally {
                blockingCounter.decr();
                if (except != null) {
                    jobScheduler.updateJobStatus(job.getJobId(), JobState.FAILED, except.getMessage(), -1);
                } else {
                    jobScheduler.updateJobStatus(job.getJobId(), JobState.FINISHED, "", System.currentTimeMillis());
                }
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            try {
                job.cancel();
            }  catch (Exception e) {
                LOG.warn(String.format("Cancel job failed job info : %s", job.toString()));
            } finally {
                blockingCounter.decr();
            }
            return super.cancel(mayInterruptIfRunning);
        }
    }

    private static class BlockingCounter {

        private int count = 0;

        private int upperBound;

        public BlockingCounter(int upperBound) {
            this.upperBound = upperBound;
        }

        public synchronized void incr() {
            while (count >= upperBound) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }

        public synchronized void decr() {
            if (count == 0) {
                return;
            }
            count--;
            notify();
        }
    }
}
