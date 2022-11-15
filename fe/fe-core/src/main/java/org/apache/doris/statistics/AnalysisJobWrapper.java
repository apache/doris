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
import org.apache.doris.statistics.AnalysisJobInfo.JobState;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.FutureTask;

public class AnalysisJobWrapper extends FutureTask<Void> {

    private static final Logger LOG = LogManager.getLogger(AnalysisJobWrapper.class);

    private final AnalysisJob job;

    private long startTime;

    private final AnalysisJobExecutor executor;

    public AnalysisJobWrapper(AnalysisJobExecutor executor, AnalysisJob job) {
        super(() -> {
            job.execute();
            return null;
        });
        this.executor = executor;
        this.job = job;
    }

    @Override
    public void run() {
        startTime = System.currentTimeMillis();
        Exception except = null;
        try {
            executor.putJob(this);
            super.run();
        } catch (Exception e) {
            except = e;
        } finally {
            executor.decr();
            if (except != null) {
                Env.getCurrentEnv().getAnalysisJobScheduler()
                        .updateJobStatus(job.getJobId(), JobState.FAILED, except.getMessage(), -1);
            } else {
                Env.getCurrentEnv().getAnalysisJobScheduler()
                        .updateJobStatus(job.getJobId(), JobState.FINISHED, "", System.currentTimeMillis());
            }
        }
    }

    public boolean cancel() {
        try {
            job.cancel();
        } catch (Exception e) {
            LOG.warn(String.format("Cancel job failed job info : %s", job.toString()));
        } finally {
            executor.decr();
        }
        return super.cancel(true);
    }

    public long getStartTime() {
        return startTime;
    }
}
