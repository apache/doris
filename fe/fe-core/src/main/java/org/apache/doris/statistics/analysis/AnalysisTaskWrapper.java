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

package org.apache.doris.statistics.analysis;

import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.statistics.analysis.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalTime;
import java.util.concurrent.CancellationException;
import java.util.concurrent.FutureTask;

public class AnalysisTaskWrapper extends FutureTask<Void> {

    private static final Logger LOG = LogManager.getLogger(AnalysisTaskWrapper.class);
    private final BaseAnalysisTask task;

    private long startTime;

    private final AnalysisTaskExecutor executor;

    public AnalysisTaskWrapper(AnalysisTaskExecutor executor, BaseAnalysisTask task) {
        super(() -> {
            task.execute();
            return null;
        });
        this.executor = executor;
        this.task = task;
    }

    @Override
    public void run() {
        startTime = System.currentTimeMillis();
        if (task.killed) {
            return;
        }
        if (task.info.scheduleType == ScheduleType.AUTOMATIC
                && !StatisticsUtil.inAnalyzeTime(LocalTime.now(TimeUtils.getTimeZone().toZoneId()))) {
            task.job.fail("Auto task doesn't get executed within specified time range");
            return;
        }
        try {
            executor.putJob(this);
            super.run();
            get();
        } catch (CancellationException e) {
            return; // The cancelling thread handles job cleanup.
        } catch (Exception e) {
            LOG.warn("Analyze {} failed.", task, e);
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_STATISTICS_FAILED_ANALYZE_TASK.increase(1L);
            }
            task.job.fail(StatisticsUtil.analyzeFailureMessage(e));
            return;
        }
        if (!task.killed && MetricRepo.isInit) {
            MetricRepo.COUNTER_STATISTICS_SUCCEED_ANALYZE_TASK.increase(1L);
        }
    }

    public boolean cancel(String msg) {
        try {
            LOG.warn("{} cancelled, cost time:{}", task.toString(), System.currentTimeMillis() - startTime);
            task.job.fail(msg);
        } catch (Exception e) {
            LOG.warn("Failed to cancel analysis job for {}", task, e);
        }
        // A task may still be writing metadata.
        return super.cancel(false);
    }

    public long getStartTime() {
        return startTime;
    }
}
