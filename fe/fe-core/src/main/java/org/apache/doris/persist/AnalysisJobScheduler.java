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

package org.apache.doris.persist;

import org.apache.doris.statistics.AnalysisJob;
import org.apache.doris.statistics.AnalysisJobExecutor;
import org.apache.doris.statistics.AnalysisJobInfo;
import org.apache.doris.statistics.AnalysisJobInfo.JobState;
import org.apache.doris.statistics.AnalysisJobInfo.JobType;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.StatisticsUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

public class AnalysisJobScheduler {

    private static final Logger LOG = LogManager.getLogger(AnalysisJobScheduler.class);

    private static final String UPDATE_JOB_STATE_SQL_TEMPLATE = "UPDATE "
            + StatisticConstants.STATISTIC_DB_NAME + "." + StatisticConstants.ANALYSIS_JOB_TABLE + " "
            + "SET state = '${jobState}' ${message} ${updateExecTime} WHERE job_id = ${jobId}";

    private final PriorityQueue<AnalysisJob> systemJobQueue =
            new PriorityQueue<AnalysisJob>(Comparator.comparingInt(AnalysisJob::getLastExecTime));

    private final Queue<AnalysisJob> manualJobQueue = new LinkedList<>();

    private final Set<AnalysisJob> systemJobSet = new HashSet<>();

    private final Set<AnalysisJob> manualJobSet = new HashSet<>();

    private final AnalysisJobExecutor jobExecutor = new AnalysisJobExecutor(this);

    {
        jobExecutor.start();
    }

    public void updateJobStatus(long jobId, JobState jobState, String message, long time) {
        Map<String, String> params = new HashMap<>();
        params.put("jobState", jobState.toString());
        params.put("message", StringUtils.isNotEmpty(message) ? String.format(", message = '%s'", message) : "");
        params.put("updateExecTime", time == -1 ? "" : ", last_exec_time_in_ms=" + time);
        params.put("jobId", String.valueOf(jobId));
        try {
            StatisticsUtil.execUpdate(new StringSubstitutor(params).replace(UPDATE_JOB_STATE_SQL_TEMPLATE));
        } catch (Exception e) {
            LOG.warn(String.format("Failed to update state for job: %s", jobId), e);
        }

    }

    public synchronized void scheduleJobs(List<AnalysisJobInfo> analysisJobInfos) {
        for (AnalysisJobInfo job : analysisJobInfos) {
            schedule(job);
        }
    }

    public synchronized void schedule(AnalysisJobInfo analysisJobInfo) {
        AnalysisJob analysisJob = new AnalysisJob(this, analysisJobInfo);
        addToManualJobQueue(analysisJob);
        if (analysisJobInfo.jobType.equals(JobType.MANUAL)) {
            return;
        }
        addToSystemQueue(analysisJob);
    }

    private void removeFromSystemQueue(AnalysisJob analysisJobInfo) {
        if (manualJobSet.contains(analysisJobInfo)) {
            systemJobQueue.remove(analysisJobInfo);
            manualJobSet.remove(analysisJobInfo);
        }
    }

    private void addToSystemQueue(AnalysisJob analysisJobInfo) {
        if (systemJobSet.contains(analysisJobInfo)) {
            return;
        }
        systemJobSet.add(analysisJobInfo);
        systemJobQueue.add(analysisJobInfo);
        notify();
    }

    private void addToManualJobQueue(AnalysisJob analysisJobInfo) {
        if (manualJobSet.contains(analysisJobInfo)) {
            return;
        }
        manualJobSet.add(analysisJobInfo);
        manualJobQueue.add(analysisJobInfo);
        notify();
    }

    public synchronized AnalysisJob getPendingJobs() {
        while (true) {
            if (!manualJobQueue.isEmpty()) {
                return manualJobQueue.poll();
            }
            if (!systemJobQueue.isEmpty()) {
                return systemJobQueue.poll();
            }
            try {
                wait();
            } catch (Exception e) {
                LOG.warn("Thread get interrupted when waiting for pending jobs", e);
                return null;
            }
        }
    }
}
