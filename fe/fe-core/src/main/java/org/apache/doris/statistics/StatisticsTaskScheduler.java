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

import org.apache.doris.analysis.AnalyzeStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.StatisticsJob.JobState;
import org.apache.doris.statistics.StatisticsTask.TaskState;
import org.apache.doris.statistics.StatsCategoryDesc.StatsCategory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/*
Schedule statistics task
 */
public class StatisticsTaskScheduler extends MasterDaemon {
    private final static Logger LOG = LogManager.getLogger(StatisticsTaskScheduler.class);

    private final Queue<StatisticsTask> queue = Queues.newLinkedBlockingQueue();

    public StatisticsTaskScheduler() {
        super("Statistics task scheduler", 0);
    }

    @Override
    protected void runAfterCatalogReady() {
        // step1: task n concurrent tasks from the queue
        List<StatisticsTask> tasks = peek();

        if (!tasks.isEmpty()) {
            ThreadPoolExecutor executor = ThreadPoolManager.newDaemonCacheThreadPool(tasks.size(),
                "statistic-pool", false);
            StatisticsJobManager jobManager = Catalog.getCurrentCatalog().getStatisticsJobManager();
            Map<Long, StatisticsJob> statisticsJobs = jobManager.getIdToStatisticsJob();
            Map<Long, List<Map<Long, Future<StatisticsTaskResult>>>> resultMap = Maps.newLinkedHashMap();

            for (StatisticsTask task : tasks) {
                queue.remove();
                long jobId = task.getJobId();
                StatisticsJob statisticsJob = statisticsJobs.get(jobId);

                if (checkJobIsValid(jobId)) {
                    // step2: execute task and save task result
                    Future<StatisticsTaskResult> future = executor.submit(task);
                    if (updateTaskAndJobState(task, statisticsJob)) {
                        Map<Long, Future<StatisticsTaskResult>> taskInfo = Maps.newHashMap();
                        taskInfo.put(task.getId(), future);
                        List<Map<Long, Future<StatisticsTaskResult>>> jobInfo = resultMap
                                .getOrDefault(jobId, Lists.newArrayList());
                        jobInfo.add(taskInfo);
                        resultMap.put(jobId, jobInfo);
                    }
                }
            }

            // step3: handle task results
            handleTaskResult(resultMap);
        }
    }

    public void addTasks(List<StatisticsTask> statisticsTaskList) throws IllegalStateException {
        queue.addAll(statisticsTaskList);
    }

    private List<StatisticsTask> peek() {
        List<StatisticsTask> tasks = Lists.newArrayList();
        int i = Config.cbo_concurrency_statistics_task_num;
        while (i > 0) {
            StatisticsTask task = queue.peek();
            if (task == null) {
                break;
            }
            tasks.add(task);
            i--;
        }
        return tasks;
    }

    /**
     * Update task and job state
     *
     * @param task statistics task
     * @param job  statistics job
     * @return true if update task and job state successfully.
     */
    private boolean updateTaskAndJobState(StatisticsTask task, StatisticsJob job) {
        try {
            // update task state
            task.updateTaskState(TaskState.RUNNING);
        } catch (DdlException e) {
            LOG.info("Update statistics task state failed, taskId: " + task.getId(), e);
        }

        try {
            // update job state
            if (task.getTaskState() != TaskState.RUNNING) {
                job.updateJobState(JobState.FAILED);
            } else {
                if (job.getJobState() == JobState.SCHEDULING) {
                    job.updateJobState(JobState.RUNNING);
                }
            }
        } catch (DdlException e) {
            LOG.info("Update statistics job state failed, jobId: " + job.getId(), e);
            return false;
        }
        return true;
    }

    private void handleTaskResult(Map<Long, List<Map<Long, Future<StatisticsTaskResult>>>> resultMap) {
        StatisticsManager statsManager = Catalog.getCurrentCatalog().getStatisticsManager();
        StatisticsJobManager jobManager = Catalog.getCurrentCatalog().getStatisticsJobManager();

        resultMap.forEach((jobId, taskMapList) -> {
            if (checkJobIsValid(jobId)) {
                String errorMsg = "";
                StatisticsJob statisticsJob = jobManager.getIdToStatisticsJob().get(jobId);
                Map<String, String> properties = statisticsJob.getProperties();
                long timeout = Long.parseLong(properties.get(AnalyzeStmt.CBO_STATISTICS_TASK_TIMEOUT_SEC));

                for (Map<Long, Future<StatisticsTaskResult>> taskInfos : taskMapList) {
                    for (Map.Entry<Long, Future<StatisticsTaskResult>> taskInfo : taskInfos.entrySet()) {
                        Long taskId = taskInfo.getKey();
                        Future<StatisticsTaskResult> future = taskInfo.getValue();

                        try {
                            StatisticsTaskResult taskResult = future.get(timeout, TimeUnit.SECONDS);
                            StatsCategoryDesc categoryDesc = taskResult.getCategoryDesc();
                            StatsCategory category = categoryDesc.getCategory();
                            if (category == StatsCategory.TABLE) {
                                // update table statistics
                                statsManager.alterTableStatistics(taskResult);
                            } else if (category == StatsCategory.COLUMN) {
                                // update column statistics
                                statsManager.alterColumnStatistics(taskResult);
                            }
                        } catch (AnalysisException | TimeoutException | ExecutionException
                                | InterruptedException | CancellationException e) {
                            errorMsg = e.getMessage();
                            LOG.info("Failed to update statistics. jobId: {}, taskId: {}, e: {}", jobId, taskId, e);
                        }

                        try {
                            // update the task and job info
                            statisticsJob.updateJobInfoByTaskId(taskId, errorMsg);
                        } catch (DdlException e) {
                            LOG.info("Failed to update statistics job info. jobId: {}, taskId: {}, e: {}", jobId, taskId, e);
                        }
                    }
                }
            }
        });
    }

    public boolean checkJobIsValid(Long jobId) {
        StatisticsJobManager jobManager = Catalog.getCurrentCatalog().getStatisticsJobManager();
        StatisticsJob statisticsJob = jobManager.getIdToStatisticsJob().get(jobId);
        if (statisticsJob == null) {
            return false;
        }
        JobState jobState = statisticsJob.getJobState();
        return jobState != JobState.CANCELLED && jobState != JobState.FAILED;
    }
}
