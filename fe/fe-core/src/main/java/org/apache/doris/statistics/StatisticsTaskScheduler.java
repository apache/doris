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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.StatisticsJob.JobState;
import org.apache.doris.statistics.StatisticsTask.TaskState;
import org.apache.doris.statistics.StatsCategoryDesc.StatsCategory;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Queue;
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
        // task n concurrent tasks from the queue
        List<StatisticsTask> tasks = peek();

        if (!tasks.isEmpty()) {
            ThreadPoolExecutor executor = ThreadPoolManager.newDaemonCacheThreadPool(tasks.size(),
                    "statistic-pool", false);
            StatisticsJobManager jobManager = Catalog.getCurrentCatalog().getStatisticsJobManager();
            Map<Long, StatisticsJob> statisticsJobs = jobManager.getIdToStatisticsJob();
            Map<Long, Future<StatisticsTaskResult>> taskMap = Maps.newLinkedHashMap();

            long jobId = -1;
            int taskSize = 0;
            for (StatisticsTask task : tasks) {
                this.queue.remove();
                // handle task result for each job
                if (taskSize > 0 && jobId != task.getJobId()) {
                    handleTaskResult(jobId, taskMap);
                    taskMap.clear();
                    taskSize = 0;
                }
                Future<StatisticsTaskResult> future = executor.submit(task);
                task.setStartTime(System.currentTimeMillis());
                task.updateTaskState(TaskState.RUNNING);
                long taskId = task.getId();
                taskMap.put(taskId, future);
                // update job state
                jobId = task.getJobId();
                StatisticsJob statisticsJob = statisticsJobs.get(jobId);
                if (statisticsJob.getJobState() == JobState.SCHEDULING) {
                    statisticsJob.setStartTime(System.currentTimeMillis());
                    statisticsJob.updateJobState(JobState.RUNNING);
                }
                taskSize++;
            }

            if (taskSize > 0) {
                handleTaskResult(jobId, taskMap);
            }
        }
    }

    public void addTasks(List<StatisticsTask> statisticsTaskList) {
        this.queue.addAll(statisticsTaskList);
    }

    private List<StatisticsTask> peek() {
        List<StatisticsTask> tasks = Lists.newArrayList();
        int i = Config.cbo_concurrency_statistics_task_num;
        while (i > 0) {
            StatisticsTask task = this.queue.peek();
            if (task == null) {
                break;
            }
            tasks.add(task);
            i--;
        }
        return tasks;
    }

    private void handleTaskResult(Long jobId, Map<Long, Future<StatisticsTaskResult>> taskMap) {
        StatisticsManager statsManager = Catalog.getCurrentCatalog().getStatisticsManager();
        StatisticsJobManager jobManager = Catalog.getCurrentCatalog().getStatisticsJobManager();

        long timeout = jobManager.getIdToStatisticsJob().get(jobId).getTaskTimeout();

        for (Map.Entry<Long, Future<StatisticsTaskResult>> entry : taskMap.entrySet()) {
            String errorMsg = "";
            long taskId = entry.getKey();
            Future<StatisticsTaskResult> future = entry.getValue();
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
            } catch (TimeoutException e) {
                errorMsg = "The statistics task was timeout";
                LOG.info("{}, jobId: {}, e: {}", errorMsg, jobId, e);
            } catch (AnalysisException e) {
                errorMsg = "Failed to update statistics. " + e;
                LOG.info("{}, jobId: {}, e: {}", errorMsg, jobId, e);
            } catch (ExecutionException e) {
                errorMsg = "Failed to execute statistics task";
                LOG.info("{}, jobId: {}, e: {}", errorMsg, jobId, e);
            } catch (InterruptedException e) {
                errorMsg = "The statistics task was interrupted";
                LOG.info("{}, jobId: {}, e: {}", errorMsg, jobId, e);
            }
            // update the job info
            jobManager.alterStatisticsJobInfo(jobId, taskId, errorMsg);
        }
    }
}
