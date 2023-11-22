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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedView;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.CustomThreadFactory;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.metric.GaugeMetric;
import org.apache.doris.metric.Metric;
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mtmv.MTMVUtils.JobState;
import org.apache.doris.mtmv.metadata.ChangeMTMVJob;
import org.apache.doris.mtmv.metadata.MTMVCheckpointData;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.mtmv.metadata.MTMVTask;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class MTMVJobManager {
    private static final Logger LOG = LogManager.getLogger(MTMVJobManager.class);

    // make sure that metrics were registered only once.
    private static volatile boolean metricsRegistered = false;

    private final Map<Long, MTMVJob> idToJobMap;
    private final Map<String, MTMVJob> nameToJobMap;

    private final MTMVTaskManager taskManager;

    private ScheduledExecutorService periodScheduler = Executors.newScheduledThreadPool(1,
            new CustomThreadFactory("mtmv-job-period-scheduler"));

    private ScheduledExecutorService cleanerScheduler = Executors.newScheduledThreadPool(1,
            new CustomThreadFactory("mtmv-job-cleaner-scheduler"));

    private final ReentrantReadWriteLock rwLock;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    public MTMVJobManager() {
        idToJobMap = Maps.newConcurrentMap();
        nameToJobMap = Maps.newConcurrentMap();
        rwLock = new ReentrantReadWriteLock(true);
        taskManager = new MTMVTaskManager();
    }

    public void start() {
        if (isStarted.compareAndSet(false, true)) {
            // check the scheduler before using it
            // since it may be shutdown when master change to follower without process shutdown.
            if (periodScheduler.isShutdown()) {
                periodScheduler = Executors.newScheduledThreadPool(1,
                        new CustomThreadFactory("mtmv-job-period-scheduler"));
            }

            registerJobs();

            if (cleanerScheduler.isShutdown()) {
                cleanerScheduler = Executors.newScheduledThreadPool(1,
                        new CustomThreadFactory("mtmv-job-cleaner-scheduler"));
            }
            cleanerScheduler.scheduleAtFixedRate(() -> {
                if (!Env.getCurrentEnv().isMaster()) {
                    LOG.warn("only master can run MTMVJob");
                    return;
                }
                removeExpiredJobs();
                taskManager.removeExpiredTasks();
            }, 0, 1, TimeUnit.MINUTES);

            taskManager.startTaskScheduler();
            initMetrics();
        }
    }

    private void initMetrics() {
        if (metricsRegistered) {
            return;
        }
        metricsRegistered = true;

        // total jobs
        GaugeMetric<Integer> totalJob = new GaugeMetric<Integer>("mtmv_job",
                Metric.MetricUnit.NOUNIT, "Total job number of mtmv.") {
            @Override
            public Integer getValue() {
                return nameToJobMap.size();
            }
        };
        totalJob.addLabel(new MetricLabel("type", "TOTAL-JOB"));
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(totalJob);

        // active jobs
        GaugeMetric<Integer> activeJob = new GaugeMetric<Integer>("mtmv_job",
                Metric.MetricUnit.NOUNIT, "Active job number of mtmv.") {
            @Override
            public Integer getValue() {
                int result = 0;
                for (MTMVJob job : getAllJobsWithLock()) {
                    if (job.getState() == JobState.ACTIVE) {
                        result++;
                    }
                }
                return result;
            }
        };
        activeJob.addLabel(new MetricLabel("type", "ACTIVE-JOB"));
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(activeJob);

        // total tasks
        GaugeMetric<Integer> totalTask = new GaugeMetric<Integer>("mtmv_task",
                Metric.MetricUnit.NOUNIT, "Total task number of mtmv.") {
            @Override
            public Integer getValue() {
                return getTaskManager().getHistoryTasks().size();
            }
        };
        totalTask.addLabel(new MetricLabel("type", "TOTAL-TASK"));
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(totalTask);

        // running tasks
        GaugeMetric<Integer> runningTask = new GaugeMetric<Integer>("mtmv_task",
                Metric.MetricUnit.NOUNIT, "Running task number of mtmv.") {
            @Override
            public Integer getValue() {
                return getTaskManager().getRunningTaskMap().size();
            }
        };
        runningTask.addLabel(new MetricLabel("type", "RUNNING-TASK"));
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(runningTask);

        // pending tasks
        GaugeMetric<Integer> pendingTask = new GaugeMetric<Integer>("mtmv_task",
                Metric.MetricUnit.NOUNIT, "Pending task number of mtmv.") {
            @Override
            public Integer getValue() {
                return getTaskManager().getPendingTaskMap().size();
            }
        };
        pendingTask.addLabel(new MetricLabel("type", "PENDING-TASK"));
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(pendingTask);

        // failed tasks
        GaugeMetric<Integer> failedTask = new GaugeMetric<Integer>("mtmv_task",
                Metric.MetricUnit.NOUNIT, "Failed task number of mtmv.") {
            @Override
            public Integer getValue() {
                return getTaskManager().getFailedTaskCount();
            }
        };
        failedTask.addLabel(new MetricLabel("type", "FAILED-TASK"));
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(failedTask);
    }

    public void stop() {
        if (isStarted.compareAndSet(true, false)) {
            periodScheduler.shutdown();
            cleanerScheduler.shutdown();
            taskManager.stopTaskScheduler();
        }
    }

    private void registerJobs() {
        for (MTMVJob job : getAllJobsWithLock()) {
            job.start();
        }
    }

    public void createJob(MTMVJob job, boolean isReplay) throws DdlException {
        createJobWithLock(job, isReplay);
        if (!isReplay) {
            job.start();
        }
    }

    private void createJobWithLock(MTMVJob job, boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (nameToJobMap.containsKey(job.getName())) {
                throw new DdlException("Job [" + job.getName() + "] already exists");
            }
            nameToJobMap.put(job.getName(), job);
            idToJobMap.put(job.getId(), job);
            if (!isReplay) {
                // log job before submit any task.
                Env.getCurrentEnv().getEditLog().logCreateMTMVJob(job);
            }
        } finally {
            writeUnlock();
        }
    }

    public void refreshMTMV(String dbName, String mvName)
            throws DdlException, MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        MaterializedView mv = (MaterializedView) db.getTableOrMetaException(mvName, TableType.MATERIALIZED_VIEW);
        MTMVJob mtmvJob = MTMVJobFactory.genOnceJob(mv, dbName);
        createJob(mtmvJob, false);
    }

    public void dropJobByName(String dbName, String mvName, boolean isReplay) {
        List<Long> jobIds = Lists.newArrayList();
        for (String jobName : nameToJobMap.keySet()) {
            MTMVJob job = nameToJobMap.get(jobName);
            if (job.getMVName().equals(mvName) && job.getDBName().equals(dbName)) {
                jobIds.add(job.getId());
            }
        }
        dropJobs(jobIds, isReplay);
    }

    public void dropJobs(List<Long> jobIds, boolean isReplay) {
        if (jobIds.isEmpty()) {
            return;
        }

        for (long jobId : jobIds) {
            dropJob(jobId, isReplay);
        }

        LOG.info("drop jobs:{}", jobIds);
    }

    private void dropJob(long jobId, boolean isReplay) {
        MTMVJob job = dropJobWithLock(jobId, isReplay);
        if (!isReplay && job != null) {
            job.stop();
        }
    }

    private MTMVJob dropJobWithLock(long jobId, boolean isReplay) {
        writeLock();
        try {
            MTMVJob job = idToJobMap.get(jobId);
            if (job == null) {
                LOG.warn("drop jobId {} failed because job is null", jobId);
                return null;
            }
            idToJobMap.remove(job.getId());
            nameToJobMap.remove(job.getName());
            if (!isReplay) {
                Env.getCurrentEnv().getEditLog().logDropMTMVJob(Collections.singletonList(jobId));
            }
            return job;
        } finally {
            writeUnlock();
        }
    }

    public List<MTMVJob> showAllJobs() {
        return showJobs(null);
    }

    public List<MTMVJob> showJobs(String dbName) {
        List<MTMVJob> jobList = Lists.newArrayList();
        if (Strings.isNullOrEmpty(dbName)) {
            jobList.addAll(getAllJobsWithLock());
        } else {
            jobList.addAll(getAllJobsWithLock().stream().filter(u -> u.getDBName().equals(dbName))
                    .collect(Collectors.toList()));
        }
        return jobList.stream().sorted().collect(Collectors.toList());
    }

    public List<MTMVJob> showJobs(String dbName, String mvName) {
        return showJobs(dbName).stream().filter(u -> u.getMVName().equals(mvName)).collect(Collectors.toList());
    }

    public void replayCreateJob(MTMVJob job) {
        if (job.getExpireTime() > 0 && MTMVUtils.getNowTimeStamp() > job.getExpireTime()) {
            return;
        }
        try {
            createJob(job, true);
        } catch (DdlException e) {
            LOG.warn("failed to replay create job [{}]", job.getName(), e);
        }
    }

    public void replayDropJobs(List<Long> jobIds) {
        dropJobs(jobIds, true);
    }

    public void replayUpdateJob(ChangeMTMVJob changeJob) {
        MTMVJob mtmvJob = idToJobMap.get(changeJob.getJobId());
        if (mtmvJob != null) {
            mtmvJob.updateJob(changeJob, true);
        }
    }

    public void replayCreateJobTask(MTMVTask task) {
        taskManager.replayCreateJobTask(task);
    }

    public void replayDropJobTasks(List<String> taskIds) {
        taskManager.dropHistoryTasks(taskIds, true);
    }

    private void removeExpiredJobs() {
        long currentTimeSeconds = MTMVUtils.getNowTimeStamp();
        List<Long> jobIdsToDelete = Lists.newArrayList();
        List<MTMVJob> jobs = getAllJobsWithLock();
        for (MTMVJob job : jobs) {
            // active job should not clean
            if (job.getState() != MTMVUtils.JobState.COMPLETE) {
                continue;
            }
            long expireTime = job.getExpireTime();
            if (expireTime > 0 && currentTimeSeconds > expireTime) {
                jobIdsToDelete.add(job.getId());
            }
        }
        dropJobs(jobIdsToDelete, false);
    }

    public MTMVJob getJob(String jobName) {
        return nameToJobMap.get(jobName);
    }

    private List<MTMVJob> getAllJobsWithLock() {
        readLock();
        try {
            return Lists.newArrayList(nameToJobMap.values());
        } finally {
            readUnlock();
        }

    }

    public MTMVTaskManager getTaskManager() {
        return taskManager;
    }

    public ScheduledExecutorService getPeriodScheduler() {
        return periodScheduler;
    }

    private void readLock() {
        this.rwLock.readLock().lock();
    }

    private void readUnlock() {
        this.rwLock.readLock().unlock();
    }

    private void writeLock() {
        this.rwLock.writeLock().lock();
    }

    private void writeUnlock() {
        this.rwLock.writeLock().unlock();
    }

    public long write(DataOutputStream dos, long checksum) throws IOException {
        MTMVCheckpointData data = new MTMVCheckpointData();
        data.jobs = new ArrayList<>(nameToJobMap.values());
        data.tasks = Lists.newArrayList(taskManager.getHistoryTasks());
        String s = GsonUtils.GSON.toJson(data);
        Text.writeString(dos, s);
        return checksum;
    }

    public static MTMVJobManager read(DataInputStream dis, long checksum) throws IOException {
        MTMVJobManager mtmvJobManager = new MTMVJobManager();
        String s = Text.readString(dis);
        MTMVCheckpointData data = GsonUtils.GSON.fromJson(s, MTMVCheckpointData.class);
        if (data != null) {
            if (data.jobs != null) {
                for (MTMVJob job : data.jobs) {
                    mtmvJobManager.replayCreateJob(job);
                }
            }

            if (data.tasks != null) {
                for (MTMVTask runStatus : data.tasks) {
                    mtmvJobManager.replayCreateJobTask(runStatus);
                }
            }
        }
        return mtmvJobManager;
    }
}
