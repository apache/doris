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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.metric.GaugeMetric;
import org.apache.doris.metric.Metric;
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mtmv.MTMVUtils.JobState;
import org.apache.doris.mtmv.MTMVUtils.TaskSubmitStatus;
import org.apache.doris.mtmv.MTMVUtils.TriggerMode;
import org.apache.doris.mtmv.metadata.ChangeMTMVJob;
import org.apache.doris.mtmv.metadata.MTMVCheckpointData;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.mtmv.metadata.MTMVJob.JobSchedule;
import org.apache.doris.mtmv.metadata.MTMVTask;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class MTMVJobManager {
    private static final Logger LOG = LogManager.getLogger(MTMVJobManager.class);

    // make sure that metrics were registered only once.
    private static volatile boolean metricsRegistered = false;

    private final Map<Long, MTMVJob> idToJobMap;
    private final Map<String, MTMVJob> nameToJobMap;
    private final Map<Long, ScheduledFuture<?>> periodFutureMap;

    private final MTMVTaskManager taskManager;

    private ScheduledExecutorService periodScheduler = Executors.newScheduledThreadPool(1);

    private ScheduledExecutorService cleanerScheduler = Executors.newScheduledThreadPool(1);

    private final ReentrantLock reentrantLock;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    public MTMVJobManager() {
        idToJobMap = Maps.newConcurrentMap();
        nameToJobMap = Maps.newConcurrentMap();
        periodFutureMap = Maps.newConcurrentMap();
        reentrantLock = new ReentrantLock(true);
        taskManager = new MTMVTaskManager(this);
    }

    public void start() {
        if (isStarted.compareAndSet(false, true)) {
            // check the scheduler before using it
            // since it may be shutdown when master change to follower without process shutdown.
            if (periodScheduler.isShutdown()) {
                periodScheduler = Executors.newScheduledThreadPool(1);
            }

            registerJobs();

            if (cleanerScheduler.isShutdown()) {
                cleanerScheduler = Executors.newScheduledThreadPool(1);
            }
            cleanerScheduler.scheduleAtFixedRate(() -> {
                if (!Env.getCurrentEnv().isMaster()) {
                    return;
                }
                if (!tryLock()) {
                    return;
                }
                try {
                    removeExpiredJobs();
                    taskManager.removeExpiredTasks();
                } catch (Exception ex) {
                    LOG.warn("failed remove expired jobs and tasks.", ex);
                } finally {
                    unlock();
                }
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
                return periodFutureMap.size();
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
        int num = nameToJobMap.size();
        int periodNum = 0;
        int onceNum = 0;
        for (MTMVJob job : nameToJobMap.values()) {
            if (!job.getState().equals(JobState.ACTIVE)) {
                continue;
            }
            if (job.getTriggerMode() == TriggerMode.PERIODICAL) {
                JobSchedule schedule = job.getSchedule();
                ScheduledFuture<?> future = periodScheduler.scheduleAtFixedRate(() -> submitJobTask(job.getName()),
                        MTMVUtils.getDelaySeconds(job), schedule.getSecondPeriod(), TimeUnit.SECONDS);
                periodFutureMap.put(job.getId(), future);
                periodNum++;
            } else if (job.getTriggerMode() == TriggerMode.ONCE) {
                MTMVTaskExecuteParams executeOption = new MTMVTaskExecuteParams();
                submitJobTask(job.getName(), executeOption);
                onceNum++;
            }
        }
        LOG.info("Register {} period jobs and {} once jobs in the total {} jobs.", periodNum, onceNum, num);
    }

    public void createJob(MTMVJob job, boolean isReplay) throws DdlException {
        if (!tryLock()) {
            throw new DdlException("Failed to get job manager lock when create Job [" + job.getName() + "]");
        }
        try {
            if (nameToJobMap.containsKey(job.getName())) {
                throw new DdlException("Job [" + job.getName() + "] already exists");
            }
            if (!isReplay) {
                Preconditions.checkArgument(job.getId() == 0);
                job.setId(Env.getCurrentEnv().getNextId());
            }
            if (job.getTriggerMode() == TriggerMode.PERIODICAL) {
                JobSchedule schedule = job.getSchedule();
                if (schedule == null) {
                    throw new DdlException("Job [" + job.getName() + "] has no scheduling");
                }
                job.setState(JobState.ACTIVE);
                nameToJobMap.put(job.getName(), job);
                idToJobMap.put(job.getId(), job);
                if (!isReplay) {
                    // log job before submit any task.
                    Env.getCurrentEnv().getEditLog().logCreateMTMVJob(job);
                    ScheduledFuture<?> future = periodScheduler.scheduleAtFixedRate(() -> submitJobTask(job.getName()),
                            MTMVUtils.getDelaySeconds(job), schedule.getSecondPeriod(), TimeUnit.SECONDS);
                    periodFutureMap.put(job.getId(), future);
                }
            } else if (job.getTriggerMode() == TriggerMode.ONCE) {
                // only change once job state from unknown to active. if job is completed, only put it in map
                if (job.getState() == JobState.UNKNOWN) {
                    job.setState(JobState.ACTIVE);
                    job.setExpireTime(MTMVUtils.getNowTimeStamp() + Config.scheduler_mtmv_job_expired);
                }
                nameToJobMap.put(job.getName(), job);
                idToJobMap.put(job.getId(), job);
                if (!isReplay) {
                    Env.getCurrentEnv().getEditLog().logCreateMTMVJob(job);
                    MTMVTaskExecuteParams executeOption = new MTMVTaskExecuteParams();
                    MTMVUtils.TaskSubmitStatus status = submitJobTask(job.getName(), executeOption);
                    if (status != TaskSubmitStatus.SUBMITTED) {
                        throw new DdlException("submit job task with: " + status.toString());
                    }
                }
            } else if (job.getTriggerMode() == TriggerMode.MANUAL) {
                // only change once job state from unknown to active. if job is completed, only put it in map
                if (job.getState() == JobState.UNKNOWN) {
                    job.setState(JobState.ACTIVE);
                }
                nameToJobMap.put(job.getName(), job);
                idToJobMap.put(job.getId(), job);
                if (!isReplay) {
                    Env.getCurrentEnv().getEditLog().logCreateMTMVJob(job);
                }
            } else {
                throw new DdlException("Unsupported trigger mode for multi-table mv.");
            }
        } finally {
            unlock();
        }
    }

    private boolean stopScheduler(String jobName) {
        MTMVJob job = nameToJobMap.get(jobName);
        if (job.getTriggerMode() != TriggerMode.PERIODICAL) {
            return false;
        }
        if (job.getState() == MTMVUtils.JobState.PAUSE) {
            return true;
        }
        JobSchedule jobSchedule = job.getSchedule();
        // this will not happen
        if (jobSchedule == null) {
            LOG.warn("fail to obtain scheduled info for job [{}]", job.getName());
            return true;
        }
        ScheduledFuture<?> future = periodFutureMap.get(job.getId());
        if (future == null) {
            LOG.warn("fail to obtain scheduled info for job [{}]", job.getName());
            return true;
        }
        // MUST not set true for "mayInterruptIfRunning".
        // Because this thread may doing bdbje write operation, it is interrupted,
        // FE may exit due to bdbje write failure.
        boolean isCancel = future.cancel(false);
        if (!isCancel) {
            LOG.warn("fail to cancel scheduler for job [{}]", job.getName());
        }
        return isCancel;
    }

    public boolean killJobTask(String jobName, boolean clearPending) {
        MTMVJob job = nameToJobMap.get(jobName);
        if (job == null) {
            return false;
        }
        return taskManager.killTask(job.getId(), clearPending);
    }

    public void refreshMTMV(String dbName, String mvName)
            throws DdlException, MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        MaterializedView mv = (MaterializedView) db.getTableOrMetaException(mvName, TableType.MATERIALIZED_VIEW);
        MTMVJob mtmvJob = MTMVJobFactory.genOnceJob(mv, dbName);
        createJob(mtmvJob, false);
    }

    public MTMVUtils.TaskSubmitStatus submitJobTask(String jobName) {
        return submitJobTask(jobName, new MTMVTaskExecuteParams());
    }

    public MTMVUtils.TaskSubmitStatus submitJobTask(String jobName, MTMVTaskExecuteParams param) {
        MTMVJob job = nameToJobMap.get(jobName);
        if (job == null) {
            return MTMVUtils.TaskSubmitStatus.FAILED;
        }
        return taskManager.submitTask(MTMVUtils.buildTask(job), param);
    }

    public void updateJob(ChangeMTMVJob changeJob, boolean isReplay) {
        if (!tryLock()) {
            return;
        }
        try {
            MTMVJob job = idToJobMap.get(changeJob.getJobId());
            if (job == null) {
                LOG.warn("change jobId {} failed because job is null", changeJob.getJobId());
                return;
            }
            job.setState(changeJob.getToStatus());
            job.setLastModifyTime(changeJob.getLastModifyTime());
            if (!isReplay) {
                Env.getCurrentEnv().getEditLog().logChangeMTMVJob(changeJob);
            }
        } finally {
            unlock();
        }
        LOG.info("change job:{}", changeJob.getJobId());
    }

    public void dropJobByName(String dbName, String mvName, boolean isReplay) {
        for (String jobName : nameToJobMap.keySet()) {
            MTMVJob job = nameToJobMap.get(jobName);
            if (job.getMVName().equals(mvName) && job.getDBName().equals(dbName)) {
                dropJobs(Collections.singletonList(job.getId()), isReplay);
                return;
            }
        }
    }

    public void dropJobs(List<Long> jobIds, boolean isReplay) {
        if (jobIds.isEmpty()) {
            return;
        }
        if (!tryLock()) {
            return;
        }
        try {
            for (long jobId : jobIds) {
                MTMVJob job = idToJobMap.get(jobId);
                if (job == null) {
                    LOG.warn("drop jobId {} failed because job is null", jobId);
                    continue;
                }
                if (job.getTriggerMode() == TriggerMode.PERIODICAL && !isReplay) {
                    boolean isCancel = stopScheduler(job.getName());
                    if (!isCancel) {
                        continue;
                    }
                    periodFutureMap.remove(job.getId());
                }
                killJobTask(job.getName(), true);
                if (!Config.keep_scheduler_mtmv_task_when_job_deleted) {
                    taskManager.clearTasksByJobName(job.getName(), isReplay);
                }
                idToJobMap.remove(job.getId());
                nameToJobMap.remove(job.getName());
            }

            if (!isReplay) {
                Env.getCurrentEnv().getEditLog().logDropMTMVJob(jobIds);
            }
        } finally {
            unlock();
        }
        LOG.info("drop jobs:{}", jobIds);
    }

    public List<MTMVJob> showAllJobs() {
        return showJobs(null);
    }

    public List<MTMVJob> showJobs(String dbName) {
        List<MTMVJob> jobList = Lists.newArrayList();
        if (Strings.isNullOrEmpty(dbName)) {
            jobList.addAll(nameToJobMap.values());
        } else {
            jobList.addAll(nameToJobMap.values().stream().filter(u -> u.getDBName().equals(dbName))
                    .collect(Collectors.toList()));
        }
        return jobList.stream().sorted().collect(Collectors.toList());
    }

    public List<MTMVJob> showJobs(String dbName, String mvName) {
        return showJobs(dbName).stream().filter(u -> u.getMVName().equals(mvName)).collect(Collectors.toList());
    }

    private boolean tryLock() {
        try {
            return reentrantLock.tryLock(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("got exception while getting job manager lock", e);
        }
        return false;
    }

    public void unlock() {
        this.reentrantLock.unlock();
    }

    public void replayCreateJob(MTMVJob job) {
        if (job.getTriggerMode() == TriggerMode.PERIODICAL) {
            JobSchedule jobSchedule = job.getSchedule();
            if (jobSchedule == null) {
                LOG.warn("replay a null schedule period job [{}]", job.getName());
                return;
            }
        }
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
        updateJob(changeJob, true);
    }

    public void replayCreateJobTask(MTMVTask task) {
        taskManager.replayCreateJobTask(task);
    }

    public void replayDropJobTasks(List<String> taskIds) {
        taskManager.dropTasks(taskIds, true);
    }

    public void removeExpiredJobs() {
        long currentTimeSeconds = MTMVUtils.getNowTimeStamp();

        List<Long> jobIdsToDelete = Lists.newArrayList();
        if (!tryLock()) {
            return;
        }
        try {
            List<MTMVJob> jobs = showJobs(null);
            for (MTMVJob job : jobs) {
                // active job should not clean
                if (job.getState() == MTMVUtils.JobState.ACTIVE) {
                    continue;
                }
                if (job.getTriggerMode() == MTMVUtils.TriggerMode.PERIODICAL) {
                    JobSchedule jobSchedule = job.getSchedule();
                    if (jobSchedule == null) {
                        jobIdsToDelete.add(job.getId());
                        LOG.warn("clean up a null schedule periodical Task [{}]", job.getName());
                        continue;
                    }

                }
                long expireTime = job.getExpireTime();
                if (expireTime > 0 && currentTimeSeconds > expireTime) {
                    jobIdsToDelete.add(job.getId());
                }
            }
        } finally {
            unlock();
        }

        dropJobs(jobIdsToDelete, false);
    }

    public MTMVJob getJob(String jobName) {
        return nameToJobMap.get(jobName);
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

    public MTMVTaskManager getTaskManager() {
        return taskManager;
    }
}
