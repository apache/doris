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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.mtmv.MtmvUtils.JobState;
import org.apache.doris.mtmv.MtmvUtils.TaskRetryPolicy;
import org.apache.doris.mtmv.MtmvUtils.TriggerMode;
import org.apache.doris.mtmv.metadata.AlterMtmvTask;
import org.apache.doris.mtmv.metadata.ChangeMvmtJob;
import org.apache.doris.mtmv.metadata.MtmvCheckpointData;
import org.apache.doris.mtmv.metadata.MtmvJob;
import org.apache.doris.mtmv.metadata.MtmvJob.JobSchedule;
import org.apache.doris.mtmv.metadata.MtmvTask;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class MtmvJobManager {
    private static final Logger LOG = LogManager.getLogger(MtmvJobManager.class);

    private final Map<Long, MtmvJob> idToJobMap;
    private final Map<String, MtmvJob> nameToJobMap;
    private final Map<Long, ScheduledFuture<?>> periodFutureMap;

    private final MtmvTaskManager taskManager;

    private final ScheduledExecutorService periodScheduler = Executors.newScheduledThreadPool(1);

    private final ScheduledExecutorService cleanerScheduler = Executors.newScheduledThreadPool(1);

    private final ReentrantLock reentrantLock;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    public MtmvJobManager() {
        idToJobMap = Maps.newConcurrentMap();
        nameToJobMap = Maps.newConcurrentMap();
        periodFutureMap = Maps.newConcurrentMap();
        reentrantLock = new ReentrantLock(true);
        taskManager = new MtmvTaskManager(this);
    }

    public void start() {
        if (isStarted.compareAndSet(false, true)) {
            taskManager.clearUnfinishedTasks();

            registerJobs();

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
            }, 0, 1, TimeUnit.DAYS);

            taskManager.startTaskScheduler();
        }
    }

    private void registerJobs() {
        for (MtmvJob job : nameToJobMap.values()) {
            if (job.getState() != JobState.ACTIVE) {
                continue;
            }
            if (job.getTriggerMode() == TriggerMode.PERIODICAL) {
                JobSchedule schedule = job.getSchedule();
                ScheduledFuture<?> future = periodScheduler.scheduleAtFixedRate(() -> submitJobTask(job.getName()),
                        MtmvUtils.getDelaySeconds(job), schedule.getPeriod(), schedule.getTimeUnit());
                periodFutureMap.put(job.getId(), future);
            } else if (job.getTriggerMode() == TriggerMode.ONCE) {
                if (job.getRetryPolicy() == TaskRetryPolicy.ALWAYS || job.getRetryPolicy() == TaskRetryPolicy.TIMES) {
                    MtmvTaskExecuteParams executeOption = new MtmvTaskExecuteParams();
                    submitJobTask(job.getName(), executeOption);
                }
            }
        }
    }

    public void createJob(MtmvJob job, boolean isReplay) throws DdlException {
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
                job.setState(JobState.ACTIVE);
                if (!isReplay) {
                    JobSchedule schedule = job.getSchedule();
                    if (schedule == null) {
                        throw new DdlException("Job [" + job.getName() + "] has no scheduling");
                    }
                    ScheduledFuture<?> future = periodScheduler.scheduleAtFixedRate(() -> submitJobTask(job.getName()),
                            MtmvUtils.getDelaySeconds(job), schedule.getPeriod(), schedule.getTimeUnit());
                    periodFutureMap.put(job.getId(), future);
                }
                nameToJobMap.put(job.getName(), job);
                idToJobMap.put(job.getId(), job);
            } else if (job.getTriggerMode() == TriggerMode.ONCE) {
                job.setState(JobState.ACTIVE);
                nameToJobMap.put(job.getName(), job);
                idToJobMap.put(job.getId(), job);
                if (!isReplay) {
                    MtmvTaskExecuteParams executeOption = new MtmvTaskExecuteParams();
                    submitJobTask(job.getName(), executeOption);
                }
            } else if (job.getTriggerMode() == TriggerMode.MANUAL) {
                job.setState(JobState.ACTIVE);
                nameToJobMap.put(job.getName(), job);
                idToJobMap.put(job.getId(), job);
            } else {
                throw new DdlException("Unsupported trigger mode for multi-table mv.");
            }
            if (!isReplay) {
                Env.getCurrentEnv().getEditLog().logCreateScheduleJob(job);
            }
        } finally {
            unlock();
        }
    }

    private boolean stopScheduler(String jobName) {
        MtmvJob job = nameToJobMap.get(jobName);
        if (job.getTriggerMode() != TriggerMode.PERIODICAL) {
            return false;
        }
        if (job.getState() == MtmvUtils.JobState.PAUSE) {
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
        boolean isCancel = future.cancel(true);
        if (!isCancel) {
            LOG.warn("fail to cancel scheduler for job [{}]", job.getName());
        }
        return isCancel;
    }

    public boolean killJobTask(String jobName, boolean clearPending) {
        MtmvJob job = nameToJobMap.get(jobName);
        if (job == null) {
            return false;
        }
        return taskManager.killTask(job.getId(), clearPending);
    }

    public MtmvUtils.TaskSubmitStatus submitJobTask(String jobName) {
        return submitJobTask(jobName, new MtmvTaskExecuteParams());
    }

    public MtmvUtils.TaskSubmitStatus submitJobTask(String jobName, MtmvTaskExecuteParams param) {
        MtmvJob job = nameToJobMap.get(jobName);
        if (job == null) {
            return MtmvUtils.TaskSubmitStatus.FAILED;
        }
        return taskManager.submitTask(MtmvUtils.buildTask(job), param);
    }

    public void updateJob(ChangeMvmtJob changeJob, boolean isReplay) {
        if (!tryLock()) {
            return;
        }
        try {
            MtmvJob job = idToJobMap.get(changeJob.getJobId());
            if (job == null) {
                LOG.warn("change jobId {} failed because job is null", changeJob.getJobId());
                return;
            }
            job.setState(changeJob.getToStatus());
            job.setLastModifyTime(changeJob.getLastModifyTime());
            if (!isReplay) {
                Env.getCurrentEnv().getEditLog().logChangeScheduleJob(changeJob);
            }
        } finally {
            unlock();
        }
        LOG.info("change job:{}", changeJob.getJobId());
    }

    public void dropJobs(List<Long> jobIds, boolean isReplay) {
        // keep  nameToJobMap and manualTaskMap consist
        if (!tryLock()) {
            return;
        }
        try {
            for (long jobId : jobIds) {
                MtmvJob job = idToJobMap.get(jobId);
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
                idToJobMap.remove(job.getId());
                nameToJobMap.remove(job.getName());
            }

            if (!isReplay) {
                Env.getCurrentEnv().getEditLog().logDropScheduleJob(jobIds);
            }
        } finally {
            unlock();
        }
        LOG.info("drop jobs:{}", jobIds);
    }

    public List<MtmvJob> showAllJobs() {
        return showJobs(null);
    }

    public List<MtmvJob> showJobs(String dbName) {
        List<MtmvJob> jobList = Lists.newArrayList();
        if (dbName == null) {
            jobList.addAll(nameToJobMap.values());
        } else {
            jobList.addAll(nameToJobMap.values().stream().filter(u -> u.getDbName().equals(dbName))
                    .collect(Collectors.toList()));
        }
        return jobList;
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

    public void replayCreateJob(MtmvJob job) {
        if (job.getTriggerMode() == TriggerMode.PERIODICAL) {
            JobSchedule jobSchedule = job.getSchedule();
            if (jobSchedule == null) {
                LOG.warn("replay a null schedule period job [{}]", job.getName());
                return;
            }
        }
        if (job.getExpireTime() > 0 && System.currentTimeMillis() > job.getExpireTime()) {
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

    public void replayUpdateJob(ChangeMvmtJob changeJob) {
        updateJob(changeJob, true);
    }

    public void replayCreateJobTask(MtmvTask task) {
        taskManager.replayCreateJobTask(task);
    }

    public void replayUpdateTask(AlterMtmvTask changeTask) {
        taskManager.replayUpdateTask(changeTask);
    }

    public void replayDropJobTasks(List<String> taskIds) {
        Map<String, String> index = Maps.newHashMapWithExpectedSize(taskIds.size());
        for (String taskId : taskIds) {
            index.put(taskId, null);
        }
        taskManager.getAllHistory().removeIf(runStatus -> index.containsKey(runStatus.getTaskId()));
    }

    public void removeExpiredJobs() {
        long currentTimeMs = System.currentTimeMillis();

        List<Long> jobIdsToDelete = Lists.newArrayList();
        if (!tryLock()) {
            return;
        }
        try {
            List<MtmvJob> jobs = showJobs(null);
            for (MtmvJob job : jobs) {
                // active periodical job should not clean
                if (job.getState() == MtmvUtils.JobState.ACTIVE) {
                    continue;
                }
                if (job.getTriggerMode() == MtmvUtils.TriggerMode.PERIODICAL) {
                    JobSchedule jobSchedule = job.getSchedule();
                    if (jobSchedule == null) {
                        jobIdsToDelete.add(job.getId());
                        LOG.warn("clean up a null schedule periodical Task [{}]", job.getName());
                        continue;
                    }

                }
                long expireTime = job.getExpireTime();
                if (expireTime > 0 && currentTimeMs > expireTime) {
                    jobIdsToDelete.add(job.getId());
                }
            }
        } finally {
            unlock();
        }

        dropJobs(jobIdsToDelete, true);
    }

    public MtmvJob getJob(String jobName) {
        return nameToJobMap.get(jobName);
    }

    public long write(DataOutputStream dos, long checksum) throws IOException {
        MtmvCheckpointData data = new MtmvCheckpointData();
        data.jobs = new ArrayList<>(nameToJobMap.values());
        data.tasks = taskManager.showTasks(null);
        String s = GsonUtils.GSON.toJson(data);
        Text.writeString(dos, s);
        return checksum;
    }

    public static MtmvJobManager read(DataInputStream dis, long checksum) throws IOException {
        MtmvJobManager mtmvJobManager = new MtmvJobManager();
        try {
            String s = Text.readString(dis);
            MtmvCheckpointData data = GsonUtils.GSON.fromJson(s, MtmvCheckpointData.class);
            if (data != null) {
                if (data.jobs != null) {
                    for (MtmvJob job : data.jobs) {
                        mtmvJobManager.replayCreateJob(job);
                    }
                }

                if (data.tasks != null) {
                    for (MtmvTask runStatus : data.tasks) {
                        mtmvJobManager.replayCreateJobTask(runStatus);
                    }
                }
            }
            LOG.info("finished replaying JobManager from image");
        } catch (EOFException e) {
            LOG.info("no job or task to replay.");
        }
        return mtmvJobManager;
    }

    // for test only
    public MtmvTaskManager getTaskManager() {
        return taskManager;
    }
}
