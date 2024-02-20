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

package org.apache.doris.load.sync;

import org.apache.doris.analysis.CreateDataSyncJobStmt;
import org.apache.doris.analysis.PauseSyncJobStmt;
import org.apache.doris.analysis.ResumeSyncJobStmt;
import org.apache.doris.analysis.StopSyncJobStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.sync.canal.CanalDestination;
import org.apache.doris.load.sync.canal.CanalSyncJob;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SyncJobManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(SyncJobManager.class);

    private Map<Long, SyncJob> idToSyncJob;

    private Map<Long, Map<String, List<SyncJob>>> dbIdToJobNameToSyncJobs;

    private ReentrantReadWriteLock lock;

    public SyncJobManager() {
        idToSyncJob = Maps.newConcurrentMap();
        dbIdToJobNameToSyncJobs = Collections.synchronizedMap(Maps.newLinkedHashMap());
        lock = new ReentrantReadWriteLock(true);
    }

    public void addDataSyncJob(CreateDataSyncJobStmt stmt) throws DdlException {
        long jobId = Env.getCurrentEnv().getNextId();
        SyncJob syncJob = SyncJob.fromStmt(jobId, stmt);
        writeLock();
        try {
            checkDuplicateRemote(syncJob);
            unprotectedAddSyncJob(syncJob);
            Env.getCurrentEnv().getEditLog().logCreateSyncJob(syncJob);
        } finally {
            writeUnlock();
        }
        LOG.info(new LogBuilder(LogKey.SYNC_JOB, syncJob.getId())
                .add("name", syncJob.getJobName())
                .add("type", syncJob.getJobType())
                .add("config", syncJob.getJobConfig())
                .add("msg", "add sync job.")
                .build());
    }

    private void checkDuplicateRemote(SyncJob syncJob) throws DdlException {
        if (syncJob.getJobType() == DataSyncJobType.CANAL) {
            CanalDestination remote = ((CanalSyncJob) syncJob).getRemote();
            List<SyncJob> unCompletedJobs = idToSyncJob.values().stream().filter(job -> !job.isCompleted())
                    .collect(Collectors.toList());
            for (SyncJob job : unCompletedJobs) {
                if (job instanceof CanalSyncJob && ((CanalSyncJob) job).getRemote().equals(remote)) {
                    throw new DdlException("Remote Canal instance already exists. conflict destination: " + remote);
                }
            }
        }
    }

    private void unprotectedAddSyncJob(SyncJob syncJob) {
        idToSyncJob.put(syncJob.getId(), syncJob);
        long dbId = syncJob.getDbId();
        if (!dbIdToJobNameToSyncJobs.containsKey(dbId)) {
            dbIdToJobNameToSyncJobs.put(syncJob.getDbId(), Maps.newConcurrentMap());
        }
        Map<String, List<SyncJob>> map = dbIdToJobNameToSyncJobs.get(dbId);
        if (!map.containsKey(syncJob.getJobName())) {
            map.put(syncJob.getJobName(), Lists.newArrayList());
        }
        map.get(syncJob.getJobName()).add(syncJob);
    }

    public void pauseSyncJob(PauseSyncJobStmt stmt) throws UserException {
        String dbName = stmt.getDbFullName();
        String jobName = stmt.getJobName();

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);

        List<SyncJob> syncJobs = Lists.newArrayList();
        readLock();
        try {
            List<SyncJob> matchJobs = getSyncJobsByDbAndJobName(db.getId(), jobName);
            if (matchJobs.isEmpty()) {
                throw new DdlException("Load job does not exist");
            }

            List<SyncJob> runningSyncJob = matchJobs.stream().filter(SyncJob::isRunning)
                    .collect(Collectors.toList());
            if (runningSyncJob.isEmpty()) {
                throw new DdlException("There is no running job with jobName `"
                        + stmt.getJobName() + "` to pause");
            }

            syncJobs.addAll(runningSyncJob);
        } finally {
            readUnlock();
        }

        for (SyncJob syncJob : syncJobs) {
            syncJob.pause();
        }
    }

    public void resumeSyncJob(ResumeSyncJobStmt stmt) throws UserException {
        String dbName = stmt.getDbFullName();
        String jobName = stmt.getJobName();

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);

        List<SyncJob> syncJobs = Lists.newArrayList();
        readLock();
        try {
            List<SyncJob> matchJobs = getSyncJobsByDbAndJobName(db.getId(), jobName);
            if (matchJobs.isEmpty()) {
                throw new DdlException("Load job does not exist");
            }

            List<SyncJob> pausedSyncJob = matchJobs.stream().filter(SyncJob::isPaused)
                    .collect(Collectors.toList());
            if (pausedSyncJob.isEmpty()) {
                throw new DdlException("There is no paused job with jobName `"
                        + stmt.getJobName() + "` to resume");
            }

            syncJobs.addAll(pausedSyncJob);
        } finally {
            readUnlock();
        }

        for (SyncJob syncJob : syncJobs) {
            syncJob.resume();
        }
    }

    public void stopSyncJob(StopSyncJobStmt stmt) throws UserException {
        String dbName = stmt.getDbFullName();
        String jobName = stmt.getJobName();

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);

        // List of sync jobs waiting to be cancelled
        List<SyncJob> syncJobs = Lists.newArrayList();
        readLock();
        try {
            List<SyncJob> matchJobs = getSyncJobsByDbAndJobName(db.getId(), jobName);
            if (matchJobs.isEmpty()) {
                throw new DdlException("Load job does not exist");
            }

            List<SyncJob> uncompletedSyncJob = matchJobs.stream().filter(entity -> !entity.isCompleted())
                    .collect(Collectors.toList());
            if (uncompletedSyncJob.isEmpty()) {
                throw new DdlException("There is no uncompleted job with jobName `"
                        + stmt.getJobName() + "`");
            }

            syncJobs.addAll(uncompletedSyncJob);
        } finally {
            readUnlock();
        }

        for (SyncJob syncJob : syncJobs) {
            syncJob.cancel(SyncFailMsg.MsgType.USER_CANCEL, "user cancel");
        }
    }

    // caller should hold the db lock
    private List<SyncJob> getSyncJobsByDbAndJobName(long dbId, String jobName) {
        List<SyncJob> syncJobs = Lists.newArrayList();
        Map<String, List<SyncJob>> jobNameToSyncJobs = dbIdToJobNameToSyncJobs.get(dbId);
        if (jobNameToSyncJobs != null) {
            if (jobNameToSyncJobs.containsKey(jobName)) {
                syncJobs.addAll(jobNameToSyncJobs.get(jobName));
            }
        }
        return syncJobs;
    }

    public List<List<Comparable>> getSyncJobsInfoByDbId(long dbId) {
        LinkedList<List<Comparable>> syncJobInfos = new LinkedList<List<Comparable>>();

        readLock();
        try {
            if (!dbIdToJobNameToSyncJobs.containsKey(dbId)) {
                return syncJobInfos;
            }
            Map<String, List<SyncJob>> jobNameToLoadJobs = dbIdToJobNameToSyncJobs.get(dbId);
            List<SyncJob> syncJobs = Lists.newArrayList();
            syncJobs.addAll(jobNameToLoadJobs.values()
                    .stream().flatMap(Collection::stream).collect(Collectors.toList()));
            for (SyncJob syncJob : syncJobs) {
                syncJobInfos.add(syncJob.getShowInfo());
            }
            return syncJobInfos;
        } finally {
            readUnlock();
        }
    }

    public List<SyncJob> getSyncJobs(SyncJob.JobState state) {
        List<SyncJob> result = Lists.newArrayList();
        readLock();
        try {
            for (SyncJob job : idToSyncJob.values()) {
                if (job.getJobState() == state) {
                    result.add(job);
                }
            }
        } finally {
            readUnlock();
        }

        return result;
    }

    public boolean isJobNameExist(String dbName, String jobName) throws DdlException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        boolean result = false;
        readLock();
        try {
            Map<String, List<SyncJob>> jobNameToSyncJobs = dbIdToJobNameToSyncJobs.get(db.getId());
            if (jobNameToSyncJobs != null && jobNameToSyncJobs.containsKey(jobName)) {
                List<SyncJob> matchJobs = jobNameToSyncJobs.get(jobName);
                for (SyncJob syncJob : matchJobs) {
                    if (!syncJob.isCancelled()) {
                        result = true;
                    }
                }
            }
        } finally {
            readUnlock();
        }

        return result;
    }

    public void updateNeedSchedule() throws UserException {
        for (SyncJob syncJob : idToSyncJob.values()) {
            if (!syncJob.isCompleted()) {
                syncJob.checkAndDoUpdate();
            }
        }
    }

    // Remove old sync jobs. Called periodically.
    // Stopped jobs will be removed after Config.label_keep_max_second.
    public void cleanOldSyncJobs() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("begin to clean old sync jobs ");
        }
        cleanFinishedSyncJobsIf(job -> job.isExpired(System.currentTimeMillis()));
    }

    /**
     * Remove completed jobs if total job num exceed Config.label_num_threshold
     */
    public void cleanOverLimitSyncJobs() {
        if (Config.label_num_threshold < 0 || idToSyncJob.size() <= Config.label_num_threshold) {
            return;
        }
        writeLock();
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("begin to clean finished sync jobs ");
            }
            Deque<SyncJob> finishedJobs = idToSyncJob
                    .values()
                    .stream()
                    .filter(SyncJob::isCompleted)
                    .sorted(Comparator.comparingLong(o -> o.finishTimeMs))
                    .collect(Collectors.toCollection(ArrayDeque::new));
            while (!finishedJobs.isEmpty() && idToSyncJob.size() > Config.label_num_threshold) {
                SyncJob syncJob = finishedJobs.pollFirst();
                if (!dbIdToJobNameToSyncJobs.containsKey(syncJob.getDbId())) {
                    continue;
                }
                idToSyncJob.remove(syncJob.getId());
                jobRemovedTrigger(syncJob);
            }
        } finally {
            writeUnlock();
        }
    }

    private void jobRemovedTrigger(SyncJob syncJob) {
        Map<String, List<SyncJob>> map = dbIdToJobNameToSyncJobs.get(syncJob.getDbId());
        List<SyncJob> list = map.get(syncJob.getJobName());
        list.remove(syncJob);
        if (list.isEmpty()) {
            map.remove(syncJob.getJobName());
        }
        if (map.isEmpty()) {
            dbIdToJobNameToSyncJobs.remove(syncJob.getDbId());
        }
    }

    public void cleanFinishedSyncJobsIf(Predicate<SyncJob> pred) {
        long currentTimeMs = System.currentTimeMillis();
        writeLock();
        try {
            Iterator<Map.Entry<Long, SyncJob>> iterator = idToSyncJob.entrySet().iterator();
            while (iterator.hasNext()) {
                SyncJob syncJob = iterator.next().getValue();
                if (pred.test(syncJob)) {
                    if (!dbIdToJobNameToSyncJobs.containsKey(syncJob.getDbId())) {
                        continue;
                    }
                    jobRemovedTrigger(syncJob);
                    iterator.remove();
                    LOG.info(new LogBuilder(LogKey.SYNC_JOB, syncJob.getId())
                            .add("finishTimeMs", syncJob.getFinishTimeMs())
                            .add("currentTimeMs", currentTimeMs)
                            .add("jobState", syncJob.getJobState())
                            .add("msg", "sync job has been cleaned")
                    );
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public SyncJob getSyncJobById(long jobId) {
        return idToSyncJob.get(jobId);
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Collection<SyncJob> syncJobs = idToSyncJob.values();
        out.writeInt(syncJobs.size());
        for (SyncJob syncJob : syncJobs) {
            syncJob.write(out);
        }
    }

    public void readField(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            SyncJob syncJob = SyncJob.read(in);
            unprotectedAddSyncJob(syncJob);
        }
    }

    public void replayAddSyncJob(SyncJob syncJob) {
        writeLock();
        try {
            unprotectedAddSyncJob(syncJob);
            LOG.info(new LogBuilder(LogKey.SYNC_JOB, syncJob.getId())
                    .add("msg", "replay create sync job.")
                    .build());
        } finally {
            writeUnlock();
        }
    }

    public void replayUpdateSyncJobState(SyncJob.SyncJobUpdateStateInfo info) {
        writeLock();
        try {
            long jobId = info.getId();
            SyncJob job = idToSyncJob.get(jobId);
            if (job == null) {
                LOG.warn(new LogBuilder(LogKey.SYNC_JOB, jobId)
                        .add("msg", "replay update sync job state failed. Job was not found.")
                        .build());
                return;
            }
            job.replayUpdateSyncJobState(info);
        } finally {
            writeUnlock();
        }
    }
}
