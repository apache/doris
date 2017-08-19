// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.alter;

import com.baidu.palo.alter.AlterJob.JobState;
import com.baidu.palo.analysis.AlterClause;
import com.baidu.palo.analysis.CancelStmt;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.MetaNotFoundException;
import com.baidu.palo.common.util.Daemon;
import com.baidu.palo.common.util.TimeUtils;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.BackendEvent;
import com.baidu.palo.system.SystemInfoObserver;
import com.baidu.palo.system.BackendEvent.BackendEventType;
import com.baidu.palo.task.AgentTask;
import com.baidu.palo.thrift.TTabletInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AlterHandler extends Daemon {
    private static final Logger LOG = LogManager.getLogger(AlterHandler.class);

    // tableId -> AlterJob
    protected Map<Long, AlterJob> alterJobs;

    protected List<AlterJob> finishedOrCancelledAlterJobs;

    // observe cluster changed
    protected AlterHandlerSystemInfoObserver clusterInfoObserver;

    /*
     * ATTN:
     *      lock order is:
     *      db lock
     *      jobs lock
     *      synchronized
     *      
     * if reversal is inevitable. use db.tryLock() instead to avoid dead lock
     */
    protected ReentrantReadWriteLock jobsLock;
    
    public void readLock() {
        jobsLock.readLock().lock();
    }
    
    public void readUnLock() {
        jobsLock.readLock().unlock();
    }
    
    public AlterHandler(String name) {
        super(name);
        alterJobs = new HashMap<Long, AlterJob>();
        finishedOrCancelledAlterJobs = new LinkedList<AlterJob>();
        jobsLock = new ReentrantReadWriteLock();

        clusterInfoObserver = new AlterHandlerSystemInfoObserver(name);
    }

    protected void addAlterJob(AlterJob alterJob) {
        this.jobsLock.writeLock().lock();
        try {
            LOG.info("add {} job[{}]", alterJob.getType(), alterJob.getTableId());
            this.alterJobs.put(alterJob.getTableId(), alterJob);
        } finally {
            this.jobsLock.writeLock().unlock();
        }
    }

    public AlterJob getAlterJob(long tableId) {
        this.jobsLock.readLock().lock();
        try {
            return this.alterJobs.get(tableId);
        } finally {
            this.jobsLock.readLock().unlock();
        }
    }

    public int getAlterJobNum(JobState state, long dbId) {
        int jobNum = 0;
        this.jobsLock.readLock().lock();
        try {
            switch (state) {
                case PENDING:
                    for (AlterJob alterJob : alterJobs.values()) {
                        if (alterJob.getState() == JobState.PENDING && alterJob.getDbId() == dbId) {
                            ++jobNum;
                        }
                    }
                    break;
                case RUNNING:
                    for (AlterJob alterJob : alterJobs.values()) {
                        if (alterJob.getState() == JobState.RUNNING && alterJob.getDbId() == dbId) {
                            ++jobNum;
                        }
                    }
                    break;
                case FINISHED:
                    for (AlterJob alterJob : alterJobs.values()) {
                        if (alterJob.getState() == JobState.FINISHED && alterJob.getDbId() == dbId) {
                            ++jobNum;
                        }
                    }

                    for (AlterJob alterJob : finishedOrCancelledAlterJobs) {
                        if (alterJob.getState() == JobState.FINISHED && alterJob.getDbId() == dbId) {
                            ++jobNum;
                        }
                    }

                    if (this instanceof SchemaChangeHandler) {
                        jobNum += ((SchemaChangeHandler) this).getDelayDeletingJobNum(dbId);
                    }

                    break;
                case CANCELLED:
                    for (AlterJob alterJob : finishedOrCancelledAlterJobs) {
                        if (alterJob.getState() == JobState.CANCELLED && alterJob.getDbId() == dbId) {
                            ++jobNum;
                        }
                    }
                    break;
                default:
                    break;
            }

            return jobNum;
        } finally {
            this.jobsLock.readLock().unlock();
        }
    }

    public Map<Long, AlterJob> unprotectedGetAlterJobs() {
        return this.alterJobs;
    }

    public List<AlterJob> unprotectedGetFinishedOrCancelledAlterJobs() {
        return this.finishedOrCancelledAlterJobs;
    }

    public void addFinishedOrCancelledAlterJob(AlterJob alterJob) {
        alterJob.clear();
        this.jobsLock.writeLock().lock();
        try {
            LOG.info("add {} job[{}] to finished or cancel list", alterJob.getType(), alterJob.getTableId());
            this.finishedOrCancelledAlterJobs.add(alterJob);
        } finally {
            this.jobsLock.writeLock().unlock();
        }
    }

    protected AlterJob removeAlterJob(long tableId) {
        this.jobsLock.writeLock().lock();
        try {
            return this.alterJobs.remove(tableId);
        } finally {
            this.jobsLock.writeLock().unlock();
        }
    }

    public void removeDbAlterJob(long dbId) {
        this.jobsLock.writeLock().lock();
        try {
            Iterator<Map.Entry<Long, AlterJob>> iterator = alterJobs.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, AlterJob> entry = iterator.next();
                AlterJob alterJob = entry.getValue();
                if (alterJob.getDbId() == dbId) {
                    iterator.remove();
                }
            }
        } finally {
            this.jobsLock.writeLock().unlock();
        }
    }

    /*
     * handle task report
     * reportVersion is used in schema change job.
     */
    public void handleFinishedReplica(AgentTask task, TTabletInfo finishTabletInfo, long reportVersion)
            throws MetaNotFoundException {
        long tableId = task.getTableId();

        AlterJob alterJob = getAlterJob(tableId);
        if (alterJob == null) {
            throw new MetaNotFoundException("Cannot find " + task.getTaskType().name() + " job[" + tableId + "]");
        }
        alterJob.handleFinishedReplica(task, finishTabletInfo, reportVersion);
    }

    /*
     * cancel alter job when drop table
     * olapTable: 
     *      table which is being dropped
     */
    public void cancelWithTable(OlapTable olapTable) {
        // make sure to hold to db write lock before calling this
        this.jobsLock.writeLock().lock();
        try {
            if (alterJobs.containsKey(olapTable.getId())) {
                AlterJob alterJob = alterJobs.remove(olapTable.getId());
                alterJob.cancel(olapTable, "table is dropped");
                this.finishedOrCancelledAlterJobs.add(alterJob);
                LOG.info("cancel {} job in table[{}] finished", alterJob.getType(), olapTable.getId());
            }
        } finally {
            this.jobsLock.writeLock().unlock();
        }
    }

    /*
     * when backend is removed or dead, handle related replicas
     * backendId:
     *      id of backend which is removed or dead
     */
    private void handleBackendRemoveEvent(long backendId) {
        this.jobsLock.readLock().lock();
        try {
            Iterator<Map.Entry<Long, AlterJob>> iterator = this.alterJobs.entrySet().iterator();
            while (iterator.hasNext()) {
                AlterJob job = iterator.next().getValue();
                job.handleBackendRemoveEvent(backendId);
            }
        } finally {
            this.jobsLock.readLock().unlock();
        }
    }

    protected void cancelInternal(AlterJob alterJob, OlapTable olapTable, String msg) {
        // remove job
        removeAlterJob(alterJob.getTableId());

        // cancel
        alterJob.cancel(olapTable, msg);

        // add to finishedOrCancelledAlterJobs
        addFinishedOrCancelledAlterJob(alterJob);
    }

    public void replayInitJob(AlterJob alterJob, Catalog catalog) {
        Database db = catalog.getDb(alterJob.getDbId());
        db.writeLock();
        try {
            alterJob.unprotectedReplayInitJob(db);
            // add rollup job
            addAlterJob(alterJob);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayFinish(AlterJob alterJob, Catalog catalog) {
        Database db = catalog.getDb(alterJob.getDbId());
        db.writeLock();
        try {
            removeAlterJob(alterJob.getTableId());
            alterJob.unprotectedReplayFinish(db);
            alterJob.setState(JobState.FINISHED);
            addFinishedOrCancelledAlterJob(alterJob);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayCancel(AlterJob alterJob, Catalog catalog) {
        removeAlterJob(alterJob.getTableId());

        alterJob.setState(JobState.CANCELLED);
        Database db = catalog.getDb(alterJob.getDbId());
        if (db != null) {
            // we log rollup job cancelled even if db is dropped.
            // so check db != null here
            db.writeLock();
            try {
                alterJob.unprotectedReplayCancel(db);
            } finally {
                db.writeUnlock();
            }
        }

        addFinishedOrCancelledAlterJob(alterJob);
    }

    @Override
    protected void runOneCycle() {
        // clean history job
        this.jobsLock.writeLock().lock();
        try {
            Iterator<AlterJob> iter = finishedOrCancelledAlterJobs.iterator();
            while (iter.hasNext()) {
                AlterJob historyJob = iter.next();
                if ((System.currentTimeMillis() - historyJob.getCreateTimeMs()) / 1000 > Config.label_keep_max_second) {
                    iter.remove();
                    LOG.info("remove history {} job[{}]. created at {}", historyJob.getType(),
                             historyJob.getTableId(), TimeUtils.longToTimeString(historyJob.getCreateTimeMs()));
                }
            }
        } finally {
            this.jobsLock.writeLock().unlock();
        }
    }

    @Override
    public void start() {
        // register observer
        Catalog.getCurrentSystemInfo().registerObserver(clusterInfoObserver);
        super.start();
    }

    /*
     * abstract
     */
    /*
     * get alter job's info for show
     */
    public abstract List<List<Comparable>> getAlterJobInfosByDb(Database db);

    /*
     * entry function. handle alter ops 
     */
    public abstract void process(List<AlterClause> alterClauses, String clusterName, Database db, OlapTable olapTable)
            throws DdlException;

    /*
     * cancel alter ops
     */
    public abstract void cancel(CancelStmt stmt) throws DdlException;

    private class AlterHandlerSystemInfoObserver extends SystemInfoObserver {

        public AlterHandlerSystemInfoObserver(String name) {
            super(name);
        }

        @Override
        public void listen(BackendEvent backendEvent) {
            BackendEventType type = backendEvent.getType();
            Long[] backendIds = backendEvent.getBackendIds();
            LOG.info("catch backend event: {}", backendEvent.toString());
            switch (type) {
                case BACKEND_DROPPED:
                case BACKEND_DECOMMISSION:
                    for (int i = 0; i < backendIds.length; i++) {
                        handleBackendRemoveEvent(backendIds[i]);
                    }
                    break;
                case BACKEND_DOWN:
                    for (int i = 0; i < backendIds.length; i++) {
                        Backend backend = Catalog.getCurrentSystemInfo().getBackend(backendIds[i]);
                        if (backend != null) {
                            long currentTime = System.currentTimeMillis();
                            if (currentTime - backend.getLastUpdateMs() > Config.max_backend_down_time_second * 1000) {
                                // this backend is done for a long time and not restart automatically.
                                // we consider it as dead
                                LOG.warn("backend[{}-{}] is down for a long time. last heartbeat: {}",
                                         backendIds[i], backend.getHost(),
                                         TimeUtils.longToTimeString(backend.getLastUpdateMs()));
                                handleBackendRemoveEvent(backendIds[i]);
                            }
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }
}
