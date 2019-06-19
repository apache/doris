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

package org.apache.doris.alter;

import org.apache.doris.alter.AlterJob.JobState;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.CancelStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.task.AgentTask;
import org.apache.doris.thrift.TTabletInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AlterHandler extends Daemon {
    private static final Logger LOG = LogManager.getLogger(AlterHandler.class);

    // tableId -> AlterJob
    protected ConcurrentHashMap<Long, AlterJob> alterJobs = new ConcurrentHashMap<Long, AlterJob>();
    
    protected ConcurrentLinkedQueue<AlterJob> finishedOrCancelledAlterJobs = new ConcurrentLinkedQueue<AlterJob>();
    
    /*
     * lock to perform atomic operations.
     * eg.
     *  When job is finished, it will be moved from alterJobs to finishedOrCancelledAlterJobs,
     *  and this requires atomic operations. So the lock must be held to do this operations.
     *  Operations like Get or Put do not need lock.
     */
    protected ReentrantLock lock = new ReentrantLock();
    
    protected void lock() {
        lock.lock();
    }
    
    protected void unlock() {
        lock.unlock();
    }
    
    public AlterHandler(String name) {
        super(name, 10000);
    }

    protected void addAlterJob(AlterJob alterJob) {
        this.alterJobs.put(alterJob.getTableId(), alterJob);
        LOG.info("add {} job[{}]", alterJob.getType(), alterJob.getTableId());
    }

    public AlterJob getAlterJob(long tableId) {
        return this.alterJobs.get(tableId);
    }
    
    public boolean hasUnfinishedAlterJob(long tableId) {
        return this.alterJobs.containsKey(tableId);
    }

    public int getAlterJobNum(JobState state, long dbId) {
        int jobNum = 0;
        if (state == JobState.PENDING || state == JobState.RUNNING || state == JobState.FINISHING) {
            for (AlterJob alterJob : alterJobs.values()) {
                if (alterJob.getState() == state && alterJob.getDbId() == dbId) {
                    ++jobNum;
                }
            }
        } else if (state == JobState.FINISHED) {
            // lock to perform atomically
            lock();
            try {
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
            } finally {
                unlock();
            }

        } else if (state == JobState.CANCELLED) {
            for (AlterJob alterJob : finishedOrCancelledAlterJobs) {
                if (alterJob.getState() == JobState.CANCELLED && alterJob.getDbId() == dbId) {
                    ++jobNum;
                }
            }
        }

        return jobNum;
    }

    public Map<Long, AlterJob> unprotectedGetAlterJobs() {
        return this.alterJobs;
    }

    public ConcurrentLinkedQueue<AlterJob> unprotectedGetFinishedOrCancelledAlterJobs() {
        return this.finishedOrCancelledAlterJobs;
    }
    
    public void addFinishedOrCancelledAlterJob(AlterJob alterJob) {
        alterJob.clear();
        LOG.info("add {} job[{}] to finished or cancel list", alterJob.getType(), alterJob.getTableId());
        this.finishedOrCancelledAlterJobs.add(alterJob);
    }

    protected AlterJob removeAlterJob(long tableId) {
        return this.alterJobs.remove(tableId);
    }

    public void removeDbAlterJob(long dbId) {
        Iterator<Map.Entry<Long, AlterJob>> iterator = alterJobs.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, AlterJob> entry = iterator.next();
            AlterJob alterJob = entry.getValue();
            if (alterJob.getDbId() == dbId) {
                iterator.remove();
            }
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
        AlterJob alterJob = getAlterJob(olapTable.getId());
        if (alterJob == null) {
            return;
        }
        alterJob.cancel(olapTable, "table is dropped");

        // remove from alterJobs and add to finishedOrCancelledAlterJobs operation should be perform atomically
        lock();
        try {
            alterJob = alterJobs.remove(olapTable.getId());
            if (alterJob != null) {
                alterJob.clear();
                finishedOrCancelledAlterJobs.add(alterJob);
            }
        } finally {
            unlock();
        }
    }

    protected void cancelInternal(AlterJob alterJob, OlapTable olapTable, String msg) {
        // cancel
        alterJob.cancel(olapTable, msg);
        jobDone(alterJob);
    }

    protected void jobDone(AlterJob alterJob) {
        lock();
        try {
            // remove job
            AlterJob alterJobRemoved  = removeAlterJob(alterJob.getTableId());
            // add to finishedOrCancelledAlterJobs
            if (alterJobRemoved != null) {
                // add alterJob not alterJobRemoved, because the alterJob maybe a new object
                // deserialized from journal, and the finished state is set to the new object
                addFinishedOrCancelledAlterJob(alterJob);
            }
        } finally {
            unlock();
        }
    }

    public void replayInitJob(AlterJob alterJob, Catalog catalog) {
        Database db = catalog.getDb(alterJob.getDbId());
        alterJob.replayInitJob(db);
        // add rollup job
        addAlterJob(alterJob);
    }
    
    public void replayFinishing(AlterJob alterJob, Catalog catalog) {
        Database db = catalog.getDb(alterJob.getDbId());
        alterJob.replayFinishing(db);
        alterJob.setState(JobState.FINISHING);
        // !!! the alter job should add to the cache again, because the alter job is deserialized from journal
        // it is a different object compared to the cache
        addAlterJob(alterJob);
    }

    public void replayFinish(AlterJob alterJob, Catalog catalog) {
        Database db = catalog.getDb(alterJob.getDbId());
        alterJob.replayFinish(db);
        alterJob.setState(JobState.FINISHED);

        jobDone(alterJob);
    }

    public void replayCancel(AlterJob alterJob, Catalog catalog) {
        removeAlterJob(alterJob.getTableId());
        alterJob.setState(JobState.CANCELLED);
        Database db = catalog.getDb(alterJob.getDbId());
        if (db != null) {
            // we log rollup job cancelled even if db is dropped.
            // so check db != null here
            alterJob.replayCancel(db);
        }

        addFinishedOrCancelledAlterJob(alterJob);
    }

    @Override
    protected void runOneCycle() {
        // clean history job
        Iterator<AlterJob> iter = finishedOrCancelledAlterJobs.iterator();
        while (iter.hasNext()) {
            AlterJob historyJob = iter.next();
            if ((System.currentTimeMillis() - historyJob.getCreateTimeMs()) / 1000 > Config.history_job_keep_max_second) {
                iter.remove();
                LOG.info("remove history {} job[{}]. created at {}", historyJob.getType(),
                         historyJob.getTableId(), TimeUtils.longToTimeString(historyJob.getCreateTimeMs()));
            }
        }
    }

    @Override
    public void start() {
        // register observer
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

    public Integer getAlterJobNumByState(JobState state) {
        int jobNum = 0;
        for (AlterJob alterJob : alterJobs.values()) {
            if (alterJob.getState() == state) {
                ++jobNum;
            }
        }
        return jobNum;
    }
}
