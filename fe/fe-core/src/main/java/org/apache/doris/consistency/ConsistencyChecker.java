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

package org.apache.doris.consistency;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MetaObject;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.consistency.CheckConsistencyJob.JobState;
import org.apache.doris.persist.ConsistencyCheckInfo;
import org.apache.doris.task.CheckConsistencyTask;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConsistencyChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(ConsistencyChecker.class);

    private static final int MAX_JOB_NUM = 100;

    private static final Comparator<MetaObject> COMPARATOR =
            (first, second) -> Long.signum(first.getLastCheckTime() - second.getLastCheckTime());

    // tabletId -> job
    private Map<Long, CheckConsistencyJob> jobs;

    /*
     * ATTN:
     *      lock order is:
     *       jobs lock
     *       CheckConsistencyJob's synchronized
     *       db lock
     *
     * if reversal is inevitable. use db.tryLock() instead to avoid dead lock
     */
    private ReentrantReadWriteLock jobsLock;

    private int startTime;
    private int endTime;

    public ConsistencyChecker() {
        super("consistency checker");

        jobs = Maps.newHashMap();
        jobsLock = new ReentrantReadWriteLock();

        if (!initWorkTime()) {
            LOG.error("failed to init time in ConsistencyChecker. exit");
            System.exit(-1);
        }
    }

    private boolean initWorkTime() {
        Date startDate = TimeUtils.getHourAsDate(Config.consistency_check_start_time);
        Date endDate = TimeUtils.getHourAsDate(Config.consistency_check_end_time);

        if (startDate == null || endDate == null) {
            return false;
        }

        Calendar calendar = Calendar.getInstance();

        calendar.setTime(startDate);
        startTime = calendar.get(Calendar.HOUR_OF_DAY);

        calendar.setTime(endDate);
        endTime = calendar.get(Calendar.HOUR_OF_DAY);

        LOG.info("consistency checker will work from {}:00 to {}:00", startTime, endTime);
        return true;
    }

    @Override
    protected void runAfterCatalogReady() {
        // for each round. try chose enough new tablets to check
        // only add new job when it's work time
        if (itsTime() && getJobNum() == 0) {
            List<Long> chosenTabletIds = chooseTablets();
            for (Long tabletId : chosenTabletIds) {
                CheckConsistencyJob job = new CheckConsistencyJob(tabletId);
                addJob(job);
            }
        }

        jobsLock.writeLock().lock();
        try {
            // handle all jobs
            Iterator<Map.Entry<Long, CheckConsistencyJob>> iterator = jobs.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, CheckConsistencyJob> entry = iterator.next();
                CheckConsistencyJob oneJob = entry.getValue();

                JobState state = oneJob.getState();
                switch (state) {
                    case PENDING:
                        if (!oneJob.sendTasks()) {
                            clearJob(oneJob);
                            iterator.remove();
                        }
                        break;
                    case RUNNING:
                        int res = oneJob.tryFinishJob();
                        if (res == -1 || res == 1) {
                            // cancelled or finished
                            clearJob(oneJob);
                            iterator.remove();
                        }
                        break;
                    default:
                        break;
                }
            } // end while
        } finally {
            jobsLock.writeLock().unlock();
        }
    }

    /*
     * check if time comes
     */
    private boolean itsTime() {
        if (startTime == endTime) {
            return false;
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        int currentTime = calendar.get(Calendar.HOUR_OF_DAY);

        boolean isTime = false;
        if (startTime < endTime) {
            if (currentTime >= startTime && currentTime <= endTime) {
                isTime = true;
            } else {
                isTime = false;
            }
        } else {
            // startTime > endTime (across the day)
            if (currentTime >= startTime || currentTime <= endTime) {
                isTime = true;
            } else {
                isTime = false;
            }
        }

        if (!isTime) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("current time is {}:00, waiting to {}:00 to {}:00",
                          currentTime, startTime, endTime);
            }
        }

        return isTime;
    }

    private void clearJob(CheckConsistencyJob job) {
        job.clear();
        if (LOG.isDebugEnabled()) {
            LOG.debug("tablet[{}] consistency checking job is cleared", job.getTabletId());
        }
    }

    private boolean addJob(CheckConsistencyJob job) {
        this.jobsLock.writeLock().lock();
        try {
            if (jobs.containsKey(job.getTabletId())) {
                return false;
            } else {
                LOG.info("add tablet[{}] to check consistency", job.getTabletId());
                jobs.put(job.getTabletId(), job);
                return true;
            }
        } finally {
            this.jobsLock.writeLock().unlock();
        }
    }

    private CheckConsistencyJob getJob(long tabletId) {
        this.jobsLock.readLock().lock();
        try {
            return jobs.get(tabletId);
        } finally {
            this.jobsLock.readLock().unlock();
        }
    }

    private int getJobNum() {
        this.jobsLock.readLock().lock();
        try {
            return jobs.size();
        } finally {
            this.jobsLock.readLock().unlock();
        }
    }

    /**
     *  choose a tablet to check it's consistency
     *  we use a priority queue to sort db/table/partition/index/tablet by 'lastCheckTime'.
     *  chose a tablet which has the smallest 'lastCheckTime'.
     */
    private List<Long> chooseTablets() {
        Env env = Env.getCurrentEnv();
        MetaObject chosenOne = null;

        List<Long> chosenTablets = Lists.newArrayList();

        // sort dbs
        List<Long> dbIds = env.getInternalCatalog().getDbIds();
        if (dbIds.isEmpty()) {
            return chosenTablets;
        }
        Queue<MetaObject> dbQueue = new PriorityQueue<>(Math.max(dbIds.size(), 1), COMPARATOR);
        for (Long dbId : dbIds) {
            if (dbId == 0L) {
                // skip 'information_schema' database
                continue;
            }
            Database db = env.getInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            dbQueue.add(db);
        }

        // must lock jobsLock first to obey the lock order rule
        this.jobsLock.readLock().lock();
        try {
            while ((chosenOne = dbQueue.poll()) != null) {
                Database db = (Database) chosenOne;
                List<Table> tables = db.getTables();
                // sort tables
                Queue<MetaObject> tableQueue = new PriorityQueue<>(Math.max(tables.size(), 1), COMPARATOR);
                for (Table table : tables) {
                    if (!table.isManagedTable()) {
                        continue;
                    }
                    tableQueue.add(table);
                }

                while ((chosenOne = tableQueue.poll()) != null) {
                    OlapTable table = (OlapTable) chosenOne;
                    table.readLock();
                    try {
                        // sort partitions
                        Queue<MetaObject> partitionQueue =
                                new PriorityQueue<>(Math.max(table.getAllPartitions().size(), 1), COMPARATOR);
                        for (Partition partition : table.getPartitions()) {
                            // check partition's replication num. if 1 replication. skip
                            if (table.getPartitionInfo().getReplicaAllocation(
                                    partition.getId()).getTotalReplicaNum() == (short) 1) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("partition[{}]'s replication num is 1. ignore", partition.getId());
                                }
                                continue;
                            }

                            // check if this partition has no data
                            if (partition.getVisibleVersion() == Partition.PARTITION_INIT_VERSION) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("partition[{}]'s version is {}. ignore", partition.getId(),
                                            Partition.PARTITION_INIT_VERSION);
                                }
                                continue;
                            }
                            partitionQueue.add(partition);
                        }

                        while ((chosenOne = partitionQueue.poll()) != null) {
                            Partition partition = (Partition) chosenOne;

                            // sort materializedIndices
                            List<MaterializedIndex> visibleIndexes
                                    = partition.getMaterializedIndices(IndexExtState.VISIBLE);
                            Queue<MetaObject> indexQueue
                                    = new PriorityQueue<>(Math.max(visibleIndexes.size(), 1), COMPARATOR);
                            indexQueue.addAll(visibleIndexes);

                            while ((chosenOne = indexQueue.poll()) != null) {
                                MaterializedIndex index = (MaterializedIndex) chosenOne;

                                // sort tablets
                                Queue<MetaObject> tabletQueue
                                        = new PriorityQueue<>(Math.max(index.getTablets().size(), 1), COMPARATOR);
                                tabletQueue.addAll(index.getTablets());

                                while ((chosenOne = tabletQueue.poll()) != null) {
                                    Tablet tablet = (Tablet) chosenOne;
                                    long chosenTabletId = tablet.getId();

                                    if (this.jobs.containsKey(chosenTabletId)) {
                                        continue;
                                    }

                                    // check if version has already been checked
                                    if (partition.getVisibleVersion() == tablet.getCheckedVersion()) {
                                        if (tablet.isConsistent()) {
                                            if (LOG.isDebugEnabled()) {
                                                LOG.debug("tablet[{}]'s version[{}] has been checked. ignore",
                                                        chosenTabletId, tablet.getCheckedVersion());
                                            }
                                        }
                                    } else {
                                        LOG.info("chose tablet[{}-{}-{}-{}-{}] to check consistency", db.getId(),
                                                table.getId(), partition.getId(), index.getId(), chosenTabletId);

                                        chosenTablets.add(chosenTabletId);
                                    }
                                } // end while tabletQueue
                            } // end while indexQueue

                            if (chosenTablets.size() >= MAX_JOB_NUM) {
                                return chosenTablets;
                            }
                        } // end while partitionQueue
                    } finally {
                        table.readUnlock();
                    }
                } // end while tableQueue
            } // end while dbQueue
        } finally {
            jobsLock.readLock().unlock();
        }

        return chosenTablets;
    }

    public void handleFinishedConsistencyCheck(CheckConsistencyTask task, long checksum) {
        long tabletId = task.getTabletId();
        long backendId = task.getBackendId();

        CheckConsistencyJob job = getJob(tabletId);
        if (job == null) {
            LOG.warn("cannot find {} job[{}]", task.getTaskType().name(), tabletId);
            return;
        }

        job.handleFinishedReplica(backendId, checksum);
    }

    public void replayFinishConsistencyCheck(ConsistencyCheckInfo info, Env env) throws MetaNotFoundException {
        Database db = env.getInternalCatalog().getDbOrMetaException(info.getDbId());
        OlapTable table = (OlapTable) db.getTableOrMetaException(info.getTableId());
        table.writeLock();
        try {
            Partition partition = table.getPartition(info.getPartitionId());
            MaterializedIndex index = partition.getIndex(info.getIndexId());
            Tablet tablet = index.getTablet(info.getTabletId());

            long lastCheckTime = info.getLastCheckTime();
            db.setLastCheckTime(lastCheckTime);
            table.setLastCheckTime(lastCheckTime);
            partition.setLastCheckTime(lastCheckTime);
            index.setLastCheckTime(lastCheckTime);
            tablet.setLastCheckTime(lastCheckTime);
            tablet.setCheckedVersion(info.getCheckedVersion());

            tablet.setIsConsistent(info.isConsistent());
        } finally {
            table.writeUnlock();
        }
    }

    // manually adding tablets to check
    public void addTabletsToCheck(List<Long> tabletIds) {
        for (Long tabletId : tabletIds) {
            CheckConsistencyJob job = new CheckConsistencyJob(tabletId);
            addJob(job);
        }
    }
}
