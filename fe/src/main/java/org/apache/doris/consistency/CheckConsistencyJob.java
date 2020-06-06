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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.Config;
import org.apache.doris.persist.ConsistencyCheckInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CheckConsistencyTask;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;

public class CheckConsistencyJob {
    private static final Logger LOG = LogManager.getLogger(CheckConsistencyJob.class);

    private static final long CHECK_CONSISTENCT_TIME_COST_PER_GIGABYTE_MS = 1000000L; // 1000s

    public enum JobState {
        PENDING,
        RUNNING
    }

    private JobState state;
    private long tabletId;
    
    // backend id -> check sum
    // add backend id to this map only after sending task
    private Map<Long, Long> checksumMap;

    private int checkedSchemaHash;
    private long checkedVersion;
    private long checkedVersionHash;

    private long createTime;
    private long timeoutMs;

    public CheckConsistencyJob(long tabletId) {
        this.state = JobState.PENDING;
        this.tabletId = tabletId;

        this.checksumMap = Maps.newHashMap();

        this.checkedSchemaHash = -1;
        this.checkedVersion = -1L;
        this.checkedVersionHash = -1L;

        this.createTime = System.currentTimeMillis();
        this.timeoutMs = 0L;
    }

    public JobState getState() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public long getTabletId() {
        return tabletId;
    }

    public synchronized void setChecksum(long backendId, long checksum) {
        this.checksumMap.put(backendId, checksum);
    }

    /*
     * return:
     *  true: continue
     *  false: cancel
     */
    public boolean sendTasks() {
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
        if (tabletMeta == null) {
            LOG.debug("tablet[{}] has been removed", tabletId);
            return false;
        }

        Database db = Catalog.getCurrentCatalog().getDb(tabletMeta.getDbId());
        if (db == null) {
            LOG.debug("db[{}] does not exist", tabletMeta.getDbId());
            return false;
        }

        // get user resource info
        TResourceInfo resourceInfo = null;
        if (ConnectContext.get() != null) {
            resourceInfo = ConnectContext.get().toResourceCtx();
        }
        
        Tablet tablet = null;

        AgentBatchTask batchTask = new AgentBatchTask();
        db.readLock();
        try {
            Table table = db.getTable(tabletMeta.getTableId());
            if (table == null) {
                LOG.debug("table[{}] does not exist", tabletMeta.getTableId());
                return false;
            }
            OlapTable olapTable = (OlapTable) table;

            Partition partition = olapTable.getPartition(tabletMeta.getPartitionId());
            if (partition == null) {
                LOG.debug("partition[{}] does not exist", tabletMeta.getPartitionId());
                return false;
            }

            // check partition's replication num. if 1 replication. skip
            short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
            if (replicationNum == (short) 1) {
                LOG.debug("partition[{}]'s replication num is 1. skip consistency check", partition.getId());
                return false;
            }

            MaterializedIndex index = partition.getIndex(tabletMeta.getIndexId());
            if (index == null) {
                LOG.debug("index[{}] does not exist", tabletMeta.getIndexId());
                return false;
            }

            tablet = index.getTablet(tabletId);
            if (tablet == null) {
                LOG.debug("tablet[{}] does not exist", tabletId);
                return false;
            }

            checkedVersion = partition.getVisibleVersion();
            checkedVersionHash = partition.getVisibleVersionHash();
            checkedSchemaHash = olapTable.getSchemaHashByIndexId(tabletMeta.getIndexId());

            int sentTaskReplicaNum = 0;
            long maxDataSize = 0;
            for (Replica replica : tablet.getReplicas()) {
                // 1. if state is CLONE, do not send task at this time
                if (replica.getState() == ReplicaState.CLONE
                        || replica.getState() == ReplicaState.DECOMMISSION) {
                    continue;
                }

                if (replica.getDataSize() > maxDataSize) {
                    maxDataSize = replica.getDataSize();
                }

                CheckConsistencyTask task = new CheckConsistencyTask(resourceInfo, replica.getBackendId(),
                                                                     tabletMeta.getDbId(),
                                                                     tabletMeta.getTableId(),
                                                                     tabletMeta.getPartitionId(),
                                                                     tabletMeta.getIndexId(),
                                                                     tabletId, checkedSchemaHash,
                                                                     checkedVersion, checkedVersionHash);

                // add task to send
                batchTask.addTask(task);

                // init checksum as '-1'
                checksumMap.put(replica.getBackendId(), -1L);

                ++sentTaskReplicaNum;
            }

            if (sentTaskReplicaNum < replicationNum / 2 + 1) {
                LOG.info("tablet[{}] does not have enough replica to check.", tabletId);
            } else {
                if (maxDataSize > 0) {
                    timeoutMs = maxDataSize / 1000 / 1000 / 1000 * CHECK_CONSISTENCT_TIME_COST_PER_GIGABYTE_MS;
                }
                timeoutMs = Math.max(timeoutMs, Config.check_consistency_default_timeout_second * 1000L);
                state = JobState.RUNNING;
            }

        } finally {
            db.readUnlock();
        }

        if (state != JobState.RUNNING) {
            // failed to send task. set tablet's checked version and version hash to avoid choosing it again
            db.writeLock();
            try {
                tablet.setCheckedVersion(checkedVersion, checkedVersionHash);
            } finally {
                db.writeUnlock();
            }
            return false;
        }

        // send task
        Preconditions.checkState(batchTask.getTaskNum() > 0);
        for (AgentTask task : batchTask.getAllTasks()) {
            AgentTaskQueue.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);
        LOG.debug("tablet[{}] send check consistency task. num: {}", tabletId, batchTask.getTaskNum());

        return true;
    }

    /*
     * return:
     *  0: not finished
     *  1: finished
     *  -1: cancel
     */
    public synchronized int tryFinishJob() {
        if (state == JobState.PENDING) {
            return 0;
        }

        // check again. in case tablet has already been removed
        TabletMeta tabletMeta = Catalog.getCurrentInvertedIndex().getTabletMeta(tabletId);
        if (tabletMeta == null) {
            LOG.warn("tablet[{}] has been removed", tabletId);
            return -1;
        }

        Database db = Catalog.getCurrentCatalog().getDb(tabletMeta.getDbId());
        if (db == null) {
            LOG.warn("db[{}] does not exist", tabletMeta.getDbId());
            return -1;
        }

        boolean isConsistent = true;
        db.writeLock();
        try {
            Table table = db.getTable(tabletMeta.getTableId());
            if (table == null) {
                LOG.warn("table[{}] does not exist", tabletMeta.getTableId());
                return -1;
            }
            OlapTable olapTable = (OlapTable) table;

            Partition partition = olapTable.getPartition(tabletMeta.getPartitionId());
            if (partition == null) {
                LOG.warn("partition[{}] does not exist", tabletMeta.getPartitionId());
                return -1;
            }

            MaterializedIndex index = partition.getIndex(tabletMeta.getIndexId());
            if (index == null) {
                LOG.warn("index[{}] does not exist", tabletMeta.getIndexId());
                return -1;
            }

            Tablet tablet = index.getTablet(tabletId);
            if (tablet == null) {
                LOG.warn("tablet[{}] does not exist", tabletId);
                return -1;
            }

            // check if schema has changed
            if (checkedSchemaHash != olapTable.getSchemaHashByIndexId(tabletMeta.getIndexId())) {
                LOG.info("index[{}]'s schema hash has been changed. [{} -> {}]. retry", tabletMeta.getIndexId(),
                        checkedSchemaHash, olapTable.getSchemaHashByIndexId(tabletMeta.getIndexId()));
                return -1;
            }

            if (!isTimeout()) {
                // check finished. remove replica in checksumMap which does not exist anymore
                boolean isFinished = true;
                Iterator<Map.Entry<Long, Long>> iter = checksumMap.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<Long, Long> entry = iter.next();
                    if (tablet.getReplicaByBackendId(entry.getKey()) == null) {
                        LOG.debug("tablet[{}]'s replica in backend[{}] does not exist. remove from checksumMap",
                                  tabletId, entry.getKey());
                        iter.remove();
                        continue;
                    }

                    if (entry.getValue() == -1) {
                        LOG.debug("tablet[{}] has unfinished replica check sum task. backend[{}]",
                                  tabletId, entry.getKey());
                        isFinished = false;
                    }
                }

                if (!isFinished) {
                    return 0;
                }

                // all clear. check checksum
                long lastChecksum = -1L;
                for (Map.Entry<Long, Long> entry : checksumMap.entrySet()) {
                    long checksum = entry.getValue();
                    if (lastChecksum == -1) {
                        lastChecksum = checksum;
                    } else {
                        if (checksum != lastChecksum) {
                            // find different one
                            isConsistent = false;
                            break;
                        }
                    }
                }

                if (isConsistent) {
                    LOG.info("tablet[{}] is consistent: {}", tabletId, checksumMap.keySet());
                } else {
                    StringBuilder sb = new StringBuilder();
                    sb.append("tablet[").append(tabletId).append("] is not consistent: ");
                    for (Map.Entry<Long, Long> entry : checksumMap.entrySet()) {
                        sb.append("[").append(entry.getKey()).append("-").append(entry.getValue()).append("]");
                    }
                    sb.append(" [").append(tabletMeta).append("]");
                    LOG.error(sb.toString());
                }
            } else {
                LOG.info("tablet[{}] check consistency job cancelled. timeout", tabletId);
            }

            // no matter timeout or not. set this tablet as finished
            // job done. set last check time to each instance
            long lastCheckTime = System.currentTimeMillis();
            db.setLastCheckTime(lastCheckTime);
            olapTable.setLastCheckTime(lastCheckTime);
            partition.setLastCheckTime(lastCheckTime);
            index.setLastCheckTime(lastCheckTime);
            tablet.setLastCheckTime(lastCheckTime);
            tablet.setIsConsistent(isConsistent);

            // set checked version
            tablet.setCheckedVersion(checkedVersion, checkedVersionHash);

            // log
            ConsistencyCheckInfo info = new ConsistencyCheckInfo(db.getId(), table.getId(), partition.getId(),
                                                                 index.getId(), tabletId, lastCheckTime,
                                                                 checkedVersion, checkedVersionHash, isConsistent);
            Catalog.getCurrentCatalog().getEditLog().logFinishConsistencyCheck(info);
            return 1;

        } finally {
            db.writeUnlock();
        }
    }

    private boolean isTimeout() {
        if (timeoutMs == 0 || System.currentTimeMillis() - createTime < timeoutMs) {
            return false;
        }
        return true;
    }

    public synchronized void handleFinishedReplica(long backendId, long checksum) {
        if (this.checksumMap.containsKey(backendId)) {
            checksumMap.put(backendId, checksum);
        } else {
            // should not happend. add log to observe
            LOG.warn("can not find backend[{}] in tablet[{}]'s consistency check job", backendId, tabletId);
        }
    }

    public synchronized void clear() {
        // clear task
        for (Long backendId : checksumMap.keySet()) {
            AgentTaskQueue.removeTask(backendId, TTaskType.CHECK_CONSISTENCY, tabletId);
        }
    }
}

