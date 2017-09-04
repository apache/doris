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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.PartitionInfo;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Replica.ReplicaState;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.catalog.TabletInvertedIndex;
import com.baidu.palo.catalog.TabletMeta;
import com.baidu.palo.clone.Clone;
import com.baidu.palo.clone.CloneJob.JobPriority;
import com.baidu.palo.cluster.Cluster;
import com.baidu.palo.persist.BackendIdsUpdateInfo;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.FeMetaVersion;
import com.baidu.palo.common.MetaNotFoundException;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.Backend.BackendState;
import com.baidu.palo.system.SystemInfoService;
import com.baidu.palo.task.AgentTask;
import com.baidu.palo.thrift.TTabletInfo;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class DecommissionBackendJob extends AlterJob {

    public enum DecommissionType {
        SystemDecommission, // after finished system decommission, the backend will be removed from Palo.
        ClusterDecommission // after finished cluster decommission, the backend will be removed from cluster.
    }

    private static final Logger LOG = LogManager.getLogger(DecommissionBackendJob.class);

    private static final Joiner JOINER = Joiner.on("; ");

    private static final int NUM_TABLETS_PER_ONE_ROUND = 1000;

    private Map<String, Map<Long, Backend>> clusterBackendsMap;
    private Set<Long> allClusterBackendIds;

    // add backendId to 'finishedBackendIds' only if no tablets exist in that
    // backend
    private Set<Long> finishedBackendIds;
    // add tabletId to 'finishedTabletIds' only if that tablet has full replicas
    private Set<Long> finishedTabletIds;

    private DecommissionType decommissionType;

    public DecommissionBackendJob() {
        // for persist
        super(JobType.DECOMMISSION_BACKEND);

        clusterBackendsMap = Maps.newHashMap();
        finishedBackendIds = Sets.newHashSet();
        finishedTabletIds = Sets.newHashSet();
        allClusterBackendIds = Sets.newHashSet();
        decommissionType = DecommissionType.SystemDecommission;
    }

    public DecommissionBackendJob(long jobId, Map<String, Map<Long, Backend>> backendIds) {
        super(JobType.DECOMMISSION_BACKEND, -1L, jobId, null);

        this.clusterBackendsMap = backendIds;
        finishedBackendIds = Sets.newHashSet();
        finishedTabletIds = Sets.newHashSet();
        allClusterBackendIds = Sets.newHashSet();
        for (Map<Long, Backend> backends : clusterBackendsMap.values()) {
            allClusterBackendIds.addAll(backends.keySet());
        }
        decommissionType = DecommissionType.SystemDecommission;
    }

    /**
     * in Multi-Tenancy example "clusterA:1,2,3;clusterB:4,5,6"
     * 
     * @return
     */
    public String getBackendIdsString() {
        final Joiner joiner = Joiner.on(",");
        final Set<String> clusterBackendsSet = new HashSet<String>();
        for (String cluster : clusterBackendsMap.keySet()) {
            final Map<Long, Backend> backends = clusterBackendsMap.get(cluster);
            final String backendStr = joiner.join(backends.keySet());
            final StringBuilder builder = new StringBuilder(cluster);
            builder.append(":").append(backendStr);
            clusterBackendsSet.add(builder.toString());
        }
        String res = JOINER.join(clusterBackendsSet);
        return res;
    }

    public Set<Long> getBackendIds(String name) {
        return clusterBackendsMap.get(name).keySet();
    }

    public synchronized int getFinishedTabletNum() {
        return finishedTabletIds.size();
    }
    
    public DecommissionType getDecommissionType() {
        return decommissionType;
    }

    public void setDecommissionType(DecommissionType decommissionType) {
        this.decommissionType = decommissionType;
    }

    @Override
    public void addReplicaId(long parentId, long replicaId, long backendId) {
        throw new NotImplementedException();
    }

    @Override
    public void setReplicaFinished(long parentId, long replicaId) {
        throw new NotImplementedException();
    }

    @Override
    public synchronized boolean sendTasks() {
        this.state = JobState.RUNNING;
        // not like other alter job,
        // here we send clone task(migration) actively.
        // always return true. because decommission backend job never cancelled
        // except cancelled by administrator manually.

        Catalog catalog = Catalog.getInstance();
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        SystemInfoService clusterInfo = Catalog.getCurrentSystemInfo();
        Clone clone = catalog.getCloneInstance();

        // clusterName -> available backends num
        final Map<String, Integer> clusterAvailBeMap = Maps.newHashMap();
        for (String clusterName : clusterBackendsMap.keySet()) {
            final Map<Long, Backend> backends = clusterBackendsMap.get(clusterName);
            final List<Backend> clusterAliveBackends = clusterInfo.getClusterBackends(clusterName,
                                                                                      true /* need alive */);
            if (clusterAliveBackends == null) {
                LOG.warn("does not belong to any cluster.");
                return true;
            }
            int aliveBackendNum = clusterAliveBackends.size();
            int availableBackendNum = aliveBackendNum - backends.keySet().size();
            if (availableBackendNum <= 0) {
                // do nothing, just log
                LOG.warn("no available backends except decommissioning ones.");
                return true;
            }
            clusterAvailBeMap.put(clusterName, availableBackendNum);
        }

        for (String clusterName : clusterBackendsMap.keySet()) {
            final Map<Long, Backend> backends = clusterBackendsMap.get(clusterName);
            Iterator<Long> backendIter = backends.keySet().iterator();
            final int availableBackendNum = clusterAvailBeMap.get(clusterName);
            while (backendIter.hasNext()) {
                long backendId = backendIter.next();
                Backend backend = clusterInfo.getBackend(backendId);

                if (backend == null || !backend.isDecommissioned()) {
                    backendIter.remove();
                    LOG.info("backend[{}] is not decommissioned. remove from decommission jobs");
                    continue;
                }

                if (finishedBackendIds.contains(backendId)) {
                    continue;
                }

                List<Long> backendTabletIds = invertedIndex.getTabletIdsByBackendId(backendId);
                LOG.info("backend[{}] which is being decommissioned has {} tablets", backendId,
                        backendTabletIds.size());
                if (backendTabletIds.isEmpty()) {
                    LOG.info("no tablets on backend[{}]. set finished.", backendId);
                    continue;
                }

                int counter = 0;
                Iterator<Long> iterator = backendTabletIds.iterator();
                while (iterator.hasNext() && counter <= NUM_TABLETS_PER_ONE_ROUND) {
                    long tabletId = iterator.next();
                    if (finishedTabletIds.contains(tabletId)) {
                        // this tablet is finished
                        continue;
                    }

                    // get tablet meta
                    TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                    if (tabletMeta == null) {
                        LOG.debug("cannot find tablet meta. tablet[{}]", tabletId);
                        continue;
                    }

                    long dbId = tabletMeta.getDbId();
                    Database db = catalog.getDb(dbId);
                    if (db == null) {
                        LOG.debug("cannot find db. tablet[{}]", tabletId);
                        continue;
                    }

                    // get db lock
                    db.readLock();
                    try {
                        long tableId = tabletMeta.getTableId();
                        Table table = db.getTable(tableId);
                        if (table == null) {
                            LOG.debug("cannot find table. tablet[{}]", tabletId);
                            continue;
                        }
                        OlapTable olapTable = (OlapTable) table;

                        long partitionId = tabletMeta.getPartitionId();
                        Partition partition = olapTable.getPartition(partitionId);
                        if (partition == null) {
                            LOG.debug("cannot find partition. tablet[{}]", tabletId);
                            continue;
                        }

                        // get replication num
                        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                        short replicationNum = partitionInfo.getReplicationNum(partitionId);
                        if (availableBackendNum < replicationNum) {
                            LOG.warn("partition[{}]'s replication num is {}. not enough available backend: {}",
                                    partitionId, replicationNum, availableBackendNum);
                            return true;
                        }

                        long indexId = tabletMeta.getIndexId();
                        MaterializedIndex index = partition.getIndex(indexId);
                        if (index == null) {
                            LOG.debug("cannot find index. tablet[{}]", tabletId);
                            continue;
                        }

                        Tablet tablet = index.getTablet(tabletId);
                        if (tablet == null) {
                            LOG.error("can this happends? tabletMeta {}: {}", tabletId, tabletMeta.toString());
                            continue;
                        }

  
                        // exclude backend in same hosts with the other replica
                        Set<String> hosts = Sets.newHashSet();
                        for (Replica replica : tablet.getReplicas()) {
                            if (replica.getBackendId() != backendId) {
                                hosts.add(clusterInfo.getBackend(replica.getBackendId()).getHost());
                            }   
                        } 

                        // choose dest backend
                        long destBackendId = -1L;
                        int num = 0;
                        while (num++ < availableBackendNum) {
                            List<Long> destBackendIds = clusterInfo.seqChooseBackendIds(1, true, false, clusterName);
                            if (destBackendIds == null) {
                                LOG.warn("no more backends available.");
                                return true;
                            }

                            if (hosts.contains(clusterInfo.getBackend(destBackendIds.get(0)).getHost())) {
                                // replica can not in same backend
                                continue;
                            }

                            destBackendId = destBackendIds.get(0);
                            break;
                        }

                        if (destBackendId == -1L) {
                            LOG.warn("no available backend. tablet:{} {}", tabletId, tabletMeta.toString());
                            return true;
                        }

                        if (clone.addCloneJob(dbId, tableId, partitionId, indexId, tabletId, destBackendId,
                                com.baidu.palo.clone.CloneJob.JobType.MIGRATION, JobPriority.NORMAL,
                                Config.clone_job_timeout_second * 1000L)) {
                            ++counter;
                        }
                    } finally {
                        db.readUnlock();
                    }
                } // end for backend tablets

                LOG.info("add {} clone tasks. total backend tablets: {}", counter, backendTabletIds.size());
            } // end for backends
        }

        return true;
    }

    @Override
    public synchronized void cancel(OlapTable olapTable, String msg) {
        // set state
        this.state = JobState.CANCELLED;
        if (msg != null) {
            this.cancelMsg = msg;
        }

        this.finishedTime = System.currentTimeMillis();

        // no need to log
        LOG.info("finished cancel decommission backend");
    }

    @Override
    public void removeReplicaRelatedTask(long parentId, long tabletId, long replicaId, long backendId) {
        throw new NotImplementedException();
    }

    @Override
    public void directRemoveReplicaTask(long replicaId, long backendId) {
        throw new NotImplementedException();
    }

    @Override
    public synchronized void handleFinishedReplica(AgentTask task, TTabletInfo finishTabletInfo, long reportVersion)
            throws MetaNotFoundException {
        throw new NotImplementedException();
    }

    @Override
    public synchronized int tryFinishJob() {
        Catalog catalog = Catalog.getInstance();
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        SystemInfoService systemInfo = Catalog.getCurrentSystemInfo();

        LOG.debug("start try finish decommission backend job: {}", getBackendIdsString());
        for (String cluster : clusterBackendsMap.keySet()) {
            final Map<Long, Backend> backends = clusterBackendsMap.get(cluster);
            // check if tablets in one backend has full replicas
            Iterator<Long> backendIter = backends.keySet().iterator();
            while (backendIter.hasNext()) {
                long backendId = backendIter.next();
                Backend backend = systemInfo.getBackend(backendId);
                if (backend == null || !backend.isDecommissioned()) {
                    backendIter.remove();
                    LOG.info("backend[{}] is not decommissioned. remove from decommission jobs");
                    continue;
                }

                if (finishedBackendIds.contains(backendId)) {
                    continue;
                }

                List<Long> backendTabletIds = invertedIndex.getTabletIdsByBackendId(backendId);
                if (backendTabletIds.isEmpty()) {
                    LOG.info("no tablet in {}", backend);
                    finishedBackendIds.add(backendId);
                    continue;
                }

                // for each tablet, check if replica is full
                boolean isBackendFinished = true;
                Iterator<Long> iterator = backendTabletIds.iterator();
                while (iterator.hasNext()) {
                    long tabletId = iterator.next();
                    if (finishedTabletIds.contains(tabletId)) {
                        continue;
                    }

                    TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                    if (tabletMeta == null) {
                        continue;
                    }

                    long dbId = tabletMeta.getDbId();
                    Database db = catalog.getDb(dbId);
                    if (db == null) {
                        continue;
                    }

                    // get db lock
                    db.readLock();
                    try {
                        long tableId = tabletMeta.getTableId();
                        Table table = db.getTable(tableId);
                        if (table == null) {
                            continue;
                        }
                        OlapTable olapTable = (OlapTable) table;

                        long partitionId = tabletMeta.getPartitionId();
                        Partition partition = olapTable.getPartition(partitionId);
                        if (partition == null) {
                            continue;
                        }

                        long indexId = tabletMeta.getIndexId();
                        MaterializedIndex index = partition.getIndex(indexId);
                        if (index == null) {
                            continue;
                        }

                        Tablet tablet = index.getTablet(tabletId);
                        if (tablet == null) {
                            LOG.warn("can this happends? tabletMeta {}: {}", tabletId, tabletMeta.toString());
                            continue;
                        }

                        // get replication num
                        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                        short replicationNum = partitionInfo.getReplicationNum(partitionId);

                        int onlineReplicaNum = 0;
                        for (Replica replica : tablet.getReplicas()) {
                            if (replica.getState() == ReplicaState.CLONE) {
                                continue;
                            }

                            if (!systemInfo.checkBackendAvailable(replica.getBackendId())) {
                                continue;
                            }
                            ++onlineReplicaNum;
                        }

                        if (onlineReplicaNum >= replicationNum) {
                            LOG.debug("tablet[{}] has full replicas.", tabletId);
                            finishedTabletIds.add(tabletId);
                            continue;
                        } else {
                            isBackendFinished = false;
                        }
                    } finally {
                        db.readUnlock();
                    }
                } // end for tablets

                if (isBackendFinished) {
                    LOG.info("{} finished migrating tablets", backend);
                    finishedBackendIds.add(backendId);
                } else {
                    LOG.info("{} lefts tablets to migrate. total finished tablets num: {}", backend,
                             finishedTabletIds.size());
                }
            } // end for backends
        }

        if (finishedBackendIds.size() >= allClusterBackendIds.size()) {
            // use '>=' not '==', because backend may be removed from backendIds
            // after it finished.
            // drop backend
            if (decommissionType == DecommissionType.SystemDecommission) {
                for (long backendId : allClusterBackendIds) {
                    try {
                        systemInfo.dropBackend(backendId);
                    } catch (DdlException e) {
                        // it's ok, backend has already been dropped
                        LOG.info("drop backend[{}] failed. cause: {}", backendId, e.getMessage());
                    }
                }
            } else {
                // Shrinking capacity in cluster
                if (decommissionType == DecommissionType.ClusterDecommission) {
                    for (String clusterName : clusterBackendsMap.keySet()) {
                        final Map<Long, Backend> idToBackend = clusterBackendsMap.get(clusterName);
                        final Cluster cluster = Catalog.getInstance().getCluster(clusterName);
                        List<Long> backendList = Lists.newArrayList();
                        for (long id : idToBackend.keySet()) {
                            final Backend backend = idToBackend.get(id);
                            backend.clearClusterName();
                            backend.setBackendState(BackendState.free);
                            backend.setDecommissioned(false);
                            backendList.add(id);
                            cluster.removeBackend(id);
                        }
                        BackendIdsUpdateInfo updateInfo = new BackendIdsUpdateInfo(backendList);
                        Catalog.getInstance().getEditLog().logUpdateClusterAndBackendState(updateInfo);
                    }
                }
            }

            this.finishedTime = System.currentTimeMillis();
            this.state = JobState.FINISHED;

            Catalog.getInstance().getEditLog().logFinishDecommissionBackend(this);

            LOG.info("finished {} decommission {} backends: {}", decommissionType.toString(),
                    allClusterBackendIds.size(), getBackendIdsString());
            return 1;
        } else {
            Set<Long> unfinishedBackendIds = Sets.newHashSet();
            for (Long backendId : allClusterBackendIds) {
                if (!finishedBackendIds.contains(backendId)) {
                    unfinishedBackendIds.add(backendId);
                }
            }
            LOG.info("waiting {} backends to finish tablets migration: {}", unfinishedBackendIds.size(),
                    unfinishedBackendIds);
            return 0;
        }
    }

    @Override
    public synchronized void clear() {
        finishedBackendIds.clear();
        finishedTabletIds.clear();
    }

    @Override
    public void unprotectedReplayInitJob(Database db) {
        // do nothing
    }

    @Override
    public void unprotectedReplayFinish(Database db) {
        // do nothing
    }

    @Override
    public void unprotectedReplayCancel(Database db) {
        // do nothing
    }

    /**
     * to Backward compatibility
     * 
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
            long clusterMapSize = in.readLong();
            while (clusterMapSize-- > 0) {
                final String cluster = Text.readString(in);
                long backendMspSize = in.readLong();
                Map<Long, Backend> backends = Maps.newHashMap();
                while (backendMspSize-- > 0) {
                    final long id = in.readLong();
                    final Backend backend = Catalog.getCurrentSystemInfo().getBackend(id);
                    backends.put(id, backend);
                    allClusterBackendIds.add(id);
                }
                clusterBackendsMap.put(cluster, backends);
            }
        } else {
            int backendNum = in.readInt();
            Map<Long, Backend> backends = Maps.newHashMap();
            for (int i = 0; i < backendNum; i++) {
                final long backendId = in.readLong();
                allClusterBackendIds.add(backendId);
                final Backend backend = Catalog.getCurrentSystemInfo().getBackend(backendId);
                backends.put(backendId, backend);
            }
            clusterBackendsMap.put(SystemInfoService.DEFAULT_CLUSTER, backends);
        }
        
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_33) {
            String str = Text.readString(in);
            // this is only for rectify misspellings...
            if (str.equals("SystemDecomission")) {
                str = "SystemDecommission";
            } else if (str.equals("ClusterDecomission")) {
                str = "ClusterDecommission";
            }
            decommissionType = DecommissionType.valueOf(str);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        out.writeLong(clusterBackendsMap.keySet().size());
        for (String cluster : clusterBackendsMap.keySet()) {
            final Map<Long, Backend> backends = clusterBackendsMap.get(cluster);
            Text.writeString(out, cluster);
            out.writeLong(backends.keySet().size());
            for (Long id : backends.keySet()) {
                out.writeLong(id);
            }
        }
        Text.writeString(out, decommissionType.toString());
    }

    public static DecommissionBackendJob read(DataInput in) throws IOException {
        DecommissionBackendJob decommissionBackendJob = new DecommissionBackendJob();
        decommissionBackendJob.readFields(in);
        return decommissionBackendJob;
    }

}
