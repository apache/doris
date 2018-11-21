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

package org.apache.doris.clone;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.clone.TabletFactory.Slot;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.CloneTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/*
 * TabletInfo contains all information which is created during tablet factory processing.
 */
public class TabletInfo implements Comparable<TabletInfo> {
    private static final Logger LOG = LogManager.getLogger(TabletInfo.class);

    public enum Priority {
        VERY_HIGH, HIGH, NORMAL, LOW
    }

    public enum State {
        PENDING, RUNNING, FINISHED, CANCELLED
    }

    private Priority priority;
    private State state;
    private TabletStatus tabletStatus;

    private String cluster;
    private long dbId;
    private long tblId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private int schemaHash;
    private TStorageMedium storageMedium;

    private long createTime;

    // components which will be set during processing
    private Tablet tablet = null;
    private long visibleVersion = -1;
    private long visibleVersionHash = -1;
    private long committedVersion = -1;
    private long committedVersionHash = -1;

    private Replica srcReplica = null;
    private long srcPathHash = -1;
    private long destBackendId = -1;
    private long destPathHash = -1;
    private String errMsg = null;

    private SystemInfoService infoService;

    public TabletInfo(String cluster, long dbId, long tblId, long partId, long idxId, long tabletId,
            int schemaHash, TStorageMedium storageMedium, long createTime) {
        this.cluster = cluster;
        this.dbId = dbId;
        this.tblId = tblId;
        this.partitionId = partId;
        this.indexId = idxId;
        this.tabletId = tabletId;
        this.schemaHash = schemaHash;
        this.storageMedium = storageMedium;
        this.createTime = createTime;
        this.infoService = Catalog.getCurrentSystemInfo();
        this.state = State.PENDING;
    }

    public CloneTask createCloneReplicaAndTask() {
        Backend srcBe = infoService.getBackend(srcReplica.getBackendId());
        if (srcBe == null) {
            LOG.info("backend {} does not exist", srcReplica.getBackendId());
            return null;
        }

        // create the clone task and clone replica.
        // we use visible version in clone task, but set the clone replica's last failed version to
        // committed version.
        // because the clone task can only clone the data with visible version.
        // so after clone is finished, the clone replica's version is visible version, but its last
        // failed version is committed version, which is larger than visible version.
        // So after clone, this clone replica is still version incomplete, and it will be repaired in
        // another clone task.
        // That is, we may need to use 2 clone tasks to create a new replica. It is inefficient,
        // but there is no other way now.
        TBackend tSrcBe = new TBackend(srcBe.getHost(), srcBe.getBePort(), srcBe.getHttpPort());
        CloneTask task = new CloneTask(destBackendId, dbId, tblId, partitionId, indexId,
                tblId, schemaHash, Lists.newArrayList(tSrcBe), storageMedium,
                visibleVersion, visibleVersionHash, destPathHash);

        Replica cloneReplica = new Replica(
                Catalog.getCurrentCatalog().getNextId(), destBackendId,
                -1 /* version */, 0 /* version hash */,
                -1 /* data size */, -1 /* row count */,
                ReplicaState.CLONE,
                committedVersion, committedVersionHash, /* use committed version as last failed version */
                -1 /* last success version */, 0 /* last success version hash */);

        // addReplica() method will and this replica to tablet inverted index too.
        tablet.addReplica(cloneReplica);

        this.state = State.RUNNING;
        return task;
    }

    public Priority getPriority() {
        return priority;
    }

    public void setPriority(Priority priority) {
        this.priority = priority;
    }

    public State getState() {
        return state;
    }

    public void setState(State state, String errMsg) {
        this.state = state;
        this.errMsg = errMsg;
    }

    public TabletStatus getTabletStatus() {
        return tabletStatus;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTblId() {
        return tblId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getIndexId() {
        return indexId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public String getCluster() {
        return cluster;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getCommittedVersion() {
        return visibleVersion;
    }

    public long getCommittedVersionHash() {
        return visibleVersionHash;
    }

    public void setTablet(Tablet tablet) {
        this.tablet = tablet;
    }

    public List<Replica> getReplicas() {
        return tablet.getReplicas();
    }

    public void setVersionInfo(long visibleVersion, long visibleVersionHash,
            long committedVersion, long committedVersionHash) {
        this.visibleVersion = visibleVersion;
        this.visibleVersionHash = visibleVersionHash;
        this.committedVersion = committedVersion;
        this.committedVersionHash = committedVersionHash;
    }

    public void setDestination(Long destBe, long destPathHash) {
        this.destBackendId = destBe;
        this.destPathHash = destPathHash;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public long getTabletSize() {
        long max = Long.MIN_VALUE;
        for (Replica replica : tablet.getReplicas()) {
            if (replica.getDataSize() > max) {
                max = replica.getDataSize();
            }
        }
        return max;
    }

    public boolean containsBE(long beId) {
        for (Replica replica : tablet.getReplicas()) {
            if (replica.getBackendId() == beId) {
                return true;
            }
        }
        return false;
    }

    public boolean chooseSrcReplica(Map<Long, Slot> backendsWorkingSlots) {
        /*
         * get all candidate source replicas
         * 1. source replica should be healthy.
         * 2. slot of this source replica is available. 
         */
        List<Replica> candidates = Lists.newArrayList();
        for (Replica replica : tablet.getReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null || !be.isAlive()) {
                // backend which is in decommission can still be the source backend
                continue;
            }

            if (replica.getLastFailedVersion() > 0) {
                continue;
            }

            if (!replica.checkVersionCatchUp(visibleVersion, visibleVersionHash)) {
                continue;
            }

            candidates.add(replica);
        }

        if (candidates.isEmpty()) {
            return false;
        }

        // choose a replica which slot is available from candidates.
        for (Replica srcReplica : candidates) {
            Slot slot = backendsWorkingSlots.get(srcReplica.getBackendId());
            if (slot == null) {
                continue;
            }

            long srcPathHash = slot.takeSlot(srcReplica.getPathHash());
            if (srcPathHash != -1) {
                this.srcReplica = srcReplica;
                this.srcPathHash = srcPathHash;
                return true;
            }
        }

        return false;
    }

    /*
     * Same rules as choosing source replica for supplement.
     * But we need to check that we can not choose the same replica as dest replica,
     * because replica info is changing all the time.
     */
    public boolean chooseSrcReplicaForVersionIncomplete(Map<Long, Slot> backendsWorkingSlots) {
        if (chooseSrcReplica(backendsWorkingSlots)) {
            if (srcReplica.getBackendId() == destBackendId) {
                return false;
            }
            return true;
        }
        return false;
    }

    /*
     * Rules to choose a destination replica for version incomplete
     * 1. replica's last failed version > 0
     * 2. better to choose a replica which has a lower last failed version
     * 3. best to choose a replica if its last success version > last failed version
     */
    public boolean chooseDestReplicaForVersionIncomplete(Map<Long, Slot> backendsWorkingSlots) {
        Replica chosenReplica = null;
        for (Replica replica : tablet.getReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null || !be.isAvailable()) {
                continue;
            }
            if (replica.getLastFailedVersion() <= 0) {
                continue;
            }

            if (chosenReplica == null) {
                chosenReplica = replica;
            } else if (replica.getLastSuccessVersion() > replica.getLastFailedVersion()) {
                chosenReplica = replica;
                break;
            } else if (replica.getLastFailedVersion() < chosenReplica.getLastFailedVersion()) {
                // its better to select a low last failed version replica
                chosenReplica = replica;
            }
        }

        if (chosenReplica == null) {
            return false;
        }

        // check if the dest replica has available slot
        Slot slot = backendsWorkingSlots.get(chosenReplica.getBackendId());
        if (slot == null) {
            return false;
        }

        long destPathHash = slot.takeSlot(chosenReplica.getPathHash());
        if (destPathHash == -1) {
            return false;
        }

        this.destBackendId = chosenReplica.getBackendId();
        this.destPathHash = chosenReplica.getPathHash();

        return true;
    }

    /*
     * release all resources before finishing this task
     */
    public void releaseResource(TabletFactory tabletFactory) {
        if (srcReplica != null) {
            Preconditions.checkState(srcPathHash != -1);
            Slot slot = tabletFactory.getBackendsWorkingSlots().get(srcReplica.getBackendId());
            if (slot != null) {
                slot.freeSlot(srcPathHash);
            }
        }

        if (destPathHash != -1) {
            Slot slot = tabletFactory.getBackendsWorkingSlots().get(destBackendId);
            if (slot != null) {
                slot.freeSlot(destPathHash);
            }
        }
    }

    public void deleteReplica(Replica replica) {
        tablet.deleteReplicaByBackendId(replica.getBackendId());
    }

    @Override
    public int compareTo(TabletInfo o) {
        if (priority.ordinal() < o.getPriority().ordinal()) {
            return -1;
        } else if (priority.ordinal() > o.getPriority().ordinal()) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tablet id: ").append(tabletId).append(", status: ").append(tabletStatus.name());
        sb.append(", state: ").append(state.name());
        if (srcReplica != null) {
            sb.append(". from backend: ").append(srcReplica.getBackendId());
            sb.append(" src path hash: ").append(srcPathHash);
        }
        if (destPathHash != -1) {
            sb.append(". to backend: ").append(destBackendId);
            sb.append(", dest path hash: ").append(destPathHash);
        }
        if (errMsg != null) {
            sb.append(". err: ").append(errMsg);
        }
        return sb.toString();
    }
}
