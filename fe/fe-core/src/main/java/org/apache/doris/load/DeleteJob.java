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

package org.apache.doris.load;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.task.PushTask;
import org.apache.doris.transaction.AbstractTxnStateChangeCallback;
import org.apache.doris.transaction.TransactionState;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class DeleteJob extends AbstractTxnStateChangeCallback {
    private static final Logger LOG = LogManager.getLogger(DeleteJob.class);

    public enum DeleteState {
        UN_QUORUM,
        QUORUM_FINISHED,
        FINISHED
    }

    private DeleteState state;

    // jobId(listenerId). use in beginTransaction to callback function
    private long id;
    // transaction id.
    private long signature;
    private String label;
    private Set<Long> totalTablets;
    private Set<Long> quorumTablets;
    private Set<Long> finishedTablets;
    Map<Long, TabletDeleteInfo> tabletDeleteInfoMap;
    private Set<PushTask> pushTasks;
    private DeleteInfo deleteInfo;

    public DeleteJob(long id, long transactionId, String label, DeleteInfo deleteInfo) {
        this.id = id;
        this.signature = transactionId;
        this.label = label;
        this.deleteInfo = deleteInfo;
        totalTablets = Sets.newHashSet();
        finishedTablets = Sets.newHashSet();
        quorumTablets = Sets.newHashSet();
        tabletDeleteInfoMap = Maps.newConcurrentMap();
        pushTasks = Sets.newHashSet();
        state = DeleteState.UN_QUORUM;
    }

    /**
     * check and update if this job's state is QUORUM_FINISHED or FINISHED
     * The meaning of state:
     * QUORUM_FINISHED: For each tablet there are more than half of its replicas have been finished
     * FINISHED: All replicas of this jobs have finished
     */
    public void checkAndUpdateQuorum() throws MetaNotFoundException {
        long dbId = deleteInfo.getDbId();
        long tableId = deleteInfo.getTableId();
        long partitionId = deleteInfo.getPartitionId();
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("can not find database "+ dbId +" when commit delete");
        }

        short replicaNum = -1;
        OlapTable table = (OlapTable) db.getTable(tableId);
        if (table == null) {
            throw new MetaNotFoundException("can not find table "+ tableId +" when commit delete");
        }
        table.readLock();
        try {
            replicaNum = table.getPartitionInfo().getReplicationNum(partitionId);
        } finally {
            table.readUnlock();
        }

        short quorumNum = (short) (replicaNum / 2 + 1);
        for (TabletDeleteInfo tDeleteInfo : getTabletDeleteInfo()) {
            if (tDeleteInfo.getFinishedReplicas().size() == replicaNum) {
                finishedTablets.add(tDeleteInfo.getTabletId());
            }
            if (tDeleteInfo.getFinishedReplicas().size() >= quorumNum) {
                quorumTablets.add(tDeleteInfo.getTabletId());
            }
        }
        LOG.info("check delete job quorum, transaction id: {}, total tablets: {}, quorum tablets: {},",
                signature, totalTablets.size(), quorumTablets.size());

        if (finishedTablets.containsAll(totalTablets)) {
            setState(DeleteState.FINISHED);
        } else if (quorumTablets.containsAll(totalTablets)) {
            setState(DeleteState.QUORUM_FINISHED);
        }
    }

    public void setState(DeleteState state) {
        this.state = state;
    }

    public DeleteState getState() {
        return this.state;
    }

    public boolean addTablet(long tabletId) {
        return totalTablets.add(tabletId);
    }

    public boolean addPushTask(PushTask pushTask) {
        return pushTasks.add(pushTask);
    }

    public boolean addFinishedReplica(long tabletId, Replica replica) {
        tabletDeleteInfoMap.putIfAbsent(tabletId, new TabletDeleteInfo(tabletId));
        TabletDeleteInfo tDeleteInfo =  tabletDeleteInfoMap.get(tabletId);
        return tDeleteInfo.addFinishedReplica(replica);
    }

    public DeleteInfo getDeleteInfo() {
        return deleteInfo;
    }

    public String getLabel() {
        return this.label;
    }

    public Set<PushTask> getPushTasks() {
        return pushTasks;
    }

    @Override
    public long getId() {
        return this.id;
    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        if (!txnOperated) {
            return;
        }
        executeFinish();
        Catalog.getCurrentCatalog().getEditLog().logFinishDelete(deleteInfo);
    }

    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason)
            throws UserException {
        // just to clean the callback
        Catalog.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(getId());
    }

    public void executeFinish() {
        setState(DeleteState.FINISHED);
        Catalog.getCurrentCatalog().getDeleteHandler().recordFinishedJob(this);
        Catalog.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(getId());
    }

    public long getTransactionId() {
        return this.signature;
    }

    public Collection<TabletDeleteInfo> getTabletDeleteInfo() {
        return tabletDeleteInfoMap.values();
    }

    public long getTimeoutMs() {
        if (FeConstants.runningUnitTest) {
            // for making unit test run fast
            return 1000;
        }
        // timeout is between 30 seconds to 5 min
        long timeout = Math.max(totalTablets.size() * Config.tablet_delete_timeout_second * 1000L, 30000L);
        return Math.min(timeout, Config.load_straggler_wait_second * 1000L);
    }
}
