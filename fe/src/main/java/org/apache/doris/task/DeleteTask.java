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

package org.apache.doris.task;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.common.Config;
import org.apache.doris.load.DeleteInfo;
import org.apache.doris.load.TabletDeleteInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class DeleteTask extends MasterTask {
    private static final Logger LOG = LogManager.getLogger(DeleteTask.class);

    private DeleteInfo deleteInfo;

    private Set<Long> totalTablets;
    private Set<Long> quorumTablets;
    Map<Long, TabletDeleteInfo> tabletDeleteInfoMap;
    private Set<PushTask> pushTasks;

    boolean isQuorum;
    boolean isCanceled;

    private Object lock;

    public DeleteTask(long transactionId, DeleteInfo deleteInfo) {
        this.signature = transactionId;
        this.deleteInfo = deleteInfo;
        totalTablets = Sets.newHashSet();
        quorumTablets = Sets.newHashSet();
        tabletDeleteInfoMap = Maps.newConcurrentMap();
        pushTasks = Sets.newHashSet();
        isQuorum = false;
        isCanceled = false;
        lock = new Object();
    }

    @Override
    protected void exec() {
        long dbId = deleteInfo.getDbId();
        long tableId = deleteInfo.getTableId();
        long partitionId = deleteInfo.getPartitionId();
        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            LOG.warn("can not find database "+ dbId +" when commit delete");
        }

        db.readLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            if (table == null) {
                LOG.warn("can not find table "+ tableId +" when commit delete");
            }

            short replicaNum = table.getPartitionInfo().getReplicationNum(partitionId);
            short quorumNum = (short) (replicaNum / 2 + 1);

            for (TabletDeleteInfo tDeleteInfo : tabletDeleteInfoMap.values()) {
                if (tDeleteInfo.getFinishedReplicas().size() >= quorumNum) {
                    quorumTablets.add(tDeleteInfo.getTabletId());
                }
            }

            LOG.info("delete task running, signature: {}, total tablets: {}, quorum tablets: {},",
                    signature, totalTablets.size(), quorumTablets.size());
            if (quorumTablets.containsAll(totalTablets)) {
                isQuorum= true;
            }
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            db.readUnlock();
        }
    }

    public boolean addTablet(long tabletId) {
        return totalTablets.add(tabletId);
    }

    public boolean addPushTask(PushTask pushTask) {
        return pushTasks.add(pushTask);
    }

    public boolean addFinishedReplica(long tabletId, Replica replica) {
        TabletDeleteInfo tDeleteInfo = tabletDeleteInfoMap.get(tabletId);
        if (tDeleteInfo == null) {
            tDeleteInfo = new TabletDeleteInfo(tabletId);
            tabletDeleteInfoMap.put(tabletId, tDeleteInfo);
        }
        return tDeleteInfo.addFinishedReplica(replica);
    }

    public DeleteInfo getDeleteInfo() {
        return deleteInfo;
    }

    public Set<PushTask> getPushTasks() {
        return pushTasks;
    }

    public Collection<TabletDeleteInfo> getTabletDeleteInfo() {
        return tabletDeleteInfoMap.values();
    }

    public boolean isQuorum() {
        return isQuorum;
    }

    public void join(long timeout) {
        synchronized (lock) {
            try {
                lock.wait(timeout);
            } catch (InterruptedException e) {
            }
        }
    }

    public void unJoin() {
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    public void setCancel() {
        isCanceled = true;
    }

    public boolean isCancel() {
        return isCanceled;
    }

    public long getTimeout() {
        // timeout is between 30 seconds to 5 min
        long timeout = Math.max(totalTablets.size() * Config.tablet_delete_timeout_second * 1000L, 30000L);
        return Math.min(timeout, Config.load_straggler_wait_second * 1000L);
    }
}
