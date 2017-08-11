// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.alter;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.MetaNotFoundException;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.BackendEvent;
import com.baidu.palo.system.BackendEvent.BackendEventType;
import com.baidu.palo.task.AgentTask;
import com.baidu.palo.thrift.TResourceInfo;
import com.baidu.palo.thrift.TTabletInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

public abstract class AlterJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(AlterJob.class);

    public enum JobState {
        PENDING,
        RUNNING,
        FINISHED,
        CANCELLED
    }

    public enum JobType {
        ROLLUP,
        SCHEMA_CHANGE,
        DECOMMISSION_BACKEND;

        @Override
        public String toString() {
            switch (this) {
                case ROLLUP:
                    return "rollup";
                case SCHEMA_CHANGE:
                    return "schema change";
                case DECOMMISSION_BACKEND:
                    return "decommission backend";
                default:
                    Preconditions.checkState(false);
                    return "invalid";
            }
        }
    }

    protected final JobType type;
    protected JobState state;

    protected long dbId;
    protected long tableId;

    protected long createTime;
    protected long finishedTime;

    protected String cancelMsg;

    protected TResourceInfo resourceInfo;

    // backendId -> replicaIds
    // this map show which replica is still alive
    // if backend is down, replica is not reachable in BE, remove replica from this map
    protected Multimap<Long, Long> backendIdToReplicaIds;

    public AlterJob(JobType type) {
        // for persist
        this.type = type;
        this.backendIdToReplicaIds = HashMultimap.create();

        this.state = JobState.PENDING;

        this.createTime = System.currentTimeMillis();
        this.finishedTime = -1L;
    }

    public AlterJob(JobType type, long dbId, long tableId, TResourceInfo resourceInfo) {
        this.type = type;
        this.state = JobState.PENDING;

        this.dbId = dbId;
        this.tableId = tableId;
        this.resourceInfo = resourceInfo;

        this.createTime = System.currentTimeMillis();
        this.finishedTime = -1L;

        this.cancelMsg = "";

        this.backendIdToReplicaIds = HashMultimap.create();
    }

    public final JobType getType() {
        return this.type;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public synchronized JobState getState() {
        return this.state;
    }

    public final long getDbId() {
        return dbId;
    }

    public final long getTableId() {
        return tableId;
    }

    public final long getCreateTimeMs() {
        return this.createTime;
    }

    public final synchronized long getFinishedTime() {
        return this.finishedTime;
    }

    public synchronized void setMsg(String msg) {
        this.cancelMsg = msg;
    }

    public final synchronized String getMsg() {
        return this.cancelMsg;
    }

    public synchronized void handleBackendRemoveEvent(long backendId) {
        if (this.backendIdToReplicaIds.containsKey(backendId)) {
            LOG.warn("{} job[{}] is handling backend[{}] removed event", type, tableId, backendId);
            Set<Long> replicaIds = Sets.newHashSet(this.backendIdToReplicaIds.get(backendId));
            for (Long replicaId : replicaIds) {
                LOG.debug("remove replica[{}] from {} job[{}] cause backend[{}] removed",
                          replicaId, type, tableId, backendId);
                directRemoveReplicaTask(replicaId, backendId);
            }
        }
    }
    
    public boolean isTimeout() {
        // 0 means never timeout
        if (Config.alter_table_timeout_second == 0
                || System.currentTimeMillis() - this.createTime < Config.alter_table_timeout_second * 1000L) {
            return false;
        }
        setMsg("timeout");
        LOG.info("{} job[{}] timeout. cancel it.", type, tableId);
        return true;
    }

    /*
     * this should be call in each round.
     * otherwise,
     * alter job will not perceived backend's down event during job created and first handle round.
     */
    public boolean checkBackendState(Replica replica) {
        LOG.debug("check backend[{}] state for replica[{}]", replica.getBackendId(), replica.getId());
        Backend backend = Catalog.getCurrentSystemInfo().getBackend(replica.getBackendId());
        if (backend == null) {
            Catalog.getCurrentSystemInfo().getEventBus()
                    .post(new BackendEvent(BackendEventType.BACKEND_DROPPED, "does not found",
                                           Long.valueOf(replica.getBackendId())));
            return false;
        } else if (!backend.isAlive()) {
            Catalog.getCurrentSystemInfo().getEventBus()
                    .post(new BackendEvent(BackendEventType.BACKEND_DOWN, "is not alive",
                                           Long.valueOf(replica.getBackendId())));
            return false;
        } else if (backend.isDecommissioned()) {
            Catalog.getCurrentSystemInfo().getEventBus()
                    .post(new BackendEvent(BackendEventType.BACKEND_DECOMMISSION,
                                           "is decommissioned",
                                           Long.valueOf(replica.getBackendId())));
            return false;
        }
        
        return true;
    }

    public static AlterJob read(DataInput in) throws IOException {
        JobType type = JobType.valueOf(Text.readString(in));
        switch (type) {
            case ROLLUP:
                return RollupJob.read(in);
            case SCHEMA_CHANGE:
                return SchemaChangeJob.read(in);
            case DECOMMISSION_BACKEND:
                return DecommissionBackendJob.read(in);
            default:
                Preconditions.checkState(false);
                return null;
        }
    }

    /*
     * abstract methods
     */
    /*
     * add replicas which need to be handled in this job
     */
    public abstract void addReplicaId(long parentId, long replicaId, long backendId);

    /*
     * set replicas as finished when replica task report sucess
     */
    public abstract void setReplicaFinished(long parentId, long replicaId);

    /*
     * send tasks to backends
     */
    public abstract boolean sendTasks();

    /*
     * cancel job
     */
    public abstract void cancel(OlapTable olapTable, String msg);

    /*
     * remove replica related tasks in some failure situation
     */
    public abstract void removeReplicaRelatedTask(long parentId, long tabletId, long replicaId, long backendId);
    
    /*
     * remove task directly
     */ 
    public abstract void directRemoveReplicaTask(long replicaId, long backendId);
    
    /*
     * handle replica finish task report 
     */
    public abstract void handleFinishedReplica(AgentTask task, TTabletInfo finishTabletInfo, long reportVersion)
            throws MetaNotFoundException;

    /*
     * return
     *      -1: need cancel
     *       0: waiting next poll
     *       1: finished
     */
    public abstract int tryFinishJob();

    /*
     * clear some date structure in this job to save memory
     */
    public abstract void clear();

    /*
     * replay methods
     *   corresponding to start/finished/cancelled
     */
    public abstract void unprotectedReplayInitJob(Database db);

    public abstract void unprotectedReplayFinish(Database db);

    public abstract void unprotectedReplayCancel(Database db);

    @Override
    public synchronized void readFields(DataInput in) throws IOException {
        // read common members as write in AlterJob.write().
        // except 'type' member, which is read in AlterJob.read()
        if (Catalog.getCurrentCatalogJournalVersion() >= 4) {
            state = JobState.valueOf(Text.readString(in));
        }

        dbId = in.readLong();
        tableId = in.readLong();

        createTime = in.readLong();
        finishedTime = in.readLong();

        cancelMsg = Text.readString(in);

        // resource
        boolean hasResourceInfo = in.readBoolean();
        if (hasResourceInfo) {
            String user = Text.readString(in);
            String group = Text.readString(in);
            resourceInfo = new TResourceInfo(user, group);
        }
    }

    @Override
    public synchronized void write(DataOutput out) throws IOException {
        // write common members
        Text.writeString(out, type.name());

        Text.writeString(out, state.name());

        out.writeLong(dbId);
        out.writeLong(tableId);

        out.writeLong(createTime);
        out.writeLong(finishedTime);

        Text.writeString(out, cancelMsg);
        
        // resourceInfo
        if (resourceInfo == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, resourceInfo.getUser());
            Text.writeString(out, resourceInfo.getGroup());
        }
    }
}
