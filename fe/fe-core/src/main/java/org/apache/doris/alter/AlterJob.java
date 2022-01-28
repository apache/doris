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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TTabletInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public abstract class AlterJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(AlterJob.class);

    public enum JobState {
        PENDING,
        RUNNING,
        FINISHING,
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
    protected long transactionId = -1;
    // not serialize it
    protected boolean isPreviousLoadFinished = false;
    protected AgentBatchTask batchClearAlterTask = null;

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

        this.state = JobState.PENDING;

        this.createTime = System.currentTimeMillis();
        this.finishedTime = -1L;
        // this.backendIdToReplicaIds = HashMultimap.create();
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
        // this.backendIdToReplicaIds = HashMultimap.create();
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
    
    public final long getTransactionId() {
        return transactionId;
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

    /**
     * this should be call in each round.
     * otherwise,
     * alter job will not perceived backend's down event during job created and first handle round.
     */
    protected boolean checkBackendState(Replica replica) {
        LOG.debug("check backend[{}] state for replica[{}]", replica.getBackendId(), replica.getId());
        Backend backend = Catalog.getCurrentSystemInfo().getBackend(replica.getBackendId());
        // not send event to event bus because there is a dead lock, job --> check state --> bus lock --> handle backend down
        // backend down --> bus lock --> handle backend down --> job.lock
        if (backend == null) {
            return false;
        } else if (!backend.isAlive()) {
            long currentTime = System.currentTimeMillis();
            // If this backend is done for a long time and not restart automatically.
            // we consider it as dead and return false.
            return backend.getLastUpdateMs() <= 0
                    || currentTime - backend.getLastUpdateMs() <= Config.max_backend_down_time_second * 1000;
        } else {
            return !backend.isDecommissioned();
        }

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

    /**
     * add replicas which need to be handled in this job
     */
    public abstract void addReplicaId(long parentId, long replicaId, long backendId);

    /**
     * set replicas as finished when replica task report success
     */
    public abstract void setReplicaFinished(long parentId, long replicaId);

    /**
     * send tasks to backends
     */
    public abstract boolean sendTasks();

    /**
     * cancel job
     */
    public abstract void cancel(OlapTable olapTable, String msg);

    /**
     * remove replica related tasks in some failure situation
     */
    public abstract void removeReplicaRelatedTask(long parentId, long tabletId, long replicaId, long backendId);
    
    /**
     * handle replica finish task report 
     */
    public abstract void handleFinishedReplica(AgentTask task, TTabletInfo finishTabletInfo, long reportVersion)
            throws MetaNotFoundException;

    /**
     * return
     *      -1: need cancel
     *       0: waiting next poll
     *       1: finishing
     */
    public abstract int tryFinishJob();

    /**
     * clear some date structure in this job to save memory
     */
    public abstract void clear();

    /**
     * do something when state transferring from FINISHING to FINISHED.
     * eg:
     *  set table's state to NORMAL
     */
    public abstract void finishJob();

    /**
     * replay methods
     *   corresponding to start/finished/cancelled
     */
    public abstract void replayInitJob(Database db);
    
    public abstract void replayFinishing(Database db);

    public abstract void replayFinish(Database db);

    public abstract void replayCancel(Database db);

    public abstract void getJobInfo(List<List<Comparable>> jobInfos, OlapTable tbl);

    // return true if all previous load job has been finished.
    // return false if not
    public boolean isPreviousLoadFinished() {
        if (isPreviousLoadFinished) {
            return true;
        } else {
            try {
                isPreviousLoadFinished = Catalog.getCurrentGlobalTransactionMgr()
                        .isPreviousTransactionsFinished(transactionId, dbId, Lists.newArrayList(tableId));
            } catch (AnalysisException e) {
                // this is a deprecated method, so just return true to make the compilation happy.
                LOG.warn("failed to check previous load status for db: {}, tbl: {}, {}", 
                        dbId, tableId, e.getMessage());
                return true;
            }
            return isPreviousLoadFinished;
        }
    }
    
    public synchronized void readFields(DataInput in) throws IOException {
        // read common members as write in AlterJob.write().
        // except 'type' member, which is read in AlterJob.read()
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_4) {
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
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_45) {
            transactionId = in.readLong();
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
        
        out.writeLong(transactionId);
        
    }
}
