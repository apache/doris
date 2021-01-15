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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AsyncDeleteJob implements Writable {

    public enum DeleteState {
        QUORUM_FINISHED,
        FINISHED
    }

    private volatile DeleteState state;

    private long jobId;
    private long dbId;
    private long tableId;
    private long partitionId;
    
    private long transactionId;

    private long partitionVersion;
    private long partitionVersionHash;
    private List<Predicate> conditions;

    private Set<Long> tabletIds;
    private Map<Long, PushTask> sendReplicaIdToPushTask;

    private Map<Long, ReplicaPersistInfo> replicaPersistInfos;

    private AsyncDeleteJob() {
        // for persist
        conditions = Lists.newArrayList();
        tabletIds = Sets.newHashSet();
        sendReplicaIdToPushTask = Maps.newHashMap();
        replicaPersistInfos = Maps.newHashMap();
        transactionId = -1;
    }

    public AsyncDeleteJob(long dbId, long tableId, long partitionId,
                          long partitionVersion, long partitionVersionHash,
                          List<Predicate> conditions) {
        this.state = DeleteState.QUORUM_FINISHED;

        this.jobId = Catalog.getCurrentCatalog().getNextId();
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;

        this.partitionVersion = partitionVersion;
        this.partitionVersionHash = partitionVersionHash;
        this.conditions = conditions;
        this.tabletIds = Sets.newHashSet();
        this.sendReplicaIdToPushTask = Maps.newHashMap();
        this.replicaPersistInfos = Maps.newHashMap();
    }

    public void setState(DeleteState state) {
        this.state = state;
    }

    public DeleteState getState() {
        return this.state;
    }

    public long getJobId() {
        return jobId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public Set<Long> getTabletIds() {
        return tabletIds;
    }

    public long getPartitionVersion() {
        return partitionVersion;
    }

    public long getPartitionVersionHash() {
        return partitionVersionHash;
    }

    public List<Predicate> getConditions() {
        return conditions;
    }

    public void addTabletId(long tabletId) {
        this.tabletIds.add(tabletId);
    }

    public void setIsSend(long replicaId, PushTask task) {
        sendReplicaIdToPushTask.put(replicaId, task);
    }

    public boolean hasSend(long replicaId) {
        return sendReplicaIdToPushTask.containsKey(replicaId);
    }

    public void clearTasks() {
        for (PushTask task : sendReplicaIdToPushTask.values()) {
            AgentTaskQueue.removePushTask(task.getBackendId(), task.getSignature(),
                                          task.getVersion(), task.getVersionHash(), 
                                          task.getPushType(), TTaskType.PUSH);
        }
    }

    public void addReplicaPersistInfos(ReplicaPersistInfo info) {
        if (!replicaPersistInfos.containsKey(info.getReplicaId())) {
            replicaPersistInfos.put(info.getReplicaId(), info);
        }
    }

    public Map<Long, ReplicaPersistInfo> getReplicaPersistInfos() {
        return replicaPersistInfos;
    }

    public static AsyncDeleteJob read(DataInput in) throws IOException {
        AsyncDeleteJob job = new AsyncDeleteJob();
        job.readFields(in);
        return job;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, state.name());
        out.writeLong(jobId);
        out.writeLong(dbId);
        out.writeLong(tableId);
        out.writeLong(partitionId);

        out.writeLong(partitionVersion);
        out.writeLong(partitionVersionHash);

        int count = conditions.size();
        out.writeInt(count);
        for (Predicate predicate : conditions) {
            if (predicate instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) predicate;
                SlotRef slotRef = (SlotRef) binaryPredicate.getChild(0);
                String columnName = slotRef.getColumnName();
                Text.writeString(out, columnName);
                Text.writeString(out, binaryPredicate.getOp().name());
                String value = ((LiteralExpr) binaryPredicate.getChild(1)).getStringValue();
                Text.writeString(out, value);
            } else if (predicate instanceof IsNullPredicate) {
                IsNullPredicate isNullPredicate = (IsNullPredicate) predicate;
                SlotRef slotRef = (SlotRef) isNullPredicate.getChild(0);
                String columnName = slotRef.getColumnName();
                Text.writeString(out, columnName);
                Text.writeString(out, "IS");
                String value = null;
                if (isNullPredicate.isNotNull()) {
                    value = "NOT NULL";
                } else {
                    value = "NULL";
                }
                Text.writeString(out, value);
            }
        }

        count = tabletIds.size();
        out.writeInt(count);
        for (Long tabletId : tabletIds) {
            out.writeLong(tabletId);
        }

        if (replicaPersistInfos == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            count = replicaPersistInfos.size();
            out.writeInt(count);
            for (ReplicaPersistInfo info : replicaPersistInfos.values()) {
                info.write(out);
            }
        }
    }

    public void readFields(DataInput in) throws IOException {
        state = DeleteState.valueOf(Text.readString(in));
        jobId = in.readLong();
        dbId = in.readLong();
        tableId = in.readLong();
        partitionId = in.readLong();

        partitionVersion = in.readLong();
        partitionVersionHash = in.readLong();

        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String key = Text.readString(in);
            String opStr = Text.readString(in);
            if (opStr.equals("IS")) {
                String value = Text.readString(in);
                IsNullPredicate predicate;
                if (value.equals("NOT NULL")) {
                    predicate = new IsNullPredicate(new SlotRef(null, key), true);
                } else {
                    predicate = new IsNullPredicate(new SlotRef(null, key), false);
                }
                conditions.add(predicate);
            } else {
                Operator op = Operator.valueOf(opStr);
                String value = Text.readString(in);
                BinaryPredicate predicate = new BinaryPredicate(op, new SlotRef(null, key), new StringLiteral(value));
                conditions.add(predicate);
            }
        }

        count = in.readInt();
        for (int i = 0; i < count; i++) {
            long tabletId = in.readLong();
            tabletIds.add(tabletId);
        }

        if (in.readBoolean()) {
            count = in.readInt();
            replicaPersistInfos = Maps.newHashMap();
            for (int i = 0; i < count; ++i) {
                ReplicaPersistInfo info = ReplicaPersistInfo.read(in);
                replicaPersistInfos.put(info.getReplicaId(), info);
            }
        }
    }
}
