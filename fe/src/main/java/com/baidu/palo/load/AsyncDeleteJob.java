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

package com.baidu.palo.load;

import com.baidu.palo.analysis.BinaryPredicate;
import com.baidu.palo.analysis.BinaryPredicate.Operator;
import com.baidu.palo.analysis.IsNullPredicate;
import com.baidu.palo.analysis.LiteralExpr;
import com.baidu.palo.analysis.Predicate;
import com.baidu.palo.analysis.SlotRef;
import com.baidu.palo.analysis.StringLiteral;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;
import com.baidu.palo.persist.ReplicaPersistInfo;
import com.baidu.palo.task.AgentTaskQueue;
import com.baidu.palo.task.PushTask;

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

    private DeleteState state;

    private long jobId;
    private long dbId;
    private long tableId;
    private long partitionId;

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
    }

    public AsyncDeleteJob(long dbId, long tableId, long partitionId,
                          long partitionVersion, long partitionVersionHash,
                          List<Predicate> conditions) {
        this.state = DeleteState.QUORUM_FINISHED;

        this.jobId = Catalog.getInstance().getNextId();
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
                                          task.getVersion(), task.getVersionHash(), task.getPushType());
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

    @Override
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
                ReplicaPersistInfo info = new ReplicaPersistInfo();
                info.readFields(in);
                replicaPersistInfos.put(info.getReplicaId(), info);
            }
        }
    }
}
