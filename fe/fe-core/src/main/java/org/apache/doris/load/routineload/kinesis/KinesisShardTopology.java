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

package org.apache.doris.load.routineload.kinesis;

import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.InternalService;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Job-owned lineage and scheduling barriers. All mutations require the job write lock. */
public class KinesisShardTopology {
    public enum ShardState {
        DISCOVERED,
        PENDING_PARENT,
        ACTIVE,
        DRAINING,
        COMPLETED
    }

    public static class ShardNode {
        @SerializedName("id")
        private String shardId;
        @SerializedName("ps")
        private Set<String> parentShardIds = new HashSet<>();
        @SerializedName("cs")
        private Set<String> childShardIds = new HashSet<>();
        @SerializedName("pos")
        private String initialStartPosition;
        @SerializedName("known")
        private boolean metadataKnown;
        @SerializedName("closed")
        private boolean sourceClosed;
        // Ancestors already outside retention at the first scan are outside this job's
        // starting boundary. They are not consumed or reported as COMPLETED.
        @SerializedName("outside")
        private boolean outsideInitialSnapshot;
        @SerializedName("endTxn")
        private long completionTxnId = -1;
        @SerializedName("st")
        private ShardState state = ShardState.DISCOVERED;

        public String getShardId() {
            return shardId;
        }

        public Set<String> getParentShardIds() {
            return Collections.unmodifiableSet(parentShardIds);
        }

        public Set<String> getChildShardIds() {
            return Collections.unmodifiableSet(childShardIds);
        }

        public String getInitialStartPosition() {
            return initialStartPosition;
        }

        public ShardState getState() {
            return state;
        }

        public boolean isSourceClosed() {
            return sourceClosed;
        }

        public boolean isOutsideInitialSnapshot() {
            return outsideInitialSnapshot;
        }

        public boolean isConsumptionFinished() {
            return state == ShardState.COMPLETED || completionTxnId != -1;
        }
    }

    @SerializedName("init")
    private boolean initialSnapshotFinalized;
    @SerializedName("nodes")
    private Map<String, ShardNode> nodes = new HashMap<>();
    @SerializedName("lineageError")
    private String lineageError;

    public boolean isInitialSnapshotFinalized() {
        return initialSnapshotFinalized;
    }

    public void setInitialSnapshotFinalized(boolean finalized) {
        initialSnapshotFinalized = finalized;
    }

    public Map<String, ShardNode> getNodes() {
        return Collections.unmodifiableMap(nodes);
    }

    public String getLineageError() {
        return lineageError;
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    public KinesisShardTopology copy() {
        return GsonUtils.GSON.fromJson(toJson(), KinesisShardTopology.class);
    }

    public void reset() {
        initialSnapshotFinalized = false;
        lineageError = null;
        nodes.clear();
    }

    public static boolean isLatest(String position) {
        return KinesisProgress.POSITION_LATEST.equalsIgnoreCase(position)
                || KinesisProgress.LATEST_VAL.equals(position);
    }

    public void mergeShardInfos(List<InternalService.PShardInfo> shardInfos,
            String initialStartPosition, String runtimeStartPosition) {
        mergeShardInfos(shardInfos, initialStartPosition, Collections.emptyMap());
    }

    public void mergeShardInfos(List<InternalService.PShardInfo> shardInfos,
            String initialStartPosition, Map<String, String> initialPositions) {
        boolean initialSnapshot = !initialSnapshotFinalized;
        Set<String> presentShardIds = new HashSet<>();
        for (InternalService.PShardInfo shardInfo : shardInfos) {
            presentShardIds.add(shardInfo.getShardId());
            ShardNode node = getOrCreate(shardInfo.getShardId());
            node.metadataKnown = true;
            if (shardInfo.hasParentShardId()) {
                addParent(node, shardInfo.getParentShardId());
            }
            if (shardInfo.hasAdjacentParentShardId()) {
                addParent(node, shardInfo.getAdjacentParentShardId());
            }
            node.sourceClosed |= shardInfo.getClosed();
            if (node.initialStartPosition == null && !node.outsideInitialSnapshot) {
                node.initialStartPosition = initialSnapshot
                        ? initialPositions.getOrDefault(node.shardId, initialStartPosition)
                        : KinesisProgress.POSITION_TRIM_HORIZON;
            }
        }
        if (initialSnapshot) {
            for (ShardNode node : nodes.values()) {
                if (!presentShardIds.contains(node.shardId)) {
                    node.outsideInitialSnapshot = true;
                }
            }
            initialSnapshotFinalized = true;
        }
        lineageError = null;
        for (ShardNode node : nodes.values()) {
            // A child reported by GetRecords may be present in the topology before the
            // ListShards scan that confirms its metadata.  Such a hint is not evidence that
            // the shard disappeared from ListShards; only a previously confirmed node can
            // trigger a lineage error.
            if (node.metadataKnown && !node.outsideInitialSnapshot && !node.isConsumptionFinished()
                    && !presentShardIds.contains(node.shardId)) {
                lineageError = "Kinesis shard " + node.shardId
                        + " is missing from ListShards before this job completed it; "
                        + "the shard may have expired. Check retention and the job's progress.";
                break;
            }
        }
        reconcileStates();
    }

    public void mergeChildShardInfos(Map<String, Set<String>> childShardParentIds,
            String runtimeStartPosition) {
        mergeChildShardInfos(childShardParentIds);
    }

    /** ChildShards is a lineage hint. ListShards must confirm metadata before scheduling it. */
    public void mergeChildShardInfos(Map<String, Set<String>> childShardParentIds) {
        for (Map.Entry<String, Set<String>> entry : childShardParentIds.entrySet()) {
            ShardNode child = getOrCreate(entry.getKey());
            for (String parentId : entry.getValue()) {
                addParent(child, parentId);
            }
            if (child.initialStartPosition == null) {
                child.initialStartPosition = KinesisProgress.POSITION_TRIM_HORIZON;
            }
        }
        reconcileStates();
    }

    public void addExplicitShard(String shardId, String position) {
        ShardNode node = getOrCreate(shardId);
        node.metadataKnown = true;
        node.initialStartPosition = position;
        initialSnapshotFinalized = true;
        reconcileNodeState(node);
    }

    public void markDraining(String shardId) {
        ShardNode node = Preconditions.checkNotNull(nodes.get(shardId), "Unknown Kinesis shard %s", shardId);
        node.metadataKnown = true;
        node.sourceClosed = true;
        reconcileNodeState(node);
    }

    public void markCompleted(String shardId) {
        ShardNode node = Preconditions.checkNotNull(nodes.get(shardId), "Unknown Kinesis shard %s", shardId);
        node.metadataKnown = true;
        node.sourceClosed = true;
        node.state = ShardState.COMPLETED;
        reconcileStates();
    }

    /** EOF is durable at COMMITTED, but neither parent nor children can run before VISIBLE. */
    public void markEndCommitted(String shardId, long txnId) {
        ShardNode node = Preconditions.checkNotNull(nodes.get(shardId), "Unknown Kinesis shard %s", shardId);
        if (node.state == ShardState.COMPLETED) {
            return;
        }
        node.sourceClosed = true;
        node.completionTxnId = txnId;
        reconcileNodeState(node);
    }

    public void completeVisibleShards(long txnId) {
        for (ShardNode node : nodes.values()) {
            if (node.completionTxnId == txnId) {
                node.state = ShardState.COMPLETED;
                node.completionTxnId = -1;
            }
        }
        reconcileStates();
    }

    public List<String> getUnresolvedLatestShardIds() {
        List<String> unresolved = new ArrayList<>();
        for (ShardNode node : nodes.values()) {
            if (!node.outsideInitialSnapshot && !node.isConsumptionFinished()
                    && isLatest(node.initialStartPosition)) {
                unresolved.add(node.shardId);
            }
        }
        Collections.sort(unresolved);
        return unresolved;
    }

    /** Journal replay is idempotent and must never roll back a shard's consumed sequence. */
    public void resolveInitialPositions(Map<String, String> positions) {
        for (Map.Entry<String, String> entry : positions.entrySet()) {
            ShardNode node = Preconditions.checkNotNull(nodes.get(entry.getKey()),
                    "Initial Kinesis topology must be journaled before positions");
            if (isLatest(node.initialStartPosition)) {
                node.initialStartPosition = entry.getValue();
            }
        }
        reconcileStates();
    }

    public void reconcileConcretePositions(Map<String, String> positions) {
        for (Map.Entry<String, String> entry : positions.entrySet()) {
            ShardNode node = nodes.get(entry.getKey());
            if (node != null && isLatest(node.initialStartPosition)) {
                node.initialStartPosition = entry.getValue();
            }
        }
        reconcileStates();
    }

    public void resetInitialPosition(String shardId, String position) {
        ShardNode node = Preconditions.checkNotNull(nodes.get(shardId), "Unknown Kinesis shard %s", shardId);
        Preconditions.checkState(node.completionTxnId == -1, "Kinesis shard still awaits VISIBLE: %s", shardId);
        node.initialStartPosition = position;
        reconcileStates();
    }

    public String getStartPosition(String shardId) {
        return Preconditions.checkNotNull(nodes.get(shardId)).initialStartPosition;
    }

    public List<String> getReadyShardIds() {
        List<String> ready = new ArrayList<>();
        for (ShardNode node : nodes.values()) {
            if ((node.state == ShardState.ACTIVE || node.state == ShardState.DRAINING)
                    && !node.isConsumptionFinished()) {
                ready.add(node.shardId);
            }
        }
        Collections.sort(ready);
        return ready;
    }

    public List<String> getOpenShardIds() {
        return getSourceShardIds(false);
    }

    public List<String> getClosedShardIds() {
        return getSourceShardIds(true);
    }

    private List<String> getSourceShardIds(boolean closed) {
        List<String> result = new ArrayList<>();
        for (ShardNode node : nodes.values()) {
            if (node.metadataKnown && !node.outsideInitialSnapshot
                    && node.sourceClosed == closed && node.state != ShardState.COMPLETED) {
                result.add(node.shardId);
            }
        }
        Collections.sort(result);
        return result;
    }

    private ShardNode getOrCreate(String shardId) {
        return nodes.computeIfAbsent(shardId, id -> {
            ShardNode node = new ShardNode();
            node.shardId = id;
            return node;
        });
    }

    private void addParent(ShardNode child, String parentId) {
        child.parentShardIds.add(parentId);
        getOrCreate(parentId).childShardIds.add(child.shardId);
    }

    private void reconcileStates() {
        for (ShardNode node : nodes.values()) {
            reconcileNodeState(node);
        }
    }

    private void reconcileNodeState(ShardNode node) {
        if (node.state == ShardState.COMPLETED) {
            return;
        }
        if (!node.metadataKnown || node.outsideInitialSnapshot
                || node.initialStartPosition == null || isLatest(node.initialStartPosition)) {
            node.state = ShardState.DISCOVERED;
            return;
        }
        for (String parentId : node.parentShardIds) {
            ShardNode parent = nodes.get(parentId);
            if (!parent.outsideInitialSnapshot && parent.state != ShardState.COMPLETED) {
                node.state = ShardState.PENDING_PARENT;
                return;
            }
        }
        node.state = node.sourceClosed ? ShardState.DRAINING : ShardState.ACTIVE;
    }
}
