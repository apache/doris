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

package org.apache.doris.catalog;

import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The OlapTraditional table is a materialized table which stored as rowcolumnar file or columnar file
 */
public class MaterializedIndex extends MetaObject implements GsonPostProcessable {
    public enum IndexState {
        NORMAL,
        @Deprecated
        ROLLUP,
        @Deprecated
        SCHEMA_CHANGE,
        SHADOW; // index in SHADOW state is visible to load process, but invisible to query

        public boolean isVisible() {
            return this == IndexState.NORMAL;
        }
    }

    public enum IndexExtState {
        ALL,
        VISIBLE, // index state in NORMAL
        SHADOW // index state in SHADOW
    }

    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "state")
    private IndexState state;
    @SerializedName(value = "rowCount")
    private long rowCount;

    // Published as a volatile immutable snapshot in lockstep with `tablets`.
    // Writers (synchronized) build a fresh HashMap and assign the field; readers
    // capture the reference once and call get/containsKey on the snapshot.
    // Invariant: `tablets ⊆ idToTablets` — any tablet visible in the list is also
    // present in the map. This is preserved by publishing the map BEFORE the list
    // on add and the list BEFORE the map on clear.
    private volatile Map<Long, Tablet> idToTablets;
    @SerializedName(value = "tablets")
    // this is for keeping tablet order
    private volatile List<Tablet> tablets;

    // for push after rollup index finished
    @SerializedName(value = "rollupIndexId")
    private long rollupIndexId;
    @SerializedName(value = "rollupFinishedVersion")
    private long rollupFinishedVersion;

    private boolean rowCountReported = false;

    public MaterializedIndex() {
        this.state = IndexState.NORMAL;
        this.idToTablets = new HashMap<>();
        this.tablets = new ArrayList<>();
    }

    public MaterializedIndex(long id, IndexState state) {
        this.id = id;

        this.state = state;
        if (this.state == null) {
            this.state = IndexState.NORMAL;
        }

        this.idToTablets = new HashMap<>();
        this.tablets = new ArrayList<>();

        this.rowCount = -1;

        this.rollupIndexId = -1L;
        this.rollupFinishedVersion = -1L;
    }

    public List<Tablet> getTablets() {
        // Volatile read: returns the current immutable snapshot; callers iterate without locking.
        return Collections.unmodifiableList(tablets);
    }

    public List<Long> getTabletIdsInOrder() {
        List<Tablet> snapshot = tablets; // single volatile read
        List<Long> tabletIds = new ArrayList<>(snapshot.size());
        for (Tablet tablet : snapshot) {
            tabletIds.add(tablet.getId());
        }
        return tabletIds;
    }

    public Tablet getTablet(long tabletId) {
        // Single volatile read of the immutable map snapshot.
        return idToTablets.get(tabletId);
    }

    public synchronized void clearTabletsForRestore() {
        // Drop the list first so iteration stops seeing tablets before
        // lookup-by-id drops them. Maintains tablets ⊆ idToTablets.
        tablets = new ArrayList<>();
        idToTablets = new HashMap<>();
    }

    public void addTablet(Tablet tablet, TabletMeta tabletMeta) {
        addTablet(tablet, tabletMeta, false);
    }

    // For bulk creation, prefer appendTablets() so the per-index list is copied
    // once per batch instead of once per tablet.
    public void addTablet(Tablet tablet, TabletMeta tabletMeta, boolean isRestore) {
        appendTabletsInternal(Collections.singletonList(tablet));
        if (!isRestore) {
            Env.getCurrentInvertedIndex().addTablet(tablet.getId(), tabletMeta);
        }
    }

    // Bulk-publish path. Does NOT register tablets in TabletInvertedIndex —
    // callers do that per tablet (needed before Tablet.addReplica in non-restore paths).
    public void appendTablets(Collection<Tablet> newTablets) {
        appendTabletsInternal(newTablets);
    }

    // Synchronized to prevent concurrent lost-update on the per-index list/map snapshots;
    // some callers (e.g. InternalCatalog.createTablets) do not hold the OlapTable write lock.
    private synchronized void appendTabletsInternal(Collection<Tablet> newTablets) {
        if (newTablets.isEmpty()) {
            return;
        }
        Map<Long, Tablet> nextMap = new HashMap<>(idToTablets);
        List<Tablet> nextList = new ArrayList<>(tablets.size() + newTablets.size());
        nextList.addAll(tablets);
        for (Tablet tablet : newTablets) {
            nextMap.put(tablet.getId(), tablet);
            nextList.add(tablet);
        }
        // Publish the map first, then the list — so any id that appears in the
        // visible `tablets` snapshot is already present in `idToTablets`.
        idToTablets = nextMap;
        tablets = nextList;
    }

    public void setIdForRestore(long idxId) {
        this.id = idxId;
    }

    public long getId() {
        return id;
    }

    public void setState(IndexState state) {
        this.state = state;
    }

    public IndexState getState() {
        return this.state;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public void setRollupIndexInfo(long rollupIndexId, long rollupFinishedVersion) {
        this.rollupIndexId = rollupIndexId;
        this.rollupFinishedVersion = rollupFinishedVersion;
    }

    public long getRollupIndexId() {
        return rollupIndexId;
    }

    public long getRollupFinishedVersion() {
        return rollupFinishedVersion;
    }

    public void clearRollupIndexInfo() {
        this.rollupIndexId = -1L;
        this.rollupFinishedVersion = -1L;
    }

    public long getDataSize(boolean singleReplica, boolean filterSizeZero) {
        long dataSize = 0;
        for (Tablet tablet : getTablets()) {
            dataSize += tablet.getDataSize(singleReplica, filterSizeZero);
        }
        return dataSize;
    }

    public long getRemoteDataSize() {
        long remoteDataSize = 0;
        for (Tablet tablet : getTablets()) {
            remoteDataSize += tablet.getRemoteDataSize();
        }
        return remoteDataSize;
    }

    public long getBinlogSize() {
        long binlogDataSize = 0;
        for (Tablet tablet : getTablets()) {
            binlogDataSize += tablet.getBinlogDataSize();
        }
        return binlogDataSize;
    }

    public long getReplicaCount() {
        long replicaCount = 0;
        for (Tablet tablet : getTablets()) {
            replicaCount += tablet.getReplicas().size();
        }
        return replicaCount;
    }

    public long getLocalIndexSize() {
        long localIndexSize = 0;
        for (Tablet tablet : getTablets()) {
            for (Replica replica : tablet.getReplicas()) {
                localIndexSize += replica.getLocalInvertedIndexSize();
            }
        }
        return localIndexSize;
    }

    public long getLocalSegmentSize() {
        long localSegmentSize = 0;
        for (Tablet tablet : getTablets()) {
            for (Replica replica : tablet.getReplicas()) {
                localSegmentSize += replica.getLocalSegmentSize();
            }
        }
        return localSegmentSize;
    }

    public long getRemoteIndexSize() {
        long remoteIndexSize = 0;
        for (Tablet tablet : getTablets()) {
            for (Replica replica : tablet.getReplicas()) {
                remoteIndexSize += replica.getRemoteInvertedIndexSize();
            }
        }
        return remoteIndexSize;
    }

    public long getRemoteSegmentSize() {
        long remoteSegmentSize = 0;
        for (Tablet tablet : getTablets()) {
            for (Replica replica : tablet.getReplicas()) {
                remoteSegmentSize += replica.getRemoteSegmentSize();
            }
        }
        return remoteSegmentSize;
    }

    public int getTabletOrderIdx(long tabletId) {
        List<Tablet> snapshot = tablets; // single volatile read
        int idx = 0;
        for (Tablet tablet : snapshot) {
            if (tablet.getId() == tabletId) {
                return idx;
            }
            idx++;
        }
        return -1;
    }

    public void setRowCountReported(boolean reported) {
        this.rowCountReported = reported;
    }

    public boolean getRowCountReported() {
        return this.rowCountReported;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MaterializedIndex)) {
            return false;
        }

        MaterializedIndex other = (MaterializedIndex) obj;

        return other.idToTablets != null
                && idToTablets.size() == other.idToTablets.size()
                && idToTablets.equals(other.idToTablets)
                && (state.equals(other.state))
                && (rowCount == other.rowCount);
    }

    @Override
    public String toString() {
        List<Tablet> snapshot = tablets; // single volatile read
        StringBuilder buffer = new StringBuilder();
        buffer.append("index id: ").append(id).append("; ");
        buffer.append("index state: ").append(state.name()).append("; ");

        buffer.append("row count: ").append(rowCount).append("; ");
        buffer.append("tablets size: ").append(snapshot.size()).append("; ");
        //
        buffer.append("tablets: [");
        for (Tablet tablet : snapshot) {
            buffer.append("tablet: ").append(tablet.toString()).append(", ");
        }
        buffer.append("]; ");

        buffer.append("rollup index id: ").append(rollupIndexId).append("; ");
        buffer.append("rollup finished version: ").append(rollupFinishedVersion).append("; ");

        return buffer.toString();
    }

    @Override
    public void gsonPostProcess() {
        // Build a fresh "idToTablets" snapshot from the deserialized "tablets" list.
        // Runs single-threaded during gson deserialization, before any concurrent
        // reader can observe this object.
        Map<Long, Tablet> map = new HashMap<>(tablets.size());
        for (Tablet tablet : tablets) {
            map.put(tablet.getId(), tablet);
        }
        idToTablets = map;
    }
}
