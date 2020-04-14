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

import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.Util;
import org.apache.doris.meta.MetaContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Internal representation of partition-related metadata.
 */
public class Partition extends MetaObject implements Writable {
    private static final Logger LOG = LogManager.getLogger(Partition.class);

    public static final long PARTITION_INIT_VERSION = 1L;
    public static final long PARTITION_INIT_VERSION_HASH = 0L;

    public enum PartitionState {
        NORMAL,
        @Deprecated
        ROLLUP,
        @Deprecated
        SCHEMA_CHANGE
    }

    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "state")
    private PartitionState state;
    @SerializedName(value = "baseIndex")
    private MaterializedIndex baseIndex;
    /*
     * Visible rollup indexes are indexes which are visible to user.
     * User can do query on them, show them in related 'show' stmt.
     */
    @SerializedName(value = "idToVisibleRollupIndex")
    private Map<Long, MaterializedIndex> idToVisibleRollupIndex = Maps.newHashMap();
    /*
     * Shadow indexes are indexes which are not visible to user.
     * Query will not run on these shadow indexes, and user can not see them neither.
     * But load process will load data into these shadow indexes.
     */
    @SerializedName(value = "idToShadowIndex")
    private Map<Long, MaterializedIndex> idToShadowIndex = Maps.newHashMap();

    /*
     * committed version(hash): after txn is committed, set committed version(hash)
     * visible version(hash): after txn is published, set visible version
     * next version(hash): next version is set after finished committing, it should equals to committed version + 1
     */

    // not have committedVersion because committedVersion = nextVersion - 1
    @SerializedName(value = "committedVersionHash")
    private long committedVersionHash;
    @SerializedName(value = "visibleVersion")
    private long visibleVersion;
    @SerializedName(value = "visibleVersionHash")
    private long visibleVersionHash;
    @SerializedName(value = "nextVersion")
    private long nextVersion;
    @SerializedName(value = "nextVersionHash")
    private long nextVersionHash;
    @SerializedName(value = "distributionInfo")
    private DistributionInfo distributionInfo;

    private Partition() {
    }

    public Partition(long id, String name, 
            MaterializedIndex baseIndex, DistributionInfo distributionInfo) {
        this.id = id;
        this.name = name;
        this.state = PartitionState.NORMAL;
        
        this.baseIndex = baseIndex;

        this.visibleVersion = PARTITION_INIT_VERSION;
        this.visibleVersionHash = PARTITION_INIT_VERSION_HASH;
        // PARTITION_INIT_VERSION == 1, so the first load version is 2 !!!
        this.nextVersion = PARTITION_INIT_VERSION + 1;
        this.nextVersionHash = Util.generateVersionHash();
        this.committedVersionHash = PARTITION_INIT_VERSION_HASH;

        this.distributionInfo = distributionInfo;
    }

    public void setIdForRestore(long id) {
        this.id = id;
    }

    public long getId() {
        return this.id;
    }

    public void setName(String newName) {
        this.name = newName;
    }

    public String getName() {
        return this.name;
    }

    public void setState(PartitionState state) {
        this.state = state;
    }

    /*
     * If a partition is overwritten by a restore job, we need to reset all version info to
     * the restored partition version info》
     */
    public void updateVersionForRestore(long visibleVersion, long visibleVersionHash) {
        this.visibleVersion = visibleVersion;
        this.visibleVersionHash = visibleVersionHash;
        this.nextVersion = this.visibleVersion + 1;
        this.nextVersionHash = Util.generateVersionHash();
        this.committedVersionHash = visibleVersionHash;
        LOG.info("update partition {} version for restore: visible: {}-{}, next: {}-{}",
                name, visibleVersion, visibleVersionHash, nextVersion, nextVersionHash);
    }

    public void updateVisibleVersionAndVersionHash(long visibleVersion, long visibleVersionHash) {
        this.visibleVersion = visibleVersion;
        this.visibleVersionHash = visibleVersionHash;
        if (MetaContext.get() != null) {
            // MetaContext is not null means we are in a edit log replay thread.
            // if it is upgrade from old palo cluster, then should update next version info
            if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_45) {
                // the partition is created and not import any data
                if (visibleVersion == PARTITION_INIT_VERSION + 1 && visibleVersionHash == PARTITION_INIT_VERSION_HASH) {
                    this.nextVersion = PARTITION_INIT_VERSION + 1;
                    this.nextVersionHash = Util.generateVersionHash();
                    this.committedVersionHash = PARTITION_INIT_VERSION_HASH;
                } else {
                    this.nextVersion = visibleVersion + 1;
                    this.nextVersionHash = Util.generateVersionHash();
                    this.committedVersionHash = visibleVersionHash;
                }
            }
        }
    }
    
    public long getVisibleVersion() {
        return visibleVersion;
    }

    public long getVisibleVersionHash() {
        return visibleVersionHash;
    }

    public PartitionState getState() {
        return this.state;
    }

    public DistributionInfo getDistributionInfo() {
        return distributionInfo;
    }

    public void createRollupIndex(MaterializedIndex mIndex) {
        if (mIndex.getState().isVisible()) {
            this.idToVisibleRollupIndex.put(mIndex.getId(), mIndex);
        } else {
            this.idToShadowIndex.put(mIndex.getId(), mIndex);
        }
    }

    public MaterializedIndex deleteRollupIndex(long indexId) {
        if (this.idToVisibleRollupIndex.containsKey(indexId)) {
            return idToVisibleRollupIndex.remove(indexId);
        } else {
            return idToShadowIndex.remove(indexId);
        }
    }

    public MaterializedIndex getBaseIndex() {
        return baseIndex;
    }

    public long getNextVersion() {
        return nextVersion;
    }

    public void setNextVersion(long nextVersion) {
        this.nextVersion = nextVersion;
    }

    public long getNextVersionHash() {
        return nextVersionHash;
    }

    public void setNextVersionHash(long nextVersionHash, long committedVersionHash) {
        this.nextVersionHash = nextVersionHash;
        this.committedVersionHash = committedVersionHash;
    }
    
    public long getCommittedVersion() {
        return this.nextVersion - 1;
    }
    
    public long getCommittedVersionHash() {
        return committedVersionHash;
    }

    public MaterializedIndex getIndex(long indexId) {
        if (baseIndex.getId() == indexId) {
            return baseIndex;
        }
        if (idToVisibleRollupIndex.containsKey(indexId)) {
            return idToVisibleRollupIndex.get(indexId);
        } else {
            return idToShadowIndex.get(indexId);
        }
    }

    public List<MaterializedIndex> getMaterializedIndices(IndexExtState extState) {
        List<MaterializedIndex> indices = Lists.newArrayList();
        switch (extState) {
            case ALL:
                indices.add(baseIndex);
                indices.addAll(idToVisibleRollupIndex.values());
                indices.addAll(idToShadowIndex.values());
                break;
            case VISIBLE:
                indices.add(baseIndex);
                indices.addAll(idToVisibleRollupIndex.values());
                break;
            case SHADOW:
                indices.addAll(idToShadowIndex.values());
            default:
                break;
        }
        return indices;
    }

    public long getDataSize() {
        long dataSize = 0;
        for (MaterializedIndex mIndex : getMaterializedIndices(IndexExtState.VISIBLE)) {
            dataSize += mIndex.getDataSize();
        }
        return dataSize;
    }

    public long getReplicaCount() {
        long replicaCount = 0;
        for (MaterializedIndex mIndex : getMaterializedIndices(IndexExtState.VISIBLE)) {
            replicaCount += mIndex.getReplicaCount();
        }
        return replicaCount;
    }

    public boolean hasData() {
        return !(visibleVersion == PARTITION_INIT_VERSION && visibleVersionHash == PARTITION_INIT_VERSION_HASH);
    }

    /*
     * Change the index' state from SHADOW to NORMAL
     * Also move it to idToVisibleRollupIndex if it is not the base index.
     */
    public boolean visualiseShadowIndex(long shadowIndexId, boolean isBaseIndex) {
        MaterializedIndex shadowIdx = idToShadowIndex.remove(shadowIndexId);
        if (shadowIdx == null) {
            return false;
        }
        Preconditions.checkState(!idToVisibleRollupIndex.containsKey(shadowIndexId), shadowIndexId);
        shadowIdx.setState(IndexState.NORMAL);
        if (isBaseIndex) {
            baseIndex = shadowIdx;
        } else {
            idToVisibleRollupIndex.put(shadowIndexId, shadowIdx);
        }
        LOG.info("visualise the shadow index: {}", shadowIndexId);
        return true;
    }

    public static Partition read(DataInput in) throws IOException {
        Partition partition = new Partition();
        partition.readFields(in);
        return partition;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        out.writeLong(id);
        Text.writeString(out, name);
        Text.writeString(out, state.name());
        
        baseIndex.write(out);

        int rollupCount = (idToVisibleRollupIndex != null) ? idToVisibleRollupIndex.size() : 0;
        out.writeInt(rollupCount);
        if (idToVisibleRollupIndex != null) {
            for (Map.Entry<Long, MaterializedIndex> entry : idToVisibleRollupIndex.entrySet()) {
                entry.getValue().write(out);
            }
        }

        out.writeInt(idToShadowIndex.size());
        for (MaterializedIndex shadowIndex : idToShadowIndex.values()) {
            shadowIndex.write(out);
        }

        out.writeLong(visibleVersion);
        out.writeLong(visibleVersionHash);

        out.writeLong(nextVersion);
        out.writeLong(nextVersionHash);
        out.writeLong(committedVersionHash);

        Text.writeString(out, distributionInfo.getType().name());
        distributionInfo.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        id = in.readLong();
        name = Text.readString(in);
        state = PartitionState.valueOf(Text.readString(in));
        
        baseIndex = MaterializedIndex.read(in);

        int rollupCount = in.readInt();
        for (int i = 0; i < rollupCount; ++i) {
            MaterializedIndex rollupTable = MaterializedIndex.read(in);
            idToVisibleRollupIndex.put(rollupTable.getId(), rollupTable);
        }
        
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_61) {
            int shadowIndexCount = in.readInt();
            for (int i = 0; i < shadowIndexCount; i++) {
                MaterializedIndex shadowIndex = MaterializedIndex.read(in);
                idToShadowIndex.put(shadowIndex.getId(), shadowIndex);
            }
        }

        visibleVersion = in.readLong();
        visibleVersionHash = in.readLong();
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_45) {
            nextVersion = in.readLong();
            nextVersionHash = in.readLong();
            committedVersionHash = in.readLong();
        } else {
            // the partition is created and not import any data
            if (visibleVersion == PARTITION_INIT_VERSION + 1 && visibleVersionHash == PARTITION_INIT_VERSION_HASH) {
                this.nextVersion = PARTITION_INIT_VERSION + 1;
                this.nextVersionHash = Util.generateVersionHash();
                this.committedVersionHash = PARTITION_INIT_VERSION_HASH;
            } else {
                this.nextVersion = visibleVersion + 1;
                this.nextVersionHash = Util.generateVersionHash();
                this.committedVersionHash = visibleVersionHash;
            }
        }
        DistributionInfoType distriType = DistributionInfoType.valueOf(Text.readString(in));
        if (distriType == DistributionInfoType.HASH) {
            distributionInfo = HashDistributionInfo.read(in);
        } else if (distriType == DistributionInfoType.RANDOM) {
            distributionInfo = RandomDistributionInfo.read(in);
        } else {
            throw new IOException("invalid distribution type: " + distriType);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Partition)) {
            return false;
        }

        Partition partition = (Partition) obj;
        if (idToVisibleRollupIndex != partition.idToVisibleRollupIndex) {
            if (idToVisibleRollupIndex.size() != partition.idToVisibleRollupIndex.size()) {
                return false;
            }
            for (Entry<Long, MaterializedIndex> entry : idToVisibleRollupIndex.entrySet()) {
                long key = entry.getKey();
                if (!partition.idToVisibleRollupIndex.containsKey(key)) {
                    return false;
                }
                if (!entry.getValue().equals(partition.idToVisibleRollupIndex.get(key))) {
                    return false;
                }
            }
        }

        return (visibleVersion == partition.visibleVersion)
                && (visibleVersionHash == partition.visibleVersionHash)
                && (baseIndex.equals(partition.baseIndex)
                && distributionInfo.eqauls(partition.distributionInfo));
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("partition_id: ").append(id).append("; ");
        buffer.append("name: ").append(name).append("; ");
        buffer.append("partition_state.name: ").append(state.name()).append("; ");

        buffer.append("base_index: ").append(baseIndex.toString()).append("; ");

        int rollupCount = (idToVisibleRollupIndex != null) ? idToVisibleRollupIndex.size() : 0;
        buffer.append("rollup count: ").append(rollupCount).append("; ");

        if (idToVisibleRollupIndex != null) {
            for (Map.Entry<Long, MaterializedIndex> entry : idToVisibleRollupIndex.entrySet()) {
                buffer.append("rollup_index: ").append(entry.getValue().toString()).append("; ");
            }
        }

        buffer.append("committedVersion: ").append(visibleVersion).append("; ");
        buffer.append("committedVersionHash: ").append(visibleVersionHash).append("; ");

        buffer.append("distribution_info.type: ").append(distributionInfo.getType().name()).append("; ");
        buffer.append("distribution_info: ").append(distributionInfo.toString());

        return buffer.toString();
    }

    public boolean convertRandomDistributionToHashDistribution(List<Column> baseSchema) {
        boolean hasChanged = false;
        if (distributionInfo.getType() == DistributionInfoType.RANDOM) {
            distributionInfo = ((RandomDistributionInfo) distributionInfo).toHashDistributionInfo(baseSchema);
            hasChanged = true;
        }
        return hasChanged;
    }
}
