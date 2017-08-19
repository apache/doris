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

package com.baidu.palo.catalog;

import com.baidu.palo.catalog.DistributionInfo.DistributionInfoType;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Map.Entry;

/**
 * Internal representation of partition-related metadata.
 */
public class Partition extends MetaObject implements Writable {
    public static final long PARTITION_INIT_VERSION = 1L;
    public static final long PARTITION_INIT_VERSION_HASH = 0L;

    public enum PartitionState {
        NORMAL,
        ROLLUP,
        SCHEMA_CHANGE
    }

    private long id;
    private String name;
    private PartitionState state;

    private MaterializedIndex baseIndex;
    private Map<Long, MaterializedIndex> idToRollupIndex;
    private long committedVersion;
    private long committedVersionHash;

    private DistributionInfo distributionInfo;

    public Partition() {
        this.idToRollupIndex = new HashMap<Long, MaterializedIndex>();
    }

    public Partition(long id, String name, MaterializedIndex baseIndex, DistributionInfo distributionInfo) {
        this.id = id;
        this.name = name;
        this.state = PartitionState.NORMAL;

        this.baseIndex = baseIndex;
        this.idToRollupIndex = new HashMap<Long, MaterializedIndex>();

        this.committedVersion = PARTITION_INIT_VERSION;
        this.committedVersionHash = PARTITION_INIT_VERSION_HASH;
        this.distributionInfo = distributionInfo;
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

    public long getCommittedVersion() {
        return committedVersion;
    }

    public void setCommittedVersion(long committedVersion) {
        this.committedVersion = committedVersion;
    }

    public long getCommittedVersionHash() {
        return committedVersionHash;
    }

    public void setCommittedVersionHash(long committedVersionHash) {
        this.committedVersionHash = committedVersionHash;
    }

    public PartitionState getState() {
        return this.state;
    }

    public DistributionInfo getDistributionInfo() {
        return distributionInfo;
    }

    public void createRollupIndex(MaterializedIndex mIndex) {
        this.idToRollupIndex.put(mIndex.getId(), mIndex);
    }

    public MaterializedIndex deleteRollupIndex(long indexId) {
        return this.idToRollupIndex.remove(indexId);
    }

    public MaterializedIndex getBaseIndex() {
        return baseIndex;
    }

    public List<MaterializedIndex> getRollupIndices() {
        List<MaterializedIndex> rollupIndices = new ArrayList<MaterializedIndex>(idToRollupIndex.size());
        for (Map.Entry<Long, MaterializedIndex> entry : idToRollupIndex.entrySet()) {
            rollupIndices.add(entry.getValue());
        }
        return rollupIndices;
    }

    public MaterializedIndex getIndex(long indexId) {
        if (baseIndex.getId() == indexId) {
            return baseIndex;
        }
        if (idToRollupIndex.containsKey(indexId)) {
            return idToRollupIndex.get(indexId);
        }
        return null;
    }

    public List<MaterializedIndex> getMaterializedIndices() {
        List<MaterializedIndex> indices = new ArrayList<MaterializedIndex>();
        indices.add(baseIndex);
        for (MaterializedIndex rollupIndex : idToRollupIndex.values()) {
            indices.add(rollupIndex);
        }
        return indices;
    }

    public static Partition read(DataInput in) throws IOException {
        Partition partition = new Partition();
        partition.readFields(in);
        return partition;
    }

    public void write(DataOutput out) throws IOException {
        super.write(out);

        out.writeLong(id);
        Text.writeString(out, name);
        Text.writeString(out, state.name());
        baseIndex.write(out);

        int rollupCount = (idToRollupIndex != null) ? idToRollupIndex.size() : 0;
        out.writeInt(rollupCount);
        if (idToRollupIndex != null) {
            for (Map.Entry<Long, MaterializedIndex> entry : idToRollupIndex.entrySet()) {
                entry.getValue().write(out);
            }
        }

        out.writeLong(committedVersion);
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
            idToRollupIndex.put(rollupTable.getId(), rollupTable);
        }

        committedVersion = in.readLong();
        committedVersionHash = in.readLong();

        DistributionInfoType distriType = DistributionInfoType.valueOf(Text.readString(in));
        if (distriType == DistributionInfoType.HASH) {
            distributionInfo = HashDistributionInfo.read(in);
        } else if (distriType == DistributionInfoType.RANDOM) {
            distributionInfo = RandomDistributionInfo.read(in);
        } else {
            throw new IOException("invalid distribution type: " + distriType);
        }
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Partition)) {
            return false;
        }

        Partition partition = (Partition) obj;
        if (idToRollupIndex != partition.idToRollupIndex) {
            if (idToRollupIndex.size() != partition.idToRollupIndex.size()) {
                return false;
            }
            for (Entry<Long, MaterializedIndex> entry : idToRollupIndex.entrySet()) {
                long key = entry.getKey();
                if (!partition.idToRollupIndex.containsKey(key)) {
                    return false;
                }
                if (!entry.getValue().equals(partition.idToRollupIndex.get(key))) {
                    return false;
                }
            }
        }

        return (committedVersion == partition.committedVersion)
                && (committedVersionHash == partition.committedVersionHash)
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

        int rollupCount = (idToRollupIndex != null) ? idToRollupIndex.size() : 0;
        buffer.append("rollup count: ").append(rollupCount).append("; ");

        if (idToRollupIndex != null) {
            for (Map.Entry<Long, MaterializedIndex> entry : idToRollupIndex.entrySet()) {
                buffer.append("rollup_index: ").append(entry.getValue().toString()).append("; ");
            }
        }

        buffer.append("committedVersion: ").append(committedVersion).append("; ");
        buffer.append("committedVersionHash: ").append(committedVersionHash).append("; ");

        buffer.append("distribution_info.type: ").append(distributionInfo.getType().name()).append("; ");
        buffer.append("distribution_info: ").append(distributionInfo.toString());

        return buffer.toString();
    }
}
