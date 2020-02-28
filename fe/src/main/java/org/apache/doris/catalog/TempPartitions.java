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

import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.common.io.Writable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

// This class saved all temp partitions of a table.
// temp partition is used to implement the overwrite load.
// user can load data into some of the temp partitions,
// and then replace the formal partitions with these temp partitions
// to make a overwrite load.
public class TempPartitions implements Writable {
    private Map<Long, Partition> idToPartition = Maps.newHashMap();
    private Map<String, Partition> nameToPartition = Maps.newHashMap();
    private RangePartitionInfo partitionInfo;

    public TempPartitions() {
    }

    public TempPartitions(List<Column> partCols) {
        partitionInfo = new RangePartitionInfo(partCols);
    }

    public RangePartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public void addPartition(Partition partition) {
        idToPartition.put(partition.getId(), partition);
        nameToPartition.put(partition.getName(), partition);
    }

    /*
     * Drop temp partitions.
     * If needDropTablet is true, also drop the tablet from tablet inverted index.
     */
    public void dropPartition(String partitionName, boolean needDropTablet) {
        Partition partition = nameToPartition.get(partitionName);
        if (partition != null) {
            idToPartition.remove(partition.getId());
            nameToPartition.remove(partitionName);

            Preconditions.checkState(partitionInfo.getType() == PartitionType.RANGE);
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            // drop partition info
            rangePartitionInfo.dropPartition(partition.getId());

            if (!Catalog.isCheckpointThread() && needDropTablet) {
                TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    for (Tablet tablet : index.getTablets()) {
                        invertedIndex.deleteTablet(tablet.getId());
                    }
                }
            }
        }
    }

    public Partition getPartition(long partitionId) {
        return idToPartition.get(partitionId);
    }

    public Partition getPartition(String partitionName) {
        return nameToPartition.get(partitionName);
    }

    public List<Partition> getAllPartitions() {
        return Lists.newArrayList(idToPartition.values());
    }

    public boolean hasPartition(String partName) {
        if (partName == null) {
            return !idToPartition.isEmpty();
        } else {
            return nameToPartition.containsKey(partName);
        }
    }

    public boolean isEmpty() {
        return idToPartition.isEmpty();
    }

    // drop all temp partitions
    public void dropAll() {
        for (String partName : nameToPartition.keySet()) {
            dropPartition(partName, true);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // PartitionInfo is hard to serialized by GSON, so I have to use the old way...
        int size = idToPartition.size();
        out.writeInt(size);
        for (Partition partition : idToPartition.values()) {
            partition.write(out);
        }
        if (partitionInfo != null) {
            out.writeBoolean(true);
            partitionInfo.write(out);
        } else {
            out.writeBoolean(false);
        }
    }

    public static TempPartitions read(DataInput in) throws IOException {
        TempPartitions tempPartitions = new TempPartitions();
        tempPartitions.readFields(in);
        return tempPartitions;
    }

    private void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Partition partition = Partition.read(in);
            idToPartition.put(partition.getId(), partition);
            nameToPartition.put(partition.getName(), partition);
        }
        if (in.readBoolean()) {
            partitionInfo = (RangePartitionInfo) RangePartitionInfo.read(in);
        }
    }
}
