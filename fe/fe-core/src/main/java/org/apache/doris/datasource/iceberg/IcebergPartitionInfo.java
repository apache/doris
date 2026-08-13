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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class IcebergPartitionInfo {
    private final Map<String, PartitionItem> nameToPartitionItem;
    private final Map<String, IcebergPartition> nameToIcebergPartition;
    private final Map<String, Set<String>> nameToIcebergPartitionNames;
    private final long retainedPayloadBytes;

    private static final IcebergPartitionInfo EMPTY = new IcebergPartitionInfo();

    private IcebergPartitionInfo() {
        this.nameToPartitionItem = Collections.emptyMap();
        this.nameToIcebergPartition = Collections.emptyMap();
        this.nameToIcebergPartitionNames = Collections.emptyMap();
        this.retainedPayloadBytes = 0L;
    }

    public IcebergPartitionInfo(Map<String, PartitionItem> nameToPartitionItem,
                                Map<String, IcebergPartition> nameToIcebergPartition,
                                Map<String, Set<String>> nameToIcebergPartitionNames) {
        this(nameToPartitionItem, nameToIcebergPartition, nameToIcebergPartitionNames,
                retainedPayloadBytes(nameToIcebergPartition));
    }

    public IcebergPartitionInfo(Map<String, PartitionItem> nameToPartitionItem,
                                Map<String, IcebergPartition> nameToIcebergPartition,
                                Map<String, Set<String>> nameToIcebergPartitionNames,
                                long retainedPayloadBytes) {
        this.nameToPartitionItem = nameToPartitionItem;
        this.nameToIcebergPartition = nameToIcebergPartition;
        this.nameToIcebergPartitionNames = nameToIcebergPartitionNames;
        this.retainedPayloadBytes = retainedPayloadBytes;
    }

    static IcebergPartitionInfo empty() {
        return EMPTY;
    }

    public Map<String, PartitionItem> getNameToPartitionItem() {
        return nameToPartitionItem;
    }

    public Map<String, IcebergPartition> getNameToIcebergPartition() {
        return nameToIcebergPartition;
    }

    Map<String, Set<String>> getNameToIcebergPartitionNames() {
        return nameToIcebergPartitionNames;
    }

    public long getRetainedPayloadBytes() {
        return retainedPayloadBytes;
    }

    private static long retainedPayloadBytes(Map<String, IcebergPartition> partitions) {
        if (partitions == null) {
            return 0L;
        }
        long bytes = 0L;
        for (IcebergPartition partition : partitions.values()) {
            if (partition != null) {
                bytes = MetaCacheWeightUtils.saturatedAdd(
                        bytes, partition.getRetainedPayloadBytes());
            }
        }
        return bytes;
    }

    public long getLatestSnapshotId(String partitionName) {
        Set<String> icebergPartitionNames = nameToIcebergPartitionNames.get(partitionName);
        if (icebergPartitionNames == null) {
            return nameToIcebergPartition.get(partitionName).getLastSnapshotId();
        }
        long latestSnapshotId = -1;
        long latestUpdateTime = -1;
        for (String name : icebergPartitionNames) {
            IcebergPartition partition = nameToIcebergPartition.get(name);
            long lastUpdateTime = partition.getLastUpdateTime();
            // Skip partitions with invalid update time (<= 0 means unknown/invalid)
            if (lastUpdateTime <= 0) {
                continue;
            }
            if (latestUpdateTime < lastUpdateTime) {
                latestUpdateTime = lastUpdateTime;
                latestSnapshotId = partition.getLastSnapshotId();
            }
        }
        return latestSnapshotId;
    }
}
