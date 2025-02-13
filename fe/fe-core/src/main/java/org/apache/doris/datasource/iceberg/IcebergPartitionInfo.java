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

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

public class IcebergPartitionInfo {
    private final Map<String, PartitionItem> nameToPartitionItem;
    private final Map<String, IcebergPartition> nameToIcebergPartition;
    private final Map<String, Set<String>> nameToIcebergPartitionNames;

    private static final IcebergPartitionInfo EMPTY = new IcebergPartitionInfo();

    private IcebergPartitionInfo() {
        this.nameToPartitionItem = Maps.newHashMap();
        this.nameToIcebergPartition = Maps.newHashMap();
        this.nameToIcebergPartitionNames = Maps.newHashMap();
    }

    public IcebergPartitionInfo(Map<String, PartitionItem> nameToPartitionItem,
                                Map<String, IcebergPartition> nameToIcebergPartition,
                                Map<String, Set<String>> nameToIcebergPartitionNames) {
        this.nameToPartitionItem = nameToPartitionItem;
        this.nameToIcebergPartition = nameToIcebergPartition;
        this.nameToIcebergPartitionNames = nameToIcebergPartitionNames;
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

    public long getLatestSnapshotId(String partitionName) {
        Set<String> icebergPartitionNames = nameToIcebergPartitionNames.get(partitionName);
        if (icebergPartitionNames == null) {
            return nameToIcebergPartition.get(partitionName).getLastSnapshotId();
        }
        long latestSnapshotId = 0;
        long latestUpdateTime = -1;
        for (String name : icebergPartitionNames) {
            IcebergPartition partition = nameToIcebergPartition.get(name);
            long lastUpdateTime = partition.getLastUpdateTime();
            if (latestUpdateTime < lastUpdateTime) {
                latestUpdateTime = lastUpdateTime;
                latestSnapshotId = partition.getLastSnapshotId();
            }
        }
        return latestSnapshotId;
    }
}
