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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.MTMV;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.MapUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MTMVRefreshSnapshot {
    @SerializedName("ps")
    private Map<String, MTMVRefreshPartitionSnapshot> partitionSnapshots;

    public MTMVRefreshSnapshot() {
        this.partitionSnapshots = Maps.newConcurrentMap();
    }

    public boolean equalsWithRelatedPartition(String mtmvPartitionName, String relatedPartitionName,
            MTMVSnapshotIf relatedPartitionCurrentSnapshot) {
        MTMVRefreshPartitionSnapshot partitionSnapshot = partitionSnapshots.get(mtmvPartitionName);
        if (partitionSnapshot == null) {
            return false;
        }
        MTMVSnapshotIf relatedPartitionSnapshot = partitionSnapshot.getPartitions().get(relatedPartitionName);
        if (relatedPartitionSnapshot == null) {
            return false;
        }
        return relatedPartitionSnapshot.equals(relatedPartitionCurrentSnapshot);
    }

    public Set<String> getSnapshotPartitions(String mtmvPartitionName) {
        MTMVRefreshPartitionSnapshot partitionSnapshot = partitionSnapshots.get(mtmvPartitionName);
        if (partitionSnapshot == null) {
            return Sets.newHashSet();
        }
        return partitionSnapshot.getPartitions().keySet();
    }

    public boolean equalsWithBaseTable(String mtmvPartitionName, BaseTableInfo tableInfo,
            MTMVSnapshotIf baseTableCurrentSnapshot) {
        MTMVRefreshPartitionSnapshot partitionSnapshot = partitionSnapshots.get(mtmvPartitionName);
        if (partitionSnapshot == null) {
            return false;
        }
        MTMVSnapshotIf relatedPartitionSnapshot = partitionSnapshot.getTableSnapshot(tableInfo);
        if (relatedPartitionSnapshot == null) {
            return false;
        }
        return relatedPartitionSnapshot.equals(baseTableCurrentSnapshot);
    }

    public void updateSnapshots(Map<String, MTMVRefreshPartitionSnapshot> addPartitionSnapshots,
            Set<String> mvPartitionNames) {
        if (!MapUtils.isEmpty(addPartitionSnapshots)) {
            this.partitionSnapshots.putAll(addPartitionSnapshots);
        }
        Iterator<String> iterator = partitionSnapshots.keySet().iterator();
        while (iterator.hasNext()) {
            String partitionName = iterator.next();
            if (!mvPartitionNames.contains(partitionName)) {
                iterator.remove();
            }
        }
    }

    @Override
    public String toString() {
        return "MTMVRefreshSnapshot{"
                + "partitionSnapshots=" + partitionSnapshots
                + '}';
    }

    public void compatible(MTMV mtmv) {
        if (MapUtils.isEmpty(partitionSnapshots)) {
            return;
        }
        for (MTMVRefreshPartitionSnapshot snapshot : partitionSnapshots.values()) {
            snapshot.compatible(mtmv);
        }
    }
}
