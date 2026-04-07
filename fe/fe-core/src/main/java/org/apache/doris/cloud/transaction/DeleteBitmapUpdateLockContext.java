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

package org.apache.doris.cloud.transaction;

import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TabletMeta;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeleteBitmapUpdateLockContext {
    private long lockId;
    private Long2LongOpenHashMap baseCompactionCnts;
    private Long2LongOpenHashMap cumulativeCompactionCnts;
    private Long2LongOpenHashMap cumulativePoints;
    private Long2LongOpenHashMap tabletStates;
    private Long2ObjectOpenHashMap<Set<Long>> tableToPartitions;
    private Long2ObjectOpenHashMap<Partition> partitions;
    private Long2ObjectOpenHashMap<Map<Long, Set<Long>>> backendToPartitionTablets;
    private Long2ObjectOpenHashMap<List<Long>> tableToTabletList;
    private Long2ObjectOpenHashMap<TabletMeta> tabletToTabletMeta;

    public DeleteBitmapUpdateLockContext(long lockId) {
        this.lockId = lockId;
        baseCompactionCnts = new Long2LongOpenHashMap();
        cumulativeCompactionCnts = new Long2LongOpenHashMap();
        cumulativePoints = new Long2LongOpenHashMap();
        tabletStates = new Long2LongOpenHashMap();
        tableToPartitions = new Long2ObjectOpenHashMap<>();
        partitions = new Long2ObjectOpenHashMap<>();
        backendToPartitionTablets = new Long2ObjectOpenHashMap<>();
        tableToTabletList = new Long2ObjectOpenHashMap<>();
        tabletToTabletMeta = new Long2ObjectOpenHashMap<>();
    }

    public long getLockId() {
        return lockId;
    }

    public Map<Long, List<Long>> getTableToTabletList() {
        return tableToTabletList;
    }

    public Map<Long, Long> getBaseCompactionCnts() {
        return baseCompactionCnts;
    }

    public Map<Long, Long> getCumulativeCompactionCnts() {
        return cumulativeCompactionCnts;
    }

    public Map<Long, Long> getCumulativePoints() {
        return cumulativePoints;
    }

    public Map<Long, Long> getTabletStates() {
        return tabletStates;
    }

    public Map<Long, Map<Long, Set<Long>>> getBackendToPartitionTablets() {
        return backendToPartitionTablets;
    }

    public Map<Long, Partition> getPartitions() {
        return partitions;
    }

    public Map<Long, Set<Long>> getTableToPartitions() {
        return tableToPartitions;
    }

    public Map<Long, TabletMeta> getTabletToTabletMeta() {
        return tabletToTabletMeta;
    }

}
