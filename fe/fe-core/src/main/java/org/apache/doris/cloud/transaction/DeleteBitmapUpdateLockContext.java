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

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeleteBitmapUpdateLockContext {
    private long lockId;
    private Map<Long, Long> baseCompactionCnts;
    private Map<Long, Long> cumulativeCompactionCnts;
    private Map<Long, Long> cumulativePoints;
    private Map<Long, Long> tabletStates;
    private Map<Long, Set<Long>> tableToPartitions;
    private Map<Long, Partition> partitions;
    private Map<Long, Map<Long, Set<Long>>> backendToPartitionTablets;
    private Map<Long, List<Long>> tableToTabletList;
    private Map<Long, TabletMeta> tabletToTabletMeta;

    public DeleteBitmapUpdateLockContext(long lockId) {
        this.lockId = lockId;
        baseCompactionCnts = Maps.newHashMap();
        cumulativeCompactionCnts = Maps.newHashMap();
        cumulativePoints = Maps.newHashMap();
        tabletStates = Maps.newHashMap();
        tableToPartitions = Maps.newHashMap();
        partitions = Maps.newHashMap();
        backendToPartitionTablets = Maps.newHashMap();
        tableToTabletList = Maps.newHashMap();
        tabletToTabletMeta = Maps.newHashMap();
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
