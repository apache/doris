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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

public class MTMVRefreshPartitionSnapshot {
    private static final Logger LOG = LogManager.getLogger(MTMV.class);
    @SerializedName("p")
    private Map<String, MTMVSnapshotIf> partitions;
    // old version only persist table id, we need `BaseTableInfo`, `tables` only for compatible old version
    @SerializedName("t")
    @Deprecated
    private Map<Long, MTMVSnapshotIf> tables;
    @SerializedName("ti")
    private Map<BaseTableInfo, MTMVSnapshotIf> tablesInfo;

    public MTMVRefreshPartitionSnapshot() {
        this.partitions = Maps.newConcurrentMap();
        this.tables = Maps.newConcurrentMap();
        this.tablesInfo = Maps.newConcurrentMap();
    }

    public Map<String, MTMVSnapshotIf> getPartitions() {
        return partitions;
    }

    public MTMVSnapshotIf getTableSnapshot(BaseTableInfo table) {
        if (tablesInfo.containsKey(table)) {
            return tablesInfo.get(table);
        }
        // for compatible old version
        return tables.get(table.getTableId());
    }

    public void addTableSnapshot(BaseTableInfo baseTableInfo, MTMVSnapshotIf tableSnapshot) {
        tablesInfo.put(baseTableInfo, tableSnapshot);
        // for compatible old version
        tables.put(baseTableInfo.getTableId(), tableSnapshot);
    }

    @Override
    public String toString() {
        return "MTMVRefreshPartitionSnapshot{"
                + "partitions=" + partitions
                + ", tablesInfo=" + tablesInfo
                + '}';
    }

    public void compatible(MTMV mtmv) {
        try {
            // snapshot add partitionId resolve problem of insert overwrite
            compatiblePartitions(mtmv);
        } catch (Throwable e) {
            LOG.warn("MTMV compatiblePartitions failed, mtmv: {}", mtmv.getName(), e);
        }
        try {
            // change table id to BaseTableInfo
            compatibleTables(mtmv);
        } catch (Throwable e) {
            LOG.warn("MTMV compatibleTables failed, mtmv: {}", mtmv.getName(), e);
        }
    }

    private void compatiblePartitions(MTMV mtmv) throws AnalysisException {
        if (!checkHasDataWithoutPartitionId()) {
            return;
        }
        OlapTable relatedTable = (OlapTable) mtmv.getMvPartitionInfo().getRelatedTable();
        for (Entry<String, MTMVSnapshotIf> entry : partitions.entrySet()) {
            MTMVVersionSnapshot versionSnapshot = (MTMVVersionSnapshot) entry.getValue();
            if (versionSnapshot.getId() == 0) {
                Partition partition = relatedTable.getPartition(entry.getKey());
                if (partition != null) {
                    (versionSnapshot).setId(partition.getId());
                }
            }
        }
    }

    private boolean checkHasDataWithoutPartitionId() {
        for (MTMVSnapshotIf snapshot : partitions.values()) {
            if (snapshot instanceof MTMVVersionSnapshot && ((MTMVVersionSnapshot) snapshot).getId() == 0) {
                return true;
            }
        }
        return false;
    }

    private void compatibleTables(MTMV mtmv) {
        if (tables.size() == tablesInfo.size()) {
            return;
        }
        MTMVRelation relation = mtmv.getRelation();
        if (relation == null || CollectionUtils.isEmpty(relation.getBaseTablesOneLevel())) {
            return;
        }
        for (Entry<Long, MTMVSnapshotIf> entry : tables.entrySet()) {
            Optional<BaseTableInfo> tableInfo = getByTableId(entry.getKey(),
                    relation.getBaseTablesOneLevel());
            if (tableInfo.isPresent()) {
                tablesInfo.put(tableInfo.get(), entry.getValue());
            } else {
                LOG.warn("MTMV compatibleTables failed, tableId: {}, relationTables: {}", entry.getKey(),
                        relation.getBaseTablesOneLevel());
            }
        }
    }

    private Optional<BaseTableInfo> getByTableId(Long tableId, Set<BaseTableInfo> baseTables) {
        for (BaseTableInfo info : baseTables) {
            if (info.getTableId() == tableId) {
                return Optional.of(info);
            }
        }
        return Optional.empty();
    }
}
