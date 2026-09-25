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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class MTMVRefreshPartitionSnapshot {
    private static final Logger LOG = LogManager.getLogger(MTMV.class);
    // old version only support one pct table
    @Deprecated
    @SerializedName("p")
    private ConcurrentMap<String, MTMVSnapshotIf> partitions;
    @SerializedName("pcts")
    private ConcurrentMap<BaseTableInfo, Map<String, MTMVSnapshotIf>> pcts;
    // old version only persist table id, we need `BaseTableInfo`, `tables` only for compatible old version
    @SerializedName("t")
    @Deprecated
    private ConcurrentMap<Long, MTMVSnapshotIf> tables;
    @SerializedName("ti")
    private ConcurrentMap<BaseTableInfo, MTMVSnapshotIf> tablesInfo;

    public MTMVRefreshPartitionSnapshot() {
        this.partitions = Maps.newConcurrentMap();
        this.pcts = Maps.newConcurrentMap();
        this.tables = Maps.newConcurrentMap();
        this.tablesInfo = Maps.newConcurrentMap();
    }

    @Deprecated
    public Map<String, MTMVSnapshotIf> getPartitions() {
        return partitions;
    }

    public Map<String, MTMVSnapshotIf> getPctSnapshot(BaseTableInfo pctTable) {
        return pcts.computeIfAbsent(pctTable, k -> Maps.newHashMap());
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

    public Map<BaseTableInfo, Map<String, MTMVSnapshotIf>> getPcts() {
        return pcts;
    }

    @Override
    public String toString() {
        return "MTMVRefreshPartitionSnapshot{"
                + "partitions=" + partitions
                + ", tablesInfo=" + tablesInfo
                + '}';
    }

    public boolean compatible(MTMV mtmv) throws Exception {
        boolean changed = false;
        // snapshot add partitionId resolve problem of insert overwrite
        changed |= compatiblePartitions(mtmv);
        // change table id to BaseTableInfo
        changed |= compatibleTables(mtmv);
        // snapshot add tableId resolve problem of recreate table
        changed |= compatibleTablesSnapshot();
        return changed;
    }

    private boolean compatiblePartitions(MTMV mtmv) throws AnalysisException {
        if (mtmv.getMvPartitionInfo().getPartitionType().equals(MTMVPartitionType.SELF_MANAGE)) {
            return false;
        }
        // Only olapTable has historical data issues that require compatibility
        if (mtmv.getMvPartitionInfo().getRelatedTableInfo().getCtlId() != InternalCatalog.INTERNAL_CATALOG_ID) {
            return false;
        }
        MTMVRelatedTableIf relatedTableIf = mtmv.getMvPartitionInfo().getRelatedTable();
        // Only olapTable has historical data issues that require compatibility
        if (!(relatedTableIf instanceof OlapTable)) {
            return false;
        }
        if (!checkHasDataWithoutPartitionId()) {
            return false;
        }
        OlapTable relatedTable = (OlapTable) relatedTableIf;
        boolean changed = false;
        for (Entry<String, MTMVSnapshotIf> entry : partitions.entrySet()) {
            MTMVVersionSnapshot versionSnapshot = (MTMVVersionSnapshot) entry.getValue();
            if (versionSnapshot.getId() == 0) {
                Partition partition = relatedTable.getPartition(entry.getKey());
                // if not find partition, may be partition has been dropped,
                // the impact is that MTMV will consider this partition to be async
                if (partition != null) {
                    (versionSnapshot).setId(partition.getId());
                    changed = true;
                }
            }
        }
        return changed;
    }

    private boolean checkHasDataWithoutPartitionId() {
        for (MTMVSnapshotIf snapshot : partitions.values()) {
            if (snapshot instanceof MTMVVersionSnapshot && ((MTMVVersionSnapshot) snapshot).getId() == 0) {
                return true;
            }
        }
        return false;
    }

    private boolean compatibleTablesSnapshot() {
        if (!checkHasDataWithoutTableId()) {
            return false;
        }
        boolean changed = false;
        for (Entry<BaseTableInfo, MTMVSnapshotIf> entry : tablesInfo.entrySet()) {
            MTMVVersionSnapshot versionSnapshot = (MTMVVersionSnapshot) entry.getValue();
            if (versionSnapshot.getId() == 0) {
                versionSnapshot.setId(entry.getKey().getTableId());
                changed = true;
            }
        }
        return changed;
    }

    private boolean checkHasDataWithoutTableId() {
        for (MTMVSnapshotIf snapshot : tablesInfo.values()) {
            if (snapshot instanceof MTMVVersionSnapshot && ((MTMVVersionSnapshot) snapshot).getId() == 0) {
                return true;
            }
        }
        return false;
    }

    private boolean compatibleTables(MTMV mtmv) throws Exception {
        if (tables.size() == tablesInfo.size()) {
            return false;
        }
        MTMVRelation relation = mtmv.getRelation();
        if (relation == null || CollectionUtils.isEmpty(relation.getBaseTablesOneLevelAndFromView())) {
            return false;
        }
        boolean changed = false;
        for (Entry<Long, MTMVSnapshotIf> entry : tables.entrySet()) {
            Optional<BaseTableInfo> tableInfo = getByTableId(entry.getKey(),
                    relation.getBaseTablesOneLevelAndFromView());
            if (tableInfo.isPresent()) {
                tablesInfo.put(tableInfo.get(), entry.getValue());
                changed = true;
            } else {
                String msg = String.format(
                        "Failed to get table info based on id during compatibility process, "
                                + "tableId: %s, relationTables: %s",
                        entry.getKey(), relation.getBaseTablesOneLevelAndFromView());
                LOG.warn(msg);
                throw new Exception(msg);
            }
        }
        return changed;
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
