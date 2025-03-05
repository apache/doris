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
import org.apache.doris.catalog.TableIf;
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

    public Optional<String> compatible(MTMV mtmv) {
        // snapshot add partitionId resolve problem of insert overwrite
        Optional<String> errMsg = compatiblePartitions(mtmv);
        if (errMsg.isPresent()) {
            return errMsg;
        }
        // change table id to BaseTableInfo
        errMsg = compatibleTables(mtmv);
        if (errMsg.isPresent()) {
            return errMsg;
        }
        // snapshot add tableId resolve problem of recreate table
        return compatibleTablesSnapshot();
    }

    private Optional<String> compatiblePartitions(MTMV mtmv) {
        MTMVRelatedTableIf relatedTableIf = null;
        try {
            relatedTableIf = mtmv.getMvPartitionInfo().getRelatedTable();
        } catch (AnalysisException e) {
            String msg = String.format(
                    "Failed to get relatedTable during compatibility process, "
                            + "mvName: %s, ", mtmv.getName());
            LOG.warn(msg, e);
            return Optional.of(msg);
        }
        // Only olapTable has historical data issues that require compatibility
        if (!(relatedTableIf instanceof OlapTable)) {
            return Optional.empty();
        }
        if (!checkHasDataWithoutPartitionId()) {
            return Optional.empty();
        }
        OlapTable relatedTable = (OlapTable) mtmv.getMvPartitionInfo().getRelatedTable();
        for (Entry<String, MTMVSnapshotIf> entry : partitions.entrySet()) {
            MTMVVersionSnapshot versionSnapshot = (MTMVVersionSnapshot) entry.getValue();
            if (versionSnapshot.getId() == 0) {
                Partition partition = relatedTable.getPartition(entry.getKey());
                // if not find partition, may be partition has been dropped,
                // the impact is that MTMV will consider this partition to be async
                if (partition != null) {
                    (versionSnapshot).setId(partition.getId());
                }
            }
        }
        return Optional.empty();
    }

    private boolean checkHasDataWithoutPartitionId() {
        for (MTMVSnapshotIf snapshot : partitions.values()) {
            if (snapshot instanceof MTMVVersionSnapshot && ((MTMVVersionSnapshot) snapshot).getId() == 0) {
                return true;
            }
        }
        return false;
    }

    private Optional<String> compatibleTablesSnapshot() {
        if (!checkHasDataWithoutTableId()) {
            return Optional.empty();
        }
        for (Entry<BaseTableInfo, MTMVSnapshotIf> entry : tablesInfo.entrySet()) {
            MTMVVersionSnapshot versionSnapshot = (MTMVVersionSnapshot) entry.getValue();
            if (versionSnapshot.getId() == 0) {
                try {
                    TableIf table = MTMVUtil.getTable(entry.getKey());
                    versionSnapshot.setId(table.getId());
                } catch (AnalysisException e) {
                    String msg = String.format(
                            "Failed to get table info based on table info during compatibility process, table info: %s",
                            entry.getKey());
                    LOG.warn(msg);
                    return Optional.of(msg);
                }
            }
        }
        return Optional.empty();
    }

    private boolean checkHasDataWithoutTableId() {
        for (MTMVSnapshotIf snapshot : tablesInfo.values()) {
            if (snapshot instanceof MTMVVersionSnapshot && ((MTMVVersionSnapshot) snapshot).getId() == 0) {
                return true;
            }
        }
        return false;
    }

    private Optional<String> compatibleTables(MTMV mtmv) {
        if (tables.size() == tablesInfo.size()) {
            return Optional.empty();
        }
        MTMVRelation relation = mtmv.getRelation();
        if (relation == null || CollectionUtils.isEmpty(relation.getBaseTablesOneLevel())) {
            return Optional.empty();
        }
        for (Entry<Long, MTMVSnapshotIf> entry : tables.entrySet()) {
            Optional<BaseTableInfo> tableInfo = getByTableId(entry.getKey(),
                    relation.getBaseTablesOneLevel());
            if (tableInfo.isPresent()) {
                tablesInfo.put(tableInfo.get(), entry.getValue());
            } else {
                String msg = String.format(
                        "Failed to get table info based on id during compatibility process, "
                                + "tableId: %s, relationTables: %s",
                        entry.getKey(), relation.getBaseTablesOneLevel());
                LOG.warn(msg);
                return Optional.of(msg);
            }
        }
        return Optional.empty();
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
