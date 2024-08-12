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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogMgr;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

public class MTMVRefreshPartitionSnapshot {
    private static final Logger LOG = LogManager.getLogger(MTMVRefreshPartitionSnapshot.class);
    @SerializedName("p")
    private Map<String, MTMVSnapshotIf> partitions;
    @Deprecated
    @SerializedName("t")
    private Map<Long, MTMVSnapshotIf> tablesId;

    @SerializedName("tn")
    private Map<BaseTableNameInfo, MTMVSnapshotIf> tables;

    public MTMVRefreshPartitionSnapshot() {
        this.partitions = Maps.newConcurrentMap();
        this.tables = Maps.newConcurrentMap();
    }

    public Map<String, MTMVSnapshotIf> getPartitions() {
        return partitions;
    }

    public Map<BaseTableNameInfo, MTMVSnapshotIf> getTables() {
        return tables;
    }

    @Override
    public String toString() {
        return "MTMVRefreshPartitionSnapshot{"
                + "partitions=" + partitions
                + ", tables=" + tables
                + '}';
    }

    public void compatible(CatalogMgr catalogMgr, MTMV mtmv) {
        if (MapUtils.isEmpty(tablesId)) {
            return;
        }
        MTMVRelation relation = mtmv.getRelation();
        if (relation == null || CollectionUtils.isEmpty(relation.getBaseTablesOneLevel())) {
            tablesId = null;
            return;
        }
        for (Entry<Long, MTMVSnapshotIf> entry : tablesId.entrySet()) {
            try {
                Optional<BaseTableNameInfo> tableInfo = getByTableId(entry.getKey(), catalogMgr,
                        relation.getBaseTablesOneLevel());
                if (tableInfo.isPresent()) {
                    tables.put(tableInfo.get(), entry.getValue());
                }
            } catch (Throwable e) {
                LOG.warn("can not transfer tableId to tableInfo: {}, "
                        + "may be cause by catalog/db/table dropped, we need rebuild MTMV", entry.getKey(), e);
            }
        }
        tablesId = null;
    }

    private Optional<BaseTableNameInfo> getByTableId(Long tableId, CatalogMgr catalogMgr, Set<BaseTableInfo> baseTables)
            throws AnalysisException {
        Optional<BaseTableInfo> baseTableInfo = getBaseTableInfo(tableId, baseTables);
        if (!baseTableInfo.isPresent()) {
            return Optional.empty();
        }
        BaseTableNameInfo baseTableNameInfo = MTMVUtil.transferIdToName(baseTableInfo.get(), catalogMgr);
        return Optional.of(baseTableNameInfo);
    }

    private Optional<BaseTableInfo> getBaseTableInfo(Long tableId, Set<BaseTableInfo> baseTables) {
        for (BaseTableInfo info : baseTables) {
            if (info.getTableId() == tableId) {
                return Optional.of(info);
            }
        }
        return Optional.empty();
    }
}
