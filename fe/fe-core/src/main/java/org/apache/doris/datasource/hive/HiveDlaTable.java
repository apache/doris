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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.mtmv.MTMVMaxTimestampSnapshot;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class HiveDlaTable extends HMSDlaTable {

    public HiveDlaTable(HMSExternalTable table) {
        super(table);
    }

    @Override
    public PartitionType getPartitionType(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumns(snapshot).size() > 0 ? PartitionType.LIST : PartitionType.UNPARTITIONED;
    }

    @Override
    public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumns(snapshot).stream()
                .map(c -> c.getName().toLowerCase()).collect(Collectors.toSet());
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        Optional<SchemaCacheValue> schemaCacheValue = hmsTable.getSchemaCacheValue();
        return schemaCacheValue.map(value -> ((HMSSchemaCacheValue) value).getPartitionColumns())
                .orElse(Collections.emptyList());
    }

    @Override
    public Map<String, PartitionItem> getAndCopyPartitionItems(Optional<MvccSnapshot> snapshot) {
        return hmsTable.getNameToPartitionItems();
    }

    @Override
    public MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
            Optional<MvccSnapshot> snapshot) throws AnalysisException {
        HiveExternalMetaCache.HivePartitionValues hivePartitionValues = hmsTable.getHivePartitionValues(snapshot);
        Long partitionId = getPartitionIdByNameOrAnalysisException(partitionName, hivePartitionValues);
        HiveExternalMetaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .hive(hmsTable.getCatalog().getId());
        HivePartition hivePartition = getHivePartitionByIdOrAnalysisException(partitionId,
                hivePartitionValues, cache);
        return new MTMVTimestampSnapshot(hivePartition.getLastModifiedTime());
    }

    /**
     * Bulk form of {@link #getPartitionSnapshot}: resolves every requested name against one partition-value
     * listing and loads all cache misses through one batched HMS request instead of one RPC per partition.
     * A name absent from the listing is omitted (the refresh context reports it per partition); any other
     * failure is normalized to the checked AnalysisException this MTMV boundary declares, so the
     * transparent-rewrite path degrades per-MV instead of a raw runtime exception disabling every MV
     * candidate at the planner hook.
     */
    Map<String, MTMVSnapshotIf> getPartitionSnapshots(Set<String> partitionNames,
            Optional<MvccSnapshot> snapshot) throws AnalysisException {
        try {
            HiveExternalMetaCache.HivePartitionValues hivePartitionValues =
                    hmsTable.getHivePartitionValues(snapshot);
            HiveExternalMetaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                    .hive(hmsTable.getCatalog().getId());
            List<String> resolvedNames = new ArrayList<>(partitionNames.size());
            List<List<String>> resolvedValues = new ArrayList<>(partitionNames.size());
            for (String partitionName : partitionNames) {
                Long partitionId = hivePartitionValues.getPartitionNameToIdMap().get(partitionName);
                if (partitionId == null) {
                    continue;
                }
                List<String> partitionValues = hivePartitionValues.getPartitionValuesMap().get(partitionId);
                if (CollectionUtils.isEmpty(partitionValues)) {
                    continue;
                }
                resolvedNames.add(partitionName);
                resolvedValues.add(partitionValues);
            }
            Map<String, MTMVSnapshotIf> result = new LinkedHashMap<>();
            if (resolvedNames.isEmpty()) {
                return result;
            }
            List<HivePartition> partitions = cache.getAllPartitionsWithCache(hmsTable, resolvedValues);
            if (partitions.size() != resolvedNames.size()) {
                throw new AnalysisException("Invalid HMS partition result: requested="
                        + resolvedNames.size() + ", returned=" + partitions.size());
            }
            for (int i = 0; i < resolvedNames.size(); i++) {
                result.put(resolvedNames.get(i),
                        new MTMVTimestampSnapshot(partitions.get(i).getLastModifiedTime()));
            }
            return result;
        } catch (AnalysisException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new AnalysisException("failed to load partition snapshots for "
                    + hmsTable.getName() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        return getTableSnapshot(snapshot);
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(Optional<MvccSnapshot> snapshot) throws AnalysisException {
        if (hmsTable.getPartitionType(snapshot) == PartitionType.UNPARTITIONED) {
            return new MTMVMaxTimestampSnapshot(hmsTable.getName(), hmsTable.getLastDdlTime());
        }
        HivePartition maxPartition = null;
        long maxVersionTime = 0L;
        long visibleVersionTime;
        HiveExternalMetaCache.HivePartitionValues hivePartitionValues = hmsTable.getHivePartitionValues(snapshot);
        HiveExternalMetaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .hive(hmsTable.getCatalog().getId());
        List<HivePartition> partitionList = cache.getAllPartitionsWithCache(hmsTable,
                Lists.newArrayList(hivePartitionValues.getPartitionValuesMap().values()));
        if (CollectionUtils.isEmpty(partitionList)) {
            return new MTMVMaxTimestampSnapshot(hmsTable.getName(), 0L);
        }
        for (HivePartition hivePartition : partitionList) {
            visibleVersionTime = hivePartition.getLastModifiedTime();
            if (visibleVersionTime > maxVersionTime) {
                maxVersionTime = visibleVersionTime;
                maxPartition = hivePartition;
            }
        }
        return new MTMVMaxTimestampSnapshot(maxPartition.getPartitionName(
                hmsTable.getPartitionColumns()), maxVersionTime);
    }

    private Long getPartitionIdByNameOrAnalysisException(String partitionName,
            HiveExternalMetaCache.HivePartitionValues hivePartitionValues)
            throws AnalysisException {
        Long partitionId = hivePartitionValues.getPartitionNameToIdMap().get(partitionName);
        if (partitionId == null) {
            throw new AnalysisException("can not find partition: " + partitionName);
        }
        return partitionId;
    }

    private HivePartition getHivePartitionByIdOrAnalysisException(Long partitionId,
            HiveExternalMetaCache.HivePartitionValues hivePartitionValues,
            HiveExternalMetaCache cache) throws AnalysisException {
        List<String> partitionValues = hivePartitionValues.getPartitionValuesMap().get(partitionId);
        if (CollectionUtils.isEmpty(partitionValues)) {
            throw new AnalysisException("can not find partitionValues: " + partitionId);
        }
        HivePartition partition = cache.getHivePartition(hmsTable, partitionValues);
        if (partition == null) {
            throw new AnalysisException("can not find partition: " + partitionId);
        }
        return partition;
    }

    @Override
    public boolean isPartitionColumnAllowNull() {
        return true;
    }
}
