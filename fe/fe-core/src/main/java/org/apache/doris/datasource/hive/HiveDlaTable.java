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
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalSchemaCache;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.hudi.HudiMvccSnapshot;
import org.apache.doris.datasource.hudi.HudiSchemaCacheKey;
import org.apache.doris.datasource.hudi.HudiUtils;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.mtmv.MTMVMaxTimestampSnapshot;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collections;
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
        if (hmsTable.getDlaType() == DLAType.HUDI) {
            return getHudiSchemaCacheValue(snapshot).getPartitionColumns();
        }
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
        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
        HiveMetaStoreCache.HivePartitionValues hivePartitionValues = cache.getPartitionValues(
                hmsTable.getDbName(), hmsTable.getName(), hmsTable.getPartitionColumnTypes(snapshot));
        Long partitionId = getPartitionIdByNameOrAnalysisException(partitionName, hivePartitionValues);
        HivePartition hivePartition = getHivePartitionByIdOrAnalysisException(partitionId,
                hivePartitionValues, cache);
        return new MTMVTimestampSnapshot(hivePartition.getLastModifiedTime());
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        if (hmsTable.getPartitionType(snapshot) == PartitionType.UNPARTITIONED) {
            return new MTMVMaxTimestampSnapshot(hmsTable.getName(), hmsTable.getLastDdlTime());
        }
        HivePartition maxPartition = null;
        long maxVersionTime = 0L;
        long visibleVersionTime;
        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
        HiveMetaStoreCache.HivePartitionValues hivePartitionValues = cache.getPartitionValues(
                hmsTable.getDbName(), hmsTable.getName(), hmsTable.getPartitionColumnTypes(snapshot));
        List<HivePartition> partitionList = cache.getAllPartitionsWithCache(hmsTable.getDbName(), hmsTable.getName(),
                Lists.newArrayList(hivePartitionValues.getPartitionValuesMap().values()));
        if (CollectionUtils.isEmpty(partitionList)) {
            throw new AnalysisException("partitionList is empty, table name: " + hmsTable.getName());
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
            HiveMetaStoreCache.HivePartitionValues hivePartitionValues)
            throws AnalysisException {
        Long partitionId = hivePartitionValues.getPartitionNameToIdMap().get(partitionName);
        if (partitionId == null) {
            throw new AnalysisException("can not find partition: " + partitionName);
        }
        return partitionId;
    }

    private HivePartition getHivePartitionByIdOrAnalysisException(Long partitionId,
            HiveMetaStoreCache.HivePartitionValues hivePartitionValues,
            HiveMetaStoreCache cache) throws AnalysisException {
        List<String> partitionValues = hivePartitionValues.getPartitionValuesMap().get(partitionId);
        if (CollectionUtils.isEmpty(partitionValues)) {
            throw new AnalysisException("can not find partitionValues: " + partitionId);
        }
        HivePartition partition = cache.getHivePartition(hmsTable.getDbName(), hmsTable.getName(), partitionValues);
        if (partition == null) {
            throw new AnalysisException("can not find partition: " + partitionId);
        }
        return partition;
    }

    public HMSSchemaCacheValue getHudiSchemaCacheValue(Optional<MvccSnapshot> snapshot) {
        TablePartitionValues snapshotCacheValue = getOrFetchHudiSnapshotCacheValue(snapshot);
        return getHudiSchemaCacheValue(snapshotCacheValue.getLastUpdateTimestamp());
    }

    private HMSSchemaCacheValue getHudiSchemaCacheValue(long timestamp) {
        ExternalSchemaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr().getSchemaCache(hmsTable.getCatalog());
        Optional<SchemaCacheValue> schemaCacheValue = cache.getSchemaValue(
                new HudiSchemaCacheKey(hmsTable.getDbName(), hmsTable.getName(), timestamp));
        if (!schemaCacheValue.isPresent()) {
            throw new CacheException("failed to getSchema for: %s.%s.%s.%s",
                    null, hmsTable.getCatalog().getName(), hmsTable.getDbName(), hmsTable.getName(), timestamp);
        }
        return (HMSSchemaCacheValue) schemaCacheValue.get();
    }

    private TablePartitionValues getOrFetchHudiSnapshotCacheValue(Optional<MvccSnapshot> snapshot) {
        if (snapshot.isPresent()) {
            return ((HudiMvccSnapshot) snapshot.get()).getTablePartitionValues();
        } else {
            return HudiUtils.getPartitionValues(Optional.empty(), hmsTable);
        }
    }

    @Override
    public boolean isPartitionColumnAllowNull() {
        return true;
    }
}
