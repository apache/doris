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

package org.apache.doris.datasource.metacache;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.hive.HiveEngineCache;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.datasource.hudi.source.HudiEngineCache;
import org.apache.doris.datasource.iceberg.IcebergEngineCache;
import org.apache.doris.datasource.iceberg.IcebergPartitionInfo;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonEngineCache;
import org.apache.doris.datasource.paimon.PaimonPartitionInfo;
import org.apache.doris.mtmv.MTMVMaxTimestampSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIdSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;

import com.google.common.collect.BiMap;
import com.google.common.collect.Maps;
import org.apache.paimon.partition.Partition;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Unified MTMV metadata bridge based on EngineMetaCache.
 */
public final class EngineMtmvSupport {
    private EngineMtmvSupport() {
    }

    public static Map<String, PartitionItem> getAndCopyPartitionItems(ExternalTable table,
            Optional<MvccSnapshot> snapshot) {
        return Maps.newHashMap(getPartitionItems(table, snapshot));
    }

    public static Map<String, PartitionItem> getPartitionItems(ExternalTable table, Optional<MvccSnapshot> snapshot) {
        EnginePartitionInfo partitionInfo = getEngineMetaCache(table).getPartitionInfo(table, snapshot);
        return partitionItems(partitionInfo, table);
    }

    public static MTMVSnapshotIf getPartitionSnapshot(ExternalTable table, String partitionName,
            Optional<MvccSnapshot> snapshot) throws AnalysisException {
        EngineMetaCache cache = getEngineMetaCache(table);
        EnginePartitionInfo partitionInfo = cache.getPartitionInfo(table, snapshot);
        EngineSnapshot engineSnapshot = cache.getSnapshot(table, snapshot);

        if (partitionInfo instanceof IcebergEngineCache.IcebergPartition
                && engineSnapshot instanceof IcebergEngineCache.IcebergSnapshotMeta) {
            IcebergPartitionInfo icebergPartitionInfo =
                    ((IcebergEngineCache.IcebergPartition) partitionInfo).getPartitionInfo();
            long latestSnapshotId = icebergPartitionInfo.getLatestSnapshotId(partitionName);
            if (latestSnapshotId <= 0) {
                long tableSnapshotId = ((IcebergEngineCache.IcebergSnapshotMeta) engineSnapshot)
                        .getSnapshot().getSnapshotId();
                if (tableSnapshotId <= 0) {
                    throw new AnalysisException("can not find partition: " + partitionName
                            + ", and table snapshot ID is also invalid");
                }
                return new MTMVSnapshotIdSnapshot(tableSnapshotId);
            }
            return new MTMVSnapshotIdSnapshot(latestSnapshotId);
        }

        if (partitionInfo instanceof PaimonEngineCache.PaimonPartition) {
            Map<String, Partition> nameToPartition =
                    ((PaimonEngineCache.PaimonPartition) partitionInfo).getPartitionInfo().getNameToPartition();
            Partition paimonPartition = nameToPartition.get(partitionName);
            if (paimonPartition == null) {
                throw new AnalysisException("can not find partition: " + partitionName);
            }
            return new MTMVTimestampSnapshot(paimonPartition.lastFileCreationTime());
        }

        if (partitionInfo instanceof HiveEngineCache.HivePartition && cache instanceof HiveEngineCache) {
            HiveMetaStoreCache.HivePartitionValues hivePartitionValues =
                    ((HiveEngineCache.HivePartition) partitionInfo).getPartitionValues();
            Long partitionId = hivePartitionValues.getPartitionNameToIdMap().get(partitionName);
            if (partitionId == null) {
                throw new AnalysisException("can not find partition: " + partitionName);
            }
            List<String> partitionValues = hivePartitionValues.getPartitionValuesMap().get(partitionId);
            if (partitionValues == null) {
                throw new AnalysisException("can not find partition values for partition id: " + partitionId);
            }
            HivePartition hivePartition = ((HiveEngineCache) cache).getMetaStoreCache()
                    .getHivePartition(table, partitionValues);
            if (hivePartition == null) {
                throw new AnalysisException("can not find partition: " + partitionName);
            }
            return new MTMVTimestampSnapshot(hivePartition.getLastModifiedTime());
        }

        if (partitionInfo instanceof HudiEngineCache.HudiPartition
                && engineSnapshot instanceof HudiEngineCache.HudiSnapshotMeta) {
            TablePartitionValues hudiPartitionValues =
                    ((HudiEngineCache.HudiPartition) partitionInfo).getPartitionValues();
            if (!hudiPartitionValues.getPartitionNameToIdMap().containsKey(partitionName)) {
                throw new AnalysisException("can not find partition: " + partitionName);
            }
            Map<String, Long> partitionNameToLastModifiedMap = hudiPartitionValues.getPartitionNameToLastModifiedMap();
            long partitionTimestamp = partitionNameToLastModifiedMap == null
                    ? 0L
                    : partitionNameToLastModifiedMap.getOrDefault(partitionName, 0L);
            if (partitionTimestamp <= 0) {
                partitionTimestamp = ((HudiEngineCache.HudiSnapshotMeta) engineSnapshot).getTimestamp();
            }
            return new MTMVTimestampSnapshot(partitionTimestamp);
        }

        throw new AnalysisException("Unsupported partition snapshot conversion for table "
                + table.getNameWithFullQualifiers() + ", partitionInfo=" + partitionInfo.getClass().getSimpleName()
                + ", snapshot=" + engineSnapshot.getClass().getSimpleName());
    }

    public static MTMVSnapshotIf getTableSnapshot(ExternalTable table, Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        EngineSnapshot engineSnapshot = getEngineMetaCache(table).getSnapshot(table, snapshot);
        if (engineSnapshot instanceof IcebergEngineCache.IcebergSnapshotMeta) {
            return new MTMVSnapshotIdSnapshot(
                    ((IcebergEngineCache.IcebergSnapshotMeta) engineSnapshot).getSnapshot().getSnapshotId());
        }
        if (engineSnapshot instanceof PaimonEngineCache.PaimonSnapshotMeta) {
            return new MTMVSnapshotIdSnapshot(
                    ((PaimonEngineCache.PaimonSnapshotMeta) engineSnapshot).getSnapshot().getSnapshotId());
        }
        if (engineSnapshot instanceof HiveEngineCache.HiveSnapshotMeta) {
            HiveEngineCache.HiveSnapshotMeta hiveSnapshotMeta = (HiveEngineCache.HiveSnapshotMeta) engineSnapshot;
            return new MTMVMaxTimestampSnapshot(hiveSnapshotMeta.getPartitionName(), hiveSnapshotMeta.getTimestamp());
        }
        if (engineSnapshot instanceof HudiEngineCache.HudiSnapshotMeta) {
            return new MTMVTimestampSnapshot(((HudiEngineCache.HudiSnapshotMeta) engineSnapshot).getTimestamp());
        }
        throw new AnalysisException("Unsupported table snapshot conversion for table "
                + table.getNameWithFullQualifiers() + ", snapshot=" + engineSnapshot.getClass().getSimpleName());
    }

    public static Map<String, Partition> getPaimonPartitionSnapshot(ExternalTable table,
            Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        return getPaimonPartitionInfo(table, snapshot).getNameToPartition();
    }

    private static EngineMetaCache getEngineMetaCache(ExternalTable table) {
        return Env.getCurrentEnv().getExtMetaCacheMgr()
                .getUnifiedMetaCacheMgr()
                .getOrCreateEngineMetaCache(table);
    }

    private static Map<String, PartitionItem> partitionItems(EnginePartitionInfo partitionInfo, ExternalTable table) {
        if (partitionInfo instanceof IcebergEngineCache.IcebergPartition) {
            return ((IcebergEngineCache.IcebergPartition) partitionInfo).getPartitionInfo().getNameToPartitionItem();
        }
        if (partitionInfo instanceof PaimonEngineCache.PaimonPartition) {
            return ((PaimonEngineCache.PaimonPartition) partitionInfo).getPartitionInfo().getNameToPartitionItem();
        }
        if (partitionInfo instanceof HiveEngineCache.HivePartition) {
            HiveMetaStoreCache.HivePartitionValues hivePartitionValues =
                    ((HiveEngineCache.HivePartition) partitionInfo).getPartitionValues();
            Map<Long, PartitionItem> idToPartitionItem = hivePartitionValues.getIdToPartitionItem();
            BiMap<Long, String> idToName = hivePartitionValues.getPartitionNameToIdMap().inverse();
            Map<String, PartitionItem> nameToPartitionItems = Maps.newHashMapWithExpectedSize(idToPartitionItem.size());
            for (Map.Entry<Long, PartitionItem> entry : idToPartitionItem.entrySet()) {
                nameToPartitionItems.put(idToName.get(entry.getKey()), entry.getValue());
            }
            return nameToPartitionItems;
        }
        if (partitionInfo instanceof HudiEngineCache.HudiPartition) {
            TablePartitionValues hudiPartitionValues =
                    ((HudiEngineCache.HudiPartition) partitionInfo).getPartitionValues();
            Map<Long, PartitionItem> idToPartitionItem = hudiPartitionValues.getIdToPartitionItem();
            Map<Long, String> partitionIdToName = hudiPartitionValues.getPartitionIdToNameMap();
            Map<String, PartitionItem> nameToPartitionItems = Maps.newHashMapWithExpectedSize(idToPartitionItem.size());
            for (Map.Entry<Long, PartitionItem> entry : idToPartitionItem.entrySet()) {
                nameToPartitionItems.put(partitionIdToName.get(entry.getKey()), entry.getValue());
            }
            return nameToPartitionItems;
        }
        throw new IllegalArgumentException("Unsupported partition info type for table "
                + table.getNameWithFullQualifiers() + ": " + partitionInfo.getClass().getSimpleName());
    }

    private static PaimonPartitionInfo getPaimonPartitionInfo(ExternalTable table, Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        EnginePartitionInfo partitionInfo = getEngineMetaCache(table).getPartitionInfo(table, snapshot);
        if (partitionInfo instanceof PaimonEngineCache.PaimonPartition) {
            return ((PaimonEngineCache.PaimonPartition) partitionInfo).getPartitionInfo();
        }
        throw new AnalysisException("Expected paimon partition info for table "
                + table.getNameWithFullQualifiers() + ", but got " + partitionInfo.getClass().getSimpleName());
    }
}
