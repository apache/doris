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

package org.apache.doris.cloud.datasource;

import org.apache.doris.analysis.DataSortInfo;
import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.MetaIdGenerator.IdGeneratorBuffer;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.persist.UpdateCloudReplicaInfo;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.CopyJobPB;
import org.apache.doris.cloud.proto.Cloud.FinishCopyRequest.Action;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.ObjectFilePB;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.storage.ObjectFile;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.proto.OlapCommon;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;
import org.apache.doris.thrift.TSortType;
import org.apache.doris.thrift.TTabletType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import doris.segment_v2.SegmentV2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class CloudInternalCatalog extends InternalCatalog {
    private static final Logger LOG = LogManager.getLogger(CloudInternalCatalog.class);

    public CloudInternalCatalog() {
        super();
    }

    // BEGIN CREATE TABLE
    @Override
    protected Partition createPartitionWithIndices(long dbId, OlapTable tbl, long partitionId,
                                                   String partitionName, Map<Long, MaterializedIndexMeta> indexIdToMeta,
                                                   DistributionInfo distributionInfo, DataProperty dataProperty,
                                                   ReplicaAllocation replicaAlloc,
                                                   Long versionInfo, Set<String> bfColumns, Set<Long> tabletIdSet,
                                                   boolean isInMemory,
                                                   TTabletType tabletType,
                                                   String storagePolicy,
                                                   IdGeneratorBuffer idGeneratorBuffer,
                                                   BinlogConfig binlogConfig,
                                                   boolean isStorageMediumSpecified,
                                                   List<Integer> clusterKeyIndexes)
            throws DdlException {
        // create base index first.
        Preconditions.checkArgument(tbl.getBaseIndexId() != -1);
        MaterializedIndex baseIndex = new MaterializedIndex(tbl.getBaseIndexId(), IndexState.NORMAL);

        LOG.info("begin create cloud partition");
        // create partition with base index
        Partition partition = new CloudPartition(partitionId, partitionName, baseIndex,
                distributionInfo, dbId, tbl.getId());

        // add to index map
        Map<Long, MaterializedIndex> indexMap = Maps.newHashMap();
        indexMap.put(tbl.getBaseIndexId(), baseIndex);

        // create rollup index if has
        for (long indexId : indexIdToMeta.keySet()) {
            if (indexId == tbl.getBaseIndexId()) {
                continue;
            }

            MaterializedIndex rollup = new MaterializedIndex(indexId, IndexState.NORMAL);
            indexMap.put(indexId, rollup);
        }

        long version = partition.getVisibleVersion();

        final String storageVaultName = tbl.getStorageVaultName();
        boolean storageVaultIdSet = tbl.getStorageVaultId().isEmpty();

        // short totalReplicaNum = replicaAlloc.getTotalReplicaNum();
        for (Map.Entry<Long, MaterializedIndex> entry : indexMap.entrySet()) {
            long indexId = entry.getKey();
            MaterializedIndex index = entry.getValue();
            MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);

            // create tablets
            int schemaHash = indexMeta.getSchemaHash();
            TabletMeta tabletMeta = new TabletMeta(dbId, tbl.getId(), partitionId,
                    indexId, schemaHash, dataProperty.getStorageMedium());
            createCloudTablets(index, ReplicaState.NORMAL, distributionInfo, version, replicaAlloc,
                    tabletMeta, tabletIdSet);

            short shortKeyColumnCount = indexMeta.getShortKeyColumnCount();
            // TStorageType storageType = indexMeta.getStorageType();
            List<Column> columns = indexMeta.getSchema();
            KeysType keysType = indexMeta.getKeysType();

            List<Index> indexes;
            if (index.getId() == tbl.getBaseIndexId()) {
                indexes = tbl.getIndexes();
            } else {
                indexes = Lists.newArrayList();
            }
            Cloud.CreateTabletsRequest.Builder requestBuilder = Cloud.CreateTabletsRequest.newBuilder();
            List<String> rowStoreColumns =
                    tbl.getTableProperty().getCopiedRowStoreColumns();
            for (Tablet tablet : index.getTablets()) {
                OlapFile.TabletMetaCloudPB.Builder builder = createTabletMetaBuilder(tbl.getId(), indexId,
                        partitionId, tablet, tabletType, schemaHash, keysType, shortKeyColumnCount,
                        bfColumns, tbl.getBfFpp(), indexes, columns, tbl.getDataSortInfo(),
                        tbl.getCompressionType(), storagePolicy, isInMemory, false, tbl.getName(), tbl.getTTLSeconds(),
                        tbl.getEnableUniqueKeyMergeOnWrite(), tbl.storeRowColumn(), indexMeta.getSchemaVersion(),
                        tbl.getCompactionPolicy(), tbl.getTimeSeriesCompactionGoalSizeMbytes(),
                        tbl.getTimeSeriesCompactionFileCountThreshold(),
                        tbl.getTimeSeriesCompactionTimeThresholdSeconds(),
                        tbl.getTimeSeriesCompactionEmptyRowsetsThreshold(),
                        tbl.getTimeSeriesCompactionLevelThreshold(),
                        tbl.disableAutoCompaction(),
                        tbl.getRowStoreColumnsUniqueIds(rowStoreColumns),
                        tbl.getEnableMowLightDelete(),
                        tbl.getInvertedIndexFileStorageFormat(),
                        tbl.rowStorePageSize());
                requestBuilder.addTabletMetas(builder);
            }
            if (!storageVaultIdSet && ((CloudEnv) Env.getCurrentEnv()).getEnableStorageVault()) {
                requestBuilder.setStorageVaultName(storageVaultName);
            }

            LOG.info("create tablets, dbId: {}, tableId: {}, tableName: {}, partitionId: {}, partitionName: {}, "
                    + "indexId: {}, vault name {}",
                    dbId, tbl.getId(), tbl.getName(), partitionId, partitionName, indexId, storageVaultName);
            Cloud.CreateTabletsResponse resp = sendCreateTabletsRpc(requestBuilder);
            // If the resp has no vault id set, it means the MS is running with enable_storage_vault false
            if (resp.hasStorageVaultId() && !storageVaultIdSet) {
                tbl.setStorageVaultId(resp.getStorageVaultId());
                storageVaultIdSet = true;
                if (storageVaultName.isEmpty()) {
                    // If user doesn't specify the vault name for this table, we should set it
                    // to make the show create table stmt return correct stmt
                    // TODO(ByteYue): setDefaultStorageVault for vaultMgr might override user's
                    // defualt vault, maybe we should set it using show default storage vault stmt
                    tbl.setStorageVaultName(resp.getStorageVaultName());
                    Env.getCurrentEnv().getStorageVaultMgr().setDefaultStorageVault(
                            Pair.of(resp.getStorageVaultName(), resp.getStorageVaultId()));
                }
            }
            if (index.getId() != tbl.getBaseIndexId()) {
                // add rollup index to partition
                partition.createRollupIndex(index);
            }
        }

        LOG.info("succeed in creating partition[{}-{}], table : [{}-{}], vault {}", partitionId, partitionName,
                tbl.getId(), tbl.getName(), tbl.getStorageVaultName());

        return partition;
    }

    public OlapFile.TabletMetaCloudPB.Builder createTabletMetaBuilder(long tableId, long indexId,
            long partitionId, Tablet tablet, TTabletType tabletType, int schemaHash, KeysType keysType,
            short shortKeyColumnCount, Set<String> bfColumns, double bfFpp, List<Index> indexes,
            List<Column> schemaColumns, DataSortInfo dataSortInfo, TCompressionType compressionType,
            String storagePolicy, boolean isInMemory, boolean isShadow,
            String tableName, long ttlSeconds, boolean enableUniqueKeyMergeOnWrite,
            boolean storeRowColumn, int schemaVersion, String compactionPolicy,
            Long timeSeriesCompactionGoalSizeMbytes, Long timeSeriesCompactionFileCountThreshold,
            Long timeSeriesCompactionTimeThresholdSeconds, Long timeSeriesCompactionEmptyRowsetsThreshold,
            Long timeSeriesCompactionLevelThreshold, boolean disableAutoCompaction,
            List<Integer> rowStoreColumnUniqueIds, boolean enableMowLightDelete,
            TInvertedIndexFileStorageFormat invertedIndexFileStorageFormat, long pageSize) throws DdlException {
        OlapFile.TabletMetaCloudPB.Builder builder = OlapFile.TabletMetaCloudPB.newBuilder();
        builder.setTableId(tableId);
        builder.setIndexId(indexId);
        builder.setPartitionId(partitionId);
        builder.setTabletId(tablet.getId());
        builder.setSchemaHash(schemaHash);
        builder.setTableName(tableName);
        builder.setCreationTime(System.currentTimeMillis() / 1000);
        builder.setCumulativeLayerPoint(-1);
        builder.setTabletState(isShadow ? OlapFile.TabletStatePB.PB_NOTREADY : OlapFile.TabletStatePB.PB_RUNNING);
        builder.setIsInMemory(isInMemory);
        builder.setTtlSeconds(ttlSeconds);
        builder.setSchemaVersion(schemaVersion);

        UUID uuid = UUID.randomUUID();
        Types.PUniqueId tabletUid = Types.PUniqueId.newBuilder()
                .setHi(uuid.getMostSignificantBits())
                .setLo(uuid.getLeastSignificantBits())
                .build();
        builder.setTabletUid(tabletUid);

        builder.setPreferredRowsetType(OlapFile.RowsetTypePB.BETA_ROWSET);
        builder.setTabletType(tabletType == TTabletType.TABLET_TYPE_DISK
                ? OlapFile.TabletTypePB.TABLET_TYPE_DISK : OlapFile.TabletTypePB.TABLET_TYPE_MEMORY);

        builder.setReplicaId(tablet.getReplicas().get(0).getId());
        builder.setEnableUniqueKeyMergeOnWrite(enableUniqueKeyMergeOnWrite);

        builder.setCompactionPolicy(compactionPolicy);
        builder.setTimeSeriesCompactionGoalSizeMbytes(timeSeriesCompactionGoalSizeMbytes);
        builder.setTimeSeriesCompactionFileCountThreshold(timeSeriesCompactionFileCountThreshold);
        builder.setTimeSeriesCompactionTimeThresholdSeconds(timeSeriesCompactionTimeThresholdSeconds);
        builder.setTimeSeriesCompactionEmptyRowsetsThreshold(timeSeriesCompactionEmptyRowsetsThreshold);
        builder.setTimeSeriesCompactionLevelThreshold(timeSeriesCompactionLevelThreshold);

        OlapFile.TabletSchemaCloudPB.Builder schemaBuilder = OlapFile.TabletSchemaCloudPB.newBuilder();
        schemaBuilder.setSchemaVersion(schemaVersion);

        if (keysType == KeysType.DUP_KEYS) {
            schemaBuilder.setKeysType(OlapFile.KeysType.DUP_KEYS);
        } else if (keysType == KeysType.UNIQUE_KEYS) {
            schemaBuilder.setKeysType(OlapFile.KeysType.UNIQUE_KEYS);
        } else if (keysType == KeysType.AGG_KEYS) {
            schemaBuilder.setKeysType(OlapFile.KeysType.AGG_KEYS);
        } else {
            throw new DdlException("invalid key types");
        }
        schemaBuilder.setNumShortKeyColumns(shortKeyColumnCount);
        schemaBuilder.setNumRowsPerRowBlock(1024);
        schemaBuilder.setCompressKind(OlapCommon.CompressKind.COMPRESS_LZ4);
        schemaBuilder.setBfFpp(bfFpp);

        int deleteSign = -1;
        int sequenceCol = -1;
        for (int i = 0; i < schemaColumns.size(); i++) {
            Column column = schemaColumns.get(i);
            if (column.isDeleteSignColumn()) {
                deleteSign = i;
            }
            if (column.isSequenceColumn()) {
                sequenceCol = i;
            }
        }
        schemaBuilder.setDeleteSignIdx(deleteSign);
        schemaBuilder.setSequenceColIdx(sequenceCol);
        schemaBuilder.setStoreRowColumn(storeRowColumn);

        if (dataSortInfo.getSortType() == TSortType.LEXICAL) {
            schemaBuilder.setSortType(OlapFile.SortType.LEXICAL);
        } else if (dataSortInfo.getSortType() == TSortType.ZORDER) {
            schemaBuilder.setSortType(OlapFile.SortType.ZORDER);
        } else {
            LOG.warn("invalid sort types:{}", dataSortInfo.getSortType());
            throw new DdlException("invalid sort types");
        }

        switch (compressionType) {
            case NO_COMPRESSION:
                schemaBuilder.setCompressionType(SegmentV2.CompressionTypePB.NO_COMPRESSION);
                break;
            case SNAPPY:
                schemaBuilder.setCompressionType(SegmentV2.CompressionTypePB.SNAPPY);
                break;
            case LZ4:
                schemaBuilder.setCompressionType(SegmentV2.CompressionTypePB.LZ4);
                break;
            case LZ4F:
                schemaBuilder.setCompressionType(SegmentV2.CompressionTypePB.LZ4F);
                break;
            case ZLIB:
                schemaBuilder.setCompressionType(SegmentV2.CompressionTypePB.ZLIB);
                break;
            case ZSTD:
                schemaBuilder.setCompressionType(SegmentV2.CompressionTypePB.ZSTD);
                break;
            default:
                schemaBuilder.setCompressionType(SegmentV2.CompressionTypePB.LZ4F);
                break;
        }

        schemaBuilder.setSortColNum(dataSortInfo.getColNum());
        for (int i = 0; i < schemaColumns.size(); i++) {
            Column column = schemaColumns.get(i);
            schemaBuilder.addColumn(column.toPb(bfColumns, indexes));
        }

        if (indexes != null) {
            for (int i = 0; i < indexes.size(); i++) {
                Index index = indexes.get(i);
                schemaBuilder.addIndex(index.toPb(schemaColumns));
            }
        }
        if (rowStoreColumnUniqueIds != null) {
            schemaBuilder.addAllRowStoreColumnUniqueIds(rowStoreColumnUniqueIds);
        }
        schemaBuilder.setDisableAutoCompaction(disableAutoCompaction);
        schemaBuilder.setEnableMowLightDelete(enableMowLightDelete);

        if (invertedIndexFileStorageFormat != null) {
            if (invertedIndexFileStorageFormat == TInvertedIndexFileStorageFormat.V1) {
                schemaBuilder.setInvertedIndexStorageFormat(OlapFile.InvertedIndexStorageFormatPB.V1);
            } else {
                schemaBuilder.setInvertedIndexStorageFormat(OlapFile.InvertedIndexStorageFormatPB.V2);
            }
        }
        schemaBuilder.setRowStorePageSize(pageSize);

        OlapFile.TabletSchemaCloudPB schema = schemaBuilder.build();
        builder.setSchema(schema);
        // rowset
        OlapFile.RowsetMetaCloudPB.Builder rowsetBuilder = createInitialRowset(tablet, partitionId,
                schemaHash, schema);
        builder.addRsMetas(rowsetBuilder);
        return builder;
    }

    private OlapFile.RowsetMetaCloudPB.Builder createInitialRowset(Tablet tablet, long partitionId,
            int schemaHash, OlapFile.TabletSchemaCloudPB schema) {
        OlapFile.RowsetMetaCloudPB.Builder rowsetBuilder = OlapFile.RowsetMetaCloudPB.newBuilder();
        rowsetBuilder.setRowsetId(0);
        rowsetBuilder.setPartitionId(partitionId);
        rowsetBuilder.setTabletId(tablet.getId());
        rowsetBuilder.setTabletSchemaHash(schemaHash);
        rowsetBuilder.setRowsetType(OlapFile.RowsetTypePB.BETA_ROWSET);
        rowsetBuilder.setRowsetState(OlapFile.RowsetStatePB.VISIBLE);
        rowsetBuilder.setStartVersion(0);
        rowsetBuilder.setEndVersion(1);
        rowsetBuilder.setNumRows(0);
        rowsetBuilder.setTotalDiskSize(0);
        rowsetBuilder.setDataDiskSize(0);
        rowsetBuilder.setIndexDiskSize(0);
        rowsetBuilder.setSegmentsOverlapPb(OlapFile.SegmentsOverlapPB.NONOVERLAPPING);
        rowsetBuilder.setNumSegments(0);
        rowsetBuilder.setEmpty(true);

        UUID uuid = UUID.randomUUID();
        String rowsetIdV2Str = String.format("%016X", 2L << 56)
                + String.format("%016X", uuid.getMostSignificantBits())
                + String.format("%016X", uuid.getLeastSignificantBits());
        rowsetBuilder.setRowsetIdV2(rowsetIdV2Str);

        rowsetBuilder.setTabletSchema(schema);
        return rowsetBuilder;
    }

    private void createCloudTablets(MaterializedIndex index, ReplicaState replicaState,
            DistributionInfo distributionInfo, long version, ReplicaAllocation replicaAlloc,
            TabletMeta tabletMeta, Set<Long> tabletIdSet) throws DdlException {
        for (int i = 0; i < distributionInfo.getBucketNum(); ++i) {
            Tablet tablet = EnvFactory.getInstance().createTablet(Env.getCurrentEnv().getNextId());

            // add tablet to inverted index first
            index.addTablet(tablet, tabletMeta);
            tabletIdSet.add(tablet.getId());

            long replicaId = Env.getCurrentEnv().getNextId();
            Replica replica = new CloudReplica(replicaId, null, replicaState, version,
                    tabletMeta.getOldSchemaHash(), tabletMeta.getDbId(), tabletMeta.getTableId(),
                    tabletMeta.getPartitionId(), tabletMeta.getIndexId(), i);
            tablet.addReplica(replica);
        }
    }

    @Override
    public void beforeCreatePartitions(long dbId, long tableId, List<Long> partitionIds, List<Long> indexIds,
                                          boolean isCreateTable)
            throws DdlException {
        if (partitionIds == null) {
            prepareMaterializedIndex(tableId, indexIds, 0);
        } else {
            preparePartition(dbId, tableId, partitionIds, indexIds);
        }
    }

    @Override
    public void afterCreatePartitions(long dbId, long tableId, List<Long> partitionIds, List<Long> indexIds,
                                         boolean isCreateTable)
            throws DdlException {
        if (partitionIds == null) {
            commitMaterializedIndex(dbId, tableId, indexIds, isCreateTable);
        } else {
            commitPartition(dbId, tableId, partitionIds, indexIds);
        }
        if (!Config.check_create_table_recycle_key_remained) {
            return;
        }
        checkCreatePartitions(dbId, tableId, partitionIds, indexIds);
    }

    private void checkCreatePartitions(long dbId, long tableId, List<Long> partitionIds, List<Long> indexIds)
            throws DdlException {
        if (partitionIds == null) {
            checkMaterializedIndex(dbId, tableId, indexIds);
        } else {
            checkPartition(dbId, tableId, partitionIds);
        }
    }

    private void preparePartition(long dbId, long tableId, List<Long> partitionIds, List<Long> indexIds)
            throws DdlException {
        if (Config.enable_check_compatibility_mode) {
            LOG.info("skip prepare partition in checking compatibility mode");
            return;
        }

        Cloud.PartitionRequest.Builder partitionRequestBuilder = Cloud.PartitionRequest.newBuilder();
        partitionRequestBuilder.setCloudUniqueId(Config.cloud_unique_id);
        partitionRequestBuilder.setTableId(tableId);
        partitionRequestBuilder.addAllPartitionIds(partitionIds);
        partitionRequestBuilder.addAllIndexIds(indexIds);
        partitionRequestBuilder.setExpiration(0);
        if (dbId > 0) {
            partitionRequestBuilder.setDbId(dbId);
        }
        final Cloud.PartitionRequest partitionRequest = partitionRequestBuilder.build();

        Cloud.PartitionResponse response = null;
        int tryTimes = 0;
        while (tryTimes++ < Config.metaServiceRpcRetryTimes()) {
            try {
                response = MetaServiceProxy.getInstance().preparePartition(partitionRequest);
                if (response.getStatus().getCode() != Cloud.MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
            } catch (RpcException e) {
                LOG.warn("tryTimes:{}, preparePartition RpcException", tryTimes, e);
                if (tryTimes + 1 >= Config.metaServiceRpcRetryTimes()) {
                    throw new DdlException(e.getMessage());
                }
            }
            sleepSeveralMs();
        }

        if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            LOG.warn("preparePartition response: {} ", response);
            throw new DdlException(response.getStatus().getMsg());
        }
    }

    private void commitPartition(long dbId, long tableId, List<Long> partitionIds, List<Long> indexIds)
            throws DdlException {
        if (Config.enable_check_compatibility_mode) {
            LOG.info("skip committing partitions in check compatibility mode");
            return;
        }

        Cloud.PartitionRequest.Builder partitionRequestBuilder = Cloud.PartitionRequest.newBuilder();
        partitionRequestBuilder.setCloudUniqueId(Config.cloud_unique_id);
        partitionRequestBuilder.addAllPartitionIds(partitionIds);
        partitionRequestBuilder.addAllIndexIds(indexIds);
        partitionRequestBuilder.setDbId(dbId);
        partitionRequestBuilder.setTableId(tableId);
        final Cloud.PartitionRequest partitionRequest = partitionRequestBuilder.build();

        Cloud.PartitionResponse response = null;
        int tryTimes = 0;
        while (tryTimes++ < Config.metaServiceRpcRetryTimes()) {
            try {
                response = MetaServiceProxy.getInstance().commitPartition(partitionRequest);
                if (response.getStatus().getCode() != Cloud.MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
            } catch (RpcException e) {
                LOG.warn("tryTimes:{}, commitPartition RpcException", tryTimes, e);
                if (tryTimes + 1 >= Config.metaServiceRpcRetryTimes()) {
                    throw new DdlException(e.getMessage());
                }
            }
            sleepSeveralMs();
        }

        if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            LOG.warn("commitPartition response: {} ", response);
            throw new DdlException(response.getStatus().getMsg());
        }
    }

    // if `expiration` = 0, recycler will delete uncommitted indexes in `retention_seconds`
    public void prepareMaterializedIndex(Long tableId, List<Long> indexIds, long expiration) throws DdlException {
        if (Config.enable_check_compatibility_mode) {
            LOG.info("skip prepare materialized index in checking compatibility mode");
            return;
        }

        Cloud.IndexRequest.Builder indexRequestBuilder = Cloud.IndexRequest.newBuilder();
        indexRequestBuilder.setCloudUniqueId(Config.cloud_unique_id);
        indexRequestBuilder.addAllIndexIds(indexIds);
        indexRequestBuilder.setTableId(tableId);
        indexRequestBuilder.setExpiration(expiration);
        final Cloud.IndexRequest indexRequest = indexRequestBuilder.build();

        Cloud.IndexResponse response = null;
        int tryTimes = 0;
        while (tryTimes++ < Config.metaServiceRpcRetryTimes()) {
            try {
                response = MetaServiceProxy.getInstance().prepareIndex(indexRequest);
                if (response.getStatus().getCode() != Cloud.MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
            } catch (RpcException e) {
                LOG.warn("tryTimes:{}, prepareIndex RpcException", tryTimes, e);
                if (tryTimes + 1 >= Config.metaServiceRpcRetryTimes()) {
                    throw new DdlException(e.getMessage());
                }
            }
            sleepSeveralMs();
        }

        if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            LOG.warn("prepareIndex response: {} ", response);
            throw new DdlException(response.getStatus().getMsg());
        }
    }

    public void commitMaterializedIndex(long dbId, long tableId, List<Long> indexIds, boolean isCreateTable)
            throws DdlException {
        if (Config.enable_check_compatibility_mode) {
            LOG.info("skip committing materialized index in checking compatibility mode");
            return;
        }

        Cloud.IndexRequest.Builder indexRequestBuilder = Cloud.IndexRequest.newBuilder();
        indexRequestBuilder.setCloudUniqueId(Config.cloud_unique_id);
        indexRequestBuilder.addAllIndexIds(indexIds);
        indexRequestBuilder.setDbId(dbId);
        indexRequestBuilder.setTableId(tableId);
        indexRequestBuilder.setIsNewTable(isCreateTable);
        final Cloud.IndexRequest indexRequest = indexRequestBuilder.build();

        Cloud.IndexResponse response = null;
        int tryTimes = 0;
        while (tryTimes++ < Config.metaServiceRpcRetryTimes()) {
            try {
                response = MetaServiceProxy.getInstance().commitIndex(indexRequest);
                if (response.getStatus().getCode() != Cloud.MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
            } catch (RpcException e) {
                LOG.warn("tryTimes:{}, commitIndex RpcException", tryTimes, e);
                if (tryTimes + 1 >= Config.metaServiceRpcRetryTimes()) {
                    throw new DdlException(e.getMessage());
                }
            }
            sleepSeveralMs();
        }

        if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            LOG.warn("commitIndex response: {} ", response);
            throw new DdlException(response.getStatus().getMsg());
        }
    }

    private void checkPartition(long dbId, long tableId, List<Long> partitionIds)
            throws DdlException {
        if (Config.enable_check_compatibility_mode) {
            LOG.info("skip checking partitions in checking compatibility mode");
            return;
        }

        Cloud.CheckKeyInfos.Builder checkKeyInfosBuilder = Cloud.CheckKeyInfos.newBuilder();
        checkKeyInfosBuilder.addAllPartitionIds(partitionIds);
        // for ms log
        checkKeyInfosBuilder.addDbIds(dbId);
        checkKeyInfosBuilder.addTableIds(tableId);

        Cloud.CheckKVRequest.Builder checkKvRequestBuilder = Cloud.CheckKVRequest.newBuilder();
        checkKvRequestBuilder.setCloudUniqueId(Config.cloud_unique_id);
        checkKvRequestBuilder.setCheckKeys(checkKeyInfosBuilder.build());
        checkKvRequestBuilder.setOp(Cloud.CheckKVRequest.Operation.CREATE_PARTITION_AFTER_FE_COMMIT);
        final Cloud.CheckKVRequest checkKVRequest = checkKvRequestBuilder.build();

        Cloud.CheckKVResponse response = null;
        int tryTimes = 0;
        while (tryTimes++ < Config.metaServiceRpcRetryTimes()) {
            try {
                response = MetaServiceProxy.getInstance().checkKv(checkKVRequest);
                break;
            } catch (RpcException e) {
                LOG.warn("tryTimes:{}, checkPartition RpcException", tryTimes, e);
                if (tryTimes + 1 >= Config.metaServiceRpcRetryTimes()) {
                    throw new DdlException(e.getMessage());
                }
            }
            sleepSeveralMs();
        }

        if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            LOG.warn("checkPartition response: {} ", response);
            throw new DdlException(response.getStatus().getMsg());
        }
    }

    public void checkMaterializedIndex(long dbId, long tableId, List<Long> indexIds)
            throws DdlException {
        if (Config.enable_check_compatibility_mode) {
            LOG.info("skip checking materialized index in checking compatibility mode");
            return;
        }

        Cloud.CheckKeyInfos.Builder checkKeyInfosBuilder = Cloud.CheckKeyInfos.newBuilder();
        checkKeyInfosBuilder.addAllIndexIds(indexIds);
        // for ms log
        checkKeyInfosBuilder.addDbIds(dbId);
        checkKeyInfosBuilder.addTableIds(tableId);

        Cloud.CheckKVRequest.Builder checkKvRequestBuilder = Cloud.CheckKVRequest.newBuilder();
        checkKvRequestBuilder.setCloudUniqueId(Config.cloud_unique_id);
        checkKvRequestBuilder.setCheckKeys(checkKeyInfosBuilder.build());
        checkKvRequestBuilder.setOp(Cloud.CheckKVRequest.Operation.CREATE_INDEX_AFTER_FE_COMMIT);
        final Cloud.CheckKVRequest checkKVRequest = checkKvRequestBuilder.build();

        Cloud.CheckKVResponse response = null;
        int tryTimes = 0;
        while (tryTimes++ < Config.metaServiceRpcRetryTimes()) {
            try {
                response = MetaServiceProxy.getInstance().checkKv(checkKVRequest);
                break;
            } catch (RpcException e) {
                LOG.warn("tryTimes:{}, checkIndex RpcException", tryTimes, e);
                if (tryTimes + 1 >= Config.metaServiceRpcRetryTimes()) {
                    throw new DdlException(e.getMessage());
                }
            }
            sleepSeveralMs();
        }

        if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            LOG.warn("checkIndex response: {} ", response);
            throw new DdlException(response.getStatus().getMsg());
        }
    }

    public Cloud.CreateTabletsResponse
            sendCreateTabletsRpc(Cloud.CreateTabletsRequest.Builder requestBuilder) throws DdlException  {
        requestBuilder.setCloudUniqueId(Config.cloud_unique_id);
        Cloud.CreateTabletsRequest createTabletsReq = requestBuilder.build();

        if (LOG.isDebugEnabled()) {
            LOG.debug("send create tablets rpc, createTabletsReq: {}", createTabletsReq);
        }
        Cloud.CreateTabletsResponse response = null;
        int tryTimes = 0;
        while (tryTimes++ < Config.metaServiceRpcRetryTimes()) {
            try {
                response = MetaServiceProxy.getInstance().createTablets(createTabletsReq);
                if (response.getStatus().getCode() != Cloud.MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
            } catch (RpcException e) {
                LOG.warn("tryTimes:{}, create tablets RpcException", tryTimes, e);
                if (tryTimes + 1 >= Config.metaServiceRpcRetryTimes()) {
                    throw new DdlException(e.getMessage());
                }
            }
            sleepSeveralMs();
        }
        LOG.info("create tablets response: {}", response);

        if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new DdlException(response.getStatus().getMsg());
        }
        return response;
    }

    // END CREATE TABLE

    // BEGIN DROP TABLE

    @Override
    public void eraseTableDropBackendReplicas(OlapTable olapTable, boolean isReplay) {
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }

        List<Long> indexs = Lists.newArrayList();
        for (Partition partition : olapTable.getAllPartitions()) {
            List<MaterializedIndex> allIndices = partition.getMaterializedIndices(IndexExtState.ALL);
            for (MaterializedIndex materializedIndex : allIndices) {
                long indexId = materializedIndex.getId();
                indexs.add(indexId);
            }
        }

        int tryCnt = 0;
        while (true) {
            if (tryCnt++ > Config.drop_rpc_retry_num) {
                LOG.warn("failed to drop index {} of table {}, try cnt {} reaches maximum retry count",
                            indexs, olapTable.getId(), tryCnt);
                break;
            }

            try {
                if (indexs.isEmpty()) {
                    break;
                }
                dropMaterializedIndex(olapTable.getId(), indexs, true);
            } catch (Exception e) {
                LOG.warn("failed to drop index {} of table {}, try cnt {}, execption {}",
                        indexs, olapTable.getId(), tryCnt, e);
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ie) {
                    LOG.warn("Thread sleep is interrupted");
                }
                continue;
            }
            break;
        }
    }

    @Override
    public void erasePartitionDropBackendReplicas(List<Partition> partitions) {
        if (!Env.getCurrentEnv().isMaster() || partitions.isEmpty()) {
            return;
        }

        long tableId = -1;
        List<Long> partitionIds = Lists.newArrayList();
        Set<Long> indexIds = new HashSet<>();
        boolean needUpdateTableVersion = false;
        for (Partition partition : partitions) {
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                indexIds.add(index.getId());
                if (tableId == -1) {
                    tableId = ((CloudReplica) index.getTablets().get(0).getReplicas().get(0)).getTableId();
                }
            }
            partitionIds.add(partition.getId());
            if (partition.hasData()) {
                // Update table version only when deleting non-empty partitions
                needUpdateTableVersion = true;
            }
        }

        CloudPartition partition0 = (CloudPartition) partitions.get(0);

        int tryCnt = 0;
        while (true) {
            if (tryCnt++ > Config.drop_rpc_retry_num) {
                LOG.warn("failed to drop partition {} of table {}, try cnt {} reaches maximum retry count",
                        partitionIds, tableId, tryCnt);
                break;
            }
            try {
                dropCloudPartition(partition0.getDbId(), tableId, partitionIds,
                        indexIds.stream().collect(Collectors.toList()), needUpdateTableVersion);
            } catch (Exception e) {
                LOG.warn("failed to drop partition {} of table {}, try cnt {}, execption {}",
                        partitionIds, tableId, tryCnt, e);
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ie) {
                    LOG.warn("Thread sleep is interrupted");
                }
                continue;
            }
            break;
        }
    }

    private void dropCloudPartition(long dbId, long tableId, List<Long> partitionIds, List<Long> indexIds,
                                    boolean needUpdateTableVersion) throws DdlException {
        if (Config.enable_check_compatibility_mode) {
            LOG.info("skip dropping cloud partitions in checking compatibility mode");
            return;
        }

        Cloud.PartitionRequest.Builder partitionRequestBuilder =
                Cloud.PartitionRequest.newBuilder();
        partitionRequestBuilder.setCloudUniqueId(Config.cloud_unique_id);
        partitionRequestBuilder.setTableId(tableId);
        partitionRequestBuilder.addAllPartitionIds(partitionIds);
        partitionRequestBuilder.addAllIndexIds(indexIds);
        partitionRequestBuilder.setNeedUpdateTableVersion(needUpdateTableVersion);
        if (dbId > 0) {
            partitionRequestBuilder.setDbId(dbId);
        }
        final Cloud.PartitionRequest partitionRequest = partitionRequestBuilder.build();

        Cloud.PartitionResponse response = null;
        int tryTimes = 0;
        while (tryTimes++ < Config.metaServiceRpcRetryTimes()) {
            try {
                response = MetaServiceProxy.getInstance().dropPartition(partitionRequest);
                if (response.getStatus().getCode() != Cloud.MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
            } catch (RpcException e) {
                LOG.warn("tryTimes:{}, dropPartition RpcException", tryTimes, e);
                if (tryTimes + 1 >= Config.metaServiceRpcRetryTimes()) {
                    throw new DdlException(e.getMessage());
                }
            }
            sleepSeveralMs();
        }

        if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            LOG.warn("dropPartition response: {} ", response);
            throw new DdlException(response.getStatus().getMsg());
        }
    }

    public void dropMaterializedIndex(long tableId, List<Long> indexIds, boolean dropTable) throws DdlException {
        if (Config.enable_check_compatibility_mode) {
            LOG.info("skip dropping materialized index in compatibility checking mode");
            return;
        }

        Cloud.IndexRequest.Builder indexRequestBuilder = Cloud.IndexRequest.newBuilder();
        indexRequestBuilder.setCloudUniqueId(Config.cloud_unique_id);
        indexRequestBuilder.addAllIndexIds(indexIds);
        indexRequestBuilder.setTableId(tableId);
        final Cloud.IndexRequest indexRequest = indexRequestBuilder.build();

        Cloud.IndexResponse response = null;
        int tryTimes = 0;
        while (tryTimes++ < Config.metaServiceRpcRetryTimes()) {
            try {
                response = MetaServiceProxy.getInstance().dropIndex(indexRequest);
                if (response.getStatus().getCode() != Cloud.MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
            } catch (RpcException e) {
                LOG.warn("tryTimes:{}, dropIndex RpcException", tryTimes, e);
                if (tryTimes + 1 >= Config.metaServiceRpcRetryTimes()) {
                    throw new DdlException(e.getMessage());
                }
            }
            sleepSeveralMs();
        }

        if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            LOG.warn("dropIndex response: {} ", response);
            throw new DdlException(response.getStatus().getMsg());
        }
    }

    /**
     * for cloud mode, drop rollup/materializedIndex in kv meta store
     * @param tableId
     * @param indexIdList
     */
    public void eraseDroppedIndex(long tableId, List<Long> indexIdList) {
        if (indexIdList == null || indexIdList.size() == 0) {
            LOG.warn("indexIdList is empty");
            return;
        }
        long tryCnt = 0;
        while (true) {
            if (tryCnt++ > Config.drop_rpc_retry_num) {
                LOG.warn("failed to drop index {} of table {}, try cnt {} reaches maximum retry count",
                            indexIdList, tableId, tryCnt);
                break;
            }

            try {
                dropMaterializedIndex(tableId, indexIdList, false);
                break;
            } catch (Exception e) {
                LOG.warn("tryCnt:{}, eraseDroppedIndex exception:", tryCnt, e);
            }
            sleepSeveralMs();
        }

        LOG.info("eraseDroppedIndex finished, tableId:{}, indexIdList:{}",
                tableId, indexIdList);
    }

    // END DROP TABLE

    @Override
    public void checkAvailableCapacity(Database db) throws DdlException {
    }

    private void sleepSeveralMs() {
        // sleep random millis [20, 200] ms, avoid txn conflict
        int randomMillis = 20 + (int) (Math.random() * (200 - 20));
        if (LOG.isDebugEnabled()) {
            LOG.debug("randomMillis:{}", randomMillis);
        }
        try {
            Thread.sleep(randomMillis);
        } catch (InterruptedException e) {
            LOG.info("ignore InterruptedException: ", e);
        }
    }

    public void replayUpdateCloudReplica(UpdateCloudReplicaInfo info) throws MetaNotFoundException {
        Database db = getDbNullable(info.getDbId());
        if (db == null) {
            LOG.warn("replay update cloud replica, unknown database {}", info.toString());
            return;
        }
        OlapTable olapTable = (OlapTable) db.getTableNullable(info.getTableId());
        if (olapTable == null) {
            LOG.warn("replay update cloud replica, unknown table {}", info.toString());
            return;
        }

        olapTable.writeLock();
        try {
            unprotectUpdateCloudReplica(olapTable, info);
        } catch (Exception e) {
            LOG.warn("unexpected exception", e);
        } finally {
            olapTable.writeUnlock();
        }
    }

    private void unprotectUpdateCloudReplica(OlapTable olapTable, UpdateCloudReplicaInfo info) {
        LOG.debug("replay update a cloud replica {}", info);
        Partition partition = olapTable.getPartition(info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());

        try {
            if (info.getTabletId() != -1) {
                Tablet tablet = materializedIndex.getTablet(info.getTabletId());
                Replica replica = tablet.getReplicaById(info.getReplicaId());
                Preconditions.checkNotNull(replica, info);

                String clusterId = info.getClusterId();
                String realClusterId = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                        .getCloudClusterIdByName(clusterId);
                LOG.debug("cluster Id {}, real cluster Id {}", clusterId, realClusterId);
                if (!Strings.isNullOrEmpty(realClusterId)) {
                    clusterId = realClusterId;
                }

                ((CloudReplica) replica).updateClusterToBe(clusterId, info.getBeId());

                LOG.debug("update single cloud replica cluster {} replica {} be {}", info.getClusterId(),
                        replica.getId(), info.getBeId());
            } else {
                List<Long> tabletIds = info.getTabletIds();
                for (int i = 0; i < tabletIds.size(); ++i) {
                    Tablet tablet = materializedIndex.getTablet(tabletIds.get(i));
                    Replica replica;
                    if (info.getReplicaIds().isEmpty()) {
                        replica = tablet.getReplicas().get(0);
                    } else {
                        replica = tablet.getReplicaById(info.getReplicaIds().get(i));
                    }
                    Preconditions.checkNotNull(replica, info);

                    String clusterId = info.getClusterId();
                    String realClusterId = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                            .getCloudClusterIdByName(clusterId);
                    LOG.debug("cluster Id {}, real cluster Id {}", clusterId, realClusterId);
                    if (!Strings.isNullOrEmpty(realClusterId)) {
                        clusterId = realClusterId;
                    }

                    LOG.debug("update cloud replica cluster {} replica {} be {}", info.getClusterId(),
                            replica.getId(), info.getBeIds().get(i));
                    ((CloudReplica) replica).updateClusterToBe(clusterId, info.getBeIds().get(i));
                }
            }
        } catch (Exception e) {
            LOG.warn("unexpected exception", e);
        }
    }

    public void createStage(Cloud.StagePB stagePB, boolean ifNotExists) throws DdlException {
        if (Config.enable_check_compatibility_mode) {
            LOG.info("skip creating stage in checking compatibility mode");
            return;
        }

        Cloud.CreateStageRequest createStageRequest = Cloud.CreateStageRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id).setStage(stagePB).build();
        Cloud.CreateStageResponse response = null;
        int retryTime = 0;
        while (retryTime++ < 3) {
            try {
                response = MetaServiceProxy.getInstance().createStage(createStageRequest);
                LOG.debug("create stage, stage: {}, {}, response: {}", stagePB, ifNotExists, response);
                if (ifNotExists && response.getStatus().getCode() == MetaServiceCode.ALREADY_EXISTED) {
                    LOG.info("stage already exists, stage_name: {}", stagePB.getName());
                    return;
                }
                if (response.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT
                        && response.getStatus().getCode() != MetaServiceCode.KV_TXN_COMMIT_ERR) {
                    break;
                }
            } catch (RpcException e) {
                LOG.warn("createStage response: {} ", response);
            }
            // sleep random millis [20, 200] ms, avoid txn conflict
            int randomMillis = 20 + (int) (Math.random() * (200 - 20));
            LOG.debug("randomMillis:{}", randomMillis);
            try {
                Thread.sleep(randomMillis);
            } catch (InterruptedException e) {
                LOG.info("InterruptedException: ", e);
            }
        }

        if (response == null) {
            LOG.warn("createStage failed.");
            throw new DdlException("createStage failed");
        }

        if (response.getStatus().getCode() != MetaServiceCode.OK) {
            LOG.warn("createStage response: {} ", response);
            throw new DdlException(response.getStatus().getMsg());
        }
    }

    public List<Cloud.StagePB> getStage(Cloud.StagePB.StageType stageType, String userName,
                                  String stageName, String userId) throws DdlException {
        Cloud.GetStageResponse response = getStageRpc(stageType, userName, stageName, userId);
        if (response.getStatus().getCode() == MetaServiceCode.OK) {
            return response.getStageList();
        }

        if (stageType == Cloud.StagePB.StageType.EXTERNAL) {
            if (response.getStatus().getCode() == MetaServiceCode.STAGE_NOT_FOUND) {
                LOG.info("Stage does not exist: {}", stageName);
                throw new DdlException("Stage does not exist: " + stageName);
            }
            LOG.warn("internal error, try later");
            throw new DdlException("internal error, try later");
        }

        if (response.getStatus().getCode() == MetaServiceCode.STATE_ALREADY_EXISTED_FOR_USER
                || response.getStatus().getCode() == MetaServiceCode.STAGE_NOT_FOUND) {
            Cloud.StagePB.Builder createStageBuilder = Cloud.StagePB.newBuilder();
            createStageBuilder.addMysqlUserName(ClusterNamespace
                    .getNameFromFullName(ConnectContext.get().getCurrentUserIdentity().getQualifiedUser()))
                .setStageId(UUID.randomUUID().toString())
                .setType(Cloud.StagePB.StageType.INTERNAL).addMysqlUserId(userId);

            boolean isAba = false;
            if (response.getStatus().getCode() == MetaServiceCode.STATE_ALREADY_EXISTED_FOR_USER) {
                List<Cloud.StagePB> stages = response.getStageList();
                if (stages.isEmpty() || stages.get(0).getMysqlUserIdCount() == 0) {
                    LOG.warn("impossible here, internal stage this err code must have one stage.");
                    throw new DdlException("internal error, try later");
                }
                String toDropMysqlUserId = stages.get(0).getMysqlUserId(0);
                // ABA user
                // 1. drop user
                isAba = true;
                String reason = String.format("get stage deal with err user [%s:%s] %s, step %s msg %s now userid [%s]",
                        userName, userId, "aba user", "1", "drop old stage", toDropMysqlUserId);
                LOG.info(reason);
                dropStage(Cloud.StagePB.StageType.INTERNAL, userName, toDropMysqlUserId, null, reason, true);
            }
            // stage not found just create and get
            // 2. create a new internal stage
            LOG.info("get stage deal with err user [{}:{}] {}, step {} msg {}", userName, userId,
                    isAba ? "aba user" : "not found", isAba ? "2" : "1",  "create a new internal stage");
            createStage(createStageBuilder.build(), true);
            // 3. get again
            // sleep random millis [20, 200] ms, avoid multiple call get stage.
            int randomMillis = 20 + (int) (Math.random() * (200 - 20));
            LOG.debug("randomMillis:{}", randomMillis);
            try {
                Thread.sleep(randomMillis);
            } catch (InterruptedException e) {
                LOG.info("InterruptedException: ", e);
            }
            LOG.info("get stage deal with err user [{}:{}] {}, step {} msg {}", userName, userId,
                    isAba ? "aba user" : "not found", isAba ? "3" : "2",  "get stage");
            response = getStageRpc(stageType, userName, stageName, userId);
            if (response.getStatus().getCode() == MetaServiceCode.OK) {
                return response.getStageList();
            }
        }
        return null;
    }

    private Cloud.GetStageResponse getStageRpc(Cloud.StagePB.StageType stageType, String userName,
                                                      String stageName, String userId) throws DdlException {
        Cloud.GetStageRequest.Builder builder = Cloud.GetStageRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id).setType(stageType);
        if (userName != null) {
            builder.setMysqlUserName(userName);
        }
        if (stageName != null) {
            builder.setStageName(stageName);
        }
        if (userId != null) {
            builder.setMysqlUserId(userId);
        }
        Cloud.GetStageResponse response = null;
        try {
            response = MetaServiceProxy.getInstance().getStage(builder.build());
            LOG.debug("get stage, stageType={}, userName={}, userId= {}, stageName:{}, response: {}",
                    stageType, userName, userId, stageName, response);
        } catch (RpcException e) {
            LOG.warn("getStage rpc exception: {} ", e.getMessage(), e);
            throw new DdlException("internal error, try later");
        }

        return response;
    }

    public void dropStage(Cloud.StagePB.StageType stageType, String userName, String userId,
                          String stageName, String reason, boolean ifExists)
            throws DdlException {
        if (Config.enable_check_compatibility_mode) {
            LOG.info("skip dropping stage in checking compatibility mode");
            return;
        }

        Cloud.DropStageRequest.Builder builder = Cloud.DropStageRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id).setType(stageType);
        if (userName != null) {
            builder.setMysqlUserName(userName);
        }
        if (userId != null) {
            builder.setMysqlUserId(userId);
        }
        if (stageName != null) {
            builder.setStageName(stageName);
        }
        if (reason != null) {
            builder.setReason(reason);
        }
        Cloud.DropStageResponse response = null;
        int retryTime = 0;
        while (retryTime++ < 3) {
            try {
                response = MetaServiceProxy.getInstance().dropStage(builder.build());
                LOG.info("drop stage, stageType:{}, userName:{}, userId:{}, stageName:{}, reason:{}, "
                        + "retry:{}, response: {}", stageType, userName, userId, stageName, reason, retryTime,
                        response);
                // just retry kv conflict
                if (response.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
            } catch (RpcException e) {
                LOG.warn("dropStage response: {} ", response);
            }
            // sleep random millis [20, 200] ms, avoid txn conflict
            int randomMillis = 20 + (int) (Math.random() * (200 - 20));
            LOG.debug("randomMillis:{}", randomMillis);
            try {
                Thread.sleep(randomMillis);
            } catch (InterruptedException e) {
                LOG.info("InterruptedException: ", e);
            }
        }

        if (response == null || !response.hasStatus()) {
            throw new DdlException("metaService exception");
        }

        if (response.getStatus().getCode() != MetaServiceCode.OK) {
            LOG.warn("dropStage response: {} ", response);
            if (response.getStatus().getCode() == MetaServiceCode.STAGE_NOT_FOUND) {
                if (ifExists) {
                    return;
                } else {
                    throw new DdlException("Stage does not exists: " + stageName);
                }
            }
            throw new DdlException("internal error, try later");
        }
    }

    public List<ObjectFilePB> beginCopy(String stageId, Cloud.StagePB.StageType stageType, long tableId,
                                        String copyJobId, int groupId, long startTime, long timeoutTime,
                                        List<ObjectFilePB> objectFiles,
                                        long sizeLimit, int fileNumLimit, int fileMetaSizeLimit) throws DdlException {
        Cloud.BeginCopyRequest request = Cloud.BeginCopyRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id).setStageId(stageId).setStageType(stageType)
                .setTableId(tableId).setCopyId(copyJobId).setGroupId(groupId).setStartTimeMs(startTime)
                .setTimeoutTimeMs(timeoutTime).addAllObjectFiles(objectFiles).setFileNumLimit(fileNumLimit)
                .setFileSizeLimit(sizeLimit).setFileMetaSizeLimit(fileMetaSizeLimit).build();
        Cloud.BeginCopyResponse response = null;
        try {
            int retry = 0;
            while (true) {
                response = MetaServiceProxy.getInstance().beginCopy(request);
                if (response.getStatus().getCode() == Cloud.MetaServiceCode.OK) {
                    return response.getFilteredObjectFilesList();
                }
                if (retry < Config.cloud_copy_txn_conflict_error_retry_num
                        && response.getStatus().getCode() == Cloud.MetaServiceCode.KV_TXN_CONFLICT) {
                    LOG.warn("begin copy error with kv txn conflict, tableId={}, stageId={}, queryId={}, retry={}",
                            tableId, stageId, copyJobId, retry);
                    retry++;
                    continue;
                }
                LOG.warn("beginCopy response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            LOG.warn("beginCopy response: {} ", response);
            throw new DdlException(e.getMessage());
        }
    }

    public Cloud.GetIamResponse getIam() throws DdlException {
        Cloud.GetIamRequest.Builder builder = Cloud.GetIamRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id);
        Cloud.GetIamResponse response = null;
        try {
            response = MetaServiceProxy.getInstance().getIam(builder.build());
        } catch (RpcException e) {
            LOG.warn("getStage rpc exception: {} ", e.getMessage(), e);
            throw new DdlException("internal error, try later");
        }
        return response;
    }

    public void finishCopy(String stageId, Cloud.StagePB.StageType stageType, long tableId, String copyJobId,
                           int groupId, Action action) throws DdlException {
        Cloud.FinishCopyRequest request = Cloud.FinishCopyRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id).setStageId(stageId).setStageType(stageType)
                .setTableId(tableId).setCopyId(copyJobId).setGroupId(groupId)
                .setAction(action).setFinishTimeMs(System.currentTimeMillis()).build();
        Cloud.FinishCopyResponse response = null;
        try {
            int retry = 0;
            while (true) {
                response = MetaServiceProxy.getInstance().finishCopy(request);
                if (response.getStatus().getCode() == Cloud.MetaServiceCode.OK) {
                    return;
                }
                if (response.getStatus().getCode() == MetaServiceCode.COPY_JOB_NOT_FOUND) {
                    if (action == Action.COMMIT) {
                        LOG.warn("finish copy error with copy job not found, tableId={}, stageId={}, queryId={}",
                                tableId, stageId, copyJobId);
                        throw new DdlException(response.getStatus().getMsg());
                    } else {
                        return;
                    }
                }
                if (retry < Config.cloud_copy_txn_conflict_error_retry_num
                        && response.getStatus().getCode() == Cloud.MetaServiceCode.KV_TXN_CONFLICT) {
                    LOG.warn("finish copy error with kv txn conflict, tableId={}, stageId={}, queryId={}, retry={}",
                            tableId, stageId, copyJobId, retry);
                    retry++;
                    continue;
                }
                LOG.warn("finishCopy response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            LOG.warn("finishCopy response: {} ", response);
            throw new DdlException(e.getMessage());
        }
    }

    public CopyJobPB getCopyJob(String stageId, long tableId, String copyJobId, int groupId) throws DdlException {
        Cloud.GetCopyJobRequest request = Cloud.GetCopyJobRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id).setStageId(stageId).setTableId(tableId).setCopyId(copyJobId)
                .setGroupId(groupId).build();
        Cloud.GetCopyJobResponse response = null;
        try {
            response = MetaServiceProxy.getInstance().getCopyJob(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("getCopyJob response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
            return response.hasCopyJob() ? response.getCopyJob() : null;
        } catch (RpcException e) {
            LOG.warn("getCopyJob response: {} ", response);
            throw new DdlException(e.getMessage());
        }
    }

    public List<ObjectFilePB> getCopyFiles(String stageId, long tableId) throws DdlException {
        Cloud.GetCopyFilesRequest.Builder builder = Cloud.GetCopyFilesRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id).setStageId(stageId).setTableId(tableId);
        Cloud.GetCopyFilesResponse response = null;
        try {
            response = MetaServiceProxy.getInstance().getCopyFiles(builder.build());
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("getCopyFiles response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
            return response.getObjectFilesList();
        } catch (RpcException e) {
            LOG.warn("getCopyFiles response: {} ", response);
            throw new DdlException(e.getMessage());
        }
    }

    public List<ObjectFilePB> filterCopyFiles(String stageId, long tableId, List<ObjectFile> objectFiles)
            throws DdlException {
        Cloud.FilterCopyFilesRequest.Builder builder = Cloud.FilterCopyFilesRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id).setStageId(stageId).setTableId(tableId);
        for (ObjectFile objectFile : objectFiles) {
            builder.addObjectFiles(
                    ObjectFilePB.newBuilder().setRelativePath(objectFile.getRelativePath())
                            .setEtag(objectFile.getEtag()).setSize(objectFile.getSize()).build());
        }
        Cloud.FilterCopyFilesResponse response = null;
        try {
            response = MetaServiceProxy.getInstance().filterCopyFiles(builder.build());
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("filterCopyFiles response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
            return response.getObjectFilesList();
        } catch (RpcException e) {
            LOG.warn("filterCopyFiles response: {} ", response);
            throw new DdlException(e.getMessage());
        }
    }
}
