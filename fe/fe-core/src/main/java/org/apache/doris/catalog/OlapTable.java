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

package org.apache.doris.catalog;

import org.apache.doris.alter.MaterializedViewHandler;
import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DataSortInfo;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.backup.Status;
import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.clone.TabletSchedCtx;
import org.apache.doris.clone.TabletScheduler;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.DeepCopy;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.Util;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.resource.Tag;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.HistogramTask;
import org.apache.doris.statistics.MVAnalysisTask;
import org.apache.doris.statistics.OlapAnalysisTask;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TFetchOption;
import org.apache.doris.thrift.TOlapTable;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TSortType;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Internal representation of tableFamilyGroup-related metadata. A OlaptableFamilyGroup contains several tableFamily.
 * Note: when you add a new olap table property, you should modify TableProperty class
 */
public class OlapTable extends Table {
    private static final Logger LOG = LogManager.getLogger(OlapTable.class);

    public enum OlapTableState {
        NORMAL,
        ROLLUP,
        SCHEMA_CHANGE,
        @Deprecated
        BACKUP,
        RESTORE,
        RESTORE_WITH_LOAD,
        /*
         * this state means table is under PENDING alter operation(SCHEMA_CHANGE or ROLLUP), and is not
         * stable. The tablet scheduler will continue fixing the tablets of this table. And the state will
         * change back to SCHEMA_CHANGE or ROLLUP after table is stable, and continue doing alter operation.
         * This state is a in-memory state and no need to persist.
         */
        WAITING_STABLE
    }

    private volatile OlapTableState state;

    // index id -> index meta
    private Map<Long, MaterializedIndexMeta> indexIdToMeta = Maps.newHashMap();
    // index name -> index id
    private Map<String, Long> indexNameToId = Maps.newHashMap();

    private KeysType keysType;
    private PartitionInfo partitionInfo;
    private Map<Long, Partition> idToPartition = new HashMap<>();
    private Map<String, Partition> nameToPartition = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    private DistributionInfo defaultDistributionInfo;

    // all info about temporary partitions are save in "tempPartitions"
    private TempPartitions tempPartitions = new TempPartitions();

    // bloom filter columns
    private Set<String> bfColumns;
    private double bfFpp;

    private String colocateGroup;

    private boolean hasSequenceCol;
    private Type sequenceType;

    private TableIndexes indexes;

    // In former implementation, base index id is same as table id.
    // But when refactoring the process of alter table job, we find that
    // using same id is not suitable for our new framework.
    // So we add this 'baseIndexId' to explicitly specify the base index id,
    // which should be different with table id.
    // The init value is -1, which means there is not partition and index at all.
    private long baseIndexId = -1;

    private TableProperty tableProperty;

    public OlapTable() {
        // for persist
        super(TableType.OLAP);

        this.bfColumns = null;
        this.bfFpp = 0;

        this.colocateGroup = null;

        this.indexes = null;

        this.tableProperty = null;

        this.hasSequenceCol = false;
    }

    public OlapTable(long id, String tableName, List<Column> baseSchema, KeysType keysType,
            PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo) {
        this(id, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo, null);
    }

    public OlapTable(long id, String tableName, List<Column> baseSchema, KeysType keysType,
            PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo, TableIndexes indexes) {
        super(id, tableName, TableType.OLAP, baseSchema);

        this.state = OlapTableState.NORMAL;

        this.keysType = keysType;
        this.partitionInfo = partitionInfo;

        this.defaultDistributionInfo = defaultDistributionInfo;

        this.bfColumns = null;
        this.bfFpp = 0;

        this.colocateGroup = null;

        this.indexes = indexes;

        this.tableProperty = null;
    }

    private TableProperty getOrCreatTableProperty() {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        return tableProperty;
    }

    public BinlogConfig getBinlogConfig() {
        return getOrCreatTableProperty().getBinlogConfig();
    }

    public void setBinlogConfig(BinlogConfig binlogConfig) {
        getOrCreatTableProperty().setBinlogConfig(binlogConfig);
    }

    public void setIsBeingSynced(boolean isBeingSynced) {
        getOrCreatTableProperty().modifyTableProperties(PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED,
                String.valueOf(isBeingSynced));
    }

    public boolean isBeingSynced() {
        return getOrCreatTableProperty().isBeingSynced();
    }

    public void setTableProperty(TableProperty tableProperty) {
        this.tableProperty = tableProperty;
    }

    public TableProperty getTableProperty() {
        return this.tableProperty;
    }

    public boolean dynamicPartitionExists() {
        return tableProperty != null
                && tableProperty.getDynamicPartitionProperty() != null
                && tableProperty.getDynamicPartitionProperty().isExist();
    }

    public boolean isZOrderSort() {
        return tableProperty != null
                && tableProperty.getDataSortInfo() != null
                && tableProperty.getDataSortInfo().getSortType() == TSortType.ZORDER;
    }

    public void setBaseIndexId(long baseIndexId) {
        this.baseIndexId = baseIndexId;
    }

    public long getBaseIndexId() {
        return baseIndexId;
    }

    public void setState(OlapTableState state) {
        this.state = state;
    }

    public OlapTableState getState() {
        return state;
    }

    public List<Index> getIndexes() {
        if (indexes == null) {
            return Lists.newArrayList();
        }
        return indexes.getIndexes();
    }

    public List<Long> getIndexIds() {
        if (indexes == null) {
            return Lists.newArrayList();
        }
        return indexes.getIndexIds();
    }

    public TableIndexes getTableIndexes() {
        return indexes;
    }

    public Map<String, Index> getIndexesMap() {
        Map<String, Index> indexMap = new HashMap<>();
        if (indexes != null) {
            Optional.ofNullable(indexes.getIndexes()).orElse(Collections.emptyList()).forEach(
                    i -> indexMap.put(i.getIndexName(), i));
        }
        return indexMap;
    }

    public void checkAndSetName(String newName, boolean onlyCheck) throws DdlException {
        // check if rollup has same name
        for (String idxName : getIndexNameToId().keySet()) {
            if (idxName.equals(newName)) {
                throw new DdlException("New name conflicts with rollup index name: " + idxName);
            }
        }
        if (!onlyCheck) {
            setName(newName);
        }
    }

    public void setName(String newName) {
        // change name in indexNameToId
        long baseIndexId = indexNameToId.remove(this.name);
        indexNameToId.put(newName, baseIndexId);

        // change name
        this.name = newName;

        // change single partition name
        if (this.partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            // use for loop, because if we use getPartition(partitionName),
            // we may not be able to get partition because this is a bug fix
            for (Partition partition : getPartitions()) {
                partition.setName(newName);
                nameToPartition.clear();
                nameToPartition.put(newName, partition);
                break;
            }
        }
    }

    public boolean hasMaterializedIndex(String indexName) {
        return indexNameToId.containsKey(indexName);
    }

    public void setIndexMeta(long indexId, String indexName, List<Column> schema, int schemaVersion, int schemaHash,
            short shortKeyColumnCount, TStorageType storageType, KeysType keysType) {
        setIndexMeta(indexId, indexName, schema, schemaVersion, schemaHash, shortKeyColumnCount, storageType,
                keysType,
                null, null, null); // indexes is null by default
    }

    public void setIndexMeta(long indexId, String indexName, List<Column> schema, int schemaVersion, int schemaHash,
            short shortKeyColumnCount, TStorageType storageType, KeysType keysType, List<Index> indexes) {
        setIndexMeta(indexId, indexName, schema, schemaVersion, schemaHash, shortKeyColumnCount, storageType,
                keysType,
                null, null, indexes);
    }

    public void setIndexMeta(long indexId, String indexName, List<Column> schema, int schemaVersion,
            int schemaHash,
            short shortKeyColumnCount, TStorageType storageType, KeysType keysType, OriginStatement origStmt,
            Analyzer analyzer) {
        setIndexMeta(indexId, indexName, schema, schemaVersion, schemaHash, shortKeyColumnCount, storageType,
                keysType, origStmt, analyzer, null); // indexes is null by default
    }

    public void setIndexMeta(long indexId, String indexName, List<Column> schema, int schemaVersion,
            int schemaHash,
            short shortKeyColumnCount, TStorageType storageType, KeysType keysType, OriginStatement origStmt,
            Analyzer analyzer, List<Index> indexes) {
        // Nullable when meta comes from schema change log replay.
        // The replay log only save the index id, so we need to get name by id.
        if (indexName == null) {
            indexName = getIndexNameById(indexId);
            Preconditions.checkState(indexName != null);
        }
        // Nullable when meta is less than VERSION_74
        if (keysType == null) {
            keysType = this.keysType;
        }
        // Nullable when meta comes from schema change
        if (storageType == null) {
            MaterializedIndexMeta oldIndexMeta = indexIdToMeta.get(indexId);
            Preconditions.checkState(oldIndexMeta != null);
            storageType = oldIndexMeta.getStorageType();
            Preconditions.checkState(storageType != null);
        } else {
            // The new storage type must be TStorageType.COLUMN
            Preconditions.checkState(storageType == TStorageType.COLUMN);
        }

        MaterializedIndexMeta indexMeta = new MaterializedIndexMeta(indexId, schema, schemaVersion, schemaHash,
                shortKeyColumnCount, storageType, keysType, origStmt, indexes, getQualifiedDbName());
        try {
            indexMeta.parseStmt(analyzer);
        } catch (Exception e) {
            LOG.warn("parse meta stmt failed", e);
        }

        indexIdToMeta.put(indexId, indexMeta);
        indexNameToId.put(indexName, indexId);
    }

    // rebuild the full schema of table
    // the order of columns in fullSchema is meaningless
    public void rebuildFullSchema() {
        fullSchema.clear();
        nameToColumn.clear();
        for (Column baseColumn : indexIdToMeta.get(baseIndexId).getSchema()) {
            fullSchema.add(baseColumn);
            nameToColumn.put(baseColumn.getName(), baseColumn);
        }
        for (MaterializedIndexMeta indexMeta : indexIdToMeta.values()) {
            for (Column column : indexMeta.getSchema()) {
                if (!nameToColumn.containsKey(column.getDefineName())) {
                    fullSchema.add(column);
                    nameToColumn.put(column.getDefineName(), column);
                }
            }
            // Column maybe renamed, rebuild the column name map
            indexMeta.initColumnNameMap();
        }
        LOG.debug("after rebuild full schema. table {}, schema size: {}", id, fullSchema.size());
    }

    public boolean deleteIndexInfo(String indexName) {
        if (!indexNameToId.containsKey(indexName)) {
            return false;
        }

        long indexId = this.indexNameToId.remove(indexName);
        this.indexIdToMeta.remove(indexId);
        // Some column of deleted index should be removed during `deleteIndexInfo` such as `mv_bitmap_union_c1`
        // If deleted index id == base index id, the schema will not be rebuilt.
        // The reason is that the base index has been removed from indexIdToMeta while the new base index
        // hasn't changed. The schema could not be rebuild in here with error base index id.
        if (indexId != baseIndexId) {
            rebuildFullSchema();
        }
        LOG.info("delete index info {} in table {}-{}", indexName, id, name);
        return true;
    }

    public Map<String, Long> getIndexNameToId() {
        return indexNameToId;
    }

    public Long getIndexIdByName(String indexName) {
        return indexNameToId.get(indexName);
    }

    public Long getSegmentV2FormatIndexId() {
        String v2RollupIndexName = MaterializedViewHandler.NEW_STORAGE_FORMAT_INDEX_NAME_PREFIX + getName();
        return indexNameToId.get(v2RollupIndexName);
    }

    public String getIndexNameById(long indexId) {
        for (Map.Entry<String, Long> entry : indexNameToId.entrySet()) {
            if (entry.getValue() == indexId) {
                return entry.getKey();
            }
        }
        return null;
    }

    public Map<Long, MaterializedIndexMeta> getVisibleIndexIdToMeta() {
        Map<Long, MaterializedIndexMeta> visibleMVs = Maps.newHashMap();
        List<MaterializedIndex> mvs = getVisibleIndex();
        for (MaterializedIndex mv : mvs) {
            visibleMVs.put(mv.getId(), indexIdToMeta.get(mv.getId()));
        }
        return visibleMVs;
    }

    public List<MaterializedIndex> getVisibleIndex() {
        Optional<Partition> partition = idToPartition.values().stream().findFirst();
        if (!partition.isPresent()) {
            partition = tempPartitions.getAllPartitions().stream().findFirst();
        }
        return partition.isPresent() ? partition.get().getMaterializedIndices(IndexExtState.VISIBLE)
                : Collections.emptyList();
    }

    public Column getVisibleColumn(String columnName) {
        for (MaterializedIndexMeta meta : getVisibleIndexIdToMeta().values()) {
            Column target = meta.getColumnByDefineName(columnName);
            if (target != null) {
                return target;
            }
        }
        return null;
    }

    @Override
    public long getUpdateTime() {
        long updateTime = tempPartitions.getUpdateTime();
        for (Partition p : idToPartition.values()) {
            if (p.getVisibleVersionTime() > updateTime) {
                updateTime = p.getVisibleVersionTime();
            }
        }
        return updateTime;
    }

    // this is only for schema change.
    public void renameIndexForSchemaChange(String name, String newName) {
        long idxId = indexNameToId.remove(name);
        indexNameToId.put(newName, idxId);
    }

    public void renameColumnNamePrefix(long idxId) {
        List<Column> columns = indexIdToMeta.get(idxId).getSchema();
        for (Column column : columns) {
            column.setName(Column.removeNamePrefix(column.getName()));
        }
    }

    /**
     * Reset properties to correct values.
     */
    public void resetPropertiesForRestore(boolean reserveDynamicPartitionEnable, boolean reserveReplica,
                                          ReplicaAllocation replicaAlloc, boolean isBeingSynced) {
        if (tableProperty != null) {
            tableProperty.resetPropertiesForRestore(reserveDynamicPartitionEnable, reserveReplica, replicaAlloc);
        }
        if (isBeingSynced) {
            TableProperty tableProperty = getOrCreatTableProperty();
            tableProperty.setIsBeingSynced();
            tableProperty.removeInvalidProperties();
            if (isAutoBucket()) {
                markAutoBucket();
            }
        }
        // remove colocate property.
        setColocateGroup(null);
    }

    public Status resetIdsForRestore(Env env, Database db, ReplicaAllocation restoreReplicaAlloc,
                                     boolean reserveReplica) {
        // table id
        id = env.getNextId();

        // copy an origin index id to name map
        Map<Long, String> origIdxIdToName = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : indexNameToId.entrySet()) {
            origIdxIdToName.put(entry.getValue(), entry.getKey());
        }

        // reset all 'indexIdToXXX' map
        for (Map.Entry<Long, String> entry : origIdxIdToName.entrySet()) {
            long newIdxId = env.getNextId();
            if (entry.getValue().equals(name)) {
                // base index
                baseIndexId = newIdxId;
            }
            indexIdToMeta.put(newIdxId, indexIdToMeta.remove(entry.getKey()));
            indexNameToId.put(entry.getValue(), newIdxId);
        }

        // generate a partition name to id map
        Map<String, Long> origPartNameToId = Maps.newHashMap();
        for (Partition partition : idToPartition.values()) {
            origPartNameToId.put(partition.getName(), partition.getId());
        }

        // reset partition info and idToPartition map
        if (partitionInfo.getType() == PartitionType.RANGE || partitionInfo.getType() == PartitionType.LIST) {
            for (Map.Entry<String, Long> entry : origPartNameToId.entrySet()) {
                long newPartId = env.getNextId();
                if (reserveReplica) {
                    ReplicaAllocation originReplicaAlloc = partitionInfo.getReplicaAllocation(entry.getValue());
                    partitionInfo.resetPartitionIdForRestore(newPartId, entry.getValue(), originReplicaAlloc, false);
                } else {
                    partitionInfo.resetPartitionIdForRestore(newPartId, entry.getValue(), restoreReplicaAlloc, false);
                }
                idToPartition.put(newPartId, idToPartition.remove(entry.getValue()));
            }
        } else {
            // Single partitioned
            long newPartId = env.getNextId();
            for (Map.Entry<String, Long> entry : origPartNameToId.entrySet()) {
                if (reserveReplica) {
                    ReplicaAllocation originReplicaAlloc = partitionInfo.getReplicaAllocation(entry.getValue());
                    partitionInfo.resetPartitionIdForRestore(newPartId, entry.getValue(), originReplicaAlloc, true);
                } else {
                    partitionInfo.resetPartitionIdForRestore(newPartId, entry.getValue(), restoreReplicaAlloc, true);
                }
                idToPartition.put(newPartId, idToPartition.remove(entry.getValue()));
            }
        }

        // for each partition, reset rollup index map
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            Partition partition = entry.getValue();
            // entry.getKey() is the new partition id, use it to get the restore specified replica allocation
            ReplicaAllocation replicaAlloc = partitionInfo.getReplicaAllocation(entry.getKey());
            for (Map.Entry<Long, String> entry2 : origIdxIdToName.entrySet()) {
                MaterializedIndex idx = partition.getIndex(entry2.getKey());
                long newIdxId = indexNameToId.get(entry2.getValue());
                int schemaHash = indexIdToMeta.get(newIdxId).getSchemaHash();
                idx.setIdForRestore(newIdxId);
                if (newIdxId != baseIndexId) {
                    // not base table, reset
                    partition.deleteRollupIndex(entry2.getKey());
                    partition.createRollupIndex(idx);
                }

                // generate new tablets in origin tablet order
                int tabletNum = idx.getTablets().size();
                idx.clearTabletsForRestore();
                for (int i = 0; i < tabletNum; i++) {
                    long newTabletId = env.getNextId();
                    Tablet newTablet = new Tablet(newTabletId);
                    idx.addTablet(newTablet, null /* tablet meta */, true /* is restore */);

                    // replicas
                    try {
                        Map<Tag, List<Long>> tag2beIds =
                                Env.getCurrentSystemInfo().selectBackendIdsForReplicaCreation(
                                        replicaAlloc, null, false, false);
                        for (Map.Entry<Tag, List<Long>> entry3 : tag2beIds.entrySet()) {
                            for (Long beId : entry3.getValue()) {
                                long newReplicaId = env.getNextId();
                                Replica replica = new Replica(newReplicaId, beId, ReplicaState.NORMAL,
                                        partition.getVisibleVersion(), schemaHash);
                                newTablet.addReplica(replica, true /* is restore */);
                            }
                        }
                    } catch (DdlException e) {
                        return new Status(ErrCode.COMMON_ERROR, e.getMessage());
                    }
                }
            }

            // reset partition id
            partition.setIdForRestore(entry.getKey());
        }

        return Status.OK;
    }

    public Map<Long, MaterializedIndexMeta> getIndexIdToMeta() {
        return indexIdToMeta;
    }

    public Map<Long, MaterializedIndexMeta> getCopyOfIndexIdToMeta() {
        return new HashMap<>(indexIdToMeta);
    }

    public Map<Long, MaterializedIndexMeta> getCopiedIndexIdToMeta() {
        return new HashMap<>(indexIdToMeta);
    }

    public MaterializedIndexMeta getIndexMetaByIndexId(long indexId) {
        return indexIdToMeta.get(indexId);
    }

    public List<Long> getIndexIdListExceptBaseIndex() {
        List<Long> result = Lists.newArrayList();
        for (Long indexId : indexIdToMeta.keySet()) {
            if (indexId != baseIndexId) {
                result.add(indexId);
            }
        }
        return result;
    }

    // schema
    public Map<Long, List<Column>> getIndexIdToSchema() {
        return getIndexIdToSchema(Util.showHiddenColumns());
    }

    // schema
    public Map<Long, List<Column>> getIndexIdToSchema(boolean full) {
        Map<Long, List<Column>> result = Maps.newHashMap();
        for (Map.Entry<Long, MaterializedIndexMeta> entry : indexIdToMeta.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getSchema(full));
        }
        return result;
    }

    public List<Column> getSchemaByIndexId(Long indexId) {
        return getSchemaByIndexId(indexId, Util.showHiddenColumns());
    }

    public List<Column> getSchemaByIndexId(Long indexId, boolean full) {
        if (full) {
            return indexIdToMeta.get(indexId).getSchema();
        } else {
            return indexIdToMeta.get(indexId).getSchema().stream().filter(column -> column.isVisible())
                    .collect(Collectors.toList());
        }
    }

    public List<Column> getBaseSchemaKeyColumns() {
        return getKeyColumnsByIndexId(baseIndexId);
    }

    public List<Column> getKeyColumnsByIndexId(Long indexId) {
        ArrayList<Column> keyColumns = Lists.newArrayList();
        List<Column> allColumns = this.getSchemaByIndexId(indexId);
        for (Column column : allColumns) {
            if (column.isKey()) {
                keyColumns.add(column);
            }
        }

        return keyColumns;
    }

    public boolean hasDeleteSign() {
        return getDeleteSignColumn() != null;
    }

    public Column getDeleteSignColumn() {
        for (Column column : getBaseSchema(true)) {
            if (column.isDeleteSignColumn()) {
                return column;
            }
        }
        return null;
    }

    // schemaHash
    public Map<Long, Integer> getIndexIdToSchemaHash() {
        Map<Long, Integer> result = Maps.newHashMap();
        for (Map.Entry<Long, MaterializedIndexMeta> entry : indexIdToMeta.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getSchemaHash());
        }
        return result;
    }

    public int getSchemaHashByIndexId(Long indexId) {
        MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
        if (indexMeta == null) {
            return -1;
        }
        return indexMeta.getSchemaHash();
    }

    public TStorageType getStorageTypeByIndexId(Long indexId) {
        MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
        if (indexMeta == null) {
            return TStorageType.COLUMN;
        }
        return indexMeta.getStorageType();
    }

    public KeysType getKeysType() {
        return keysType;
    }

    public KeysType getKeysTypeByIndexId(long indexId) {
        MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
        Preconditions.checkNotNull(indexMeta, "index id:" + indexId + " meta is null");
        return indexMeta.getKeysType();
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public Set<String> getPartitionColumnNames() throws DdlException {
        Set<String> partitionColumnNames = Sets.newHashSet();
        if (partitionInfo instanceof SinglePartitionInfo) {
            return partitionColumnNames;
        } else if (partitionInfo instanceof RangePartitionInfo) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            return rangePartitionInfo.getPartitionColumns().stream()
                    .map(c -> c.getName().toLowerCase()).collect(Collectors.toSet());
        } else if (partitionInfo instanceof ListPartitionInfo) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            return listPartitionInfo.getPartitionColumns().stream()
                    .map(c -> c.getName().toLowerCase()).collect(Collectors.toSet());
        } else {
            throw new DdlException("Unknown partition info type: " + partitionInfo.getType().name());
        }
    }

    public DistributionInfo getDefaultDistributionInfo() {
        return defaultDistributionInfo;
    }

    public void markAutoBucket() {
        defaultDistributionInfo.markAutoBucket();
    }

    public Set<String> getDistributionColumnNames() {
        Set<String> distributionColumnNames = Sets.newHashSet();
        if (defaultDistributionInfo instanceof RandomDistributionInfo) {
            return distributionColumnNames;
        }
        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) defaultDistributionInfo;
        List<Column> partitionColumns = hashDistributionInfo.getDistributionColumns();
        for (Column column : partitionColumns) {
            distributionColumnNames.add(column.getName().toLowerCase());
        }
        return distributionColumnNames;
    }

    public void renamePartition(String partitionName, String newPartitionName) {
        if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            // bug fix
            for (Partition partition : idToPartition.values()) {
                partition.setName(newPartitionName);
                nameToPartition.clear();
                nameToPartition.put(newPartitionName, partition);
                LOG.info("rename partition {} in table {}", newPartitionName, name);
                break;
            }
        } else {
            Partition partition = nameToPartition.remove(partitionName);
            partition.setName(newPartitionName);
            nameToPartition.put(newPartitionName, partition);
        }
    }

    public void addPartition(Partition partition) {
        idToPartition.put(partition.getId(), partition);
        nameToPartition.put(partition.getName(), partition);
    }

    // This is a private method.
    // Call public "dropPartitionAndReserveTablet" and "dropPartition"
    private Partition dropPartition(long dbId, String partitionName, boolean isForceDrop, boolean reserveTablets) {
        // 1. If "isForceDrop" is false, the partition will be added to the Catalog Recyle bin, and all tablets of this
        //    partition will not be deleted.
        // 2. If "ifForceDrop" is true, the partition will be dropped immediately, but whether to drop the tablets
        //    of this partition depends on "reserveTablets"
        //    If "reserveTablets" is true, the tablets of this partition will not be deleted.
        //    Otherwise, the tablets of this partition will be deleted immediately.
        Partition partition = nameToPartition.get(partitionName);
        if (partition != null) {
            idToPartition.remove(partition.getId());
            nameToPartition.remove(partitionName);

            Preconditions.checkState(partitionInfo.getType() == PartitionType.RANGE
                    || partitionInfo.getType() == PartitionType.LIST);

            if (!isForceDrop) {
                // recycle partition
                if (partitionInfo.getType() == PartitionType.RANGE) {
                    Env.getCurrentRecycleBin().recyclePartition(dbId, id, name, partition,
                            partitionInfo.getItem(partition.getId()).getItems(),
                            new ListPartitionItem(Lists.newArrayList(new PartitionKey())),
                            partitionInfo.getDataProperty(partition.getId()),
                            partitionInfo.getReplicaAllocation(partition.getId()),
                            partitionInfo.getIsInMemory(partition.getId()),
                            partitionInfo.getIsMutable(partition.getId()));

                } else if (partitionInfo.getType() == PartitionType.LIST) {
                    // construct a dummy range
                    List<Column> dummyColumns = new ArrayList<>();
                    dummyColumns.add(new Column("dummy", PrimitiveType.INT));
                    PartitionKey dummyKey = null;
                    try {
                        dummyKey = PartitionKey.createInfinityPartitionKey(dummyColumns, false);
                    } catch (AnalysisException e) {
                        LOG.warn("should not happen", e);
                    }
                    Range<PartitionKey> dummyRange = Range.open(new PartitionKey(), dummyKey);

                    Env.getCurrentRecycleBin().recyclePartition(dbId, id, name, partition,
                            dummyRange,
                            partitionInfo.getItem(partition.getId()),
                            partitionInfo.getDataProperty(partition.getId()),
                            partitionInfo.getReplicaAllocation(partition.getId()),
                            partitionInfo.getIsInMemory(partition.getId()),
                            partitionInfo.getIsMutable(partition.getId()));
                }
            } else if (!reserveTablets) {
                Env.getCurrentEnv().onErasePartition(partition);
            }

            // drop partition info
            partitionInfo.dropPartition(partition.getId());
        }
        return partition;
    }

    public Partition dropPartitionAndReserveTablet(String partitionName) {
        return dropPartition(-1, partitionName, true, true);
    }

    public Partition dropPartition(long dbId, String partitionName, boolean isForceDrop) {
        return dropPartition(dbId, partitionName, isForceDrop, !isForceDrop);
    }

    /*
     * A table may contain both formal and temporary partitions.
     * There are several methods to get the partition of a table.
     * Typically divided into two categories:
     *
     * 1. Get partition by id
     * 2. Get partition by name
     *
     * According to different requirements, the caller may want to obtain
     * a formal partition or a temporary partition. These methods are
     * described below in order to obtain the partition by using the correct method.
     *
     * 1. Get by name
     *
     * This type of request usually comes from a user with partition names. Such as
     * `select * from tbl partition(p1);`.
     * This type of request has clear information to indicate whether to obtain a
     * formal or temporary partition.
     * Therefore, we need to get the partition through this method:
     *
     * `getPartition(String partitionName, boolean isTemp)`
     *
     * To avoid modifying too much code, we leave the `getPartition(String
     * partitionName)`, which is same as:
     *
     * `getPartition(partitionName, false)`
     *
     * 2. Get by id
     *
     * This type of request usually means that the previous step has obtained
     * certain partition ids in some way,
     * so we only need to get the corresponding partition through this method:
     *
     * `getPartition(long partitionId)`.
     *
     * This method will try to get both formal partitions and temporary partitions.
     *
     * 3. Get all partition instances
     *
     * Depending on the requirements, the caller may want to obtain all formal
     * partitions,
     * all temporary partitions, or all partitions. Therefore we provide 3 methods,
     * the caller chooses according to needs.
     *
     * `getPartitions()`
     * `getTempPartitions()`
     * `getAllPartitions()`
     *
     */

    // get partition by name, not including temp partitions
    @Override
    public Partition getPartition(String partitionName) {
        return getPartition(partitionName, false);
    }

    // get partition by name
    public Partition getPartition(String partitionName, boolean isTempPartition) {
        if (isTempPartition) {
            return tempPartitions.getPartition(partitionName);
        } else {
            return nameToPartition.get(partitionName);
        }
    }

    // get partition by id, including temp partitions
    public Partition getPartition(long partitionId) {
        Partition partition = idToPartition.get(partitionId);
        if (partition == null) {
            partition = tempPartitions.getPartition(partitionId);
        }
        return partition;
    }

    // get all partitions except temp partitions
    public Collection<Partition> getPartitions() {
        return idToPartition.values();
    }

    // get only temp partitions
    public Collection<Partition> getTempPartitions() {
        return tempPartitions.getAllPartitions();
    }

    // get all partitions including temp partitions
    public Collection<Partition> getAllPartitions() {
        List<Partition> partitions = Lists.newArrayList(idToPartition.values());
        partitions.addAll(tempPartitions.getAllPartitions());
        return partitions;
    }

    // get all partitions' name except the temp partitions
    public Set<String> getPartitionNames() {
        return Sets.newHashSet(nameToPartition.keySet());
    }

    public List<Long> getPartitionIds() {
        return new ArrayList<>(idToPartition.keySet());
    }

    public Set<String> getCopiedBfColumns() {
        if (bfColumns == null) {
            return null;
        }
        return Sets.newHashSet(bfColumns);
    }

    public List<Index> getCopiedIndexes() {
        if (indexes == null) {
            return Lists.newArrayList();
        }
        return indexes.getCopiedIndexes();
    }

    public double getBfFpp() {
        return bfFpp;
    }

    public void setBloomFilterInfo(Set<String> bfColumns, double bfFpp) {
        this.bfColumns = bfColumns;
        this.bfFpp = bfFpp;
    }

    public String getSequenceMapCol() {
        if (tableProperty == null) {
            return null;
        }
        return tableProperty.getSequenceMapCol();
    }

    // map the sequence column to other column
    public void setSequenceMapCol(String colName) {
        getOrCreatTableProperty().setSequenceMapCol(colName);
    }

    public void setSequenceInfo(Type type) {
        this.hasSequenceCol = true;
        this.sequenceType = type;

        Column sequenceCol;
        if (getEnableUniqueKeyMergeOnWrite()) {
            // sequence column is value column with NONE aggregate type for
            // unique key table with merge on write
            sequenceCol = ColumnDef.newSequenceColumnDef(type, AggregateType.NONE).toColumn();
        } else {
            // sequence column is value column with REPLACE aggregate type for
            // unique key table
            sequenceCol = ColumnDef.newSequenceColumnDef(type, AggregateType.REPLACE).toColumn();
        }
        // add sequence column at last
        fullSchema.add(sequenceCol);
        nameToColumn.put(Column.SEQUENCE_COL, sequenceCol);
        for (MaterializedIndexMeta indexMeta : indexIdToMeta.values()) {
            List<Column> schema = indexMeta.getSchema();
            schema.add(sequenceCol);
        }
    }

    public Column getSequenceCol() {
        for (Column column : getBaseSchema(true)) {
            if (column.isSequenceColumn()) {
                return column;
            }
        }
        return null;
    }

    public Column getRowStoreCol() {
        for (Column column : getBaseSchema(true)) {
            if (column.isRowStoreColumn()) {
                return column;
            }
        }
        return null;
    }

    public Boolean hasSequenceCol() {
        return getSequenceCol() != null;
    }

    public boolean hasHiddenColumn() {
        return getBaseSchema().stream().anyMatch(column -> !column.isVisible());
    }

    public Type getSequenceType() {
        if (getSequenceCol() == null) {
            return null;
        } else {
            return getSequenceCol().getType();
        }
    }

    public void setIndexes(List<Index> indexes) {
        if (this.indexes == null) {
            this.indexes = new TableIndexes(null);
        }
        this.indexes.setIndexes(indexes);
    }

    public boolean isColocateTable() {
        return colocateGroup != null;
    }

    public String getColocateGroup() {
        return colocateGroup;
    }

    public void setColocateGroup(String colocateGroup) {
        this.colocateGroup = colocateGroup;
    }

    // when the table is creating new rollup and enter finishing state, should tell be not auto load to new rollup
    // it is used for stream load
    // the caller should get db lock when call this method
    public boolean shouldLoadToNewRollup() {
        return false;
    }

    @Override
    public TTableDescriptor toThrift() {
        TOlapTable tOlapTable = new TOlapTable(getName());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.OLAP_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setOlapTable(tOlapTable);
        return tTableDescriptor;
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        if (info.analysisType.equals(AnalysisType.HISTOGRAM)) {
            return new HistogramTask(info);
        }
        if (info.analysisType.equals(AnalysisType.FUNDAMENTALS)) {
            return new OlapAnalysisTask(info);
        }
        return new MVAnalysisTask(info);
    }

    @Override
    public long getRowCount() {
        long rowCount = 0;
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            rowCount += entry.getValue().getBaseIndex().getRowCount();
        }
        return rowCount;
    }

    @Override
    public long getAvgRowLength() {
        long rowCount = 0;
        long dataSize = 0;
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            rowCount += entry.getValue().getBaseIndex().getRowCount();
            dataSize += entry.getValue().getBaseIndex().getDataSize(false);
        }
        if (rowCount > 0) {
            return dataSize / rowCount;
        } else {
            return 0;
        }
    }

    @Override
    public long getDataLength() {
        long dataSize = 0;
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            dataSize += entry.getValue().getBaseIndex().getDataSize(false);
        }
        return dataSize;
    }

    @Override
    public CreateTableStmt toCreateTableStmt(String dbName) {
        throw new RuntimeException("Don't support anymore");
    }

    // Get the md5 of signature string of this table with specified partitions.
    // This method is used to determine whether the tables have the same schema.
    // Contains:
    // table name, table type, index name, index schema, short key column count, storage type
    // bloom filter, partition type and columns, distribution type and columns.
    // buckets number.
    public String getSignature(int signatureVersion, List<String> partNames) {
        StringBuilder sb = new StringBuilder(signatureVersion);
        sb.append(name);
        sb.append(type);
        Set<String> indexNames = Sets.newTreeSet(indexNameToId.keySet());
        for (String indexName : indexNames) {
            long indexId = indexNameToId.get(indexName);
            MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
            sb.append(indexName);
            sb.append(Util.getSchemaSignatureString(indexMeta.getSchema()));
            sb.append(indexMeta.getShortKeyColumnCount());
            sb.append(indexMeta.getStorageType());
        }

        // bloom filter
        if (bfColumns != null && !bfColumns.isEmpty()) {
            for (String bfCol : bfColumns) {
                sb.append(bfCol);
            }
            sb.append(bfFpp);
        }

        // partition type
        sb.append(partitionInfo.getType());
        if (partitionInfo.getType() == PartitionType.RANGE) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
            sb.append(Util.getSchemaSignatureString(partitionColumns));
        }

        // partition and distribution
        Collections.sort(partNames, String.CASE_INSENSITIVE_ORDER);
        for (String partName : partNames) {
            Partition partition = getPartition(partName);
            Preconditions.checkNotNull(partition, partName);
            DistributionInfo distributionInfo = partition.getDistributionInfo();
            sb.append(partName);
            sb.append(distributionInfo.getType());
            if (distributionInfo.getType() == DistributionInfoType.HASH) {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                sb.append(Util.getSchemaSignatureString(hashDistributionInfo.getDistributionColumns()));
                sb.append(hashDistributionInfo.getBucketNum());
            }
        }

        String md5 = DigestUtils.md5Hex(sb.toString());
        LOG.debug("get signature of table {}: {}. signature string: {}", name, md5, sb.toString());
        return md5;
    }

    // get intersect partition names with the given table "anotherTbl". not including temp partitions
    public Status getIntersectPartNamesWith(OlapTable anotherTbl, List<String> intersectPartNames) {
        if (this.getPartitionInfo().getType() != anotherTbl.getPartitionInfo().getType()) {
            return new Status(ErrCode.COMMON_ERROR, "Table's partition type is different");
        }

        Set<String> intersect = this.getPartitionNames();
        intersect.retainAll(anotherTbl.getPartitionNames());
        intersectPartNames.addAll(intersect);
        return Status.OK;
    }

    @Override
    public boolean isPartitioned() {
        int numSegs = 0;
        for (Partition part : getPartitions()) {
            numSegs += part.getDistributionInfo().getBucketNum();
            if (numSegs > 1) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        // state
        Text.writeString(out, state.name());

        // indices' schema
        int counter = indexNameToId.size();
        out.writeInt(counter);
        for (Map.Entry<String, Long> entry : indexNameToId.entrySet()) {
            String indexName = entry.getKey();
            long indexId = entry.getValue();
            Text.writeString(out, indexName);
            out.writeLong(indexId);
            indexIdToMeta.get(indexId).write(out);
        }

        Text.writeString(out, keysType.name());
        Text.writeString(out, partitionInfo.getType().name());
        partitionInfo.write(out);
        Text.writeString(out, defaultDistributionInfo.getType().name());
        defaultDistributionInfo.write(out);

        // partitions
        int partitionCount = idToPartition.size();
        out.writeInt(partitionCount);
        for (Partition partition : idToPartition.values()) {
            partition.write(out);
        }

        // bloom filter columns
        if (bfColumns == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(bfColumns.size());
            for (String bfColumn : bfColumns) {
                Text.writeString(out, bfColumn);
            }
            out.writeDouble(bfFpp);
        }

        // colocateTable
        if (colocateGroup == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, colocateGroup);
        }

        out.writeLong(baseIndexId);

        // write indexes
        if (indexes != null) {
            out.writeBoolean(true);
            indexes.write(out);
        } else {
            out.writeBoolean(false);
        }

        // tableProperty
        if (tableProperty == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            tableProperty.write(out);
        }

        tempPartitions.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        this.state = OlapTableState.valueOf(Text.readString(in));

        // indices's schema
        int counter = in.readInt();
        // tmp index meta list
        List<MaterializedIndexMeta> tmpIndexMetaList = Lists.newArrayList();
        for (int i = 0; i < counter; i++) {
            String indexName = Text.readString(in);
            long indexId = in.readLong();
            this.indexNameToId.put(indexName, indexId);
            MaterializedIndexMeta indexMeta = MaterializedIndexMeta.read(in);
            indexIdToMeta.put(indexId, indexMeta);
        }

        // partition and distribution info
        keysType = KeysType.valueOf(Text.readString(in));

        // add the correct keys type in tmp index meta
        for (MaterializedIndexMeta indexMeta : tmpIndexMetaList) {
            indexMeta.setKeysType(keysType);
            indexIdToMeta.put(indexMeta.getIndexId(), indexMeta);
        }

        PartitionType partType = PartitionType.valueOf(Text.readString(in));
        if (partType == PartitionType.UNPARTITIONED) {
            partitionInfo = SinglePartitionInfo.read(in);
        } else if (partType == PartitionType.RANGE) {
            partitionInfo = RangePartitionInfo.read(in);
        } else if (partType == PartitionType.LIST) {
            partitionInfo = ListPartitionInfo.read(in);
        } else {
            throw new IOException("invalid partition type: " + partType);
        }

        DistributionInfoType distriType = DistributionInfoType.valueOf(Text.readString(in));
        if (distriType == DistributionInfoType.HASH) {
            defaultDistributionInfo = HashDistributionInfo.read(in);
        } else if (distriType == DistributionInfoType.RANDOM) {
            defaultDistributionInfo = RandomDistributionInfo.read(in);
        } else {
            throw new IOException("invalid distribution type: " + distriType);
        }

        int partitionCount = in.readInt();
        for (int i = 0; i < partitionCount; ++i) {
            Partition partition = Partition.read(in);
            idToPartition.put(partition.getId(), partition);
            nameToPartition.put(partition.getName(), partition);
        }

        if (in.readBoolean()) {
            int bfColumnCount = in.readInt();
            bfColumns = Sets.newHashSet();
            for (int i = 0; i < bfColumnCount; i++) {
                bfColumns.add(Text.readString(in));
            }

            bfFpp = in.readDouble();
        }

        if (in.readBoolean()) {
            colocateGroup = Text.readString(in);
        }
        baseIndexId = in.readLong();

        // read indexes
        if (in.readBoolean()) {
            this.indexes = TableIndexes.read(in);
        }
        // tableProperty
        if (in.readBoolean()) {
            tableProperty = TableProperty.read(in);
        }
        if (isAutoBucket()) {
            defaultDistributionInfo.markAutoBucket();
        }

        // temp partitions
        tempPartitions = TempPartitions.read(in);
        RangePartitionInfo tempRangeInfo = tempPartitions.getPartitionInfo();
        if (tempRangeInfo != null) {
            for (long partitionId : tempRangeInfo.getIdToItem(false).keySet()) {
                this.partitionInfo.addPartition(partitionId, true,
                        tempRangeInfo.getItem(partitionId), tempRangeInfo.getDataProperty(partitionId),
                        tempRangeInfo.getReplicaAllocation(partitionId), tempRangeInfo.getIsInMemory(partitionId),
                        tempRangeInfo.getIsMutable(partitionId));
            }
        }
        tempPartitions.unsetPartitionInfo();

        // In the present, the fullSchema could be rebuilt by schema change while the properties is changed by MV.
        // After that, some properties of fullSchema and nameToColumn may be not same as properties of base columns.
        // So, here we need to rebuild the fullSchema to ensure the correctness of the properties.
        rebuildFullSchema();
    }

    public OlapTable selectiveCopy(Collection<String> reservedPartitions, IndexExtState extState, boolean isForBackup) {
        OlapTable copied = new OlapTable();
        if (!DeepCopy.copy(this, copied, OlapTable.class, FeConstants.meta_version)) {
            LOG.warn("failed to copy olap table: " + getName());
            return null;
        }

        // remove shadow index from copied table
        // NOTICE that there maybe not partition in table.
        List<MaterializedIndex> shadowIndex = Lists.newArrayList();
        Optional<Partition> firstPartition = copied.getPartitions().stream().findFirst();
        if (firstPartition.isPresent()) {
            shadowIndex = firstPartition.get().getMaterializedIndices(IndexExtState.SHADOW);
        }

        for (MaterializedIndex deleteIndex : shadowIndex) {
            LOG.debug("copied table delete shadow index : {}", deleteIndex.getId());
            copied.deleteIndexInfo(copied.getIndexNameById(deleteIndex.getId()));
        }
        copied.setState(OlapTableState.NORMAL);
        for (Partition partition : copied.getPartitions()) {
            // remove shadow index from partition
            for (MaterializedIndex deleteIndex : shadowIndex) {
                partition.deleteRollupIndex(deleteIndex.getId());
            }
            partition.setState(PartitionState.NORMAL);
            if (isForBackup) {
                // set storage medium to HDD for backup job, because we want that the backuped table
                // can be able to restored to another Doris cluster without SSD disk.
                // But for other operation such as truncate table, keep the origin storage medium.
                copied.getPartitionInfo().setDataProperty(partition.getId(), new DataProperty(TStorageMedium.HDD));
            }
            for (MaterializedIndex idx : partition.getMaterializedIndices(extState)) {
                idx.setState(IndexState.NORMAL);
                for (Tablet tablet : idx.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.setState(ReplicaState.NORMAL);
                    }
                }
            }
        }

        if (reservedPartitions == null || reservedPartitions.isEmpty()) {
            // reserve all
            return copied;
        }

        Set<String> partNames = Sets.newHashSet();
        partNames.addAll(copied.getPartitionNames());

        // partition name is case insensitive:
        Set<String> lowerReservedPartitionNames = reservedPartitions.stream()
                .map(String::toLowerCase).collect(Collectors.toSet());
        for (String partName : partNames) {
            if (!lowerReservedPartitionNames.contains(partName.toLowerCase())) {
                copied.dropPartitionAndReserveTablet(partName);
            }
        }

        return copied;
    }

    /*
     * this method is currently used for truncating table(partitions).
     * the new partition has new id, so we need to change all 'id-related' members
     *
     * return the old partition.
     */
    public Partition replacePartition(Partition newPartition) {
        Partition oldPartition = nameToPartition.remove(newPartition.getName());
        idToPartition.remove(oldPartition.getId());

        idToPartition.put(newPartition.getId(), newPartition);
        nameToPartition.put(newPartition.getName(), newPartition);

        DataProperty dataProperty = partitionInfo.getDataProperty(oldPartition.getId());
        ReplicaAllocation replicaAlloc = partitionInfo.getReplicaAllocation(oldPartition.getId());
        boolean isInMemory = partitionInfo.getIsInMemory(oldPartition.getId());
        boolean isMutable = partitionInfo.getIsMutable(oldPartition.getId());

        if (partitionInfo.getType() == PartitionType.RANGE
                || partitionInfo.getType() == PartitionType.LIST) {
            PartitionItem item = partitionInfo.getItem(oldPartition.getId());
            partitionInfo.dropPartition(oldPartition.getId());
            partitionInfo.addPartition(newPartition.getId(), false, item, dataProperty,
                    replicaAlloc, isInMemory, isMutable);
        } else {
            partitionInfo.dropPartition(oldPartition.getId());
            partitionInfo.addPartition(newPartition.getId(), dataProperty, replicaAlloc, isInMemory, isMutable);
        }

        return oldPartition;
    }

    public long getDataSize() {
        long dataSize = 0;
        for (Partition partition : getAllPartitions()) {
            dataSize += partition.getDataSize(false);
        }
        return dataSize;
    }

    public long getRemoteDataSize() {
        long remoteDataSize = 0;
        for (Partition partition : getAllPartitions()) {
            remoteDataSize += partition.getRemoteDataSize();
        }
        return remoteDataSize;
    }

    public long getReplicaCount() {
        long replicaCount = 0;
        for (Partition partition : getAllPartitions()) {
            replicaCount += partition.getReplicaCount();
        }
        return replicaCount;
    }

    public void checkNormalStateForAlter() throws DdlException {
        if (state != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + name + "]'s state is not NORMAL. Do not allow doing ALTER ops");
        }
    }

    public boolean isStable(SystemInfoService infoService, TabletScheduler tabletScheduler, String clusterName) {
        List<Long> aliveBeIds = infoService.getAllBackendIds(true);
        for (Partition partition : idToPartition.values()) {
            long visibleVersion = partition.getVisibleVersion();
            ReplicaAllocation replicaAlloc = partitionInfo.getReplicaAllocation(partition.getId());
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.ALL)) {
                for (Tablet tablet : mIndex.getTablets()) {
                    if (tabletScheduler.containsTablet(tablet.getId())) {
                        LOG.info("table {} is not stable because tablet {} is in tablet scheduler. replicas: {}",
                                id, tablet.getId(), tablet.getReplicas());
                        return false;
                    }

                    Pair<TabletStatus, TabletSchedCtx.Priority> statusPair = tablet.getHealthStatusWithPriority(
                            infoService, visibleVersion, replicaAlloc, aliveBeIds);
                    if (statusPair.first != TabletStatus.HEALTHY) {
                        LOG.info("table {} is not stable because tablet {} status is {}. replicas: {}",
                                id, tablet.getId(), statusPair.first, tablet.getReplicas());
                        return false;
                    }
                }
            }
        }
        return true;
    }

    // arbitrarily choose a partition, and get the buckets backends sequence from base index.
    public Map<Tag, List<List<Long>>> getArbitraryTabletBucketsSeq() throws DdlException {
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        Map<Tag, List<List<Long>>> backendsPerBucketSeq = Maps.newHashMap();
        for (Partition partition : idToPartition.values()) {
            ReplicaAllocation replicaAlloc = partitionInfo.getReplicaAllocation(partition.getId());
            short totalReplicaNum = replicaAlloc.getTotalReplicaNum();
            MaterializedIndex baseIdx = partition.getBaseIndex();
            for (Long tabletId : baseIdx.getTabletIdsInOrder()) {
                Tablet tablet = baseIdx.getTablet(tabletId);
                List<Long> replicaBackendIds = tablet.getNormalReplicaBackendIds();
                if (replicaBackendIds.size() != totalReplicaNum) {
                    // this should not happen, but in case, throw an exception to terminate this process
                    throw new DdlException("Normal replica number of tablet " + tabletId + " is: "
                            + replicaBackendIds.size() + ", but expected: " + totalReplicaNum);
                }

                // check tag
                Map<Tag, Short> currentReplicaAlloc = Maps.newHashMap();
                Map<Tag, List<Long>> tag2beIds = Maps.newHashMap();
                for (long beId : replicaBackendIds) {
                    Backend be = infoService.getBackend(beId);
                    if (be == null || !be.isMixNode()) {
                        continue;
                    }
                    short num = currentReplicaAlloc.getOrDefault(be.getLocationTag(), (short) 0);
                    currentReplicaAlloc.put(be.getLocationTag(), (short) (num + 1));
                    List<Long> beIds = tag2beIds.getOrDefault(be.getLocationTag(), Lists.newArrayList());
                    beIds.add(beId);
                    tag2beIds.put(be.getLocationTag(), beIds);
                }
                if (!currentReplicaAlloc.equals(replicaAlloc.getAllocMap())) {
                    throw new DdlException("The relica allocation is " + currentReplicaAlloc.toString()
                            + ", but expected: " + replicaAlloc.toCreateStmt());
                }

                for (Map.Entry<Tag, List<Long>> entry : tag2beIds.entrySet()) {
                    backendsPerBucketSeq.putIfAbsent(entry.getKey(), Lists.newArrayList());
                    backendsPerBucketSeq.get(entry.getKey()).add(entry.getValue());
                }
            }
            break;
        }
        return backendsPerBucketSeq;
    }

    /**
     * Get the proximate row count of this table, if you need accurate row count should select count(*) from table.
     *
     * @return proximate row count
     */
    public long proximateRowCount() {
        long totalCount = 0;
        for (Partition partition : getPartitions()) {
            long version = partition.getVisibleVersion();
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    long tabletRowCount = 0L;
                    for (Replica replica : tablet.getReplicas()) {
                        if (replica.checkVersionCatchUp(version, false)
                                && replica.getRowCount() > tabletRowCount) {
                            tabletRowCount = replica.getRowCount();
                        }
                    }
                    totalCount += tabletRowCount;
                }
            }
        }
        return totalCount;
    }

    @Override
    public List<Column> getBaseSchema() {
        return getSchemaByIndexId(baseIndexId);
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        return getSchemaByIndexId(baseIndexId, full);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OlapTable other = (OlapTable) o;

        if (!Objects.equals(defaultDistributionInfo, other.defaultDistributionInfo)) {
            return false;
        }

        return Double.compare(other.bfFpp, bfFpp) == 0 && hasSequenceCol == other.hasSequenceCol
                && baseIndexId == other.baseIndexId && state == other.state && Objects.equals(indexIdToMeta,
                other.indexIdToMeta) && Objects.equals(indexNameToId, other.indexNameToId) && keysType == other.keysType
                && Objects.equals(partitionInfo, other.partitionInfo) && Objects.equals(
                idToPartition, other.idToPartition) && Objects.equals(nameToPartition,
                other.nameToPartition) && Objects.equals(tempPartitions, other.tempPartitions)
                && Objects.equals(bfColumns, other.bfColumns) && Objects.equals(colocateGroup,
                other.colocateGroup) && Objects.equals(sequenceType, other.sequenceType)
                && Objects.equals(indexes, other.indexes) && Objects.equals(tableProperty,
                other.tableProperty);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, indexIdToMeta, indexNameToId, keysType, partitionInfo, idToPartition,
                nameToPartition, defaultDistributionInfo, tempPartitions, bfColumns, bfFpp, colocateGroup,
                hasSequenceCol, sequenceType, indexes, baseIndexId, tableProperty);
    }

    public Column getBaseColumn(String columnName) {
        for (Column column : getBaseSchema()) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return column;
            }
        }
        return null;
    }

    public int getKeysNum() {
        int keysNum = 0;
        for (Column column : getBaseSchema()) {
            if (column.isKey()) {
                keysNum += 1;
            }
        }
        return keysNum;
    }

    public boolean convertHashDistributionToRandomDistribution() {
        boolean hasChanged = false;
        if (defaultDistributionInfo.getType() == DistributionInfoType.HASH) {
            defaultDistributionInfo = ((HashDistributionInfo) defaultDistributionInfo).toRandomDistributionInfo();
            hasChanged = true;
            for (Partition partition : idToPartition.values()) {
                partition.convertHashDistributionToRandomDistribution();
            }
        }
        return hasChanged;
    }

    public void ignoreInvaildPropertiesWhenSynced(Map<String, String> properties) {
        // ignore colocate table
        PropertyAnalyzer.analyzeColocate(properties);
        // ignore storage policy
        if (!PropertyAnalyzer.analyzeStoragePolicy(properties).isEmpty()) {
            properties.remove(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY);
        }
    }

    public void checkChangeReplicaAllocation() throws DdlException {
        if (isColocateTable()) {
            throw new DdlException("Cannot change replication allocation of colocate table.");
        }
    }

    public void setReplicationAllocation(ReplicaAllocation replicaAlloc) {
        getOrCreatTableProperty().setReplicaAlloc(replicaAlloc);
    }

    public ReplicaAllocation getDefaultReplicaAllocation() {
        if (tableProperty != null) {
            return tableProperty.getReplicaAllocation();
        }
        return ReplicaAllocation.DEFAULT_ALLOCATION;
    }

    public Boolean isInMemory() {
        if (tableProperty != null) {
            return tableProperty.isInMemory();
        }
        return false;
    }

    public void setIsInMemory(boolean isInMemory) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_INMEMORY,
                Boolean.valueOf(isInMemory).toString());
        tableProperty.buildInMemory();
    }

    public Boolean isAutoBucket() {
        if (tableProperty != null) {
            return tableProperty.isAutoBucket();
        }
        return false;
    }

    public void setIsAutoBucket(boolean isAutoBucket) {
        getOrCreatTableProperty().modifyTableProperties(PropertyAnalyzer.PROPERTIES_AUTO_BUCKET,
                Boolean.valueOf(isAutoBucket).toString());
    }

    public void setEstimatePartitionSize(String estimatePartitionSize) {
        getOrCreatTableProperty().modifyTableProperties(PropertyAnalyzer.PROPERTIES_ESTIMATE_PARTITION_SIZE,
                estimatePartitionSize);
    }

    public String getEstimatePartitionSize() {
        if (tableProperty != null) {
            return tableProperty.getEstimatePartitionSize();
        }
        return "";
    }

    public boolean getEnableLightSchemaChange() {
        if (tableProperty != null) {
            return tableProperty.getUseSchemaLightChange();
        }
        // property is set false by default
        return false;
    }

    public void setEnableLightSchemaChange(boolean enableLightSchemaChange) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_ENABLE_LIGHT_SCHEMA_CHANGE,
                Boolean.valueOf(enableLightSchemaChange).toString());
        tableProperty.buildEnableLightSchemaChange();
    }

    public void setStoragePolicy(String storagePolicy) throws UserException {
        if (!Config.enable_storage_policy && !Strings.isNullOrEmpty(storagePolicy)) {
            throw new UserException("storage policy feature is disabled by default. "
                    + "Enable it by setting 'enable_storage_policy=true' in fe.conf");
        }
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY, storagePolicy);
        tableProperty.buildStoragePolicy();
        partitionInfo.refreshTableStoragePolicy(storagePolicy);
    }

    public String getStoragePolicy() {
        if (tableProperty != null) {
            return tableProperty.getStoragePolicy();
        }
        return "";
    }

    public void setDisableAutoCompaction(boolean disableAutoCompaction) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_DISABLE_AUTO_COMPACTION,
                Boolean.valueOf(disableAutoCompaction).toString());
        tableProperty.buildDisableAutoCompaction();
    }

    public Boolean disableAutoCompaction() {
        if (tableProperty != null) {
            return tableProperty.disableAutoCompaction();
        }
        return false;
    }

    public void setEnableSingleReplicaCompaction(boolean enableSingleReplicaCompaction) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_ENABLE_SINGLE_REPLICA_COMPACTION,
                Boolean.valueOf(enableSingleReplicaCompaction).toString());
        tableProperty.buildEnableSingleReplicaCompaction();
    }

    public Boolean enableSingleReplicaCompaction() {
        if (tableProperty != null) {
            return tableProperty.enableSingleReplicaCompaction();
        }
        return false;
    }

    public void setStoreRowColumn(boolean storeRowColumn) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_STORE_ROW_COLUMN,
                Boolean.valueOf(storeRowColumn).toString());
        tableProperty.buildStoreRowColumn();
    }

    public Boolean storeRowColumn() {
        if (tableProperty != null) {
            return tableProperty.storeRowColumn();
        }
        return false;
    }

    public void setSkipWriteIndexOnLoad(boolean skipWriteIndexOnLoad) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_SKIP_WRITE_INDEX_ON_LOAD,
                Boolean.valueOf(skipWriteIndexOnLoad).toString());
        tableProperty.buildSkipWriteIndexOnLoad();
    }

    public Boolean skipWriteIndexOnLoad() {
        if (tableProperty != null) {
            return tableProperty.skipWriteIndexOnLoad();
        }
        return false;
    }

    public void setCompactionPolicy(String compactionPolicy) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_COMPACTION_POLICY, compactionPolicy);
        tableProperty.buildCompactionPolicy();
    }

    public String getCompactionPolicy() {
        if (tableProperty != null) {
            return tableProperty.compactionPolicy();
        }
        return "";
    }

    public void setTimeSeriesCompactionGoalSizeMbytes(long timeSeriesCompactionGoalSizeMbytes) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES,
                                                        Long.valueOf(timeSeriesCompactionGoalSizeMbytes).toString());
        tableProperty.buildTimeSeriesCompactionGoalSizeMbytes();
    }

    public Long getTimeSeriesCompactionGoalSizeMbytes() {
        if (tableProperty != null) {
            return tableProperty.timeSeriesCompactionGoalSizeMbytes();
        }
        return null;
    }

    public void setTimeSeriesCompactionFileCountThreshold(long timeSeriesCompactionFileCountThreshold) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD,
                                                    Long.valueOf(timeSeriesCompactionFileCountThreshold).toString());
        tableProperty.buildTimeSeriesCompactionFileCountThreshold();
    }

    public Long getTimeSeriesCompactionFileCountThreshold() {
        if (tableProperty != null) {
            return tableProperty.timeSeriesCompactionFileCountThreshold();
        }
        return null;
    }

    public void setTimeSeriesCompactionTimeThresholdSeconds(long timeSeriesCompactionTimeThresholdSeconds) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer
                                                    .PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS,
                                                    Long.valueOf(timeSeriesCompactionTimeThresholdSeconds).toString());
        tableProperty.buildTimeSeriesCompactionTimeThresholdSeconds();
    }

    public Long getTimeSeriesCompactionTimeThresholdSeconds() {
        if (tableProperty != null) {
            return tableProperty.timeSeriesCompactionTimeThresholdSeconds();
        }
        return null;
    }

    public Boolean isDynamicSchema() {
        if (tableProperty != null) {
            return tableProperty.isDynamicSchema();
        }
        return false;
    }

    public void setIsDynamicSchema(boolean isDynamicSchema) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(
                PropertyAnalyzer.PROPERTIES_DYNAMIC_SCHEMA, Boolean.valueOf(isDynamicSchema).toString());
        tableProperty.buildDynamicSchema();
    }

    public int getBaseSchemaVersion() {
        MaterializedIndexMeta baseIndexMeta = indexIdToMeta.get(baseIndexId);
        return baseIndexMeta.getSchemaVersion();
    }

    public int getIndexSchemaVersion(long indexId) {
        MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
        return indexMeta.getSchemaVersion();
    }

    public void setDataSortInfo(DataSortInfo dataSortInfo) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyDataSortInfoProperties(dataSortInfo);
        tableProperty.buildDataSortInfo();
    }

    // return true if partition with given name already exist, both in partitions and temp partitions.
    // return false otherwise
    public boolean checkPartitionNameExist(String partitionName) {
        if (nameToPartition.containsKey(partitionName)) {
            return true;
        }
        return tempPartitions.hasPartition(partitionName);
    }

    // if includeTempPartition is true, check if temp partition with given name exist,
    // if includeTempPartition is false, check if normal partition with given name exist.
    // return true if exist, otherwise, return false;
    public boolean checkPartitionNameExist(String partitionName, boolean isTempPartition) {
        if (isTempPartition) {
            return tempPartitions.hasPartition(partitionName);
        } else {
            return nameToPartition.containsKey(partitionName);
        }
    }

    // drop temp partition. if needDropTablet is true, tablets of this temp partition
    // will be dropped from tablet inverted index.
    public void dropTempPartition(String partitionName, boolean needDropTablet) {
        Partition partition = getPartition(partitionName, true);
        if (partition != null) {
            partitionInfo.dropPartition(partition.getId());
            tempPartitions.dropPartition(partitionName, needDropTablet);
        }
    }

    /*
     * replace partitions in 'partitionNames' with partitions in 'tempPartitionNames'.
     * If strictRange is true, the replaced ranges must be exactly same.
     * What is "exactly same"?
     *      1. {[0, 10), [10, 20)} === {[0, 20)}
     *      2. {[0, 10), [15, 20)} === {[0, 10), [15, 18), [18, 20)}
     *      3. {[0, 10), [15, 20)} === {[0, 10), [15, 20)}
     *      4. {[0, 10), [15, 20)} !== {[0, 20)}
     *
     * If useTempPartitionName is false and replaced partition number are equal,
     * the replaced partitions' name will remain unchanged.
     * What is "remain unchange"?
     *      1. replace partition (p1, p2) with temporary partition (tp1, tp2). After replacing, the partition
     *         names are still p1 and p2.
     *
     */
    public void replaceTempPartitions(List<String> partitionNames, List<String> tempPartitionNames,
            boolean strictRange, boolean useTempPartitionName) throws DdlException {
        // check partition items
        checkPartition(partitionNames, tempPartitionNames, strictRange);

        // begin to replace
        // 1. drop old partitions
        for (String partitionName : partitionNames) {
            // This will also drop all tablets of the partition from TabletInvertedIndex
            dropPartition(-1, partitionName, true);
        }

        // 2. add temp partitions' range info to rangeInfo, and remove them from tempPartitionInfo
        for (String partitionName : tempPartitionNames) {
            Partition partition = tempPartitions.getPartition(partitionName);
            // add
            addPartition(partition);
            // drop
            tempPartitions.dropPartition(partitionName, false);
            // move the range from idToTempRange to idToRange
            partitionInfo.moveFromTempToFormal(partition.getId());
        }

        // change the name so that after replacing, the partition name remain unchanged
        if (!useTempPartitionName && partitionNames.size() == tempPartitionNames.size()) {
            for (int i = 0; i < tempPartitionNames.size(); i++) {
                renamePartition(tempPartitionNames.get(i), partitionNames.get(i));
            }
        }
    }

    private void checkPartition(List<String> partitionNames, List<String> tempPartitionNames,
            boolean strictRange) throws DdlException {
        if (strictRange) {
            List<PartitionItem> list = Lists.newArrayList();
            List<PartitionItem> tempList = Lists.newArrayList();
            for (String partName : partitionNames) {
                Partition partition = nameToPartition.get(partName);
                Preconditions.checkNotNull(partition);
                list.add(partitionInfo.getItem(partition.getId()));
            }
            for (String partName : tempPartitionNames) {
                Partition partition = tempPartitions.getPartition(partName);
                Preconditions.checkNotNull(partition);
                tempList.add(partitionInfo.getItem(partition.getId()));
            }
            partitionInfo.checkPartitionItemListsMatch(list, tempList);
        } else {
            // check after replacing, whether the range will conflict
            Set<Long> replacePartitionIds = Sets.newHashSet();
            for (String partName : partitionNames) {
                Partition partition = nameToPartition.get(partName);
                Preconditions.checkNotNull(partition);
                replacePartitionIds.add(partition.getId());
            }
            // get all items except for partitions in "replacePartitionIds"
            List<PartitionItem> currentItemList = partitionInfo.getItemList(replacePartitionIds, false);

            List<PartitionItem> replacePartitionItems = Lists.newArrayList();
            for (String partName : tempPartitionNames) {
                Partition partition = tempPartitions.getPartition(partName);
                Preconditions.checkNotNull(partition);
                replacePartitionItems.add(partitionInfo.getItem(partition.getId()));
            }

            partitionInfo.checkPartitionItemListsConflict(currentItemList, replacePartitionItems);
        }
    }

    public void addTempPartition(Partition partition) {
        tempPartitions.addPartition(partition);
    }

    public void dropAllTempPartitions() {
        for (Partition partition : tempPartitions.getAllPartitions()) {
            partitionInfo.dropPartition(partition.getId());
        }
        tempPartitions.dropAll();
    }

    public boolean existTempPartitions() {
        return !tempPartitions.isEmpty();
    }

    public void setCompressionType(TCompressionType compressionType) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_COMPRESSION, compressionType.name());
        tableProperty.buildCompressionType();
    }

    public void setStorageFormat(TStorageFormat storageFormat) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT, storageFormat.name());
        tableProperty.buildStorageFormat();
    }

    public TStorageFormat getStorageFormat() {
        if (tableProperty == null) {
            return TStorageFormat.DEFAULT;
        }
        return tableProperty.getStorageFormat();
    }

    public TCompressionType getCompressionType() {
        if (tableProperty == null) {
            return TCompressionType.LZ4F;
        }
        return tableProperty.getCompressionType();
    }

    public DataSortInfo getDataSortInfo() {
        return getOrCreatTableProperty().getDataSortInfo();
    }

    public void setEnableUniqueKeyMergeOnWrite(boolean speedup) {
        getOrCreatTableProperty().setEnableUniqueKeyMergeOnWrite(speedup);
    }

    public boolean getEnableUniqueKeyMergeOnWrite() {
        if (tableProperty == null) {
            return false;
        }
        return tableProperty.getEnableUniqueKeyMergeOnWrite();
    }

    public boolean isDuplicateWithoutKey() {
        return getKeysType() == KeysType.DUP_KEYS && getKeysNum() == 0;
    }

    // For non partitioned table:
    //   The table's distribute hash columns need to be a subset of the aggregate columns.
    //
    // For partitioned table:
    //   1. The table's partition columns need to be a subset of the table's hash columns.
    //   2. The table's distribute hash columns need to be a subset of the aggregate columns.
    public boolean meetAggDistributionRequirements(AggregateInfo aggregateInfo) {
        ArrayList<Expr> groupingExps = aggregateInfo.getGroupingExprs();
        if (groupingExps == null || groupingExps.isEmpty()) {
            return false;
        }
        List<Expr> partitionExps = aggregateInfo.getPartitionExprs() != null
                ? aggregateInfo.getPartitionExprs()
                : groupingExps;
        DistributionInfo distribution = getDefaultDistributionInfo();
        if (distribution instanceof HashDistributionInfo) {
            List<Column> distributeColumns = ((HashDistributionInfo) distribution).getDistributionColumns();
            PartitionInfo partitionInfo = getPartitionInfo();
            if (partitionInfo instanceof RangePartitionInfo) {
                List<Column> rangeColumns = partitionInfo.getPartitionColumns();
                if (!distributeColumns.containsAll(rangeColumns)) {
                    return false;
                }
            }
            List<SlotRef> partitionSlots = partitionExps.stream().map(Expr::unwrapSlotRef).collect(Collectors.toList());
            if (partitionSlots.contains(null)) {
                return false;
            }
            List<Column> hashColumns = partitionSlots.stream()
                    .map(SlotRef::getDesc).map(SlotDescriptor::getColumn).collect(Collectors.toList());
            return hashColumns.containsAll(distributeColumns);
        }
        return false;
    }

    // for ut
    public void checkReplicaAllocation() throws UserException {
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        for (Partition partition : getPartitions()) {
            ReplicaAllocation replicaAlloc = getPartitionInfo().getReplicaAllocation(partition.getId());
            Map<Tag, Short> allocMap = replicaAlloc.getAllocMap();
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                for (Tablet tablet : mIndex.getTablets()) {
                    Map<Tag, Short> curMap = Maps.newHashMap();
                    for (Replica replica : tablet.getReplicas()) {
                        Backend be = infoService.getBackend(replica.getBackendId());
                        if (be == null || !be.isMixNode()) {
                            continue;
                        }
                        short num = curMap.getOrDefault(be.getLocationTag(), (short) 0);
                        curMap.put(be.getLocationTag(), (short) (num + 1));
                    }
                    if (!curMap.equals(allocMap)) {
                        throw new UserException(
                                "replica allocation of tablet " + tablet.getId() + " is not expected" + ", expected: "
                                        + allocMap.toString() + ", actual: " + curMap.toString());
                    }
                }
            }
        }
    }

    public void setReplicaAllocation(Map<String, String> properties) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(properties);
        } else {
            tableProperty.modifyTableProperties(properties);
        }
        tableProperty.buildReplicaAllocation();
    }

    // for light schema change
    public void initSchemaColumnUniqueId() {
        if (!getEnableLightSchemaChange()) {
            return;
        }

        for (MaterializedIndexMeta indexMeta : indexIdToMeta.values()) {
            indexMeta.initSchemaColumnUniqueId();
        }
    }

    public Set<Long> getPartitionKeys() {
        return idToPartition.keySet();
    }

    public boolean isDupKeysOrMergeOnWrite() {
        return getKeysType() == KeysType.DUP_KEYS
                || (getKeysType() == KeysType.UNIQUE_KEYS
                && getEnableUniqueKeyMergeOnWrite());
    }

    /**
     * generate two phase read fetch option from this olap table.
     *
     * @param selectedIndexId the index want to scan
     */
    public TFetchOption generateTwoPhaseReadOption(long selectedIndexId) {
        TFetchOption fetchOption = new TFetchOption();
        fetchOption.setFetchRowStore(this.storeRowColumn());
        fetchOption.setUseTwoPhaseFetch(true);
        fetchOption.setNodesInfo(SystemInfoService.createAliveNodesInfo());
        if (!this.storeRowColumn()) {
            List<TColumn> columnsDesc = Lists.newArrayList();
            getColumnDesc(selectedIndexId, columnsDesc, null, null);
            fetchOption.setColumnDesc(columnsDesc);
        }
        return fetchOption;
    }

    public void getColumnDesc(long selectedIndexId, List<TColumn> columnsDesc, List<String> keyColumnNames,
            List<TPrimitiveType> keyColumnTypes) {
        if (selectedIndexId != -1) {
            for (Column col : this.getSchemaByIndexId(selectedIndexId, true)) {
                TColumn tColumn = col.toThrift();
                col.setIndexFlag(tColumn, this);
                if (columnsDesc != null) {
                    columnsDesc.add(tColumn);
                }
                if ((Util.showHiddenColumns() || (!Util.showHiddenColumns() && col.isVisible())) && col.isKey()) {
                    if (keyColumnNames != null) {
                        keyColumnNames.add(col.getName());
                    }
                    if (keyColumnTypes != null) {
                        keyColumnTypes.add(col.getDataType().toThrift());
                    }
                }
            }
        }
    }

    @Override
    public void analyze(String dbName) {
        for (MaterializedIndexMeta meta : indexIdToMeta.values()) {
            try {
                ConnectContext connectContext = new ConnectContext();
                connectContext.setCluster(SystemInfoService.DEFAULT_CLUSTER);
                connectContext.setDatabase(dbName);
                Analyzer analyzer = new Analyzer(Env.getCurrentEnv(), connectContext);
                meta.parseStmt(analyzer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
