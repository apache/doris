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
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.CreateTableStmt;
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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.DeepCopy;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.RangeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TOlapTable;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.Adler32;

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

    public TableIndexes getTableIndexes() {
        return indexes;
    }

    public Map<String, Index> getIndexesMap() {
        Map<String, Index> indexMap = new HashMap<>();
        if (indexes != null) {
            Optional.ofNullable(indexes.getIndexes()).orElse(Collections.emptyList()).stream().forEach(
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
        setIndexMeta(indexId, indexName, schema, schemaVersion, schemaHash, shortKeyColumnCount, storageType, keysType,
                null);
    }

    public void setIndexMeta(long indexId, String indexName, List<Column> schema, int schemaVersion, int schemaHash,
            short shortKeyColumnCount, TStorageType storageType, KeysType keysType, OriginStatement origStmt) {
        // Nullable when meta comes from schema change log replay.
        // The replay log only save the index id, so we need to get name by id.
        if (indexName == null) {
            indexName = getIndexNameById(indexId);
            Preconditions.checkState(indexName != null);
        }
        // Nullable when meta is less then VERSION_74
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

        MaterializedIndexMeta indexMeta = new MaterializedIndexMeta(indexId, schema, schemaVersion,
                schemaHash, shortKeyColumnCount, storageType, keysType, origStmt);
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
                if (!nameToColumn.containsKey(column.getName())) {
                    fullSchema.add(column);
                    nameToColumn.put(column.getName(), column);
                }
            }
        }
        LOG.debug("after rebuild full schema. table {}, schema: {}", id, fullSchema);
    }

    public boolean deleteIndexInfo(String indexName) {
        if (!indexNameToId.containsKey(indexName)) {
            return false;
        }

        long indexId = this.indexNameToId.remove(indexName);
        this.indexIdToMeta.remove(indexId);
        // Some column of deleted index should be removed during `deleteIndexInfo` such as `mv_bitmap_union_c1`
        // If deleted index id == base index id, the schema will not be rebuilt.
        // The reason is that the base index has been removed from indexIdToMeta while the new base index hasn't changed.
        // The schema could not be rebuild in here with error base index id.
        if (indexId != baseIndexId) {
            rebuildFullSchema();
        }
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
        Partition partition = idToPartition.values().stream().findFirst().get();
        return partition.getMaterializedIndices(IndexExtState.VISIBLE);
    }

    public Column getVisibleColumn(String columnName) {
        for (MaterializedIndexMeta meta : getVisibleIndexIdToMeta().values()) {
            for (Column column : meta.getSchema()) {
                if (column.getName().equalsIgnoreCase(columnName)) {
                    return column;
                }
            }
        }
        return null;
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

    public Status resetIdsForRestore(Catalog catalog, Database db, int restoreReplicationNum) {
        // table id
        id = catalog.getNextId();

        // copy an origin index id to name map
        Map<Long, String> origIdxIdToName = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : indexNameToId.entrySet()) {
            origIdxIdToName.put(entry.getValue(), entry.getKey());
        }

        // reset all 'indexIdToXXX' map
        for (Map.Entry<Long, String> entry : origIdxIdToName.entrySet()) {
            long newIdxId = catalog.getNextId();
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
        if (partitionInfo.getType() == PartitionType.RANGE) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            for (Map.Entry<String, Long> entry : origPartNameToId.entrySet()) {
                long newPartId = catalog.getNextId();
                rangePartitionInfo.idToDataProperty.put(newPartId,
                                                        rangePartitionInfo.idToDataProperty.remove(entry.getValue()));
                rangePartitionInfo.idToReplicationNum.remove(entry.getValue());
                rangePartitionInfo.idToReplicationNum.put(newPartId,
                                                          (short) restoreReplicationNum);
                rangePartitionInfo.getIdToRange(false).put(newPartId,
                        rangePartitionInfo.getIdToRange(false).remove(entry.getValue()));

                rangePartitionInfo.idToInMemory.put(newPartId, rangePartitionInfo.idToInMemory.remove(entry.getValue()));
                idToPartition.put(newPartId, idToPartition.remove(entry.getValue()));
            }
        } else {
            // Single partitioned
            long newPartId = catalog.getNextId();
            for (Map.Entry<String, Long> entry : origPartNameToId.entrySet()) {
                partitionInfo.idToDataProperty.put(newPartId, partitionInfo.idToDataProperty.remove(entry.getValue()));
                partitionInfo.idToReplicationNum.remove(entry.getValue());
                partitionInfo.idToReplicationNum.put(newPartId, (short) restoreReplicationNum);
                partitionInfo.idToInMemory.put(newPartId, partitionInfo.idToInMemory.remove(entry.getValue()));
                idToPartition.put(newPartId, idToPartition.remove(entry.getValue()));
            }
        }

        // for each partition, reset rollup index map
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            Partition partition = entry.getValue();
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
                    long newTabletId = catalog.getNextId();
                    Tablet newTablet = new Tablet(newTabletId);
                    idx.addTablet(newTablet, null /* tablet meta */, true /* is restore */);

                    // replicas
                    List<Long> beIds = Catalog.getCurrentSystemInfo().seqChooseBackendIds(partitionInfo.getReplicationNum(entry.getKey()),
                                                                                          true, true,
                                                                                          db.getClusterName());
                    if (beIds == null) {
                        return new Status(ErrCode.COMMON_ERROR, "failed to find "
                                + partitionInfo.getReplicationNum(entry.getKey())
                                + " different hosts to create table: " + name);
                    }
                    for (Long beId : beIds) {
                        long newReplicaId = catalog.getNextId();
                        Replica replica = new Replica(newReplicaId, beId, ReplicaState.NORMAL,
                                partition.getVisibleVersion(), partition.getVisibleVersionHash(), schemaHash);
                        newTablet.addReplica(replica, true /* is restore */);
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
            return indexIdToMeta.get(indexId).getSchema().stream().filter(column ->
                    column.isVisible()).collect(Collectors.toList());
        }
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

    public Set<String> getPartitionColumnNames() {
        Set<String> partitionColumnNames = Sets.newHashSet();
        if (partitionInfo instanceof SinglePartitionInfo) {
            return partitionColumnNames;
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
        List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
        for (Column column : partitionColumns) {
            partitionColumnNames.add(column.getName().toLowerCase());
        }
        return partitionColumnNames;
    }

    public DistributionInfo getDefaultDistributionInfo() {
        return defaultDistributionInfo;
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

    public Partition dropPartition(long dbId, String partitionName, boolean isForceDrop) {
        Partition partition = nameToPartition.get(partitionName);
        if (partition != null) {
            idToPartition.remove(partition.getId());
            nameToPartition.remove(partitionName);

            Preconditions.checkState(partitionInfo.getType() == PartitionType.RANGE);
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            
            if (!isForceDrop) {
                // recycle partition
                Catalog.getCurrentRecycleBin().recyclePartition(dbId, id, partition,
                                          rangePartitionInfo.getRange(partition.getId()),
                                          rangePartitionInfo.getDataProperty(partition.getId()),
                                          rangePartitionInfo.getReplicationNum(partition.getId()),
                                          rangePartitionInfo.getIsInMemory(partition.getId()));
            }

            // drop partition info
            rangePartitionInfo.dropPartition(partition.getId());
        }
        return partition;
    }

    public Partition dropPartitionForBackup(String partitionName) {
        return dropPartition(-1, partitionName, true);
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

    public void setSequenceInfo(Type type) {
        this.hasSequenceCol = true;
        this.sequenceType = type;

        // sequence column is value column with REPLACE aggregate type
        Column sequenceCol = ColumnDef.newSequenceColumnDef(type, AggregateType.REPLACE).toColumn();
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

    public long getRowCount() {
        long rowCount = 0;
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            rowCount += entry.getValue().getBaseIndex().getRowCount();
        }
        return rowCount;
    }

    @Override
    public CreateTableStmt toCreateTableStmt(String dbName) {
        throw new RuntimeException("Don't support anymore");
    }

    public int getSignature(int signatureVersion, List<String> partNames) {
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);
        final String charsetName = "UTF-8";

        try {
            // table name
            adler32.update(name.getBytes(charsetName));
            LOG.debug("signature. table name: {}", name);
            // type
            adler32.update(type.name().getBytes(charsetName));
            LOG.debug("signature. table type: {}", type.name());

            // all indices(should be in order)
            Set<String> indexNames = Sets.newTreeSet();
            indexNames.addAll(indexNameToId.keySet());
            for (String indexName : indexNames) {
                long indexId = indexNameToId.get(indexName);
                adler32.update(indexName.getBytes(charsetName));
                LOG.debug("signature. index name: {}", indexName);
                MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
                // schema hash
                adler32.update(indexMeta.getSchemaHash());
                LOG.debug("signature. index schema hash: {}", indexMeta.getSchemaHash());
                // short key column count
                adler32.update(indexMeta.getShortKeyColumnCount());
                LOG.debug("signature. index short key: {}", indexMeta.getShortKeyColumnCount());
                // storage type
                adler32.update(indexMeta.getStorageType().name().getBytes(charsetName));
                LOG.debug("signature. index storage type: {}", indexMeta.getStorageType());
            }

            // bloom filter
            if (bfColumns != null && !bfColumns.isEmpty()) {
                for (String bfCol : bfColumns) {
                    adler32.update(bfCol.getBytes());
                    LOG.debug("signature. bf col: {}", bfCol);
                }
                adler32.update(String.valueOf(bfFpp).getBytes());
                LOG.debug("signature. bf fpp: {}", bfFpp);
            }

            // partition type
            adler32.update(partitionInfo.getType().name().getBytes(charsetName));
            LOG.debug("signature. partition type: {}", partitionInfo.getType().name());
            // partition columns
            if (partitionInfo.getType() == PartitionType.RANGE) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
                adler32.update(Util.schemaHash(0, partitionColumns, null, 0));
                LOG.debug("signature. partition col hash: {}", Util.schemaHash(0, partitionColumns, null, 0));
            }

            // partition and distribution
            Collections.sort(partNames, String.CASE_INSENSITIVE_ORDER);
            for (String partName : partNames) {
                Partition partition = getPartition(partName);
                Preconditions.checkNotNull(partition, partName);
                adler32.update(partName.getBytes(charsetName));
                LOG.debug("signature. partition name: {}", partName);
                DistributionInfo distributionInfo = partition.getDistributionInfo();
                adler32.update(distributionInfo.getType().name().getBytes(charsetName));
                if (distributionInfo.getType() == DistributionInfoType.HASH) {
                    HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                    adler32.update(Util.schemaHash(0, hashDistributionInfo.getDistributionColumns(), null, 0));
                    LOG.debug("signature. distribution col hash: {}",
                              Util.schemaHash(0, hashDistributionInfo.getDistributionColumns(), null, 0));
                    adler32.update(hashDistributionInfo.getBucketNum());
                    LOG.debug("signature. bucket num: {}", hashDistributionInfo.getBucketNum());
                }
            }

        } catch (UnsupportedEncodingException e) {
            LOG.error("encoding error", e);
            return -1;
        }

        LOG.debug("signature: {}", Math.abs((int) adler32.getValue()));
        return Math.abs((int) adler32.getValue());
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

        //colocateTable
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

            if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_75) {
                // schema
                int colCount = in.readInt();
                List<Column> schema = new LinkedList<>();
                for (int j = 0; j < colCount; j++) {
                    Column column = Column.read(in);
                    schema.add(column);
                }

                // storage type
                TStorageType storageType = TStorageType.valueOf(Text.readString(in));

                // indices's schema version
                int schemaVersion = in.readInt();

                // indices's schema hash
                int schemaHash = in.readInt();

                // indices's short key column count
                short shortKeyColumnCount = in.readShort();

                // The keys type in here is incorrect
                MaterializedIndexMeta indexMeta = new MaterializedIndexMeta(indexId, schema,
                        schemaVersion, schemaHash, shortKeyColumnCount, storageType, KeysType.AGG_KEYS, null);
                tmpIndexMetaList.add(indexMeta);
            } else {
                MaterializedIndexMeta indexMeta = MaterializedIndexMeta.read(in);
                indexIdToMeta.put(indexId, indexMeta);
            }
        }

        // partition and distribution info
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
            keysType = KeysType.valueOf(Text.readString(in));
        } else {
            keysType = KeysType.AGG_KEYS;
        }

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

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_9) {
            if (in.readBoolean()) {
                int bfColumnCount = in.readInt();
                bfColumns = Sets.newHashSet();
                for (int i = 0; i < bfColumnCount; i++) {
                    bfColumns.add(Text.readString(in));
                }

                bfFpp = in.readDouble();
            }
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_46) {
            if (in.readBoolean()) {
                colocateGroup = Text.readString(in);
            }
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_57) {
            baseIndexId = in.readLong();
        } else {
            // the old table use table id as base index id
            baseIndexId = id;
        }

        // read indexes
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_70) {
            if (in.readBoolean()) {
                this.indexes = TableIndexes.read(in);
            }
        }
        // tableProperty
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_71) {
            if (in.readBoolean()) {
                tableProperty = TableProperty.read(in);
            }
        }

        // temp partitions
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_74) {
            tempPartitions = TempPartitions.read(in);
            if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_77) {
                RangePartitionInfo tempRangeInfo = tempPartitions.getPartitionInfo();
                if (tempRangeInfo != null) {
                    for (long partitionId : tempRangeInfo.getIdToRange(false).keySet()) {
                        ((RangePartitionInfo) this.partitionInfo).addPartition(partitionId, true,
                                tempRangeInfo.getRange(partitionId), tempRangeInfo.getDataProperty(partitionId),
                                tempRangeInfo.getReplicationNum(partitionId), tempRangeInfo.getIsInMemory(partitionId));
                    }
                }
                tempPartitions.unsetPartitionInfo();
            }
        }

        // In the present, the fullSchema could be rebuilt by schema change while the properties is changed by MV.
        // After that, some properties of fullSchema and nameToColumn may be not same as properties of base columns.
        // So, here we need to rebuild the fullSchema to ensure the correctness of the properties.
        rebuildFullSchema();
    }

    @Override
    public boolean equals(Table table) {
        if (this == table) {
            return true;
        }
        return table instanceof OlapTable;
    }

    public OlapTable selectiveCopy(Collection<String> reservedPartitions, boolean resetState, IndexExtState extState) {
        OlapTable copied = new OlapTable();
        if (!DeepCopy.copy(this, copied, OlapTable.class)) {
            LOG.warn("failed to copy olap table: " + getName());
            return null;
        }
        
        if (resetState) {
            // remove shadow index from copied table
            List<MaterializedIndex> shadowIndex = copied.getPartitions().stream().findFirst().get().getMaterializedIndices(IndexExtState.SHADOW);
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
                copied.getPartitionInfo().setDataProperty(partition.getId(), new DataProperty(TStorageMedium.HDD));
                for (MaterializedIndex idx : partition.getMaterializedIndices(extState)) {
                    idx.setState(IndexState.NORMAL);
                    for (Tablet tablet : idx.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            replica.setState(ReplicaState.NORMAL);
                        }
                    }
                }
            }
        }

        if (reservedPartitions == null || reservedPartitions.isEmpty()) {
            // reserve all
            return copied;
        }
        
        Set<String> partNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        partNames.addAll(copied.getPartitionNames());
        
        for (String partName : partNames) {
            if (!reservedPartitions.contains(partName)) {
                copied.dropPartitionForBackup(partName);
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
        short replicationNum = partitionInfo.getReplicationNum(oldPartition.getId());
        boolean isInMemory = partitionInfo.getIsInMemory(oldPartition.getId());

        if (partitionInfo.getType() == PartitionType.RANGE) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            Range<PartitionKey> range = rangePartitionInfo.getRange(oldPartition.getId());
            rangePartitionInfo.dropPartition(oldPartition.getId());
            rangePartitionInfo.addPartition(newPartition.getId(), false, range, dataProperty,
                    replicationNum, isInMemory);
        } else {
            partitionInfo.dropPartition(oldPartition.getId());
            partitionInfo.addPartition(newPartition.getId(), dataProperty, replicationNum, isInMemory);
        }

        return oldPartition;
    }

    public long getDataSize() {
        long dataSize = 0;
        for (Partition partition : getAllPartitions()) {
            dataSize += partition.getDataSize();
        }
        return dataSize;
    }

    public long getReplicaCount() {
        long replicaCount = 0;
        for (Partition partition : getAllPartitions()) {
            replicaCount += partition.getReplicaCount();
        }
        return replicaCount;
    }

    public void checkStableAndNormal(String clusterName) throws DdlException {
        if (state != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + name + "]'s state is not NORMAL. "
                    + "Do not allow doing materialized view");
        }
        // check if all tablets are healthy, and no tablet is in tablet scheduler
        boolean isStable = isStable(Catalog.getCurrentSystemInfo(),
                Catalog.getCurrentCatalog().getTabletScheduler(),
                clusterName);
        if (!isStable) {
            throw new DdlException("table [" + name + "] is not stable."
                    + " Some tablets of this table may not be healthy or are being "
                    + "scheduled."
                    + " You need to repair the table first"
                    + " or stop cluster balance. See 'help admin;'.");
        }
    }

    public boolean isStable(SystemInfoService infoService, TabletScheduler tabletScheduler, String clusterName) {
        List<Long> aliveBeIdsInCluster = infoService.getClusterBackendIds(clusterName, true);
        for (Partition partition : idToPartition.values()) {
            long visibleVersion = partition.getVisibleVersion();
            long visibleVersionHash = partition.getVisibleVersionHash();
            short replicationNum = partitionInfo.getReplicationNum(partition.getId());
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.ALL)) {
                for (Tablet tablet : mIndex.getTablets()) {
                    if (tabletScheduler.containsTablet(tablet.getId())) {
                        LOG.info("table {} is not stable because tablet {} is in tablet scheduler. replicas: {}",
                                id, tablet.getId(), tablet.getReplicas());
                        return false;
                    }

                    Pair<TabletStatus, TabletSchedCtx.Priority> statusPair = tablet.getHealthStatusWithPriority(
                            infoService, clusterName, visibleVersion, visibleVersionHash, replicationNum,
                            aliveBeIdsInCluster);
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
    public List<List<Long>> getArbitraryTabletBucketsSeq() throws DdlException {
        List<List<Long>> backendsPerBucketSeq = Lists.newArrayList();
        for (Partition partition : idToPartition.values()) {
            short replicationNum = partitionInfo.getReplicationNum(partition.getId());
            MaterializedIndex baseIdx = partition.getBaseIndex();
            for (Long tabletId : baseIdx.getTabletIdsInOrder()) {
                Tablet tablet = baseIdx.getTablet(tabletId);
                List<Long> replicaBackendIds = tablet.getNormalReplicaBackendIds();
                if (replicaBackendIds.size() < replicationNum) {
                    // this should not happen, but in case, throw an exception to terminate this process
                    throw new DdlException("Normal replica number of tablet " + tabletId + " is: "
                            + replicaBackendIds.size() + ", which is less than expected: " + replicationNum);
                }
                backendsPerBucketSeq.add(replicaBackendIds.subList(0, replicationNum));
            }
            break;
        }
        return backendsPerBucketSeq;
    }

    /**
     * Get the proximate row count of this table, if you need accurate row count should select count(*) from table.
     * @return proximate row count
     */
    public long proximateRowCount() {
        long totalCount = 0;
        for (Partition partition : getPartitions()) {
            long version = partition.getVisibleVersion();
            long versionHash = partition.getVisibleVersionHash();
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    long tabletRowCount = 0L;
                    for (Replica replica : tablet.getReplicas()) {
                        if (replica.checkVersionCatchUp(version, versionHash, false)
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

    public Column getBaseColumn(String columnName) {
        for (Column column : getBaseSchema()) {
            if (column.getName().equalsIgnoreCase(columnName)){
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

    public boolean convertRandomDistributionToHashDistribution() {
        boolean hasChanged = false;
        List<Column> baseSchema = getBaseSchema();
        if (defaultDistributionInfo.getType() == DistributionInfoType.RANDOM) {
            defaultDistributionInfo = ((RandomDistributionInfo) defaultDistributionInfo).toHashDistributionInfo(baseSchema);
            hasChanged = true;
        }
        
        for (Partition partition : idToPartition.values()) {
            if (partition.convertRandomDistributionToHashDistribution(baseSchema)) {
                hasChanged = true;
            }
        }
        return hasChanged;
    }

    public void setReplicationNum(Short replicationNum) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, replicationNum.toString());
        tableProperty.buildReplicationNum();
    }

    public Short getDefaultReplicationNum() {
        if (tableProperty != null) {
            return tableProperty.getReplicationNum();
        }
        return FeConstants.default_replication_num;
    }

    public Boolean isInMemory() {
        if (tableProperty != null) {
            return tableProperty.IsInMemory();
        }
        return false;
    }

    public void setIsInMemory(boolean isInMemory) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_INMEMORY, Boolean.valueOf(isInMemory).toString());
        tableProperty.buildInMemory();
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
        RangePartitionInfo rangeInfo = (RangePartitionInfo) partitionInfo;

        if (strictRange) {
            // check if range of partitions and temp partitions are exactly same
            List<Range<PartitionKey>> rangeList = Lists.newArrayList();
            List<Range<PartitionKey>> tempRangeList = Lists.newArrayList();
            for (String partName : partitionNames) {
                Partition partition = nameToPartition.get(partName);
                Preconditions.checkNotNull(partition);
                rangeList.add(rangeInfo.getRange(partition.getId()));
            }

            for (String partName : tempPartitionNames) {
                Partition partition = tempPartitions.getPartition(partName);
                Preconditions.checkNotNull(partition);
                tempRangeList.add(rangeInfo.getRange(partition.getId()));
            }
            RangeUtils.checkRangeListsMatch(rangeList, tempRangeList);
        } else {
            // check after replacing, whether the range will conflict
            Set<Long> replacePartitionIds = Sets.newHashSet();
            for (String partName : partitionNames) {
                Partition partition = nameToPartition.get(partName);
                Preconditions.checkNotNull(partition);
                replacePartitionIds.add(partition.getId());
            }
            List<Range<PartitionKey>> replacePartitionRanges = Lists.newArrayList();
            for (String partName : tempPartitionNames) {
                Partition partition = tempPartitions.getPartition(partName);
                Preconditions.checkNotNull(partition);
                replacePartitionRanges.add(rangeInfo.getRange(partition.getId()));
            }
            List<Range<PartitionKey>> sortedRangeList = rangeInfo.getRangeList(replacePartitionIds, false);
            RangeUtils.checkRangeConflict(sortedRangeList, replacePartitionRanges);
        }
        
        // begin to replace
        // 1. drop old partitions
        List<Partition> droppedPartitions = Lists.newArrayList();
        for (String partitionName : partitionNames) {
            Partition partition = dropPartition(-1, partitionName, true);
            droppedPartitions.add(partition);
        }

        // 2. add temp partitions' range info to rangeInfo, and remove them from tempPartitionInfo
        for (String partitionName : tempPartitionNames) {
            Partition partition = tempPartitions.getPartition(partitionName);
            // add
            addPartition(partition);
            // drop
            tempPartitions.dropPartition(partitionName, false);
            // move the range from idToTempRange to idToRange
            rangeInfo.moveRangeFromTempToFormal(partition.getId());
        }

        // 3. delete old partition's tablets in inverted index
        for (Partition partition : droppedPartitions) {
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    Catalog.getCurrentInvertedIndex().deleteTablet(tablet.getId());
                }
            }
        }

        // change the name so that after replacing, the partition name remain unchanged
        if (!useTempPartitionName && partitionNames.size() == tempPartitionNames.size()) {
            for (int i = 0; i < tempPartitionNames.size(); i++) {
                renamePartition(tempPartitionNames.get(i), partitionNames.get(i));
            }
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

    public void setStorageFormat(TStorageFormat storageFormat) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT, storageFormat.name());
        tableProperty.buildStorageFormat();
    }

    public TStorageFormat getStorageFormat() {
        if (tableProperty == null) {
            return TStorageFormat.DEFAULT;
        }
        return tableProperty.getStorageFormat();
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
        List<Expr> partitionExps = aggregateInfo.getPartitionExprs() != null ?
                aggregateInfo.getPartitionExprs() : groupingExps;
        DistributionInfo distribution = getDefaultDistributionInfo();
        if(distribution instanceof HashDistributionInfo) {
            List<Column> distributeColumns =
                    ((HashDistributionInfo)distribution).getDistributionColumns();
            PartitionInfo partitionInfo = getPartitionInfo();
            if (partitionInfo instanceof RangePartitionInfo) {
                List<Column> rangeColumns = ((RangePartitionInfo)partitionInfo).getPartitionColumns();
                if (!distributeColumns.containsAll(rangeColumns)) {
                    return false;
                }
            }
            List<SlotRef> partitionSlots =
                    partitionExps.stream().map(Expr::unwrapSlotRef).collect(Collectors.toList());
            if (partitionSlots.contains(null)) {
                return false;
            }
            List<Column> hashColumns = partitionSlots.stream()
                    .map(SlotRef::getDesc).map(SlotDescriptor::getColumn).collect(Collectors.toList());
            return hashColumns.containsAll(distributeColumns);
        }
        return false;
    }
}
