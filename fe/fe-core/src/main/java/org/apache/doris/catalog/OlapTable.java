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
import org.apache.doris.analysis.CreateMaterializedViewStmt;
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
import org.apache.doris.clone.TabletScheduler;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.VersionHelper;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.DeepCopy;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVVersionSnapshot;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.UseMvHint;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.resource.Tag;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.HistogramTask;
import org.apache.doris.statistics.OlapAnalysisTask;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TFetchOption;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;
import org.apache.doris.thrift.TOlapTable;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TSortType;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Internal representation of tableFamilyGroup-related metadata. A OlaptableFamilyGroup contains several tableFamily.
 * Note: when you add a new olap table property, you should modify TableProperty class
 */
public class OlapTable extends Table implements MTMVRelatedTableIf, GsonPostProcessable {
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

    @SerializedName(value = "tst", alternate = {"state"})
    private volatile OlapTableState state;

    // index id -> index meta
    @SerializedName(value = "itm", alternate = {"indexIdToMeta"})
    private Map<Long, MaterializedIndexMeta> indexIdToMeta = Maps.newHashMap();
    // index name -> index id
    @SerializedName(value = "inti", alternate = {"indexNameToId"})
    private Map<String, Long> indexNameToId = Maps.newHashMap();

    @SerializedName(value = "kt", alternate = {"keysType"})
    private KeysType keysType;
    @Setter
    @SerializedName(value = "pi", alternate = {"partitionInfo"})
    private PartitionInfo partitionInfo;
    @SerializedName(value = "itp", alternate = {"idToPartition"})
    @Getter
    private ConcurrentHashMap<Long, Partition> idToPartition = new ConcurrentHashMap<>();
    // handled in postgsonprocess
    @Getter
    private Map<String, Partition> nameToPartition = Maps.newTreeMap();

    @SerializedName(value = "di", alternate = {"distributionInfo"})
    private DistributionInfo defaultDistributionInfo;

    // all info about temporary partitions are save in "tempPartitions"
    @Getter
    @SerializedName(value = "tps", alternate = {"tempPartitions"})
    private TempPartitions tempPartitions = new TempPartitions();

    // bloom filter columns
    @SerializedName(value = "bfc", alternate = {"bfColumns"})
    private Set<String> bfColumns;

    @SerializedName(value = "bfFpp")
    private double bfFpp;

    @SerializedName(value = "cgs", alternate = "colocateGroup")
    private String colocateGroup;

    private boolean hasSequenceCol;
    private Type sequenceType;

    @SerializedName(value = "indexes")
    private TableIndexes indexes;

    // In former implementation, base index id is same as table id.
    // But when refactoring the process of alter table job, we find that
    // using same id is not suitable for our new framework.
    // So we add this 'baseIndexId' to explicitly specify the base index id,
    // which should be different with table id.
    // The init value is -1, which means there is not partition and index at all.
    @SerializedName(value = "bid", alternate = {"baseIndexId"})
    private long baseIndexId = -1;

    @SerializedName(value = "tp", alternate = {"tableProperty"})
    private TableProperty tableProperty;

    @SerializedName(value = "aIncg")
    private AutoIncrementGenerator autoIncrementGenerator;

    private volatile Statistics statistics = new Statistics();

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

    public void setStorageVaultName(String storageVaultName) throws DdlException {
        if (storageVaultName == null || storageVaultName.isEmpty()) {
            return;
        }
        getOrCreatTableProperty().setStorageVaultName(storageVaultName);
    }

    public String getStorageVaultName() {
        return getOrCreatTableProperty().getStorageVaultName();
    }

    public void setStorageVaultId(String setStorageVaultId) throws DdlException {
        if (setStorageVaultId == null || setStorageVaultId.isEmpty()) {
            throw new DdlException("Invalid Storage Vault, please set one useful storage vault");
        }
        getOrCreatTableProperty().setStorageVaultId(setStorageVaultId);
    }

    public String getStorageVaultId() {
        return getOrCreatTableProperty().getStorageVaultId();
    }

    public boolean isBeingSynced() {
        return getOrCreatTableProperty().isBeingSynced();
    }

    public boolean isTemporaryPartition(long partitionId) {
        return tempPartitions.hasPartition(partitionId);
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("after rebuild full schema. table {}, schema size: {}", id, fullSchema.size());
        }
    }

    public void rebuildDistributionInfo() {
        if (!Objects.equals(defaultDistributionInfo.getType(), DistributionInfoType.HASH)) {
            return;
        }
        HashDistributionInfo distributionInfo = (HashDistributionInfo) defaultDistributionInfo;
        Set<String> originalColumnsNames =
                distributionInfo.getDistributionColumns()
                        .stream()
                        .map(Column::getName)
                        .collect(Collectors.toSet());

        List<Column> newDistributionColumns = getBaseSchema()
                .stream()
                .filter(column -> originalColumnsNames.contains(column.getName()))
                .map(Column::new)
                .collect(Collectors.toList());
        distributionInfo.setDistributionColumns(newDistributionColumns);

        getPartitions()
                .stream()
                .map(Partition::getDistributionInfo)
                .forEach(info -> ((HashDistributionInfo) info).setDistributionColumns(newDistributionColumns));
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

    public List<Long> getAllTabletIds() {
        List<Long> tabletIds = new ArrayList<>();
        try {
            rwLock.readLock().lock();
            for (Partition partition : getPartitions()) {
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    tabletIds.addAll(index.getTablets().stream()
                                                        .map(tablet -> tablet.getId())
                                                        .collect(Collectors.toList()));
                }
            }
        } catch (Exception e) {
            LOG.warn("get all tablet ids failed {}", e.getMessage());
        } finally {
            rwLock.readLock().unlock();
        }
        return tabletIds;
    }

    public Map<Long, MaterializedIndexMeta> getVisibleIndexIdToMeta() {
        Map<Long, MaterializedIndexMeta> visibleMVs = Maps.newHashMap();
        List<MaterializedIndex> mvs = getVisibleIndex();
        for (MaterializedIndex mv : mvs) {
            visibleMVs.put(mv.getId(), indexIdToMeta.get(mv.getId()));
        }
        return visibleMVs;
    }

    public Long getBestMvIdWithHint(List<Long> orderedMvs) {
        Optional<UseMvHint> useMvHint = getUseMvHint("USE_MV");
        Optional<UseMvHint> noUseMvHint = getUseMvHint("NO_USE_MV");
        if (useMvHint.isPresent() && noUseMvHint.isPresent()) {
            if (noUseMvHint.get().getNoUseMVName(this.name).contains(useMvHint.get().getUseMvName(this.name))) {
                String errorMsg = "conflict mv exist in use_mv and no_use_mv in the same time"
                        + useMvHint.get().getUseMvName(this.name);
                useMvHint.get().setStatus(Hint.HintStatus.SYNTAX_ERROR);
                useMvHint.get().setErrorMessage(errorMsg);
                noUseMvHint.get().setStatus(Hint.HintStatus.SYNTAX_ERROR);
                noUseMvHint.get().setErrorMessage(errorMsg);
            }
            return getMvIdWithUseMvHint(useMvHint.get(), orderedMvs);
        } else if (useMvHint.isPresent()) {
            return getMvIdWithUseMvHint(useMvHint.get(), orderedMvs);
        } else if (noUseMvHint.isPresent()) {
            return getMvIdWithNoUseMvHint(noUseMvHint.get(), orderedMvs);
        }
        return orderedMvs.get(0);
    }

    private Long getMvIdWithUseMvHint(UseMvHint useMvHint, List<Long> orderedMvs) {
        if (useMvHint.isAllMv()) {
            useMvHint.setStatus(Hint.HintStatus.SYNTAX_ERROR);
            useMvHint.setErrorMessage("use_mv hint should only have one mv in one table: "
                    + this.name);
            return orderedMvs.get(0);
        } else {
            String mvName = useMvHint.getUseMvName(this.name);
            if (mvName != null) {
                if (mvName.equals("`*`")) {
                    useMvHint.setStatus(Hint.HintStatus.SYNTAX_ERROR);
                    useMvHint.setErrorMessage("use_mv hint should only have one mv in one table: "
                            + this.name);
                    return orderedMvs.get(0);
                }
                Long choosedIndexId = indexNameToId.get(mvName);
                if (orderedMvs.contains(choosedIndexId)) {
                    useMvHint.setStatus(Hint.HintStatus.SUCCESS);
                    return choosedIndexId;
                } else {
                    useMvHint.setStatus(Hint.HintStatus.SYNTAX_ERROR);
                    useMvHint.setErrorMessage("do not have mv: " + mvName + " in table: " + this.name);
                }
            }
        }
        return orderedMvs.get(0);
    }

    private Long getMvIdWithNoUseMvHint(UseMvHint noUseMvHint, List<Long> orderedMvs) {
        if (noUseMvHint.isAllMv()) {
            noUseMvHint.setStatus(Hint.HintStatus.SUCCESS);
            return getBaseIndex().getId();
        } else {
            List<String> mvNames = noUseMvHint.getNoUseMVName(this.name);
            Set<Long> forbiddenIndexIds = Sets.newHashSet();
            for (int i = 0; i < mvNames.size(); i++) {
                if (mvNames.get(i).equals("`*`")) {
                    noUseMvHint.setStatus(Hint.HintStatus.SUCCESS);
                    return getBaseIndex().getId();
                }
                if (hasMaterializedIndex(mvNames.get(i))) {
                    Long forbiddenIndexId = indexNameToId.get(mvNames.get(i));
                    forbiddenIndexIds.add(forbiddenIndexId);
                } else {
                    noUseMvHint.setStatus(Hint.HintStatus.SYNTAX_ERROR);
                    noUseMvHint.setErrorMessage("do not have mv: " + mvNames.get(i) + " in table: " + this.name);
                    break;
                }
            }
            for (int i = 0; i < orderedMvs.size(); i++) {
                if (forbiddenIndexIds.contains(orderedMvs.get(i))) {
                    noUseMvHint.setStatus(Hint.HintStatus.SUCCESS);
                } else {
                    return orderedMvs.get(i);
                }
            }
        }
        return orderedMvs.get(0);
    }

    private Optional<UseMvHint> getUseMvHint(String useMvName) {
        for (Hint hint : ConnectContext.get().getStatementContext().getHints()) {
            if (hint.isSyntaxError()) {
                continue;
            }
            if (hint.getHintName().equalsIgnoreCase(useMvName)) {
                return Optional.of((UseMvHint) hint);
            }
        }
        return Optional.empty();
    }

    public List<MaterializedIndex> getVisibleIndex() {
        Optional<Partition> partition = idToPartition.values().stream().findFirst();
        if (!partition.isPresent()) {
            partition = tempPartitions.getAllPartitions().stream().findFirst();
        }
        return partition.isPresent() ? partition.get().getMaterializedIndices(IndexExtState.VISIBLE)
                : Collections.emptyList();
    }

    public MaterializedIndex getBaseIndex() {
        Optional<Partition> partition = idToPartition.values().stream().findFirst();
        if (!partition.isPresent()) {
            partition = tempPartitions.getAllPartitions().stream().findFirst();
        }
        return partition.isPresent() ? partition.get().getBaseIndex() : null;
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

    /**
     * This function is for statistics collection only. To get all the index ids that contains the given columnName.
     * For base index, return -1 as its id, this is for compatibility with older version of column stats.
     * @param columnName
     * @return index id list that contains the given columnName.
     */
    public List<Long> getMvColumnIndexIds(String columnName) {
        List<Long> ids = Lists.newArrayList();
        for (MaterializedIndexMeta meta : getVisibleIndexIdToMeta().values()) {
            Column target = meta.getColumnByDefineName(columnName);
            if (target != null) {
                ids.add(meta.getIndexId() == baseIndexId ? -1 : meta.getIndexId());
            }
        }
        return ids;
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
            setBeingSyncedProperties();
        }
        // remove colocate property.
        setColocateGroup(null);
    }

    /**
     * Set the related properties when is_being_synced properties is true.
     *
     * Some properties, like storage_policy, colocate_with, are not supported by the ccr syncer.
     */
    public void setBeingSyncedProperties() {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.setIsBeingSynced();
        tableProperty.removeInvalidProperties();
        if (isAutoBucket()) {
            markAutoBucket();
        }
    }

    public void resetVersionForRestore() {
        for (Partition partition : idToPartition.values()) {
            partition.setNextVersion(partition.getVisibleVersion() + 1);
        }
    }

    public Status resetIdsForRestore(Env env, Database db, ReplicaAllocation restoreReplicaAlloc,
            boolean reserveReplica, String srcDbName) {
        // ATTN: The meta of the restore may come from different clusters, so the
        // original ID in the meta may conflict with the ID of the new cluster. For
        // example, if a newly allocated ID happens to be the same as an original ID,
        // the original one may be overwritten when executing `put`, then causes a
        // NullPointerException.

        // table id
        id = env.getNextId();

        // copy an origin index id to name map
        Map<Long, String> origIdxIdToName = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : indexNameToId.entrySet()) {
            origIdxIdToName.put(entry.getValue(), entry.getKey());
        }

        // reset all 'indexIdToXXX' map
        Map<Long, MaterializedIndexMeta> origIdxIdToMeta = indexIdToMeta;
        indexIdToMeta = Maps.newHashMap();
        for (Map.Entry<Long, String> entry : origIdxIdToName.entrySet()) {
            long newIdxId = env.getNextId();
            if (entry.getValue().equals(name)) {
                // base index
                baseIndexId = newIdxId;
            }
            MaterializedIndexMeta indexMeta = origIdxIdToMeta.get(entry.getKey());
            indexMeta.resetIndexIdForRestore(newIdxId, srcDbName, db.getFullName());
            indexIdToMeta.put(newIdxId, indexMeta);
            indexNameToId.put(entry.getValue(), newIdxId);
        }

        // generate a partition name to id map
        Map<String, Long> origPartNameToId = Maps.newHashMap();
        for (Partition partition : idToPartition.values()) {
            origPartNameToId.put(partition.getName(), partition.getId());
        }

        // reset partition info and idToPartition map
        Map<Long, Long> partitionMap = Maps.newHashMap();
        Map<Long, Partition> origIdToPartition = idToPartition;
        idToPartition = new ConcurrentHashMap<>();
        for (Map.Entry<String, Long> entry : origPartNameToId.entrySet()) {
            long newPartId = env.getNextId();
            idToPartition.put(newPartId, origIdToPartition.get(entry.getValue()));
            partitionMap.put(newPartId, entry.getValue());
        }
        boolean isSinglePartition = partitionInfo.getType() != PartitionType.RANGE
                && partitionInfo.getType() != PartitionType.LIST;
        partitionInfo.resetPartitionIdForRestore(partitionMap,
                reserveReplica ? null : restoreReplicaAlloc, isSinglePartition);

        // for each partition, reset rollup index map
        Map<Tag, Integer> nextIndexes = Maps.newHashMap();
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            Partition partition = entry.getValue();
            long visibleVersion = partition.getVisibleVersion();
            // entry.getKey() is the new partition id, use it to get the restore specified
            // replica allocation
            ReplicaAllocation replicaAlloc = partitionInfo.getReplicaAllocation(entry.getKey());
            // save the materialized indexes before create new index, to avoid ids confliction
            // between two cluster.
            Map<Long, MaterializedIndex> idToIndex = Maps.newHashMap();
            for (Map.Entry<Long, String> entry2 : origIdxIdToName.entrySet()) {
                MaterializedIndex idx = partition.getIndex(entry2.getKey());
                long newIdxId = indexNameToId.get(entry2.getValue());
                idx.setIdForRestore(newIdxId);
                idToIndex.put(newIdxId, idx);
                if (newIdxId != baseIndexId) {
                    // not base table, delete it.
                    partition.deleteRollupIndex(entry2.getKey());
                }
            }
            for (Map.Entry<Long, MaterializedIndex> entry2 : idToIndex.entrySet()) {
                Long idxId = entry2.getKey();
                MaterializedIndex idx = entry2.getValue();
                if (idxId != baseIndexId) {
                    // not base table, add it.
                    partition.createRollupIndex(idx);
                }
                int schemaHash = indexIdToMeta.get(idxId).getSchemaHash();
                // generate new tablets in origin tablet order
                int tabletNum = idx.getTablets().size();
                idx.clearTabletsForRestore();
                for (int i = 0; i < tabletNum; i++) {
                    long newTabletId = env.getNextId();
                    Tablet newTablet = EnvFactory.getInstance().createTablet(newTabletId);
                    idx.addTablet(newTablet, null /* tablet meta */, true /* is restore */);

                    // replicas
                    try {
                        Pair<Map<Tag, List<Long>>, TStorageMedium> tag2beIdsAndMedium =
                                Env.getCurrentSystemInfo().selectBackendIdsForReplicaCreation(
                                        replicaAlloc, nextIndexes, null, false, false);
                        Map<Tag, List<Long>> tag2beIds = tag2beIdsAndMedium.first;
                        for (Map.Entry<Tag, List<Long>> entry3 : tag2beIds.entrySet()) {
                            for (Long beId : entry3.getValue()) {
                                long newReplicaId = env.getNextId();
                                Replica replica = new Replica(newReplicaId, beId, ReplicaState.NORMAL,
                                        visibleVersion, schemaHash);
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

        // reset the indexes and update the indexes in materialized index meta too.
        if (this.indexes != null) {
            List<Index> indexes = this.indexes.getIndexes();
            for (Index idx : indexes) {
                idx.setIndexId(env.getNextId());
            }
            for (Map.Entry<Long, MaterializedIndexMeta> entry : indexIdToMeta.entrySet()) {
                entry.getValue().setIndexes(indexes);
            }
        }

        return Status.OK;
    }

    public int getIndexNumber() {
        return indexIdToMeta.size();
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

    public List<Long> getIndexIdList() {
        List<Long> result = Lists.newArrayList();
        for (Long indexId : indexIdToMeta.keySet()) {
            result.add(indexId);
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

    @Override
    public Set<Column> getSchemaAllIndexes(boolean full) {
        Set<Column> columns = Sets.newHashSet();
        for (Long indexId : indexIdToMeta.keySet()) {
            columns.addAll(getSchemaByIndexId(indexId, full));
        }
        return columns;
    }

    public List<Column> getMvColumns(boolean full) {
        List<Column> columns = Lists.newArrayList();
        for (Long indexId : indexIdToMeta.keySet()) {
            if (indexId == baseIndexId) {
                continue;
            }
            columns.addAll(getSchemaByIndexId(indexId, full));
        }
        return columns;
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

    @Override
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

    @Override
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

    public boolean isRandomDistribution() {
        return defaultDistributionInfo instanceof RandomDistributionInfo;
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

    // Priority is given to querying from the partition. If not found, query from the tempPartition
    @Override
    public Partition getPartition(String partitionName) {
        Partition partition = getPartition(partitionName, false);
        if (partition != null) {
            return partition;
        }
        return getPartition(partitionName, true);
    }

    public Partition getPartitionOrAnalysisException(String partitionName) throws AnalysisException {
        Partition partition = getPartition(partitionName, false);
        if (partition == null) {
            partition = getPartition(partitionName, true);
        }
        if (partition == null) {
            throw new AnalysisException("partition not found: " + partitionName);
        }
        return partition;
    }

    // get partition by name
    public Partition getPartition(String partitionName, boolean isTempPartition) {
        if (isTempPartition) {
            return tempPartitions.getPartition(partitionName);
        } else {
            return nameToPartition.get(partitionName);
        }
    }

    // Priority is given to querying from the partition. If not found, query from the tempPartition
    public Partition getPartition(long partitionId) {
        Partition partition = idToPartition.get(partitionId);
        if (partition == null) {
            partition = tempPartitions.getPartition(partitionId);
        }
        return partition;
    }

    public PartitionItem getPartitionItemOrAnalysisException(String partitionName) throws AnalysisException {
        Partition partition = nameToPartition.get(partitionName);
        if (partition == null) {
            partition = tempPartitions.getPartition(partitionName);
        }
        if (partition == null) {
            throw new AnalysisException("partition not found: " + partitionName);
        }
        return partitionInfo.getItem(partition.getId());
    }

    public Partition getPartitionOrAnalysisException(long partitionId) throws AnalysisException {
        Partition partition = idToPartition.get(partitionId);
        if (partition == null) {
            partition = tempPartitions.getPartition(partitionId);
        }
        if (partition == null) {
            throw new AnalysisException("partition not found: " + partitionId);
        }
        return partition;
    }

    public void getVersionInBatchForCloudMode(Collection<Long> partitionIds) {
        if (Config.isCloudMode()) { // do nothing for non-cloud mode
            List<CloudPartition> partitions = partitionIds.stream()
                    .sorted()
                    .map(this::getPartition)
                    .map(partition -> (CloudPartition) partition)
                    .collect(Collectors.toList());
            try {
                CloudPartition.getSnapshotVisibleVersion(partitions);
            } catch (RpcException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // select the non-empty partition ids belonging to this table.
    //
    // ATTN: partitions not belonging to this table will be filtered.
    public List<Long> selectNonEmptyPartitionIds(Collection<Long> partitionIds) {
        if (Config.isCloudMode() && Config.enable_cloud_snapshot_version) {
            // Assumption: all partitions are CloudPartition.
            List<CloudPartition> partitions = partitionIds.stream()
                    .map(this::getPartition)
                    .filter(p -> p != null)
                    .filter(p -> p instanceof CloudPartition)
                    .map(p -> (CloudPartition) p)
                    .collect(Collectors.toList());
            return CloudPartition.selectNonEmptyPartitionIds(partitions);
        }

        List<Long> nonEmptyIds = Lists.newArrayListWithCapacity(partitionIds.size());
        for (Long partitionId : partitionIds) {
            Partition partition = getPartition(partitionId);
            if (partition != null && partition.hasData()) {
                nonEmptyIds.add(partitionId);
            }
        }
        return nonEmptyIds;
    }

    public int getPartitionNum() {
        return idToPartition.size();
    }

    // get all partitions except temp partitions
    public Collection<Partition> getPartitions() {
        return idToPartition.values();
    }

    // get only temp partitions
    public Collection<Partition> getAllTempPartitions() {
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

    // for those elements equal in partiton ids, get their names.
    public List<String> getEqualPartitionNames(List<Long> partitionIds1, List<Long> partitionIds2) {
        List<String> names = new ArrayList<String>();
        for (int i = 0; i < partitionIds1.size(); i++) {
            if (partitionIds1.get(i).equals(partitionIds2.get(i))) {
                names.add(getPartition(partitionIds1.get(i)).getName());
            }
        }
        return names;
    }

    public List<Long> getPartitionIds() {
        readLock();
        try {
            return new ArrayList<>(idToPartition.keySet());
        } finally {
            readUnlock();
        }
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

    public void setRowStoreColumns(List<String> rowStoreColumns) {
        getOrCreatTableProperty().setRowStoreColumns(rowStoreColumns);
    }

    public List<Integer> getRowStoreColumnsUniqueIds(List<String> rowStoreColumns) {
        List<Integer> columnIds = Lists.newArrayList();
        if (rowStoreColumns != null) {
            for (String colName : rowStoreColumns) {
                Column col = nameToColumn.get(colName);
                Preconditions.checkNotNull(col);
                columnIds.add(col.getUniqueId());
            }
        }
        return columnIds;
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

    public void setEnableMowLightDelete(boolean enable) {
        getOrCreatTableProperty().setEnableMowLightDelete(enable);
    }

    public boolean getEnableMowLightDelete() {
        return getOrCreatTableProperty().getEnableMowLightDelete();
    }

    public void setGroupCommitIntervalMs(int groupCommitInterValMs) {
        getOrCreatTableProperty().setGroupCommitIntervalMs(groupCommitInterValMs);
    }

    public int getGroupCommitIntervalMs() {
        return getOrCreatTableProperty().getGroupCommitIntervalMs();
    }

    public void setGroupCommitDataBytes(int groupCommitInterValMs) {
        getOrCreatTableProperty().setGroupCommitDataBytes(groupCommitInterValMs);
    }

    public int getGroupCommitDataBytes() {
        return getOrCreatTableProperty().getGroupCommitDataBytes();
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
        } else {
            return new OlapAnalysisTask(info);
        }
    }

    @Override
    public Set<Pair<String, String>> getColumnIndexPairs(Set<String> columns) {
        Set<Pair<String, String>> ret = Sets.newHashSet();
        // Check the schema of all indexes for each given column name,
        // If the column name exists in the index, add the <IndexName, ColumnName> pair to return list.
        for (String column : columns) {
            for (MaterializedIndexMeta meta : indexIdToMeta.values()) {
                Column col = meta.getColumnByName(column);
                if (col == null || StatisticsUtil.isUnsupportedType(col.getType())) {
                    continue;
                }
                ret.add(Pair.of(getIndexNameById(meta.getIndexId()), column));
            }
        }
        return ret;
    }

    @Override
    public long fetchRowCount() {
        return getRowCountForIndex(baseIndexId, false);
    }

    public long getRowCountForIndex(long indexId, boolean strict) {
        long rowCount = 0;
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            MaterializedIndex index = entry.getValue().getIndex(indexId);
            if (strict && !index.getRowCountReported()) {
                return -1;
            }
            rowCount += (index == null || index.getRowCount() == -1) ? 0 : index.getRowCount();
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

    // Get the signature string of this table with specified partitions.
    // This method is used to determine whether the tables have the same schema.
    // Contains:
    // table name, table type, index name, index schema, short key column count, storage type,
    // bloom filter, partition type and columns, distribution type and columns, buckets number,
    // indexes and columns.
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

        // indexes
        if (this.indexes != null) {
            Map<String, Index> indexes = Maps.newTreeMap();
            for (Index idx : this.indexes.getIndexes()) {
                indexes.put(idx.getIndexName(), idx);
            }
            for (Map.Entry<String, Index> entry : indexes.entrySet()) {
                Index idx = entry.getValue();
                sb.append(entry.getKey());
                sb.append(idx.getIndexType());
                sb.append(Joiner.on(",").join(idx.getColumns()));
            }
        }

        String signature = sb.toString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("get signature of table {}. signature string: {}", name, sb.toString());
        }
        return signature;
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
    public boolean isPartitionedTable() {
        return !PartitionType.UNPARTITIONED.equals(partitionInfo.getType());
    }

    // Return true if data is distributed by one more partitions or buckets.
    @Override
    public boolean isPartitionDistributed() {
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
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Deprecated
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

            // HACK: the index id in MaterializedIndexMeta is not equals to the index id
            // saved in OlapTable, because the table restore from snapshot is not reset
            // the MaterializedIndexMeta correctly.
            if (indexMeta.getIndexId() != indexId) {
                LOG.warn("HACK: the index id {} in materialized index meta of {} is not equals"
                        + " to the index saved in table {} ({}), reset it to {}",
                        indexMeta.getIndexId(), indexName, name, id, indexId);
                indexMeta.resetIndexIdForRestore(indexId, null, null);
            }
        }

        // partition and distribution info
        keysType = KeysType.valueOf(Text.readString(in));

        // useless code see pr 3036
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

        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_124) {
            // autoIncrementGenerator
            if (in.readBoolean()) {
                autoIncrementGenerator = AutoIncrementGenerator.read(in);
                autoIncrementGenerator.setEditLog(Env.getCurrentEnv().getEditLog());
            }
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

    @Override
    public void gsonPostProcess() throws IOException {

        // HACK: the index id in MaterializedIndexMeta is not equals to the index id
        // saved in OlapTable, because the table restore from snapshot is not reset
        // the MaterializedIndexMeta correctly.
        // for each index, reset the index id in MaterializedIndexMeta
        for (Map.Entry<Long, MaterializedIndexMeta> entry : indexIdToMeta.entrySet()) {
            long indexId = entry.getKey();
            MaterializedIndexMeta indexMeta = entry.getValue();
            if (indexMeta.getIndexId() != indexId) {
                LOG.warn("HACK: the index id {} in materialized index meta of {} is not equals"
                        + " to the index saved in table {} ({}), reset it to {}",
                        indexMeta.getIndexId(), indexNameToId.get(indexId), name, id, indexId);
                indexMeta.resetIndexIdForRestore(indexId, null, null);
            }
        }

        // for each idToPartition, add partition to nameToPartition
        for (Partition partition : idToPartition.values()) {
            nameToPartition.put(partition.getName(), partition);
        }

        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_124
                    && autoIncrementGenerator != null) {
            autoIncrementGenerator.setEditLog(Env.getCurrentEnv().getEditLog());
        }
        if (isAutoBucket()) {
            defaultDistributionInfo.markAutoBucket();
        }
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
        OlapTable copied = DeepCopy.copy(this, OlapTable.class, FeConstants.meta_version);
        if (copied == null) {
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("copied table delete shadow index : {}", deleteIndex.getId());
            }
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

    public static OlapTable read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_136) {
            OlapTable t = new OlapTable();
            t.readFields(in);
            return t;
        }
        return GsonUtils.GSON.fromJson(Text.readString(in), OlapTable.class);
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

    public void checkNormalStateForAlter() throws DdlException {
        if (state != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + name + "]'s state(" + state.toString()
                    + ") is not NORMAL. Do not allow doing ALTER ops");
        }
        if (tableProperty != null && tableProperty.isInAtomicRestore()) {
            throw new DdlException("Table[" + name + "] is in atomic restore state. "
                    + "Do not allow doing ALTER ops");
        }
    }

    public boolean isStable(SystemInfoService infoService, TabletScheduler tabletScheduler) {
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

                    TabletStatus status = tablet.getHealth(infoService, visibleVersion,
                            replicaAlloc, aliveBeIds).status;
                    if (status != TabletStatus.HEALTHY) {
                        LOG.info("table {} is not stable because tablet {} status is {}. replicas: {}",
                                id, tablet.getId(), status, tablet.getReplicas());
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
        return (int) baseIndexId;
    }

    public Column getBaseColumn(String columnName) {
        for (Column column : getBaseSchema()) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return column;
            }
        }
        return null;
    }

    public Column getBaseColumn(int colUniqueId) {
        for (Column column : getBaseSchema()) {
            if (column.getUniqueId() == colUniqueId) {
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

    public String getKeyColAsString() {
        StringBuilder str = new StringBuilder();
        str.append("");
        for (Column column : getBaseSchema()) {
            if (column.isKey()) {
                if (str.length() != 0) {
                    str.append(",");
                }
                str.append(column.getName());
            }
        }
        return str.toString();
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

    public boolean containsPartition(String partitionName) {
        return nameToPartition.containsKey(partitionName);
    }

    public void setInAtomicRestore() {
        getOrCreatTableProperty().setInAtomicRestore().buildInAtomicRestore();
    }

    public void clearInAtomicRestore() {
        getOrCreatTableProperty().clearInAtomicRestore().buildInAtomicRestore();
    }

    public boolean isInAtomicRestore() {
        if (tableProperty != null) {
            return tableProperty.isInAtomicRestore();
        }
        return false;
    }

    public long getTTLSeconds() {
        if (tableProperty != null) {
            return tableProperty.getTTLSeconds();
        }
        return 0L;
    }

    public void setTTLSeconds(long ttlSeconds) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_FILE_CACHE_TTL_SECONDS,
                                            Long.valueOf(ttlSeconds).toString());
        tableProperty.buildTTLSeconds();
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

    public short getMinLoadReplicaNum() {
        if (tableProperty != null) {
            return tableProperty.getMinLoadReplicaNum();
        }

        return -1;
    }

    public void setMinLoadReplicaNum(short minLoadReplicaNum) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_MIN_LOAD_REPLICA_NUM,
                Short.valueOf(minLoadReplicaNum).toString());
        tableProperty.buildMinLoadReplicaNum();
    }

    public int getLoadRequiredReplicaNum(long partitionId) {
        int totalReplicaNum = partitionInfo.getReplicaAllocation(partitionId).getTotalReplicaNum();
        int minLoadReplicaNum = getMinLoadReplicaNum();
        if (minLoadReplicaNum > 0) {
            return Math.min(minLoadReplicaNum, totalReplicaNum);
        }

        int quorum = totalReplicaNum / 2 + 1;
        if (Config.min_load_replica_num > 0) {
            return Math.min(quorum, Config.min_load_replica_num);
        }

        return quorum;
    }

    public void setStorageMedium(TStorageMedium medium) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM,
                medium == null ? "" : medium.name());
        tableProperty.buildStorageMedium();
    }

    public TStorageMedium getStorageMedium() {
        if (tableProperty != null) {
            return tableProperty.getStorageMedium();
        }
        return null;
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

    public void setVariantEnableFlattenNested(boolean flattenNested) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_VARIANT_ENABLE_FLATTEN_NESTED,
                Boolean.valueOf(flattenNested).toString());
        tableProperty.buildVariantEnableFlattenNested();
    }

    public Boolean variantEnableFlattenNested() {
        if (tableProperty != null) {
            return tableProperty.variantEnableFlattenNested();
        }
        return false;
    }

    public int getBaseSchemaVersion() {
        MaterializedIndexMeta baseIndexMeta = indexIdToMeta.get(baseIndexId);
        return baseIndexMeta.getSchemaVersion();
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
        return PropertyAnalyzer.SIZE_BASED_COMPACTION_POLICY;
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
        return PropertyAnalyzer.TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES_DEFAULT_VALUE;
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
        return PropertyAnalyzer.TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD_DEFAULT_VALUE;
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
        return PropertyAnalyzer.TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS_DEFAULT_VALUE;
    }

    public void setTimeSeriesCompactionEmptyRowsetsThreshold(long timeSeriesCompactionEmptyRowsetsThreshold) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD,
                                                Long.valueOf(timeSeriesCompactionEmptyRowsetsThreshold).toString());
        tableProperty.buildTimeSeriesCompactionEmptyRowsetsThreshold();
    }

    public Long getTimeSeriesCompactionEmptyRowsetsThreshold() {
        if (tableProperty != null) {
            return tableProperty.timeSeriesCompactionEmptyRowsetsThreshold();
        }
        return PropertyAnalyzer.TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD_DEFAULT_VALUE;
    }

    public void setTimeSeriesCompactionLevelThreshold(long timeSeriesCompactionLevelThreshold) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_LEVEL_THRESHOLD,
                                                Long.valueOf(timeSeriesCompactionLevelThreshold).toString());
        tableProperty.buildTimeSeriesCompactionLevelThreshold();
    }

    public Long getTimeSeriesCompactionLevelThreshold() {
        if (tableProperty != null) {
            return tableProperty.timeSeriesCompactionLevelThreshold();
        }
        return PropertyAnalyzer.TIME_SERIES_COMPACTION_LEVEL_THRESHOLD_DEFAULT_VALUE;
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
    public Partition dropTempPartition(String partitionName, boolean needDropTablet) {
        Partition partition = getPartition(partitionName, true);
        if (partition != null) {
            partitionInfo.dropPartition(partition.getId());
            tempPartitions.dropPartition(partitionName, needDropTablet);
        }
        return partition;
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
    public void replaceTempPartitions(long dbId, List<String> partitionNames, List<String> tempPartitionNames,
            boolean strictRange, boolean useTempPartitionName, boolean isForceDropOld) throws DdlException {
        // check partition items
        checkPartition(partitionNames, tempPartitionNames, strictRange);

        // begin to replace
        // 1. drop old partitions
        for (String partitionName : partitionNames) {
            dropPartition(dbId, partitionName, isForceDropOld);
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

    public void setRowStorePageSize(long pageSize) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_ROW_STORE_PAGE_SIZE,
                Long.valueOf(pageSize).toString());
        tableProperty.buildRowStorePageSize();
    }

    public long rowStorePageSize() {
        if (tableProperty != null) {
            return tableProperty.rowStorePageSize();
        }
        return PropertyAnalyzer.ROW_STORE_PAGE_SIZE_DEFAULT_VALUE;
    }

    public void setStorageFormat(TStorageFormat storageFormat) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT, storageFormat.name());
        tableProperty.buildStorageFormat();
    }

    public void setInvertedIndexFileStorageFormat(TInvertedIndexFileStorageFormat invertedIndexFileStorageFormat) {
        TableProperty tableProperty = getOrCreatTableProperty();
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_INVERTED_INDEX_STORAGE_FORMAT,
                invertedIndexFileStorageFormat.name());
        tableProperty.buildInvertedIndexFileStorageFormat();
    }

    public TStorageFormat getStorageFormat() {
        if (tableProperty == null) {
            return TStorageFormat.DEFAULT;
        }
        return tableProperty.getStorageFormat();
    }

    public TInvertedIndexFileStorageFormat getInvertedIndexFileStorageFormat() {
        if (tableProperty == null) {
            return TInvertedIndexFileStorageFormat.V2;
        }
        return tableProperty.getInvertedIndexFileStorageFormat();
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

    public boolean isUniqKeyMergeOnWrite() {
        return getKeysType() == KeysType.UNIQUE_KEYS && getEnableUniqueKeyMergeOnWrite();
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
        return keysType == KeysType.DUP_KEYS || isUniqKeyMergeOnWrite();
    }

    public void initAutoIncrementGenerator(long dbId) {
        for (Column column : fullSchema) {
            if (column.isAutoInc()) {
                autoIncrementGenerator = new AutoIncrementGenerator(dbId, id, column.getUniqueId(),
                        column.getAutoIncInitValue());
                autoIncrementGenerator.setEditLog(Env.getCurrentEnv().getEditLog());
                break;
            }
        }
    }

    public AutoIncrementGenerator getAutoIncrementGenerator() {
        return autoIncrementGenerator;
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
                connectContext.setDatabase(dbName);
                Analyzer analyzer = new Analyzer(Env.getCurrentEnv(), connectContext);
                meta.parseStmt(analyzer);
            } catch (IOException e) {
                LOG.info(e);
            }
        }
    }

    public boolean isDistributionColumn(String columnName) {
        Set<String> distributeColumns = getDistributionColumnNames()
                .stream().map(String::toLowerCase).collect(Collectors.toSet());
        return distributeColumns.contains(columnName.toLowerCase());
    }

    @Override
    public boolean isPartitionColumn(String columnName) {
        if (columnName.startsWith(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX)) {
            columnName = columnName.substring(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX.length());
        }
        String finalColumnName = columnName;
        return getPartitionInfo().getPartitionColumns().stream()
                .anyMatch(c -> c.getName().equalsIgnoreCase(finalColumnName));
    }

    /**
     * For olap table, we need to acquire read lock when plan.
     * Because we need to make sure the partition's version remain unchanged when plan.
     *
     * @return
     */
    @Override
    public boolean needReadLockWhenPlan() {
        return true;
    }

    public boolean hasVariantColumns() {
        for (Column column : getBaseSchema()) {
            if (column.getType().isVariantType()) {
                return true;
            }
        }
        return false;
    }

    public List<Tablet> getAllTablets() throws AnalysisException {
        List<Tablet> tablets = Lists.newArrayList();
        for (Partition partition : getPartitions()) {
            tablets.addAll(partition.getBaseIndex().getTablets());
        }
        return tablets;
    }

    // Get sample tablets for remote desc schema
    // 1. Estimate tablets for a partition, 1 at least
    // 2. Pick the partition sorted with id in desc order, greater id with the newest partition
    // 3. Truncate to sampleSize
    public List<Tablet> getSampleTablets(int sampleSize) {
        List<Tablet> sampleTablets = new ArrayList<>();
        // Filter partition with empty data
        Collection<Partition> partitions = getPartitions()
                .stream()
                .filter(partition -> partition.getVisibleVersion() > Partition.PARTITION_INIT_VERSION)
                .collect(Collectors.toList());
        if (partitions.isEmpty()) {
            return sampleTablets;
        }
        // 1. Estimate tablets for a partition, 1 at least
        int estimatePartitionTablets = Math.max(sampleSize / partitions.size(), 1);

        // 2. Sort the partitions by id in descending order (greater id means the newest partition)
        List<Partition> sortedPartitions = partitions.stream().sorted(new Comparator<Partition>() {
            @Override
            public int compare(Partition p1, Partition p2) {
                // compare with desc order
                return Long.compare(p2.getId(), p1.getId());
            }
        }).collect(Collectors.toList());

        // 3. Collect tablets from partitions
        for (Partition partition : sortedPartitions) {
            List<Tablet> targetTablets = new ArrayList<>(partition.getBaseIndex().getTablets());
            Collections.shuffle(targetTablets);
            if (!targetTablets.isEmpty()) {
                // Ensure we do not exceed the available number of tablets
                int tabletsToFetch = Math.min(targetTablets.size(), estimatePartitionTablets);
                sampleTablets.addAll(targetTablets.subList(0, tabletsToFetch));
            }

            if (sampleTablets.size() >= sampleSize) {
                break;
            }
        }

        // 4. Truncate to sample size if needed
        if (sampleTablets.size() > sampleSize) {
            sampleTablets = sampleTablets.subList(0, sampleSize);
        }

        return sampleTablets;
    }

    // During `getNextVersion` and `updateVisibleVersionAndTime` period,
    // the write lock on the table should be held continuously
    public void updateVisibleVersionAndTime(long visibleVersion, long visibleVersionTime) {
        LOG.info("updateVisibleVersionAndTime, tableName: {}, visibleVersion, {}, visibleVersionTime: {}", name,
                visibleVersion, visibleVersionTime);
        tableAttributes.updateVisibleVersionAndTime(visibleVersion, visibleVersionTime);
    }

    // During `getNextVersion` and `updateVisibleVersionAndTime` period,
    // the write lock on the table should be held continuously
    public long getNextVersion() {
        if (!Config.isCloudMode()) {
            return tableAttributes.getNextVersion();
        } else {
            // cloud mode should not reach here
            if (LOG.isDebugEnabled()) {
                LOG.debug("getNextVersion in Cloud mode in OlapTable {} ", getName());
            }
            return getVisibleVersion() + 1;
        }
    }

    public long getVisibleVersion() {
        if (Config.isNotCloudMode()) {
            return tableAttributes.getVisibleVersion();
        }

        // get version rpc
        Cloud.GetVersionRequest request = Cloud.GetVersionRequest.newBuilder()
                .setDbId(this.getDatabase().getId())
                .setTableId(this.id)
                .setBatchMode(false)
                .setIsTableVersion(true)
                .build();

        try {
            Cloud.GetVersionResponse resp = VersionHelper.getVersionFromMeta(request);
            long version = -1;
            if (resp.getStatus().getCode() == Cloud.MetaServiceCode.OK) {
                version = resp.getVersion();
            } else {
                assert resp.getStatus().getCode() == Cloud.MetaServiceCode.VERSION_NOT_FOUND;
                version = 0;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("get version from meta service, version: {}, table: {}", version, getId());
            }
            if (version == 0) {
                version = 1;
            }
            return version;
        } catch (RpcException e) {
            throw new RuntimeException("get version from meta service failed", e);
        }
    }

    // Get the table versions in batch.
    public static List<Long> getVisibleVersionByTableIds(Collection<Long> tableIds) {
        List<OlapTable> tables = new ArrayList<>();

        InternalCatalog catalog = Env.getCurrentEnv().getInternalCatalog();
        for (long tableId : tableIds) {
            Table table = catalog.getTableByTableId(tableId);
            if (table == null) {
                throw new RuntimeException("get table visible version failed, no such table " + tableId + " exists");
            }
            if (table.getType() != TableType.OLAP) {
                throw new RuntimeException(
                        "get table visible version failed, table " + tableId + " is not a OLAP table");
            }
            tables.add((OlapTable) table);
        }

        return getVisibleVersionInBatch(tables);
    }

    // Get the table versions in batch.
    public static List<Long> getVisibleVersionInBatch(Collection<OlapTable> tables) {
        if (tables.isEmpty()) {
            return new ArrayList<>();
        }

        if (Config.isNotCloudMode()) {
            return tables.stream()
                    .map(table -> table.tableAttributes.getVisibleVersion())
                    .collect(Collectors.toList());
        }

        List<Long> dbIds = new ArrayList<>();
        List<Long> tableIds = new ArrayList<>();
        for (OlapTable table : tables) {
            dbIds.add(table.getDatabase().getId());
            tableIds.add(table.getId());
        }

        return getVisibleVersionFromMeta(dbIds, tableIds);
    }

    private static List<Long> getVisibleVersionFromMeta(List<Long> dbIds, List<Long> tableIds) {
        // get version rpc
        Cloud.GetVersionRequest request = Cloud.GetVersionRequest.newBuilder()
                .setDbId(-1)
                .setTableId(-1)
                .setPartitionId(-1)
                .addAllDbIds(dbIds)
                .addAllTableIds(tableIds)
                .setBatchMode(true)
                .setIsTableVersion(true)
                .build();

        try {
            Cloud.GetVersionResponse resp = VersionHelper.getVersionFromMeta(request);
            if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                throw new RpcException("get table visible version", "unexpected status " + resp.getStatus());
            }

            List<Long> versions = resp.getVersionsList();
            if (versions.size() != tableIds.size()) {
                throw new RpcException("get table visible version",
                        "wrong number of versions, required " + tableIds.size() + ", but got " + versions.size());
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("get table version from meta service, tables: {}, versions: {}", tableIds, versions);
            }

            for (int i = 0; i < versions.size(); i++) {
                // Set visible version to 1 if no such table version exists.
                if (versions.get(i) <= 0L) {
                    versions.set(i, 1L);
                }
            }

            return versions;
        } catch (RpcException e) {
            throw new RuntimeException("get table version from meta service failed", e);
        }
    }

    public long getVisibleVersionTime() {
        return tableAttributes.getVisibleVersionTime();
    }

    @Override
    public PartitionType getPartitionType() {
        return partitionInfo.getType();
    }

    @Override
    public Map<String, PartitionItem> getAndCopyPartitionItems() {
        readLock();
        try {
            Map<String, PartitionItem> res = Maps.newHashMap();
            for (Entry<Long, PartitionItem> entry : getPartitionInfo().getIdToItem(false).entrySet()) {
                Partition partition = idToPartition.get(entry.getKey());
                if (partition != null) {
                    res.put(partition.getName(), entry.getValue());
                }
            }
            return res;
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<Column> getPartitionColumns() {
        return getPartitionInfo().getPartitionColumns();
    }

    @Override
    public MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context)
            throws AnalysisException {
        Map<String, Long> partitionVersions = context.getBaseVersions().getPartitionVersions();
        long partitionId = getPartitionOrAnalysisException(partitionName).getId();
        long visibleVersion = partitionVersions.containsKey(partitionName) ? partitionVersions.get(partitionName)
                : getPartitionOrAnalysisException(partitionName).getVisibleVersion();
        return new MTMVVersionSnapshot(visibleVersion, partitionId);
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context) {
        Map<Long, Long> tableVersions = context.getBaseVersions().getTableVersions();
        long visibleVersion = tableVersions.containsKey(id) ? tableVersions.get(id) : getVisibleVersion();
        return new MTMVVersionSnapshot(visibleVersion);
    }

    @Override
    public boolean needAutoRefresh() {
        return true;
    }

    @Override
    public boolean isPartitionColumnAllowNull() {
        return true;
    }

    public void setStatistics(Statistics statistics) {
        this.statistics = statistics;
    }

    public static class Statistics {
        @Getter
        private String dbName;
        @Getter
        private String tableName;

        @Getter
        private Long dataSize; // single replica data size
        @Getter
        private Long totalReplicaDataSize;

        @Getter
        private Long remoteDataSize; // single replica remote data size

        @Getter
        private Long replicaCount;

        @Getter
        private Long rowCount;

        @Getter
        private Long rowsetCount;

        @Getter
        private Long segmentCount;

        public Statistics() {
            this.dbName = null;
            this.tableName = null;

            this.dataSize = 0L;
            this.totalReplicaDataSize = 0L;

            this.remoteDataSize = 0L;

            this.replicaCount = 0L;

            this.rowCount = 0L;
            this.rowsetCount = 0L;
            this.segmentCount = 0L;

        }

        public Statistics(String dbName, String tableName,
                Long dataSize, Long totalReplicaDataSize,
                Long remoteDataSize, Long replicaCount, Long rowCount,
                Long rowsetCount, Long segmentCount) {

            this.dbName = dbName;
            this.tableName = tableName;

            this.dataSize = dataSize;
            this.totalReplicaDataSize = totalReplicaDataSize;

            this.remoteDataSize = remoteDataSize;

            this.replicaCount = replicaCount;

            this.rowCount = rowCount;
            this.rowsetCount = rowsetCount;
            this.segmentCount = segmentCount;
        }
    }

    public long getDataSize() {
        return getDataSize(false);
    }

    public long getDataSize(boolean singleReplica) {
        if (singleReplica) {
            statistics.getDataSize();
        }

        return statistics.getTotalReplicaDataSize();
    }

    public long getRemoteDataSize() {
        return statistics.getRemoteDataSize();
    }

    public long getReplicaCount() {
        return statistics.getReplicaCount();
    }

    public boolean isShadowIndex(long indexId) {
        String indexName = getIndexNameById(indexId);
        if (indexName != null && indexName.startsWith(org.apache.doris.alter.SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean autoAnalyzeEnabled() {
        if (tableProperty == null) {
            return super.autoAnalyzeEnabled();
        }
        Map<String, String> properties = tableProperty.getProperties();
        if (properties == null || !properties.containsKey(PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY)
                || properties.get(PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY)
                    .equalsIgnoreCase(PropertyAnalyzer.USE_CATALOG_AUTO_ANALYZE_POLICY)) {
            return super.autoAnalyzeEnabled();
        }
        return properties.get(PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY)
                .equalsIgnoreCase(PropertyAnalyzer.ENABLE_AUTO_ANALYZE_POLICY);
    }
}
