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

package org.apache.doris.alter;

import org.apache.doris.analysis.AddColumnClause;
import org.apache.doris.analysis.AddColumnsClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.BuildIndexClause;
import org.apache.doris.analysis.CancelAlterTableStmt;
import org.apache.doris.analysis.CancelStmt;
import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.analysis.CreateIndexClause;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.DropColumnClause;
import org.apache.doris.analysis.DropIndexClause;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.ModifyColumnClause;
import org.apache.doris.analysis.ModifyTablePropertiesClause;
import org.apache.doris.analysis.ReorderColumnsClause;
import org.apache.doris.analysis.ShowAlterStmt.AlterType;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.GeneratedColumnInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.MetaIdGenerator.IdGeneratorBuffer;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaContext;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DbUtil;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.IdGeneratorUtil;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.persist.AlterLightSchemaChangeInfo;
import org.apache.doris.persist.RemoveAlterJobV2OperationLog;
import org.apache.doris.persist.TableAddOrDropColumnsInfo;
import org.apache.doris.persist.TableAddOrDropInvertedIndicesInfo;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.ClearAlterTask;
import org.apache.doris.task.UpdateTabletMetaInfoTask;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

public class SchemaChangeHandler extends AlterHandler {
    private static final Logger LOG = LogManager.getLogger(SchemaChangeHandler.class);

    // all shadow indexes should have this prefix in name
    public static final String SHADOW_NAME_PREFIX = "__doris_shadow_";

    public static final int MAX_ACTIVE_SCHEMA_CHANGE_JOB_V2_SIZE = 10;

    public static final int CYCLE_COUNT_TO_CHECK_EXPIRE_SCHEMA_CHANGE_JOB = 20;

    public final ThreadPoolExecutor schemaChangeThreadPool = ThreadPoolManager.newDaemonCacheThreadPool(
            MAX_ACTIVE_SCHEMA_CHANGE_JOB_V2_SIZE, "schema-change-pool", true);

    public final Map<Long, AlterJobV2> activeSchemaChangeJobsV2 = Maps.newConcurrentMap();

    public final Map<Long, AlterJobV2> runnableSchemaChangeJobV2 = Maps.newConcurrentMap();

    // queue of inverted index job
    public ConcurrentMap<Long, IndexChangeJob> indexChangeJobs = Maps.newConcurrentMap();

    public final Map<Long, IndexChangeJob> activeIndexChangeJob = Maps.newConcurrentMap();

    public final Map<Long, IndexChangeJob> runnableIndexChangeJob = Maps.newConcurrentMap();

    public int cycleCount = 0;

    public SchemaChangeHandler() {
        super("schema change", Config.default_schema_change_scheduler_interval_millisecond);
    }

    /**
     * @param alterClause
     * @param olapTable
     * @param indexSchemaMap
     * @param colUniqueIdSupplierMap for multi add columns clause, we need stash middle state of maxColUniqueId
     * @return true: can light schema change, false: cannot light schema change
     * @throws DdlException
     */
    private boolean processAddColumn(AddColumnClause alterClause, OlapTable olapTable,
                                     Map<Long, LinkedList<Column>> indexSchemaMap,
                                     Map<Long, IntSupplier> colUniqueIdSupplierMap)
            throws DdlException {
        Column column = alterClause.getColumn();
        ColumnPosition columnPos = alterClause.getColPos();
        String targetIndexName = alterClause.getRollupName();
        checkIndexExists(olapTable, targetIndexName);

        String baseIndexName = olapTable.getName();
        checkAssignedTargetIndexName(baseIndexName, targetIndexName);

        long baseIndexId = olapTable.getBaseIndexId();
        long targetIndexId = -1L;
        if (targetIndexName != null) {
            targetIndexId = olapTable.getIndexIdByName(targetIndexName);
        }

        Set<String> newColNameSet = Sets.newHashSet(column.getName());

        return addColumnInternal(olapTable, column, columnPos, targetIndexId, baseIndexId, indexSchemaMap,
                newColNameSet, false, colUniqueIdSupplierMap);
    }

    private void processAddColumn(AddColumnClause alterClause, Table externalTable, List<Column> newSchema)
            throws DdlException {
        Column column = alterClause.getColumn();
        ColumnPosition columnPos = alterClause.getColPos();
        Set<String> newColNameSet = Sets.newHashSet(column.getName());

        addColumnInternal(column, columnPos, newSchema, newColNameSet);
    }

    private void processAddColumns(AddColumnsClause alterClause, Table externalTable, List<Column> newSchema)
            throws DdlException {
        List<Column> columns = alterClause.getColumns();
        Set<String> newColNameSet = Sets.newHashSet();
        for (Column column : alterClause.getColumns()) {
            newColNameSet.add(column.getName());
        }

        for (Column newColumn : columns) {
            addColumnInternal(newColumn, null, newSchema, newColNameSet);
        }
    }

    /**
     * @param alterClause
     * @param olapTable
     * @param indexSchemaMap
     * @param ignoreSameColumn
     * @param colUniqueIdSupplierMap for multi add columns clause, we need stash middle state of maxColUniqueId
     * @return true: can light schema change, false: cannot light schema change
     * @throws DdlException
     */
    public boolean processAddColumns(AddColumnsClause alterClause, OlapTable olapTable,
                                     Map<Long, LinkedList<Column>> indexSchemaMap, boolean ignoreSameColumn,
                                     Map<Long, IntSupplier> colUniqueIdSupplierMap) throws DdlException {
        List<Column> columns = alterClause.getColumns();
        String targetIndexName = alterClause.getRollupName();
        checkIndexExists(olapTable, targetIndexName);

        Set<String> newColNameSet = Sets.newHashSet();
        for (Column column : columns) {
            newColNameSet.add(column.getName());
        }

        String baseIndexName = olapTable.getName();
        checkAssignedTargetIndexName(baseIndexName, targetIndexName);

        long baseIndexId = olapTable.getBaseIndexId();
        long targetIndexId = -1L;
        if (targetIndexName != null) {
            targetIndexId = olapTable.getIndexIdByName(targetIndexName);
        }

        boolean lightSchemaChange = true;
        for (Column column : columns) {
            boolean result = addColumnInternal(olapTable, column, null, targetIndexId, baseIndexId, indexSchemaMap,
                    newColNameSet, ignoreSameColumn, colUniqueIdSupplierMap);
            if (!result) {
                lightSchemaChange = false;
            }
        }
        return lightSchemaChange;
    }

    private void processDropColumn(DropColumnClause alterClause, Table externalTable, List<Column> newSchema)
            throws DdlException {
        String dropColName = alterClause.getColName();

        // find column in base index and remove it
        boolean found = false;
        Iterator<Column> baseIter = newSchema.iterator();
        while (baseIter.hasNext()) {
            Column column = baseIter.next();
            if (column.getName().equalsIgnoreCase(dropColName)) {
                if (newSchema.size() > 1) {
                    baseIter.remove();
                    found = true;
                } else {
                    throw new DdlException(
                            "Do not allow remove last column of table: " + externalTable.getName() + " column: "
                                    + dropColName);
                }
                break;
            }
        }

        if (!found) {
            throw new DdlException("Column does not exists: " + dropColName);
        }
    }

    /**
     * @param alterClause
     * @param olapTable
     * @param indexSchemaMap
     * @param indexes
     * @return true: can light schema change, false: cannot
     * @throws DdlException
     */
    private boolean processDropColumn(DropColumnClause alterClause, OlapTable olapTable,
                                      Map<Long, LinkedList<Column>> indexSchemaMap, List<Index> indexes)
            throws DdlException {

        String dropColName = alterClause.getColName();
        String targetIndexName = alterClause.getRollupName();
        checkIndexExists(olapTable, targetIndexName);

        String baseIndexName = olapTable.getName();
        checkAssignedTargetIndexName(baseIndexName, targetIndexName);
        long baseIndexId = olapTable.getBaseIndexId();

        long targetIndexId = -1L;
        if (targetIndexName != null) {
            targetIndexId = olapTable.getIndexIdByName(targetIndexName);
        }

        boolean lightSchemaChange = olapTable.getEnableLightSchemaChange();
        /*
         * UNIQUE:
         *      Can not drop any key column.
         * AGGREGATION:
         *      Can not drp any key column is has value with REPLACE method
         */
        if (KeysType.UNIQUE_KEYS == olapTable.getKeysType()) {
            List<Column> baseSchema = indexSchemaMap.get(baseIndexId);
            for (Column column : baseSchema) {
                if (column.getName().equalsIgnoreCase(dropColName)) {
                    if (column.isKey()) {
                        throw new DdlException("Can not drop key column in Unique data model table");
                    } else if (column.isClusterKey()) {
                        throw new DdlException("Can not drop cluster key column in Unique data model table");
                    }
                }
            }
            if (olapTable.hasSequenceCol() && dropColName.equalsIgnoreCase(olapTable.getSequenceMapCol())) {
                throw new DdlException("Can not drop sequence mapping column[" + dropColName
                        + "] in Unique data model table[" + olapTable.getName() + "]");
            }
        } else if (KeysType.AGG_KEYS == olapTable.getKeysType()) {
            if (null == targetIndexName) {
                // drop column in base table
                List<Column> baseSchema = indexSchemaMap.get(baseIndexId);
                boolean isKey = false;
                boolean hasReplaceColumn = false;
                for (Column column : baseSchema) {
                    if (column.isKey() && column.getName().equalsIgnoreCase(dropColName)) {
                        isKey = true;
                        lightSchemaChange = false;
                    } else if (AggregateType.REPLACE == column.getAggregationType()
                            || AggregateType.REPLACE_IF_NOT_NULL == column.getAggregationType()) {
                        hasReplaceColumn = true;
                    }
                }
                if (isKey && hasReplaceColumn) {
                    throw new DdlException(
                            "Can not drop key column when table has value column with REPLACE aggregation method");
                }
            } else {
                // drop column in rollup and base index
                // find column
                List<Column> targetIndexSchema = indexSchemaMap.get(targetIndexId);
                boolean isKey = false;
                boolean hasReplaceColumn = false;
                for (Column column : targetIndexSchema) {
                    if (column.isKey() && column.getName().equalsIgnoreCase(dropColName)) {
                        isKey = true;
                        lightSchemaChange = false;
                    } else if (AggregateType.REPLACE == column.getAggregationType()
                            || AggregateType.REPLACE_IF_NOT_NULL == column.getAggregationType()) {
                        hasReplaceColumn = true;
                    }
                }
                if (isKey && hasReplaceColumn) {
                    throw new DdlException(
                            "Can not drop key column when rollup has value column with REPLACE aggregation method");
                }
            }
        } else if (KeysType.DUP_KEYS == olapTable.getKeysType()) {
            List<Column> baseSchema = indexSchemaMap.get(baseIndexId);
            for (Column column : baseSchema) {
                if (column.isKey() && column.getName().equalsIgnoreCase(dropColName)) {
                    lightSchemaChange = false;
                    break;
                }
            }
        }

        // generated column check
        Map<String, Column> nameToColumn = new HashMap<>();
        for (Column c : indexSchemaMap.get(baseIndexId)) {
            nameToColumn.put(c.getName(), c);
        }
        if (null == targetIndexName) {
            if (nameToColumn.containsKey(dropColName)) {
                Column column = nameToColumn.get(dropColName);
                Set<String> generatedColumnsThatReferToThis = column.getGeneratedColumnsThatReferToThis();
                if (!generatedColumnsThatReferToThis.isEmpty()) {
                    throw new DdlException(
                            "Column '" + dropColName + "' has a generated column dependency on :"
                                    + generatedColumnsThatReferToThis);
                }
            }
        }

        Iterator<Index> it = indexes.iterator();
        while (it.hasNext()) {
            Index index = it.next();
            for (String indexCol : index.getColumns()) {
                if (dropColName.equalsIgnoreCase(indexCol)) {
                    it.remove();
                    break;
                }
            }
        }

        if (targetIndexName == null) {
            // if not specify rollup index, column should be dropped from both base and rollup indexes.
            List<Long> indexIds = new ArrayList<Long>();
            indexIds.add(baseIndexId);
            indexIds.addAll(olapTable.getIndexIdListExceptBaseIndex());

            // find column in base index and remove it
            List<Column> baseSchema = indexSchemaMap.get(baseIndexId);
            boolean found = false;
            Iterator<Column> baseIter = baseSchema.iterator();
            while (baseIter.hasNext()) {
                Column column = baseIter.next();
                if (column.getName().equalsIgnoreCase(dropColName)) {
                    baseIter.remove();
                    found = true;
                    // find generated column referred column
                    removeColumnWhenDropGeneratedColumn(column, nameToColumn);
                    break;
                }
            }
            if (!found) {
                throw new DdlException("Column does not exists: " + dropColName);
            }

            for (int i = 1; i < indexIds.size(); i++) {
                List<Column> rollupSchema = indexSchemaMap.get(indexIds.get(i));
                Iterator<Column> iter = rollupSchema.iterator();
                while (iter.hasNext()) {
                    Column column = iter.next();
                    boolean containedByMV = column.getName().equalsIgnoreCase(dropColName);
                    if (!containedByMV && column.getDefineExpr() != null) {
                        List<SlotRef> slots = new ArrayList<>();
                        column.getDefineExpr().collect(SlotRef.class, slots);
                        for (SlotRef slot : slots) {
                            if (slot.getColumnName().equalsIgnoreCase(dropColName)) {
                                containedByMV = true;
                                break;
                            }
                        }
                    }
                    if (containedByMV) {
                        throw new DdlException("Can not drop column contained by mv, mv="
                                + olapTable.getIndexNameById(indexIds.get(i)));
                    }
                }
            }
        } else {
            // if specify rollup index, only drop column from specified rollup index
            // find column
            List<Column> targetIndexSchema = indexSchemaMap.get(targetIndexId);
            boolean found = false;
            Iterator<Column> iter = targetIndexSchema.iterator();
            while (iter.hasNext()) {
                Column column = iter.next();
                if (column.getName().equalsIgnoreCase(dropColName)) {
                    iter.remove();
                    found = true;
                    if (column.isKey()) {
                        lightSchemaChange = false;
                    }
                    break;
                }
            }
            if (!found) {
                throw new DdlException("Column does not exists: " + dropColName);
            }
        }
        return lightSchemaChange;
    }

    // User can modify column type and column position
    private void processModifyColumn(ModifyColumnClause alterClause, Table externalTable, List<Column> newSchema)
            throws DdlException {
        Column modColumn = alterClause.getColumn();
        ColumnPosition columnPos = alterClause.getColPos();

        // find modified column
        String newColName = modColumn.getName();
        boolean hasColPos = (columnPos != null && !columnPos.isFirst());
        boolean found = false;

        int modColIndex = -1;
        int lastColIndex = -1;
        for (int i = 0; i < newSchema.size(); i++) {
            Column col = newSchema.get(i);
            if (col.getName().equalsIgnoreCase(newColName)) {
                modColIndex = i;
                found = true;
            }
            if (hasColPos) {
                if (col.getNonShadowName().equalsIgnoreCase(columnPos.getLastCol())) {
                    lastColIndex = i;
                }
            } else {
                // save the last Key position
                if (col.isKey()) {
                    lastColIndex = i;
                }
            }
        }
        // mod col not find
        if (!found) {
            throw new DdlException("Column[" + newColName + "] does not exists");
        }

        // last col not find
        if (hasColPos && lastColIndex == -1) {
            throw new DdlException("Column[" + columnPos.getLastCol() + "] does not exists");
        }

        // check if add to first
        if (columnPos != null && columnPos.isFirst()) {
            lastColIndex = -1;
            hasColPos = true;
        }

        Column oriColumn = newSchema.get(modColIndex);
        // retain old column name
        modColumn.setName(oriColumn.getName());

        // handle the move operation in 'indexForFindingColumn' if has
        if (hasColPos) {
            // move col
            if (lastColIndex > modColIndex) {
                newSchema.add(lastColIndex + 1, modColumn);
                newSchema.remove(modColIndex);
            } else if (lastColIndex < modColIndex) {
                newSchema.remove(modColIndex);
                newSchema.add(lastColIndex + 1, modColumn);
            } else {
                throw new DdlException("Column[" + columnPos.getLastCol() + "] modify position is invalid");
            }
        } else {
            newSchema.set(modColIndex, modColumn);
        }
    }

    // User can modify column type and column position
    private boolean processModifyColumn(ModifyColumnClause alterClause, OlapTable olapTable,
                                        Map<Long, LinkedList<Column>> indexSchemaMap) throws DdlException {
        Column modColumn = alterClause.getColumn();
        boolean lightSchemaChange = false;

        if (KeysType.AGG_KEYS == olapTable.getKeysType()) {
            if (modColumn.isKey() && null != modColumn.getAggregationType()) {
                throw new DdlException("Can not assign aggregation method on key column: " + modColumn.getName());
            } else if (!modColumn.isKey() && null == modColumn.getAggregationType()) {
                throw new DdlException("Aggregate method must be specified for value column: " + modColumn.getName());
            }
        } else if (KeysType.UNIQUE_KEYS == olapTable.getKeysType()) {
            if (null != modColumn.getAggregationType()) {
                throw new DdlException("Can not assign aggregation method" + " on column in Unique data model table: "
                        + modColumn.getName());
            }
            if (!modColumn.isKey()) {
                if (olapTable.getEnableUniqueKeyMergeOnWrite()) {
                    modColumn.setAggregationType(AggregateType.NONE, true);
                } else {
                    modColumn.setAggregationType(AggregateType.REPLACE, true);
                }
            }
        } else {
            if (null != modColumn.getAggregationType()) {
                throw new DdlException(
                        "Can not assign aggregation method" + " on column in Duplicate data model table: "
                                + modColumn.getName());
            }
            if (!modColumn.isKey()) {
                modColumn.setAggregationType(AggregateType.NONE, true);
            }
        }
        ColumnPosition columnPos = alterClause.getColPos();
        String targetIndexName = alterClause.getRollupName();
        checkIndexExists(olapTable, targetIndexName);

        String baseIndexName = olapTable.getName();
        checkAssignedTargetIndexName(baseIndexName, targetIndexName);

        if (targetIndexName != null && columnPos == null) {
            throw new DdlException("Do not need to specify index name when just modifying column type");
        }

        String indexNameForFindingColumn = targetIndexName;
        if (indexNameForFindingColumn == null) {
            indexNameForFindingColumn = baseIndexName;
        }

        long indexIdForFindingColumn = olapTable.getIndexIdByName(indexNameForFindingColumn);

        // find modified column
        List<Column> schemaForFinding = indexSchemaMap.get(indexIdForFindingColumn);
        String newColName = modColumn.getName();
        boolean hasColPos = (columnPos != null && !columnPos.isFirst());
        boolean found = false;
        boolean typeChanged = false;
        int modColIndex = -1;
        int lastColIndex = -1;
        for (int i = 0; i < schemaForFinding.size(); i++) {
            Column col = schemaForFinding.get(i);
            if (col.getName().equalsIgnoreCase(newColName)) {
                modColIndex = i;
                found = true;
                if (!col.equals(modColumn)) {
                    typeChanged = true;
                    // TODO:the case where columnPos is not empty has not been considered
                    if (columnPos == null && col.getDataType() == PrimitiveType.VARCHAR
                            && modColumn.getDataType() == PrimitiveType.VARCHAR) {
                        col.checkSchemaChangeAllowed(modColumn);
                        lightSchemaChange = olapTable.getEnableLightSchemaChange();
                    }
                    if (col.isClusterKey()) {
                        throw new DdlException("Can not modify cluster key column: " + col.getName());
                    }
                }
            }
            if (hasColPos) {
                if (col.getNonShadowName().equalsIgnoreCase(columnPos.getLastCol())) {
                    lastColIndex = i;
                }
            } else {
                // save the last Key position
                if (col.isKey()) {
                    lastColIndex = i;
                }
            }
        }
        // mod col not find
        if (!found) {
            throw new DdlException("Column[" + newColName + "] does not exists");
        }

        // last col not find
        if (hasColPos && lastColIndex == -1) {
            throw new DdlException("Column[" + columnPos.getLastCol() + "] does not exists");
        }

        // sequence col can not change type
        if (KeysType.UNIQUE_KEYS == olapTable.getKeysType() && typeChanged && modColumn.getName()
                .equalsIgnoreCase(olapTable.getSequenceMapCol())) {
            throw new DdlException("Can not alter sequence column[" + modColumn.getName() + "]");
        }

        // check if add to first
        if (columnPos != null && columnPos.isFirst()) {
            lastColIndex = -1;
            hasColPos = true;
        }

        Column oriColumn = schemaForFinding.get(modColIndex);
        // retain old column name
        modColumn.setName(oriColumn.getName());
        modColumn.setUniqueId(oriColumn.getUniqueId());

        if (!modColumn.equals(oriColumn) && oriColumn.isAutoInc() != modColumn.isAutoInc()) {
            throw new DdlException("Can't modify the column["
                    + oriColumn.getName() + "]'s auto-increment attribute.");
        }

        // handle the move operation in 'indexForFindingColumn' if has
        if (hasColPos) {
            // move col
            if (lastColIndex > modColIndex) {
                schemaForFinding.add(lastColIndex + 1, modColumn);
                schemaForFinding.remove(modColIndex);
            } else if (lastColIndex < modColIndex) {
                schemaForFinding.remove(modColIndex);
                schemaForFinding.add(lastColIndex + 1, modColumn);
            } else {
                throw new DdlException("Column[" + columnPos.getLastCol() + "] modify position is invalid");
            }
        } else {
            schemaForFinding.set(modColIndex, modColumn);
        }

        // check if column being mod
        if (!modColumn.equals(oriColumn)) {
            // column is mod. we have to mod this column in all indices

            // handle other indices
            // 1 find other indices which contain this column
            List<Long> otherIndexIds = new ArrayList<Long>();
            for (Map.Entry<Long, List<Column>> entry : olapTable.getIndexIdToSchema().entrySet()) {
                if (entry.getKey() == indexIdForFindingColumn) {
                    // skip the index we used to find column. it has been handled before
                    continue;
                }
                List<Column> schema = entry.getValue();
                for (Column column : schema) {
                    String columnName = column.getName();
                    if (column.isMaterializedViewColumn()) {
                        columnName = MaterializedIndexMeta.normalizeName(
                                CreateMaterializedViewStmt.mvColumnBreaker(columnName));
                    }
                    if (columnName.equalsIgnoreCase(modColumn.getName())) {
                        otherIndexIds.add(entry.getKey());
                        break;
                    }
                }
            }
            for (Long otherIndexId : otherIndexIds) {
                List<Column> otherIndexSchema = indexSchemaMap.get(otherIndexId);
                for (int i = 0; i < otherIndexSchema.size(); i++) {
                    modColIndex = -1;
                    Column otherCol = null;
                    Column col = otherIndexSchema.get(i);
                    String columnName = col.getName();
                    if (col.isMaterializedViewColumn()) {
                        columnName = MaterializedIndexMeta.normalizeName(
                                CreateMaterializedViewStmt.mvColumnBreaker(columnName));
                    }
                    if (!columnName.equalsIgnoreCase(modColumn.getName())) {
                        continue;
                    }
                    modColIndex = i;
                    otherCol = new Column(modColumn);
                    otherCol.setName(col.getName());
                    otherCol.setDefineExpr(col.getDefineExpr());
                    Preconditions.checkState(modColIndex != -1);
                    Preconditions.checkState(otherCol != null);
                    // replace the old column
                    if (KeysType.AGG_KEYS != olapTable.getKeysType()
                            && KeysType.UNIQUE_KEYS != olapTable.getKeysType()) {
                        Column oldCol = otherIndexSchema.get(modColIndex);
                        otherCol.setIsKey(oldCol.isKey());
                        otherCol.setAggregationType(oldCol.getAggregationType(), oldCol.isAggregationTypeImplicit());
                    }
                    if (typeChanged && !lightSchemaChange) {
                        otherCol.setName(SHADOW_NAME_PREFIX + otherCol.getName());
                    }
                    otherIndexSchema.set(modColIndex, otherCol);
                }
            } //  end for other indices
        } // end for handling other indices

        if (typeChanged && !lightSchemaChange) {
            Optional<Column> autoIncCol = olapTable.getBaseSchema(true).stream()
                    .filter(col -> col.isAutoInc()).findFirst();
            if (autoIncCol.isPresent()) {
                throw new DdlException("Can not modify column " + modColumn.getName() + " becasue table "
                        + olapTable.getName() + " has auto-increment column " + autoIncCol.get().getName());
            }
            /*
             * In new alter table process (AlterJobV2), any modified columns are treated as new columns.
             * But the modified columns' name does not changed. So in order to distinguish this, we will add
             * a prefix in the name of these modified columns.
             * This prefix only exist during the schema change process. Once the schema change is finished,
             * it will be removed.
             *
             * After adding this prefix, modify a column is just same as 'add' a column.
             *
             * And if the column type is not changed, the same column name is still to the same column type,
             * so no need to add prefix.
             */
            modColumn.setName(SHADOW_NAME_PREFIX + modColumn.getName());
        }
        LOG.info("modify column {} ", modColumn);
        return lightSchemaChange;
    }

    private void processReorderColumn(ReorderColumnsClause alterClause, Table externalTable, List<Column> newSchema)
            throws DdlException {
        List<String> orderedColNames = alterClause.getColumnsByPos();

        newSchema.clear();
        List<Column> targetIndexSchema = externalTable.getBaseSchema();

        // check and create new ordered column list
        Set<String> colNameSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String colName : orderedColNames) {
            Column oneCol = null;
            for (Column column : targetIndexSchema) {
                if (column.getName().equalsIgnoreCase(colName) && column.isVisible()) {
                    oneCol = column;
                    break;
                }
            }
            if (oneCol == null) {
                throw new DdlException("Column[" + colName + "] not exists");
            }
            newSchema.add(oneCol);
            if (colNameSet.contains(colName)) {
                throw new DdlException("Reduplicative column[" + colName + "]");
            } else {
                colNameSet.add(colName);
            }
        }

        if (newSchema.size() != targetIndexSchema.size()) {
            throw new DdlException("Reorder stmt should contains all columns");
        }
    }

    private void processReorderColumn(ReorderColumnsClause alterClause, OlapTable olapTable,
                                      Map<Long, LinkedList<Column>> indexSchemaMap) throws DdlException {
        List<String> orderedColNames = alterClause.getColumnsByPos();
        String targetIndexName = alterClause.getRollupName();
        checkIndexExists(olapTable, targetIndexName);

        String baseIndexName = olapTable.getName();
        checkAssignedTargetIndexName(baseIndexName, targetIndexName);

        if (targetIndexName == null) {
            targetIndexName = baseIndexName;
        }
        long targetIndexId = olapTable.getIndexIdByName(targetIndexName);

        LinkedList<Column> newSchema = new LinkedList<Column>();
        List<Column> targetIndexSchema = indexSchemaMap.get(targetIndexId);
        // When rollup is specified, there is no need to check the order of generated columns.
        // When rollup is not specified and the order of baseIndex needs to be modified, the order needs to be checked.
        if (alterClause.getRollupName() == null) {
            checkOrder(targetIndexSchema, orderedColNames);
        }
        // check and create new ordered column list
        Set<String> colNameSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String colName : orderedColNames) {
            Column oneCol = null;
            for (Column column : targetIndexSchema) {
                if (column.getName().equalsIgnoreCase(colName) && column.isVisible()) {
                    oneCol = column;
                    break;
                }
            }
            if (oneCol == null) {
                throw new DdlException("Column[" + colName + "] not exists");
            }
            newSchema.add(oneCol);
            if (colNameSet.contains(colName)) {
                throw new DdlException("Reduplicative column[" + colName + "]");
            } else {
                colNameSet.add(colName);
            }
        }
        if (olapTable.getKeysType() == KeysType.UNIQUE_KEYS) {
            for (Column column : targetIndexSchema) {
                if (!column.isVisible()) {
                    newSchema.add(column);
                }
                if (column.isClusterKey()) {
                    throw new DdlException("Can not modify column order in Unique data model table");
                }
            }
        }
        if (newSchema.size() != targetIndexSchema.size()) {
            throw new DdlException("Reorder stmt should contains all columns");
        }
        // replace the old column list
        indexSchemaMap.put(targetIndexId, newSchema);
    }

    /*
     * Add 'newColumn' to specified index.
     * Modified schema will be saved in 'indexSchemaMap'
     */
    private void addColumnInternal(Column newColumn, ColumnPosition columnPos, List<Column> modIndexSchema,
                                   Set<String> newColNameSet) throws DdlException {
        String newColName = newColumn.getName();
        int posIndex = -1;
        boolean hasPos = (columnPos != null && !columnPos.isFirst());

        if (newColumn.isAutoInc()) {
            throw new DdlException("Can not add auto-increment column " + newColumn.getName());
        }

        for (int i = 0; i < modIndexSchema.size(); i++) {
            Column col = modIndexSchema.get(i);
            if (col.getName().equalsIgnoreCase(newColName)) {
                if (!newColNameSet.contains(newColName)) {
                    // if this is not a base index, we should check if user repeatedly add columns
                    throw new DdlException("Repeatedly add column: " + newColName);
                }
                // this is a base index, and the column we check here is added by previous 'add column clause'
                // in same ALTER stmt.
                // so here we will check if the 2 columns is exactly same. if not, throw exception
                if (!col.equals(newColumn)) {
                    throw new DdlException("Repeatedly add same column with different definition: " + newColName);
                }

                // column already exist, return
                return;
            }

            if (hasPos) {
                // after the field
                if (col.getName().equalsIgnoreCase(columnPos.getLastCol())) {
                    posIndex = i;
                }
            }
        }

        // check if lastCol was found
        if (hasPos && posIndex == -1) {
            throw new DdlException("Column[" + columnPos.getLastCol() + "] does not found");
        }

        // check if add to first
        if (columnPos != null && columnPos.isFirst()) {
            posIndex = -1;
            hasPos = true;
        }

        if (hasPos) {
            // key
            modIndexSchema.add(posIndex + 1, newColumn);
        } else {
            modIndexSchema.add(newColumn);
        }
    }

    /**
     * @param olapTable
     * @param newColumn              Add 'newColumn' to specified index.
     * @param columnPos
     * @param targetIndexId
     * @param baseIndexId
     * @param indexSchemaMap         Modified schema will be saved in 'indexSchemaMap'
     * @param newColNameSet
     * @param ignoreSameColumn
     * @param colUniqueIdSupplierMap
     * @return true: can light schema change, false: cannot
     * @throws DdlException
     */
    private boolean addColumnInternal(OlapTable olapTable, Column newColumn, ColumnPosition columnPos,
                                      long targetIndexId, long baseIndexId,
                                      Map<Long, LinkedList<Column>> indexSchemaMap,
                                      Set<String> newColNameSet, boolean ignoreSameColumn,
                                      Map<Long, IntSupplier> colUniqueIdSupplierMap)
            throws DdlException {

        //only new table generate ColUniqueId, exist table do not.
        boolean lightSchemaChange = olapTable.getEnableLightSchemaChange();
        String newColName = newColumn.getName();

        if (newColumn.isAutoInc()) {
            throw new DdlException("Can not add auto-increment column " + newColumn.getName());
        }

        // check the validation of aggregation method on column.
        // also fill the default aggregation method if not specified.
        if (KeysType.AGG_KEYS == olapTable.getKeysType()) {
            if (newColumn.isKey() && newColumn.getAggregationType() != null) {
                throw new DdlException("Can not assign aggregation method on key column: " + newColName);
            } else if (null == newColumn.getAggregationType()) {
                newColumn.setIsKey(true);
            } else if (newColumn.getAggregationType() == AggregateType.SUM && newColumn.getDefaultValue() != null
                    && !newColumn.getDefaultValue().equals("0")) {
                throw new DdlException(
                        "The default value of '" + newColName + "' with SUM aggregation function must be zero");
            } else if (olapTable.getDefaultDistributionInfo() instanceof RandomDistributionInfo) {
                if (newColumn.getAggregationType() == AggregateType.REPLACE
                        || newColumn.getAggregationType() == AggregateType.REPLACE_IF_NOT_NULL) {
                    throw new DdlException(
                            "Can not add value column with aggregation type " + newColumn.getAggregationType()
                                    + " for olap table with random distribution : " + newColName);
                }
            }
        } else if (KeysType.UNIQUE_KEYS == olapTable.getKeysType()) {
            if (newColumn.getAggregationType() != null) {
                throw new DdlException(
                        "Can not assign aggregation method" + " on column in Unique data model table: " + newColName);
            }
            if (!newColumn.isKey()) {
                if (olapTable.getEnableUniqueKeyMergeOnWrite()) {
                    newColumn.setAggregationType(AggregateType.NONE, false);
                } else {
                    newColumn.setAggregationType(AggregateType.REPLACE, true);
                }
            }
        } else {
            if (newColumn.getAggregationType() != null) {
                throw new DdlException(
                        "Can not assign aggregation method" + " on column in Duplicate data model table: "
                                + newColName);
            }
            if (!newColumn.isKey()) {
                if (targetIndexId != -1L
                        && olapTable.getIndexMetaByIndexId(targetIndexId).getKeysType() == KeysType.AGG_KEYS) {
                    throw new DdlException("Please add non-key column on base table directly");
                }
                newColumn.setAggregationType(AggregateType.NONE, true);
            } else if (olapTable.isDuplicateWithoutKey()) {
                throw new DdlException("Duplicate table without keys do not support add key column!");
            }
        }

        if (newColumn.getType().isTime() || newColumn.getType().isTimeV2()) {
            throw new DdlException("Time type is not supported for olap table");
        }

        // hll must be used in agg_keys
        if (newColumn.getType().isHllType() && KeysType.AGG_KEYS != olapTable.getKeysType()) {
            throw new DdlException("HLL type column can only be in Aggregation data model table: " + newColName);
        }

        if (newColumn.getAggregationType() == AggregateType.BITMAP_UNION
                && KeysType.AGG_KEYS != olapTable.getKeysType()) {
            throw new DdlException("BITMAP_UNION must be used in AGG_KEYS");
        }

        //type key column do not allow light schema change.
        if (newColumn.isKey()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("newColumn: {}, isKey()==true", newColumn);
            }
            lightSchemaChange = false;
        }

        // check if the new column already exist in base schema.
        // do not support adding new column which already exist in base schema.
        List<Column> baseSchema = olapTable.getBaseSchema(true);
        boolean found = false;
        Column foundColumn = null;
        for (Column column : baseSchema) {
            if (column.getName().equalsIgnoreCase(newColName)) {
                found = true;
                foundColumn = column;
                break;
            }
        }
        if (found) {
            if (newColName.equalsIgnoreCase(Column.DELETE_SIGN)) {
                throw new DdlException("Can not enable batch delete support, already supported batch delete.");
            } else if (newColName.equalsIgnoreCase(Column.SEQUENCE_COL)) {
                throw new DdlException("Can not enable sequence column support, already supported sequence column.");
            } else if (newColName.equalsIgnoreCase(Column.VERSION_COL)) {
                throw new DdlException("Can not enable version column support, already supported version column.");
            } else {
                if (ignoreSameColumn && newColumn.equals(foundColumn)) {
                    //for add columns rpc, allow add same type column.
                } else {
                    throw new DdlException("Can not add column which already exists in base table: " + newColName);
                }
            }
        }

        if (newColumn.getGeneratedColumnInfo() != null) {
            throw new DdlException("Not supporting alter table add generated columns.");
        }

        /*
         * add new column to indexes.
         * UNIQUE:
         *      1. If new column is key, it should be added to all indexes.
         *      2. Else, add the new column to base index and specified rollup index.
         * DUPLICATE:
         *      1. If not specify rollup index, just add it to base index.
         *      2. Else, first add it to specify rollup index. Then if the new column is key, add it to base
         *          index, at the end of all other existing key columns. If new new column is value, add it to
         *          base index by user specified position.
         * AGGREGATION:
         *      1. Add it to base index, as well as specified rollup index.
         */
        if (KeysType.UNIQUE_KEYS == olapTable.getKeysType()) {
            List<Column> modIndexSchema;
            if (newColumn.isKey()) {
                // add key column to unique key table
                // add to all indexes including base and rollup
                for (Map.Entry<Long, LinkedList<Column>> entry : indexSchemaMap.entrySet()) {
                    modIndexSchema = entry.getValue();
                    boolean isBaseIdex = entry.getKey() == baseIndexId;
                    IntSupplier colUniqueIdSupplier = colUniqueIdSupplierMap.get(entry.getKey());
                    int newColumnUniqueId = olapTable.getEnableLightSchemaChange() ? colUniqueIdSupplier.getAsInt()
                            : Column.COLUMN_UNIQUE_ID_INIT_VALUE;
                    checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, isBaseIdex,
                            newColumnUniqueId);
                }
            } else {
                // 1. add to base table
                modIndexSchema = indexSchemaMap.get(baseIndexId);
                IntSupplier baseIndexColUniqueIdSupplier = colUniqueIdSupplierMap.get(baseIndexId);
                int baseIndexNewColumnUniqueId = olapTable.getEnableLightSchemaChange()
                        ? baseIndexColUniqueIdSupplier.getAsInt() : Column.COLUMN_UNIQUE_ID_INIT_VALUE;
                checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, true,
                        baseIndexNewColumnUniqueId);
                if (targetIndexId == -1L) {
                    return lightSchemaChange;
                }
                // 2. add to rollup
                modIndexSchema = indexSchemaMap.get(targetIndexId);
                IntSupplier targetIndexColUniqueIdSupplier = colUniqueIdSupplierMap.get(targetIndexId);
                int rollUpNewColumnUniqueId = olapTable.getEnableLightSchemaChange()
                        ? targetIndexColUniqueIdSupplier.getAsInt() : Column.COLUMN_UNIQUE_ID_INIT_VALUE;
                checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, false, rollUpNewColumnUniqueId);
            }
        } else if (KeysType.DUP_KEYS == olapTable.getKeysType()) {
            //get baseIndexColUniqueIdSupplier
            IntSupplier baseIndexColUniqueIdSupplier = colUniqueIdSupplierMap.get(baseIndexId);
            int baseIndexNewColumnUniqueId = olapTable.getEnableLightSchemaChange()
                    ? baseIndexColUniqueIdSupplier.getAsInt() : Column.COLUMN_UNIQUE_ID_INIT_VALUE;

            if (targetIndexId == -1L) {
                // add to base index
                List<Column> modIndexSchema = indexSchemaMap.get(baseIndexId);
                checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, true,
                        baseIndexNewColumnUniqueId);
                // no specified target index. return
                return lightSchemaChange;
            } else {
                // add to rollup index
                List<Column> modIndexSchema = indexSchemaMap.get(targetIndexId);
                IntSupplier targetIndexColUniqueIdSupplier = colUniqueIdSupplierMap.get(targetIndexId);
                int rollUpNewColumnUniqueId = olapTable.getEnableLightSchemaChange()
                        ? targetIndexColUniqueIdSupplier.getAsInt() : Column.COLUMN_UNIQUE_ID_INIT_VALUE;
                checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, false, rollUpNewColumnUniqueId);

                if (newColumn.isKey()) {
                    /*
                     * if add column in rollup is key,
                     * then put the column in base table as the last key column
                     */

                    modIndexSchema = indexSchemaMap.get(baseIndexId);
                    checkAndAddColumn(modIndexSchema, newColumn, null, newColNameSet, true, baseIndexNewColumnUniqueId);
                } else {
                    modIndexSchema = indexSchemaMap.get(baseIndexId);
                    checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, true,
                            baseIndexNewColumnUniqueId);
                }
            }
        } else {
            // check if has default value. this should be done in Analyze phase
            // 1. add to base index first
            IntSupplier baseIndexColUniqueIdSupplier = colUniqueIdSupplierMap.get(baseIndexId);
            int baseIndexNewColumnUniqueId = olapTable.getEnableLightSchemaChange()
                    ? baseIndexColUniqueIdSupplier.getAsInt() : Column.COLUMN_UNIQUE_ID_INIT_VALUE;
            List<Column> modIndexSchema = indexSchemaMap.get(baseIndexId);
            checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, true, baseIndexNewColumnUniqueId);

            if (targetIndexId == -1L) {
                // no specified target index. return
                return lightSchemaChange;
            }

            // 2. add to rollup index
            IntSupplier targetIndexColUniqueIdSupplier = colUniqueIdSupplierMap.get(targetIndexId);
            int rollUpNewColumnUniqueId = olapTable.getEnableLightSchemaChange()
                    ? targetIndexColUniqueIdSupplier.getAsInt() : Column.COLUMN_UNIQUE_ID_INIT_VALUE;
            modIndexSchema = indexSchemaMap.get(targetIndexId);
            checkAndAddColumn(modIndexSchema, newColumn, columnPos, newColNameSet, false, rollUpNewColumnUniqueId);
        }
        return lightSchemaChange;
    }

    /*
     * add new column to specified index schema('modIndexSchema').
     * if 'isBaseIndex' is true, which means 'modIndexSchema' is base index's schema.
     * so we will not check repeat adding of column.
     * For example, user want to add column k1 to both rollup1 and rollup2 in one alter stmt:
     *      ADD COLUMN k1 int to rollup1,
     *      ADD COLUMN k1 int to rollup2
     * So that k1 will be added to base index 'twice', and we just ignore this repeat adding.
     */
    private void checkAndAddColumn(List<Column> modIndexSchema, Column newColumn, ColumnPosition columnPos,
                                   Set<String> newColNameSet, boolean isBaseIndex, int newColumnUniqueId)
            throws DdlException {
        int posIndex = -1;
        int lastVisibleIdx = -1;
        String newColName = newColumn.getName();
        boolean hasPos = (columnPos != null && !columnPos.isFirst());
        for (int i = 0; i < modIndexSchema.size(); i++) {
            Column col = modIndexSchema.get(i);
            if (col.getName().equalsIgnoreCase(newColName)) {
                if (!isBaseIndex || !newColNameSet.contains(newColName)) {
                    // if this is not a base index, we should check if user repeatedly add columns
                    throw new DdlException("Repeatedly add column: " + newColName);
                }
                // this is a base index, and the column we check here is added by previous 'add column clause'
                // in same ALTER stmt.
                // so here we will check if the 2 columns is exactly same. if not, throw exception
                if (!col.equals(newColumn)) {
                    throw new DdlException("Repeatedly add same column with different definition: " + newColName);
                }

                // column already exist, return
                return;
            }
            if (col.isVisible()) {
                lastVisibleIdx = i;
            }

            if (hasPos) {
                // after the field
                if (col.getName().equalsIgnoreCase(columnPos.getLastCol())) {
                    posIndex = i;
                }
            } else {
                // save the last Key position
                if (col.isKey()) {
                    posIndex = i;
                }
            }
        }

        // check if lastCol was found
        if (hasPos && posIndex == -1) {
            throw new DdlException("Column[" + columnPos.getLastCol() + "] does not found");
        }

        // check if add to first
        if (columnPos != null && columnPos.isFirst()) {
            posIndex = -1;
            hasPos = true;
        }

        // newColumn may add to baseIndex or rollups, so we need copy before change UniqueId
        Column toAddColumn = new Column(newColumn);
        toAddColumn.setUniqueId(newColumnUniqueId);
        if (hasPos) {
            modIndexSchema.add(posIndex + 1, toAddColumn);
        } else if (toAddColumn.isKey()) {
            // key
            modIndexSchema.add(posIndex + 1, toAddColumn);
        } else if (lastVisibleIdx != -1 && lastVisibleIdx < modIndexSchema.size() - 1) {
            // has hidden columns
            modIndexSchema.add(lastVisibleIdx + 1, toAddColumn);
        } else {
            // value
            modIndexSchema.add(toAddColumn);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("newColumn setUniqueId({}), modIndexSchema:{}", newColumnUniqueId, modIndexSchema);
        }
    }

    private void checkIndexExists(OlapTable olapTable, String targetIndexName) throws DdlException {
        if (targetIndexName != null && !olapTable.hasMaterializedIndex(targetIndexName)) {
            throw new DdlException(
                    "Index[" + targetIndexName + "] does not exist in table[" + olapTable.getName() + "]");
        }
    }

    private void checkAssignedTargetIndexName(String baseIndexName, String targetIndexName) throws DdlException {
        // user cannot assign base index to do schema change
        if (targetIndexName != null) {
            if (targetIndexName.equals(baseIndexName)) {
                throw new DdlException("Do not need to assign base index[" + baseIndexName + "] to do schema change");
            }
        }
    }

    private void createJob(String rawSql, long dbId, OlapTable olapTable, Map<Long, LinkedList<Column>> indexSchemaMap,
                           Map<String, String> propertyMap, List<Index> indexes) throws UserException {
        checkReplicaCount(olapTable);

        // process properties first
        // for now. properties has 3 options
        // property 1. to specify short key column count.
        // eg.
        //     "indexname1#short_key" = "3"
        //     "indexname2#short_key" = "4"
        Map<Long, Map<String, String>> indexIdToProperties = new HashMap<Long, Map<String, String>>();
        if (propertyMap.size() > 0) {
            for (String key : propertyMap.keySet()) {
                if (key.endsWith(PropertyAnalyzer.PROPERTIES_SHORT_KEY)) {
                    // short key
                    String[] keyArray = key.split("#");
                    if (keyArray.length != 2 || keyArray[0].isEmpty() || !keyArray[1].equals(
                            PropertyAnalyzer.PROPERTIES_SHORT_KEY)) {
                        throw new DdlException("Invalid alter table property: " + key);
                    }

                    HashMap<String, String> prop = new HashMap<String, String>();

                    if (!olapTable.hasMaterializedIndex(keyArray[0])) {
                        throw new DdlException("Index[" + keyArray[0] + "] does not exist");
                    }

                    prop.put(PropertyAnalyzer.PROPERTIES_SHORT_KEY, propertyMap.get(key));
                    indexIdToProperties.put(olapTable.getIndexIdByName(keyArray[0]), prop);
                }
            } // end for property keys
        }

        // for bitmapIndex
        boolean hasIndexChange = false;
        Set<Index> newSet = new HashSet<>(indexes);
        Set<Index> oriSet = new HashSet<>(olapTable.getIndexes());
        if (!newSet.equals(oriSet)) {
            hasIndexChange = true;
        }

        // property 2. bloom filter
        // eg. "bloom_filter_columns" = "k1,k2", "bloom_filter_fpp" = "0.05"
        Set<String> bfColumns = null;
        double bfFpp = 0;
        try {
            bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(propertyMap,
                    indexSchemaMap.get(olapTable.getBaseIndexId()), olapTable.getKeysType());
            bfFpp = PropertyAnalyzer.analyzeBloomFilterFpp(propertyMap);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // check bloom filter has change
        boolean hasBfChange = false;
        Set<String> oriBfColumns = olapTable.getCopiedBfColumns();
        double oriBfFpp = olapTable.getBfFpp();
        if (bfColumns != null) {
            if (bfFpp == 0) {
                // columns: yes, fpp: no
                if (bfColumns.equals(oriBfColumns)) {
                    throw new DdlException("Bloom filter index has no change");
                }

                if (oriBfColumns == null) {
                    bfFpp = FeConstants.default_bloom_filter_fpp;
                } else {
                    bfFpp = oriBfFpp;
                }
            } else {
                // columns: yes, fpp: yes
                if (bfColumns.equals(oriBfColumns) && bfFpp == oriBfFpp) {
                    throw new DdlException("Bloom filter index has no change");
                }
            }

            hasBfChange = true;
        } else {
            if (bfFpp == 0) {
                // columns: no, fpp: no
                bfFpp = oriBfFpp;
            } else {
                // columns: no, fpp: yes
                if (bfFpp == oriBfFpp) {
                    throw new DdlException("Bloom filter index has no change");
                }
                if (oriBfColumns == null) {
                    throw new DdlException("Bloom filter index has no change");
                }

                hasBfChange = true;
            }

            bfColumns = oriBfColumns;
        }

        if (bfColumns != null && bfColumns.isEmpty()) {
            bfColumns = null;
        }
        if (bfColumns == null) {
            bfFpp = 0;
        }

        Index.checkConflict(newSet, bfColumns);

        // property 3: timeout
        long timeoutSecond = PropertyAnalyzer.analyzeTimeout(propertyMap, Config.alter_table_timeout_second);

        TStorageFormat storageFormat = PropertyAnalyzer.analyzeStorageFormat(propertyMap);

        // property store_row_column && row_store_columns
        // eg. "store_row_column" = "true"
        // eg. "row_store_columns" = "k1, k2"
        List<String> rsColumns = Lists.newArrayList();
        boolean storeRowColumn = false;
        try {
            storeRowColumn = PropertyAnalyzer.analyzeStoreRowColumn(propertyMap);
            rsColumns = PropertyAnalyzer.analyzeRowStoreColumns(propertyMap,
                        olapTable.getColumns().stream().map(Column::getName).collect(Collectors.toList()));
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        // check row store column has change
        boolean hasRowStoreChanged = false;
        if (storeRowColumn || (rsColumns != null && !rsColumns.isEmpty())) {
            List<String> oriRowStoreColumns = olapTable.getTableProperty().getCopiedRowStoreColumns();
            if ((oriRowStoreColumns != null && !oriRowStoreColumns.equals(rsColumns))
                    || storeRowColumn != olapTable.storeRowColumn()) {
                hasRowStoreChanged = true;
            }
        }

        // begin checking each table
        // ATTN: DO NOT change any meta in this loop
        long tableId = olapTable.getId();
        Map<Long, Short> indexIdToShortKeyColumnCount = Maps.newHashMap();
        Map<Long, List<Column>> changedIndexIdToSchema = Maps.newHashMap();
        for (Long alterIndexId : indexSchemaMap.keySet()) {
            // Must get all columns including invisible columns.
            // Because in alter process, all columns must be considered.
            List<Column> originSchema = olapTable.getSchemaByIndexId(alterIndexId, true);
            List<Column> alterSchema = indexSchemaMap.get(alterIndexId);
            Set<Column> needAlterColumns = Sets.newHashSet();

            // 0. check if unchanged
            boolean hasColumnChange = false;
            if (alterSchema.size() != originSchema.size()) {
                hasColumnChange = true;
            } else {
                for (int i = 0; i < alterSchema.size(); i++) {
                    Column alterColumn = alterSchema.get(i);
                    if (!alterColumn.equals(originSchema.get(i))) {
                        needAlterColumns.add(alterColumn);
                        hasColumnChange = true;
                    } else {
                        Column oriColumn = originSchema.get(i);
                        if ((oriColumn.getGeneratedColumnInfo() != null
                                || alterColumn.getGeneratedColumnInfo() != null)
                                        && !oriColumn.getGeneratedColumnInfo().getExprSql()
                                                .equals(alterColumn.getGeneratedColumnInfo().getExprSql())) {
                            throw new DdlException("Not supporting alter table modify generated columns.");
                        }
                    }
                }
            }

            // if has column changed, alter it.
            // else:
            //     if no bf change, no alter
            //     if has bf change, should check
            boolean needAlter = false;
            if (hasColumnChange) {
                needAlter = true;
            } else if (hasBfChange) {
                for (Column alterColumn : alterSchema) {
                    String columnName = alterColumn.getName();

                    boolean isOldBfColumn = false;
                    if (oriBfColumns != null && oriBfColumns.contains(columnName)) {
                        isOldBfColumn = true;
                    }

                    boolean isNewBfColumn = false;
                    if (bfColumns != null && bfColumns.contains(columnName)) {
                        isNewBfColumn = true;
                    }

                    if (isOldBfColumn != isNewBfColumn) {
                        // bf column change
                        needAlter = true;
                    } else if (isOldBfColumn && isNewBfColumn && oriBfFpp != bfFpp) {
                        // bf fpp change
                        needAlter = true;
                    }

                    if (needAlter) {
                        break;
                    }
                }
            } else if (hasIndexChange) {
                needAlter = true;
            } else if (hasRowStoreChanged) {
                needAlter = true;
            } else if (storageFormat == TStorageFormat.V2) {
                if (olapTable.getStorageFormat() != TStorageFormat.V2) {
                    needAlter = true;
                }
            }

            if (!needAlter) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("index[{}] is not changed. ignore", alterIndexId);
                }
                continue;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("index[{}] is changed. start checking...", alterIndexId);
            }
            // 1. check order: a) has key; b) value after key
            boolean meetValue = false;
            boolean hasKey = false;
            for (Column column : alterSchema) {
                if (column.isKey() && meetValue) {
                    throw new DdlException(
                            "Invalid column order. value should be after key. index[" + olapTable.getIndexNameById(
                                    alterIndexId) + "]");
                }
                if (!column.isKey()) {
                    meetValue = true;
                } else {
                    hasKey = true;
                }
            }
            if (!hasKey && !olapTable.isDuplicateWithoutKey()) {
                throw new DdlException("No key column left. index[" + olapTable.getIndexNameById(alterIndexId) + "]");
            }

            // 2. check compatible
            for (Column alterColumn : alterSchema) {
                for (Column oriColumn : originSchema) {
                    if (alterColumn.nameEquals(oriColumn.getName(), true /* ignore prefix */)) {
                        if (!alterColumn.equals(oriColumn)) {
                            // 3.1 check type
                            oriColumn.checkSchemaChangeAllowed(alterColumn);
                        }
                    }
                } // end for ori
            } // end for alter

            // 3. check partition key
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (partitionInfo.getType() == PartitionType.RANGE || partitionInfo.getType() == PartitionType.LIST) {
                List<Column> partitionColumns = partitionInfo.getPartitionColumns();
                for (Column partitionCol : partitionColumns) {
                    boolean found = false;
                    for (Column alterColumn : alterSchema) {
                        if (alterColumn.nameEquals(partitionCol.getName(), true)) {
                            // 2.1 partition column cannot be modified
                            if (needAlterColumns.contains(alterColumn) && !alterColumn.equals(partitionCol)) {
                                throw new DdlException(
                                        "Can not modify partition column[" + partitionCol.getName() + "]. index["
                                                + olapTable.getIndexNameById(alterIndexId) + "]");
                            }
                            found = true;
                            break;
                        }
                    } // end for alterColumns

                    if (!found && alterIndexId == olapTable.getBaseIndexId()) {
                        // 2.1 partition column cannot be deleted.
                        throw new DdlException(
                                "Partition column[" + partitionCol.getName() + "] cannot be dropped. index["
                                        + olapTable.getIndexNameById(alterIndexId) + "]");
                        // ATTN. partition columns' order also need remaining unchanged.
                        // for now, we only allow one partition column, so no need to check order.
                    }
                } // end for partitionColumns
            }

            // 4. check distribution key:
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            if (distributionInfo.getType() == DistributionInfoType.HASH) {
                List<Column> distributionColumns = ((HashDistributionInfo) distributionInfo).getDistributionColumns();
                for (Column distributionCol : distributionColumns) {
                    boolean found = false;
                    for (Column alterColumn : alterSchema) {
                        if (alterColumn.nameEquals(distributionCol.getName(), true)) {
                            // 3.1 distribution column cannot be modified
                            if (needAlterColumns.contains(alterColumn) && !alterColumn.equals(distributionCol)) {
                                throw new DdlException(
                                        "Can not modify distribution column[" + distributionCol.getName() + "]. index["
                                                + olapTable.getIndexNameById(alterIndexId) + "]");
                            }
                            found = true;
                            break;
                        }
                    } // end for alterColumns

                    if (!found && alterIndexId == olapTable.getBaseIndexId()) {
                        // 2.2 distribution column cannot be deleted.
                        throw new DdlException(
                                "Distribution column[" + distributionCol.getName() + "] cannot be dropped. index["
                                        + olapTable.getIndexNameById(alterIndexId) + "]");
                    }
                } // end for distributionCols
            }

            // 5. calc short key
            short newShortKeyColumnCount = Env.calcShortKeyColumnCount(alterSchema,
                    indexIdToProperties.get(alterIndexId), !olapTable.isDuplicateWithoutKey());
            if (LOG.isDebugEnabled()) {
                LOG.debug("alter index[{}] short key column count: {}", alterIndexId, newShortKeyColumnCount);
            }
            indexIdToShortKeyColumnCount.put(alterIndexId, newShortKeyColumnCount);

            // 6. store the changed columns for edit log
            changedIndexIdToSchema.put(alterIndexId, alterSchema);

            if (LOG.isDebugEnabled()) {
                LOG.debug("schema change[{}-{}-{}] check pass.", dbId, tableId, alterIndexId);
            }
        } // end for indices

        if (changedIndexIdToSchema.isEmpty() && !hasIndexChange && !hasRowStoreChanged) {
            throw new DdlException("Nothing is changed. please check your alter stmt.");
        }

        // create job
        long bufferSize = IdGeneratorUtil.getBufferSizeForAlterTable(olapTable, changedIndexIdToSchema.keySet());
        IdGeneratorBuffer idGeneratorBuffer = Env.getCurrentEnv().getIdGeneratorBuffer(bufferSize);
        long jobId = idGeneratorBuffer.getNextId();
        SchemaChangeJobV2 schemaChangeJob =
                AlterJobV2Factory.createSchemaChangeJobV2(rawSql, jobId, dbId, olapTable.getId(), olapTable.getName(),
                        timeoutSecond * 1000);
        schemaChangeJob.setBloomFilterInfo(hasBfChange, bfColumns, bfFpp);
        schemaChangeJob.setAlterIndexInfo(hasIndexChange, indexes);
        schemaChangeJob.setStoreRowColumnInfo(hasRowStoreChanged, storeRowColumn, rsColumns);

        // If StorageFormat is set to TStorageFormat.V2
        // which will create tablet with preferred_rowset_type set to BETA
        // for both base table and rollup index
        if (hasIndexChange) {
            // only V2 support index, so if there is index changed, storage format must be V2
            storageFormat = TStorageFormat.V2;
        }
        schemaChangeJob.setStorageFormat(storageFormat);

        // the following operations are done outside the 'for indices' loop
        // to avoid partial check success

        /*
         * Create schema change job
         * 1. For each index which has been changed, create a SHADOW index,
         *    and save the mapping of origin index to SHADOW index.
         * 2. Create all tablets and replicas of all SHADOW index, add them to tablet inverted index.
         * 3. Change table's state as SCHEMA_CHANGE
         */
        for (Map.Entry<Long, List<Column>> entry : changedIndexIdToSchema.entrySet()) {
            long originIndexId = entry.getKey();
            MaterializedIndexMeta currentIndexMeta = olapTable.getIndexMetaByIndexId(originIndexId);
            // 1. get new schema version/schema version hash, short key column count
            int currentSchemaVersion = currentIndexMeta.getSchemaVersion();
            int newSchemaVersion = currentSchemaVersion + 1;
            // generate schema hash for new index has to generate a new schema hash not equal to current schema hash
            int currentSchemaHash = currentIndexMeta.getSchemaHash();
            int newSchemaHash = Util.generateSchemaHash();
            while (currentSchemaHash == newSchemaHash) {
                newSchemaHash = Util.generateSchemaHash();
            }
            String newIndexName = SHADOW_NAME_PREFIX + olapTable.getIndexNameById(originIndexId);
            short newShortKeyColumnCount = indexIdToShortKeyColumnCount.get(originIndexId);
            long shadowIndexId = idGeneratorBuffer.getNextId();

            // create SHADOW index for each partition
            List<Tablet> addedTablets = Lists.newArrayList();
            for (Partition partition : olapTable.getPartitions()) {
                long partitionId = partition.getId();
                TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
                // index state is SHADOW
                MaterializedIndex shadowIndex = new MaterializedIndex(shadowIndexId, IndexState.SHADOW);
                MaterializedIndex originIndex = partition.getIndex(originIndexId);
                ReplicaAllocation replicaAlloc = olapTable.getPartitionInfo().getReplicaAllocation(partitionId);
                Short totalReplicaNum = replicaAlloc.getTotalReplicaNum();
                for (Tablet originTablet : originIndex.getTablets()) {
                    TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, partitionId, shadowIndexId,
                            newSchemaHash, medium);
                    long originTabletId = originTablet.getId();
                    long shadowTabletId = idGeneratorBuffer.getNextId();

                    Tablet shadowTablet = EnvFactory.getInstance().createTablet(shadowTabletId);
                    shadowIndex.addTablet(shadowTablet, shadowTabletMeta);
                    addedTablets.add(shadowTablet);

                    schemaChangeJob.addTabletIdMap(partitionId, shadowIndexId, shadowTabletId, originTabletId);
                    List<Replica> originReplicas = originTablet.getReplicas();

                    int healthyReplicaNum = 0;
                    for (Replica originReplica : originReplicas) {
                        long shadowReplicaId = idGeneratorBuffer.getNextId();
                        long backendId = originReplica.getBackendId();

                        if (originReplica.getState() == Replica.ReplicaState.CLONE
                                || originReplica.getState() == Replica.ReplicaState.DECOMMISSION
                                || originReplica.getState() == ReplicaState.COMPACTION_TOO_SLOW
                                || originReplica.getLastFailedVersion() > 0) {
                            LOG.info("origin replica {} of tablet {} state is {},"
                                            + " and last failed version is {}, skip creating shadow replica",
                                    originReplica.getId(), originReplica, originReplica.getState(),
                                    originReplica.getLastFailedVersion());
                            continue;
                        }
                        Preconditions.checkState(originReplica.getState() == ReplicaState.NORMAL,
                                originReplica.getState());
                        ReplicaContext context = new ReplicaContext();
                        context.replicaId = shadowReplicaId;
                        context.backendId = backendId;
                        context.state = ReplicaState.ALTER;
                        context.version = Partition.PARTITION_INIT_VERSION;
                        context.schemaHash = newSchemaHash;
                        context.dbId = dbId;
                        context.tableId = tableId;
                        context.partitionId = partitionId;
                        context.indexId = shadowIndexId;
                        context.originReplica = originReplica;
                        // replica's init state is ALTER, so that tablet report process will ignore its report
                        Replica shadowReplica = EnvFactory.getInstance().createReplica(context);
                        shadowTablet.addReplica(shadowReplica);
                        healthyReplicaNum++;
                    }

                    if (healthyReplicaNum < totalReplicaNum / 2 + 1) {
                        /*
                         * TODO(cmy): This is a bad design.
                         * Because in the schema change job, we will only send tasks to the shadow replicas
                         * that have been created, without checking whether the quorum of replica number are satisfied.
                         * This will cause the job to fail until we find that the quorum of replica number
                         * is not satisfied until the entire job is done.
                         * So here we check the replica number strictly and do not allow to submit the job
                         * if the quorum of replica number is not satisfied.
                         */
                        for (Tablet tablet : addedTablets) {
                            Env.getCurrentInvertedIndex().deleteTablet(tablet.getId());
                        }
                        throw new DdlException(
                                "tablet " + originTabletId + " has few healthy replica: " + healthyReplicaNum);
                    }
                }

                schemaChangeJob.addPartitionShadowIndex(partitionId, shadowIndexId, shadowIndex);
            } // end for partition
            schemaChangeJob.addIndexSchema(shadowIndexId, originIndexId, newIndexName, newSchemaVersion, newSchemaHash,
                    newShortKeyColumnCount, entry.getValue());
        } // end for index

        // set table state
        olapTable.setState(OlapTableState.SCHEMA_CHANGE);

        // 2. add schemaChangeJob
        addAlterJobV2(schemaChangeJob);

        // 3. write edit log
        Env.getCurrentEnv().getEditLog().logAlterJob(schemaChangeJob);
        LOG.info("finished to create schema change job: {}", schemaChangeJob.getJobId());
    }

    @Override
    protected void runAfterCatalogReady() {
        if (cycleCount >= CYCLE_COUNT_TO_CHECK_EXPIRE_SCHEMA_CHANGE_JOB) {
            clearFinishedOrCancelledSchemaChangeJobV2();
            clearExpireFinishedOrCancelledIndexChangeJobs();
            super.runAfterCatalogReady();
            cycleCount = 0;
        }
        runAlterJobV2();
        runIndexChangeJob();
        cycleCount++;
    }

    private void runAlterJobV2() {
        if (Config.forbid_running_alter_job) {
            return;
        }
        runnableSchemaChangeJobV2.values().forEach(alterJobsV2 -> {
            if (!alterJobsV2.isDone() && !activeSchemaChangeJobsV2.containsKey(alterJobsV2.getJobId())
                    && activeSchemaChangeJobsV2.size() < MAX_ACTIVE_SCHEMA_CHANGE_JOB_V2_SIZE) {
                if (FeConstants.runningUnitTest) {
                    alterJobsV2.run();
                } else {
                    schemaChangeThreadPool.submit(() -> {
                        if (activeSchemaChangeJobsV2.putIfAbsent(alterJobsV2.getJobId(), alterJobsV2) == null) {
                            try {
                                alterJobsV2.run();
                            } finally {
                                activeSchemaChangeJobsV2.remove(alterJobsV2.getJobId());
                            }
                        }
                    });
                }
            }
        });
    }

    private void runIndexChangeJob() {
        runnableIndexChangeJob.values().forEach(indexChangeJob -> {
            if (!indexChangeJob.isDone() && !activeIndexChangeJob.containsKey(indexChangeJob.getJobId())
                    && activeIndexChangeJob.size() < MAX_ACTIVE_SCHEMA_CHANGE_JOB_V2_SIZE) {
                if (FeConstants.runningUnitTest) {
                    indexChangeJob.run();
                } else {
                    schemaChangeThreadPool.submit(() -> {
                        if (activeIndexChangeJob.putIfAbsent(
                                    indexChangeJob.getJobId(), indexChangeJob) == null) {
                            try {
                                indexChangeJob.run();
                            } finally {
                                activeIndexChangeJob.remove(indexChangeJob.getJobId());
                            }
                        }
                    });
                }
            }

            if (indexChangeJob.isDone()) {
                runnableIndexChangeJob.remove(indexChangeJob.getJobId());
            }
        });
    }

    private void changeTableState(long dbId, long tableId, OlapTableState olapTableState) {
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
            OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableId, Table.TableType.OLAP);
            olapTable.writeLockOrMetaException();
            try {
                if (olapTable.getState() == olapTableState) {
                    return;
                } else if (olapTable.getState() == OlapTableState.SCHEMA_CHANGE) {
                    olapTable.setState(olapTableState);
                }
            } finally {
                olapTable.writeUnlock();
            }
        } catch (MetaNotFoundException e) {
            LOG.warn("[INCONSISTENT META] changing table status failed after schema change job done", e);
        }
    }

    public Map<Long, IndexChangeJob> getIndexChangeJobs() {
        return indexChangeJobs;
    }

    public List<List<Comparable>> getAllIndexChangeJobInfos() {
        List<List<Comparable>> indexChangeJobInfos = new LinkedList<>();
        for (IndexChangeJob indexChangeJob : ImmutableList.copyOf(indexChangeJobs.values())) {
            // no need to check priv here. This method is only called in show proc stmt,
            // which already check the ADMIN priv.
            indexChangeJob.getInfo(indexChangeJobInfos);
        }

        // sort by "JobId", "TableName", "PartitionName", "CreateTime", "FinishTime"
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1, 2, 3, 4);
        indexChangeJobInfos.sort(comparator);
        return indexChangeJobInfos;
    }

    public List<List<Comparable>> getAllIndexChangeJobInfos(Database db) {
        List<List<Comparable>> indexChangeJobInfos = new LinkedList<>();
        for (IndexChangeJob indexChangeJob : ImmutableList.copyOf(indexChangeJobs.values())) {
            if (indexChangeJob.getDbId() != db.getId()) {
                continue;
            }
            indexChangeJob.getInfo(indexChangeJobInfos);
        }


        // sort by "JobId", "TableName", "PartitionName", "CreateTime", "FinishTime"
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1, 2, 3, 4);
        indexChangeJobInfos.sort(comparator);
        return indexChangeJobInfos;
    }

    public List<List<Comparable>> getAllAlterJobInfos() {
        List<List<Comparable>> schemaChangeJobInfos = new LinkedList<>();
        for (AlterJobV2 alterJob : ImmutableList.copyOf(alterJobsV2.values())) {
            // no need to check priv here. This method is only called in show proc stmt,
            // which already check the ADMIN priv.
            alterJob.getInfo(schemaChangeJobInfos);
        }

        // sort by "JobId", "PartitionName", "CreateTime", "FinishTime", "IndexName", "IndexState"
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1, 2, 3, 4, 5);
        schemaChangeJobInfos.sort(comparator);
        return schemaChangeJobInfos;
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        List<List<Comparable>> schemaChangeJobInfos = new LinkedList<>();
        getAlterJobV2Infos(db, schemaChangeJobInfos);

        // sort by "JobId", "PartitionName", "CreateTime", "FinishTime", "IndexName", "IndexState"
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1, 2, 3, 4, 5);
        schemaChangeJobInfos.sort(comparator);
        return schemaChangeJobInfos;
    }

    private void getAlterJobV2Infos(Database db, List<AlterJobV2> alterJobsV2,
                                    List<List<Comparable>> schemaChangeJobInfos) {
        ConnectContext ctx = ConnectContext.get();
        for (AlterJobV2 alterJob : alterJobsV2) {
            if (alterJob.getDbId() != db.getId()) {
                continue;
            }
            if (ctx != null) {
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ctx, InternalCatalog.INTERNAL_CATALOG_NAME, db.getFullName(),
                                alterJob.getTableName(), PrivPredicate.ALTER)) {
                    continue;
                }
            }
            alterJob.getInfo(schemaChangeJobInfos);
        }
    }

    private void getAlterJobV2Infos(Database db, List<List<Comparable>> schemaChangeJobInfos) {
        getAlterJobV2Infos(db, ImmutableList.copyOf(alterJobsV2.values()), schemaChangeJobInfos);
    }

    @Override
    public void process(String rawSql, List<AlterClause> alterClauses, Database db,
                        OlapTable olapTable)
            throws UserException {
        olapTable.writeLockOrDdlException();
        try {
            olapTable.checkNormalStateForAlter();
            //alterClauses can or cannot light schema change
            boolean lightSchemaChange = true;
            boolean lightIndexChange = false;
            boolean buildIndexChange = false;

            // index id -> index schema
            Map<Long, LinkedList<Column>> indexSchemaMap = new HashMap<>();

            //for multi add colmuns clauses
            //index id -> index col_unique_id supplier
            Map<Long, IntSupplier> colUniqueIdSupplierMap = new HashMap<>();
            for (Map.Entry<Long, List<Column>> entry : olapTable.getIndexIdToSchema(true).entrySet()) {
                indexSchemaMap.put(entry.getKey(), new LinkedList<>(entry.getValue()));

                IntSupplier colUniqueIdSupplier = null;
                if (olapTable.getEnableLightSchemaChange()) {
                    colUniqueIdSupplier = new IntSupplier() {
                        public int pendingMaxColUniqueId = olapTable.getIndexMetaByIndexId(entry.getKey())
                                .getMaxColUniqueId();
                        public long indexId = entry.getKey();

                        @Override
                        public int getAsInt() {
                            pendingMaxColUniqueId++;
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("index id:{}, pendingMaxColUniqueId:{}", indexId, pendingMaxColUniqueId);
                            }
                            return pendingMaxColUniqueId;
                        }
                    };
                }
                colUniqueIdSupplierMap.put(entry.getKey(), colUniqueIdSupplier);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("in process indexSchemaMap:{}", indexSchemaMap);
            }

            List<Index> newIndexes = olapTable.getCopiedIndexes();
            List<Index> alterIndexes = new ArrayList<>();
            Map<Long, Set<String>> invertedIndexOnPartitions = new HashMap<>();
            boolean isDropIndex = false;
            Map<String, String> propertyMap = new HashMap<>();
            for (AlterClause alterClause : alterClauses) {
                Map<String, String> properties = alterClause.getProperties();
                if (properties != null) {
                    if (propertyMap.isEmpty()) {
                        propertyMap.putAll(properties);
                    } else {
                        throw new DdlException("reduplicated PROPERTIES");
                    }

                    // modification of colocate property is handle alone.
                    // And because there should be only one colocate property modification clause in stmt,
                    // so just return after finished handling.
                    if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)) {
                        String colocateGroup = properties.get(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH);
                        Env.getCurrentEnv().modifyTableColocate(db, olapTable, colocateGroup, false, null);
                        return;
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE)) {
                        String distributionType = properties.get(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE);
                        if (!distributionType.equalsIgnoreCase("random")) {
                            throw new DdlException(
                                    "Only support modifying distribution type of table from" + " hash to random");
                        }
                        Env.getCurrentEnv().convertDistributionType(db, olapTable);
                        return;
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK)) {
                        /*
                         * This is only for fixing bug when upgrading Doris from 0.9.x to 0.10.x.
                         */
                        sendClearAlterTask(db, olapTable);
                        return;
                    } else if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(properties)) {
                        DynamicPartitionUtil.checkDynamicPartitionPropertyKeysValid(properties);
                        if (!olapTable.dynamicPartitionExists()) {
                            try {
                                DynamicPartitionUtil.checkInputDynamicPartitionProperties(properties,
                                        olapTable);
                            } catch (DdlException e) {
                                // This table is not a dynamic partition table
                                // and didn't supply all dynamic partition properties
                                throw new DdlException("Table " + db.getFullName() + "." + olapTable.getName()
                                        + " is not a dynamic partition table." + " Use command `HELP ALTER TABLE` "
                                        + "to see how to change a normal table to a dynamic partition table.");
                            }
                        }
                        Env.getCurrentEnv().modifyTableDynamicPartition(db, olapTable, properties);
                        return;
                    } else if (properties.containsKey(
                            "default." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION)) {
                        Preconditions.checkNotNull(
                                properties.get("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION));
                        Env.getCurrentEnv().modifyTableDefaultReplicaAllocation(db, olapTable, properties);
                        return;
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION)) {
                        Env.getCurrentEnv().modifyTableReplicaAllocation(db, olapTable, properties);
                        return;
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY)) {
                        olapTable.setStoragePolicy(properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY));
                        return;
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_LIGHT_SCHEMA_CHANGE)) {
                        lightSchemaChange = Boolean.parseBoolean(
                                properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_LIGHT_SCHEMA_CHANGE));
                        if (Objects.equals(olapTable.getEnableLightSchemaChange(), lightSchemaChange)) {
                            return;
                        }
                        if (!lightSchemaChange) {
                            throw new DdlException("Can not alter light_schema_change from true to false currently");
                        }
                        enableLightSchemaChange(db, olapTable);
                        return;
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORE_ROW_COLUMN)
                                || properties.containsKey(PropertyAnalyzer.PROPERTIES_ROW_STORE_COLUMNS)) {
                        String value = properties.get(PropertyAnalyzer.PROPERTIES_STORE_ROW_COLUMN);
                        if (value != null && value.equalsIgnoreCase("false")) {
                            throw new DdlException("Can not alter store_row_column from true to false currently");
                        }
                        if (!olapTable.storeRowColumn()) {
                            Column rowColumn = ColumnDefinition
                                    .newRowStoreColumnDefinition(null).translateToCatalogStyle();
                            int maxColUniqueId = olapTable
                                    .getIndexMetaByIndexId(olapTable.getBaseIndexId()).getMaxColUniqueId();
                            rowColumn.setUniqueId(maxColUniqueId + 1);
                            indexSchemaMap.get(olapTable.getBaseIndexId()).add(rowColumn);
                        }
                    }
                }

                // the following operations can not be done when there are temp partitions exist.
                if (olapTable.existTempPartitions()) {
                    throw new DdlException("Can not alter table when there are temp partitions in table");
                }

                if (alterClause instanceof AddColumnClause) {
                    // add column
                    boolean clauseCanLightSchemaChange = processAddColumn((AddColumnClause) alterClause, olapTable,
                            indexSchemaMap, colUniqueIdSupplierMap);
                    if (!clauseCanLightSchemaChange) {
                        lightSchemaChange = false;
                    }
                } else if (alterClause instanceof AddColumnsClause) {
                    // add columns
                    boolean clauseCanLigthSchemaChange = processAddColumns((AddColumnsClause) alterClause, olapTable,
                            indexSchemaMap, false, colUniqueIdSupplierMap);
                    if (!clauseCanLigthSchemaChange) {
                        lightSchemaChange = false;
                    }
                } else if (alterClause instanceof DropColumnClause) {
                    // drop column and drop indexes on this column
                    boolean clauseCanLigthSchemaChange = processDropColumn((DropColumnClause) alterClause, olapTable,
                            indexSchemaMap, newIndexes);
                    if (!clauseCanLigthSchemaChange) {
                        lightSchemaChange = false;
                    }
                } else if (alterClause instanceof ModifyColumnClause) {
                    // modify column
                    boolean clauseCanLightSchemaChange = processModifyColumn((ModifyColumnClause) alterClause,
                            olapTable, indexSchemaMap);
                    if (!clauseCanLightSchemaChange) {
                        lightSchemaChange = false;
                    }
                } else if (alterClause instanceof ReorderColumnsClause) {
                    // reorder column
                    processReorderColumn((ReorderColumnsClause) alterClause, olapTable, indexSchemaMap);
                    lightSchemaChange = false;
                } else if (alterClause instanceof ModifyTablePropertiesClause) {
                    // modify table properties
                    // do nothing, properties are already in propertyMap
                    lightSchemaChange = false;
                } else if (alterClause instanceof CreateIndexClause) {
                    CreateIndexClause createIndexClause = (CreateIndexClause) alterClause;
                    Index index = createIndexClause.getIndex();
                    if (processAddIndex(createIndexClause, olapTable, newIndexes)) {
                        return;
                    }
                    lightSchemaChange = false;

                    if (index.isLightIndexChangeSupported() && !Config.isCloudMode()) {
                        alterIndexes.add(index);
                        isDropIndex = false;
                        // now only support light index change for inverted index
                        lightIndexChange = true;
                    }
                } else if (alterClause instanceof BuildIndexClause) {
                    BuildIndexClause buildIndexClause = (BuildIndexClause) alterClause;
                    IndexDef indexDef = buildIndexClause.getIndexDef();
                    Index index = buildIndexClause.getIndex();
                    if (!index.isLightIndexChangeSupported() || Config.isCloudMode()) {
                        throw new DdlException("BUILD INDEX can not be used since index "
                                + indexDef.getIndexName() + " with type " + indexDef.getIndexType()
                                + " does not support light index change or cluster cloud mode "
                                + Config.isCloudMode() + " is true");
                    }

                    if (!olapTable.isPartitionedTable()) {
                        List<String> specifiedPartitions = indexDef.getPartitionNames();
                        if (!specifiedPartitions.isEmpty()) {
                            throw new DdlException("table " + olapTable.getName()
                                    + " is not partitioned, cannot build index with partitions.");
                        }
                    }
                    List<Index> existedIndexes = olapTable.getIndexes();
                    boolean found = false;
                    for (Index existedIdx : existedIndexes) {
                        if (existedIdx.getIndexName().equalsIgnoreCase(indexDef.getIndexName())) {
                            found = true;
                            index.setIndexId(existedIdx.getIndexId());
                            index.setColumns(existedIdx.getColumns());
                            index.setProperties(existedIdx.getProperties());
                            if (indexDef.getPartitionNames().isEmpty()) {
                                invertedIndexOnPartitions.put(index.getIndexId(), olapTable.getPartitionNames());
                            } else {
                                invertedIndexOnPartitions.put(
                                        index.getIndexId(), new HashSet<>(indexDef.getPartitionNames()));
                            }
                            break;
                        }
                    }
                    if (!found) {
                        throw new DdlException("index " + indexDef.getIndexName()
                                + " not exist, cannot build it with defferred.");
                    }

                    if (indexDef.isInvertedIndex()) {
                        alterIndexes.add(index);
                    }
                    buildIndexChange = true;
                    lightSchemaChange = false;
                } else if (alterClause instanceof DropIndexClause) {
                    if (processDropIndex((DropIndexClause) alterClause, olapTable, newIndexes)) {
                        return;
                    }
                    lightSchemaChange = false;

                    DropIndexClause dropIndexClause = (DropIndexClause) alterClause;
                    List<Index> existedIndexes = olapTable.getIndexes();
                    Index found = null;
                    for (Index existedIdx : existedIndexes) {
                        if (existedIdx.getIndexName().equalsIgnoreCase(dropIndexClause.getIndexName())) {
                            found = existedIdx;
                            break;
                        }
                    }
                    if (found.isLightIndexChangeSupported() && !Config.isCloudMode()) {
                        alterIndexes.add(found);
                        isDropIndex = true;
                        lightIndexChange = true;
                    }
                } else {
                    Preconditions.checkState(false);
                }
            } // end for alter clauses

            if (LOG.isDebugEnabled()) {
                LOG.debug("table: {}({}), lightSchemaChange: {}, lightIndexChange: {},"
                        + " buildIndexChange: {}, indexSchemaMap:{}",
                        olapTable.getName(), olapTable.getId(), lightSchemaChange,
                        lightIndexChange, buildIndexChange, indexSchemaMap);
            }

            if (lightSchemaChange) {
                long jobId = Env.getCurrentEnv().getNextId();
                //for schema change add/drop value column optimize, direct modify table meta.
                modifyTableLightSchemaChange(rawSql, db, olapTable, indexSchemaMap, newIndexes,
                                             null, isDropIndex, jobId, false);
            } else if (Config.enable_light_index_change && lightIndexChange) {
                long jobId = Env.getCurrentEnv().getNextId();
                //for schema change add/drop inverted index optimize, direct modify table meta firstly.
                modifyTableLightSchemaChange(rawSql, db, olapTable, indexSchemaMap, newIndexes,
                                             alterIndexes, isDropIndex, jobId, false);
            } else if (buildIndexChange) {
                if (Config.enable_light_index_change) {
                    buildOrDeleteTableInvertedIndices(db, olapTable, indexSchemaMap,
                                                      alterIndexes, invertedIndexOnPartitions, false);
                }
            } else {
                createJob(rawSql, db.getId(), olapTable, indexSchemaMap, propertyMap, newIndexes);
            }
        } finally {
            olapTable.writeUnlock();
        }
    }


    private void enableLightSchemaChange(Database db, OlapTable olapTable) throws DdlException {
        final AlterLightSchChangeHelper alterLightSchChangeHelper = new AlterLightSchChangeHelper(db, olapTable);
        try {
            alterLightSchChangeHelper.enableLightSchemaChange();
        } catch (IllegalStateException e) {
            throw new DdlException(String.format("failed to enable light schema change for table %s.%s",
                    db.getFullName(), olapTable.getName()), e);
        }
    }

    public void replayAlterLightSchChange(AlterLightSchemaChangeInfo info) throws MetaNotFoundException {
        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrMetaException(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(info.getTableId(), TableType.OLAP);
        olapTable.writeLock();
        final AlterLightSchChangeHelper alterLightSchChangeHelper = new AlterLightSchChangeHelper(db, olapTable);
        try {
            alterLightSchChangeHelper.updateTableMeta(info);
        } catch (IllegalStateException e) {
            LOG.warn("failed to replay alter light schema change", e);
        } finally {
            olapTable.writeUnlock();
        }
    }

    @Override
    public void processExternalTable(List<AlterClause> alterClauses, Database db, Table externalTable)
            throws UserException {
        externalTable.writeLockOrDdlException();
        try {
            // copy the external table schema columns
            List<Column> newSchema = Lists.newArrayList();
            newSchema.addAll(externalTable.getBaseSchema(true));

            for (AlterClause alterClause : alterClauses) {
                if (alterClause instanceof AddColumnClause) {
                    // add column
                    processAddColumn((AddColumnClause) alterClause, externalTable, newSchema);
                } else if (alterClause instanceof AddColumnsClause) {
                    // add columns
                    processAddColumns((AddColumnsClause) alterClause, externalTable, newSchema);
                } else if (alterClause instanceof DropColumnClause) {
                    // drop column and drop indexes on this column
                    processDropColumn((DropColumnClause) alterClause, externalTable, newSchema);
                } else if (alterClause instanceof ModifyColumnClause) {
                    // modify column
                    processModifyColumn((ModifyColumnClause) alterClause, externalTable, newSchema);
                } else if (alterClause instanceof ReorderColumnsClause) {
                    // reorder column
                    processReorderColumn((ReorderColumnsClause) alterClause, externalTable, newSchema);
                } else {
                    Preconditions.checkState(false);
                }
            } // end for alter clauses
            // replace the old column list
            externalTable.setNewFullSchema(newSchema);
            // refresh external table column in edit log
            Env.getCurrentEnv().refreshExternalTableSchema(db, externalTable, newSchema);
        } finally {
            externalTable.writeUnlock();
        }
    }

    private void sendClearAlterTask(Database db, OlapTable olapTable) {
        AgentBatchTask batchTask = new AgentBatchTask();
        olapTable.readLock();
        try {
            for (Partition partition : olapTable.getPartitions()) {
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    int schemaHash = olapTable.getSchemaHashByIndexId(index.getId());
                    for (Tablet tablet : index.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            ClearAlterTask alterTask = new ClearAlterTask(replica.getBackendId(), db.getId(),
                                    olapTable.getId(), partition.getId(), index.getId(), tablet.getId(), schemaHash);
                            batchTask.addTask(alterTask);
                        }
                    }
                }
            }
        } finally {
            olapTable.readUnlock();
        }

        AgentTaskExecutor.submit(batchTask);
        LOG.info("send clear alter task for table {}, number: {}", olapTable.getName(), batchTask.getTaskNum());
    }

    static long storagePolicyNameToId(String storagePolicy) throws DdlException {
        if (storagePolicy == null) {
            return -1; // don't update storage policy
        }
        // if storagePolicy is "", means to reset the storage policy of this table
        if (storagePolicy.isEmpty()) {
            return 0; // reset storage policy
        } else {
            Optional<Policy> policy = Env.getCurrentEnv().getPolicyMgr()
                    .findPolicy(storagePolicy, PolicyTypeEnum.STORAGE);
            if (!policy.isPresent()) {
                throw new DdlException("StoragePolicy[" + storagePolicy + "] not exist");
            }
            return policy.get().getId();
        }
    }

    /**
     * Update all partitions' properties of table
     */
    public void updateTableProperties(Database db, String tableName, Map<String, String> properties)
            throws UserException {
        Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_COMPACTION_POLICY)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_INTERVAL_MS)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_DATA_BYTES)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_MOW_LIGHT_DELETE)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_SINGLE_REPLICA_COMPACTION)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_DISABLE_AUTO_COMPACTION)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_SKIP_WRITE_INDEX_ON_LOAD)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_LEVEL_THRESHOLD)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY));

        Env.getCurrentEnv().getAlterInstance().checkNoForceProperty(properties);
        List<Partition> partitions = Lists.newArrayList();
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableName, Table.TableType.OLAP);
        boolean enableUniqueKeyMergeOnWrite = false;
        olapTable.readLock();
        try {
            enableUniqueKeyMergeOnWrite = olapTable.getEnableUniqueKeyMergeOnWrite();
            partitions.addAll(olapTable.getPartitions());
        } finally {
            olapTable.readUnlock();
        }

        String inMemory = properties.get(PropertyAnalyzer.PROPERTIES_INMEMORY);
        int isInMemory = -1; // < 0 means don't update inMemory properties
        if (inMemory != null) {
            isInMemory = Boolean.parseBoolean(inMemory) ? 1 : 0;
            if ((isInMemory > 0) == olapTable.isInMemory()) {
                // already up-to-date
                isInMemory = -1;
            }
        }
        String storagePolicy = properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY);
        long storagePolicyId = storagePolicyNameToId(storagePolicy);

        String compactionPolicy = properties.get(PropertyAnalyzer.PROPERTIES_COMPACTION_POLICY);
        if (compactionPolicy != null
                                    && !compactionPolicy.equals(PropertyAnalyzer.TIME_SERIES_COMPACTION_POLICY)
                                    && !compactionPolicy.equals(PropertyAnalyzer.SIZE_BASED_COMPACTION_POLICY)) {
            throw new UserException("Table compaction policy only support for "
                                                + PropertyAnalyzer.TIME_SERIES_COMPACTION_POLICY
                                                + " or " + PropertyAnalyzer.SIZE_BASED_COMPACTION_POLICY);
        }

        Map<String, Long> timeSeriesCompactionConfig = new HashMap<>();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES)) {
            timeSeriesCompactionConfig
                        .put(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES,
                        Long.parseLong(properties
                        .get(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES)));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD)) {
            timeSeriesCompactionConfig
                        .put(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD,
                        Long.parseLong(properties
                        .get(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD)));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS)) {
            timeSeriesCompactionConfig
                        .put(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS,
                        Long.parseLong(properties
                        .get(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS)));
        }

        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD)) {
            timeSeriesCompactionConfig
                        .put(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD,
                        Long.parseLong(properties
                        .get(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD)));
        }

        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_LEVEL_THRESHOLD)) {
            timeSeriesCompactionConfig
                        .put(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_LEVEL_THRESHOLD,
                        Long.parseLong(properties
                        .get(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_LEVEL_THRESHOLD)));
        }

        if (isInMemory < 0 && storagePolicyId < 0 && compactionPolicy == null && timeSeriesCompactionConfig.isEmpty()
                && !properties.containsKey(PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED)
                && !properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_MOW_LIGHT_DELETE)
                && !properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_SINGLE_REPLICA_COMPACTION)
                && !properties.containsKey(PropertyAnalyzer.PROPERTIES_DISABLE_AUTO_COMPACTION)
                && !properties.containsKey(PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_INTERVAL_MS)
                && !properties.containsKey(PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_DATA_BYTES)
                && !properties.containsKey(PropertyAnalyzer.PROPERTIES_SKIP_WRITE_INDEX_ON_LOAD)
                && !properties.containsKey(PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY)) {
            LOG.info("Properties already up-to-date");
            return;
        }

        String singleCompaction = properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_SINGLE_REPLICA_COMPACTION);
        int enableSingleCompaction = -1; // < 0 means don't update
        if (singleCompaction != null) {
            enableSingleCompaction = Boolean.parseBoolean(singleCompaction) ? 1 : 0;
        }

        if (enableUniqueKeyMergeOnWrite && Boolean.parseBoolean(singleCompaction)) {
            throw new UserException(
                    "enable_single_replica_compaction property is not supported for merge-on-write table");
        }

        String enableMowLightDelete = properties.get(
                PropertyAnalyzer.PROPERTIES_ENABLE_MOW_LIGHT_DELETE);
        if (enableMowLightDelete != null && !enableUniqueKeyMergeOnWrite) {
            throw new UserException(
                    "enable_mow_light_delete property is only supported for unique merge-on-write table");
        }

        String disableAutoCompactionBoolean = properties.get(PropertyAnalyzer.PROPERTIES_DISABLE_AUTO_COMPACTION);
        int disableAutoCompaction = -1; // < 0 means don't update
        if (disableAutoCompactionBoolean != null) {
            disableAutoCompaction = Boolean.parseBoolean(disableAutoCompactionBoolean) ? 1 : 0;
        }

        String skipWriteIndexOnLoad = properties.get(PropertyAnalyzer.PROPERTIES_SKIP_WRITE_INDEX_ON_LOAD);
        int skip = -1; // < 0 means don't update
        if (skipWriteIndexOnLoad != null) {
            skip = Boolean.parseBoolean(skipWriteIndexOnLoad) ? 1 : 0;
        }

        for (Partition partition : partitions) {
            updatePartitionProperties(db, olapTable.getName(), partition.getName(), storagePolicyId, isInMemory,
                                    null, compactionPolicy, timeSeriesCompactionConfig, enableSingleCompaction, skip,
                                    disableAutoCompaction);
        }

        olapTable.writeLockOrDdlException();
        try {
            Env.getCurrentEnv().modifyTableProperties(db, olapTable, properties);
        } finally {
            olapTable.writeUnlock();
        }
    }

    /**
     * Update some specified partitions' properties of table
     */
    public void updatePartitionsProperties(Database db, String tableName, List<String> partitionNames,
                                           Map<String, String> properties) throws DdlException, MetaNotFoundException {
        Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY));

        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableName, Table.TableType.OLAP);
        String inMemory = properties.get(PropertyAnalyzer.PROPERTIES_INMEMORY);
        int isInMemory = -1; // < 0 means don't update inMemory properties
        if (inMemory != null) {
            isInMemory = Boolean.parseBoolean(inMemory) ? 1 : 0;
            if ((isInMemory > 0) == olapTable.isInMemory()) {
                // already up-to-date
                isInMemory = -1;
            }
        }
        String storagePolicy = properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY);
        boolean enableUniqueKeyMergeOnWrite = olapTable.getEnableUniqueKeyMergeOnWrite();
        if (enableUniqueKeyMergeOnWrite && !Strings.isNullOrEmpty(storagePolicy)) {
            throw new DdlException(
                    "Can not set UNIQUE KEY table that enables Merge-On-write" + " with storage policy(" + storagePolicy
                            + ")");
        }
        long storagePolicyId = storagePolicyNameToId(storagePolicy);

        if (isInMemory < 0 && storagePolicyId < 0) {
            LOG.info("Properties already up-to-date");
            return;
        }

        for (String partitionName : partitionNames) {
            try {
                updatePartitionProperties(db, olapTable.getName(), partitionName, storagePolicyId,
                                                                            isInMemory, null, null, null, -1, -1, -1);
            } catch (Exception e) {
                String errMsg = "Failed to update partition[" + partitionName + "]'s 'in_memory' property. "
                        + "The reason is [" + e.getMessage() + "]";
                throw new DdlException(errMsg);
            }
        }
    }

    /**
     * Update one specified partition's properties by partition name of table
     * This operation may return partial successfully, with an exception to inform user to retry
     */
    public void updatePartitionProperties(Database db, String tableName, String partitionName, long storagePolicyId,
                                          int isInMemory, BinlogConfig binlogConfig, String compactionPolicy,
                                          Map<String, Long> timeSeriesCompactionConfig,
                                          int enableSingleCompaction, int skipWriteIndexOnLoad,
                                          int disableAutoCompaction) throws UserException {
        // be id -> <tablet id,schemaHash>
        Map<Long, Set<Pair<Long, Integer>>> beIdToTabletIdWithHash = Maps.newHashMap();
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableName, Table.TableType.OLAP);
        olapTable.readLock();
        try {
            Partition partition = olapTable.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException(
                        "Partition[" + partitionName + "] does not exist in table[" + olapTable.getName() + "]");
            }

            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                int schemaHash = olapTable.getSchemaHashByIndexId(index.getId());
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        Set<Pair<Long, Integer>> tabletIdWithHash = beIdToTabletIdWithHash.computeIfAbsent(
                                replica.getBackendId(), k -> Sets.newHashSet());
                        tabletIdWithHash.add(Pair.of(tablet.getId(), schemaHash));
                    }
                }
            }
        } finally {
            olapTable.readUnlock();
        }

        int totalTaskNum = beIdToTabletIdWithHash.keySet().size();
        MarkedCountDownLatch<Long, Set<Pair<Long, Integer>>> countDownLatch = new MarkedCountDownLatch<>(totalTaskNum);
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Map.Entry<Long, Set<Pair<Long, Integer>>> kv : beIdToTabletIdWithHash.entrySet()) {
            countDownLatch.addMark(kv.getKey(), kv.getValue());
            UpdateTabletMetaInfoTask task = new UpdateTabletMetaInfoTask(kv.getKey(), kv.getValue(), isInMemory,
                                            storagePolicyId, binlogConfig, countDownLatch, compactionPolicy,
                                            timeSeriesCompactionConfig, enableSingleCompaction, skipWriteIndexOnLoad,
                                            disableAutoCompaction);
            batchTask.addTask(task);
        }
        if (!FeConstants.runningUnitTest) {
            // send all tasks and wait them finished
            AgentTaskQueue.addBatchTask(batchTask);
            AgentTaskExecutor.submit(batchTask);
            LOG.info("send update tablet meta task for table {}, partitions {}, number: {}", tableName, partitionName,
                    batchTask.getTaskNum());

            // estimate timeout
            long timeout = DbUtil.getCreateReplicasTimeoutMs(totalTaskNum);
            boolean ok = false;
            try {
                ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
            }

            if (!ok || !countDownLatch.getStatus().ok()) {
                String errMsg = "Failed to update partition[" + partitionName + "]. tablet meta.";
                // clear tasks
                AgentTaskQueue.removeBatchTask(batchTask, TTaskType.UPDATE_TABLET_META_INFO);

                if (!countDownLatch.getStatus().ok()) {
                    errMsg += " Error: " + countDownLatch.getStatus().getErrorMsg();
                } else {
                    // only show at most 3 results
                    List<String> subList = countDownLatch.getLeftMarks().stream().limit(3)
                            .map(item -> "(backendId = " + item.getKey() + ", tabletsWithHash = "
                                    + item.getValue() + ")")
                            .collect(Collectors.toList());
                    if (!subList.isEmpty()) {
                        errMsg += " Unfinished: " + Joiner.on(", ").join(subList);
                    }
                }
                errMsg += ". This operation maybe partial successfully, You should retry until success.";
                LOG.warn(errMsg);
                throw new DdlException(errMsg);
            }
        }
    }

    @Override
    public void cancel(CancelStmt stmt) throws DdlException {
        CancelAlterTableStmt cancelAlterTableStmt = (CancelAlterTableStmt) stmt;
        if (cancelAlterTableStmt.getAlterType() == AlterType.INDEX) {
            cancelIndexJob(cancelAlterTableStmt);
        } else {
            cancelColumnJob(cancelAlterTableStmt);
        }
    }

    private void cancelColumnJob(CancelAlterTableStmt cancelAlterTableStmt) throws DdlException {
        String dbName = cancelAlterTableStmt.getDbName();
        String tableName = cancelAlterTableStmt.getTableName();
        Preconditions.checkState(!Strings.isNullOrEmpty(dbName));
        Preconditions.checkState(!Strings.isNullOrEmpty(tableName));

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        AlterJobV2 schemaChangeJobV2 = null;

        OlapTable olapTable = db.getOlapTableOrDdlException(tableName);
        olapTable.writeLockOrDdlException();
        try {
            if (olapTable.getState() != OlapTableState.SCHEMA_CHANGE
                    && olapTable.getState() != OlapTableState.WAITING_STABLE) {
                throw new DdlException("Table[" + tableName + "] is not under SCHEMA_CHANGE.");
            }

            // find from new alter jobs first
            List<AlterJobV2> schemaChangeJobV2List = getUnfinishedAlterJobV2ByTableId(olapTable.getId());
            // current schemaChangeJob job doesn't support batch operation,so just need to get one job
            schemaChangeJobV2 = schemaChangeJobV2List.size() == 0 ? null
                    : Iterables.getOnlyElement(schemaChangeJobV2List);
            if (schemaChangeJobV2 == null) {
                throw new DdlException(
                        "Table[" + tableName + "] is under schema change state" + " but could not find related job");
            }
        } finally {
            olapTable.writeUnlock();
        }

        // alter job v2's cancel must be called outside the database lock
        if (schemaChangeJobV2 != null) {
            if (!schemaChangeJobV2.cancel("user cancelled")) {
                throw new DdlException("Job can not be cancelled. State: " + schemaChangeJobV2.getJobState());
            }
            return;
        }
    }

    private void cancelIndexJob(CancelAlterTableStmt cancelAlterTableStmt) throws DdlException {
        String dbName = cancelAlterTableStmt.getDbName();
        String tableName = cancelAlterTableStmt.getTableName();
        Preconditions.checkState(!Strings.isNullOrEmpty(dbName));
        Preconditions.checkState(!Strings.isNullOrEmpty(tableName));

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);

        List<IndexChangeJob> jobList = new ArrayList<>();

        Table olapTable = db.getTableOrDdlException(tableName, Table.TableType.OLAP);
        olapTable.writeLock();
        try {
            // find from index change jobs first
            if (cancelAlterTableStmt.getAlterJobIdList() != null
                    && cancelAlterTableStmt.getAlterJobIdList().size() > 0) {
                for (Long jobId : cancelAlterTableStmt.getAlterJobIdList()) {
                    IndexChangeJob job = indexChangeJobs.get(jobId);
                    if (job == null) {
                        continue;
                    }
                    jobList.add(job);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("add build index job {} on table {} for specific id", jobId, tableName);
                    }
                }
            } else {
                for (IndexChangeJob job : indexChangeJobs.values()) {
                    if (!job.isDone() && job.getTableId() == olapTable.getId()) {
                        jobList.add(job);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("add build index job {} on table {} for all", job.getJobId(), tableName);
                        }
                    }
                }
            }
        } finally {
            olapTable.writeUnlock();
        }

        // alter job v2's cancel must be called outside the table lock
        if (jobList.size() > 0) {
            for (IndexChangeJob job : jobList) {
                long jobId = job.getJobId();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("cancel build index job {} on table {}", jobId, tableName);
                }
                if (!job.cancel("user cancelled")) {
                    LOG.warn("cancel build index job {} on table {} failed", jobId, tableName);
                    throw new DdlException("Job can not be cancelled. State: " + job.getJobState());
                } else {
                    LOG.info("cancel build index job {} on table {} success", jobId, tableName);
                }
            }
        } else {
            throw new DdlException("No job to cancel for Table[" + tableName + "]");
        }
    }

    /**
     * Returns true if the index already exists, there is no need to create the job to add the index.
     * Otherwise return false, there is need to create a job to add the index.
     */
    private boolean processAddIndex(CreateIndexClause alterClause, OlapTable olapTable, List<Index> newIndexes)
            throws UserException {
        Index alterIndex = alterClause.getIndex();
        if (alterIndex == null) {
            return false;
        }

        List<Index> existedIndexes = olapTable.getIndexes();
        IndexDef indexDef = alterClause.getIndexDef();
        Set<String> newColset = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        newColset.addAll(indexDef.getColumns());
        for (Index existedIdx : existedIndexes) {
            if (existedIdx.getIndexName().equalsIgnoreCase(indexDef.getIndexName())) {
                if (indexDef.isSetIfNotExists()) {
                    LOG.info("create index[{}] which already exists on table[{}]", indexDef.getIndexName(),
                            olapTable.getName());
                    return true;
                }
                throw new DdlException("index `" + indexDef.getIndexName() + "` already exist.");
            }
            Set<String> existedIdxColSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            existedIdxColSet.addAll(existedIdx.getColumns());
            if (existedIdx.getIndexType() == indexDef.getIndexType() && newColset.equals(existedIdxColSet)) {
                throw new DdlException(
                        indexDef.getIndexType() + " index for columns (" + String.join(",", indexDef.getColumns())
                                + " ) already exist.");
            }
        }

        for (String col : indexDef.getColumns()) {
            Column column = olapTable.getColumn(col);
            if (column != null) {
                indexDef.checkColumn(column, olapTable.getKeysType(),
                        olapTable.getTableProperty().getEnableUniqueKeyMergeOnWrite());
            } else {
                throw new DdlException("index column does not exist in table. invalid column: " + col);
            }
        }

        // the column name in CreateIndexClause is not check case sensitivity,
        // when send index description to BE, there maybe cannot find column by name,
        // so here update column name in CreateIndexClause after checkColumn for indexDef,
        // there will use the column name in olapTable insead of the column name in CreateIndexClause.
        alterIndex.setColumns(indexDef.getColumns());
        newIndexes.add(alterIndex);
        return false;
    }

    /**
     * Returns true if the index does not exist, there is no need to create the job to drop the index.
     * Otherwise return false, there is need to create a job to drop the index.
     */
    private boolean processDropIndex(DropIndexClause alterClause, OlapTable olapTable, List<Index> indexes)
            throws DdlException {
        String indexName = alterClause.getIndexName();
        List<Index> existedIndexes = olapTable.getIndexes();
        Index found = null;
        for (Index existedIdx : existedIndexes) {
            if (existedIdx.getIndexName().equalsIgnoreCase(indexName)) {
                found = existedIdx;
                break;
            }
        }
        if (found == null) {
            if (alterClause.isSetIfExists()) {
                LOG.info("drop index[{}] which does not exist on table[{}]", indexName, olapTable.getName());
                return true;
            }
            throw new DdlException("index " + indexName + " does not exist");
        }

        Iterator<Index> itr = indexes.iterator();
        while (itr.hasNext()) {
            Index idx = itr.next();
            if (idx.getIndexName().equalsIgnoreCase(alterClause.getIndexName())) {
                itr.remove();
                break;
            }
        }
        return false;
    }

    @Override
    public void addAlterJobV2(AlterJobV2 alterJob) throws AnalysisException {
        super.addAlterJobV2(alterJob);
        runnableSchemaChangeJobV2.put(alterJob.getJobId(), alterJob);
    }

    public void addIndexChangeJob(IndexChangeJob indexChangeJob) {
        indexChangeJobs.put(indexChangeJob.getJobId(), indexChangeJob);
        runnableIndexChangeJob.put(indexChangeJob.getJobId(), indexChangeJob);
        LOG.info("add inverted index job {}", indexChangeJob.getJobId());
    }

    private void clearFinishedOrCancelledSchemaChangeJobV2() {
        Iterator<Map.Entry<Long, AlterJobV2>> iterator = runnableSchemaChangeJobV2.entrySet().iterator();
        while (iterator.hasNext()) {
            AlterJobV2 alterJobV2 = iterator.next().getValue();
            if (alterJobV2.isDone()) {
                iterator.remove();
            }
        }
    }

    private void clearExpireFinishedOrCancelledIndexChangeJobs() {
        Iterator<Map.Entry<Long, IndexChangeJob>> iterator = indexChangeJobs.entrySet().iterator();
        while (iterator.hasNext()) {
            IndexChangeJob indexChangeJob = iterator.next().getValue();
            if (indexChangeJob.isExpire()) {
                iterator.remove();
                LOG.info("remove expired inverted index job {}. finish at {}",
                        indexChangeJob.getJobId(), TimeUtils.longToTimeString(indexChangeJob.getFinishedTimeMs()));
            }
        }

        Iterator<Map.Entry<Long, IndexChangeJob>> runnableIterator = runnableIndexChangeJob.entrySet().iterator();
        while (runnableIterator.hasNext()) {
            IndexChangeJob indexChangeJob = runnableIterator.next().getValue();
            if (indexChangeJob.isDone()) {
                runnableIterator.remove();
            }
        }
    }

    @Override
    public void replayRemoveAlterJobV2(RemoveAlterJobV2OperationLog log) {
        if (runnableSchemaChangeJobV2.containsKey(log.getJobId())) {
            runnableSchemaChangeJobV2.remove(log.getJobId());
        }
        super.replayRemoveAlterJobV2(log);
    }

    @Override
    public void replayAlterJobV2(AlterJobV2 alterJob) throws AnalysisException {
        if (!alterJob.isDone() && !runnableSchemaChangeJobV2.containsKey(alterJob.getJobId())) {
            runnableSchemaChangeJobV2.put(alterJob.getJobId(), alterJob);
        }
        super.replayAlterJobV2(alterJob);
    }

    // the invoker should keep table's write lock
    public void modifyTableLightSchemaChange(String rawSql, Database db, OlapTable olapTable,
                                             Map<Long, LinkedList<Column>> indexSchemaMap, List<Index> indexes,
                                             List<Index> alterIndexes, boolean isDropIndex,
                                             long jobId, boolean isReplay)
            throws DdlException, AnalysisException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("indexSchemaMap:{}, indexes:{}", indexSchemaMap, indexes);
        }
        // for bitmapIndex
        boolean hasIndexChange = false;
        Set<Index> newSet = new HashSet<>(indexes);
        Set<Index> oriSet = new HashSet<>(olapTable.getIndexes());
        if (!newSet.equals(oriSet)) {
            hasIndexChange = true;
        }

        // begin checking each table
        Map<Long, List<Column>> changedIndexIdToSchema = Maps.newHashMap();
        try {
            changedIndexIdToSchema = checkTable(db, olapTable, indexSchemaMap);
        } catch (DdlException e) {
            throw new DdlException("Table " + db.getFullName() + "." + olapTable.getName() + " check failed", e);
        }

        if (changedIndexIdToSchema.isEmpty() && !hasIndexChange) {
            throw new DdlException("Nothing is changed. please check your alter stmt.");
        }

        //for compatibility, we need create a finished state schema change job v2

        SchemaChangeJobV2 schemaChangeJob = AlterJobV2Factory.createSchemaChangeJobV2(
                rawSql, jobId, db.getId(), olapTable.getId(),
                olapTable.getName(), 1000);

        for (Map.Entry<Long, List<Column>> entry : changedIndexIdToSchema.entrySet()) {
            long originIndexId = entry.getKey();
            String newIndexName = SHADOW_NAME_PREFIX + olapTable.getIndexNameById(originIndexId);
            MaterializedIndexMeta currentIndexMeta = olapTable.getIndexMetaByIndexId(originIndexId);
            // 1. get new schema version/schema version hash, short key column count
            int currentSchemaVersion = currentIndexMeta.getSchemaVersion();
            int newSchemaVersion = currentSchemaVersion + 1;
            // generate schema hash for new index has to generate a new schema hash not equal to current schema hash
            schemaChangeJob.addIndexSchema(originIndexId, originIndexId, newIndexName, newSchemaVersion,
                    currentIndexMeta.getSchemaHash(), currentIndexMeta.getShortKeyColumnCount(), entry.getValue());
        }

        //update base index schema
        try {
            updateBaseIndexSchema(olapTable, indexSchemaMap, indexes);
        } catch (Exception e) {
            throw new DdlException(e.getMessage());
        }

        schemaChangeJob.stateWait("FE.LIGHT_SCHEMA_CHANGE");
        // set Job state then add job
        schemaChangeJob.setJobState(AlterJobV2.JobState.FINISHED);
        schemaChangeJob.setFinishedTimeMs(System.currentTimeMillis());
        this.addAlterJobV2(schemaChangeJob);

        if (alterIndexes != null) {
            if (!isReplay) {
                TableAddOrDropInvertedIndicesInfo info = new TableAddOrDropInvertedIndicesInfo(rawSql, db.getId(),
                        olapTable.getId(), indexSchemaMap, indexes, alterIndexes, isDropIndex, jobId);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("logModifyTableAddOrDropInvertedIndices info:{}", info);
                }
                Env.getCurrentEnv().getEditLog().logModifyTableAddOrDropInvertedIndices(info);
                // Drop table column stats after light schema change finished.
                Env.getCurrentEnv().getAnalysisManager().dropStats(olapTable, null);

                if (isDropIndex) {
                    // send drop rpc to be
                    Map<Long, Set<String>> invertedIndexOnPartitions = new HashMap<>();
                    for (Index index : alterIndexes) {
                        invertedIndexOnPartitions.put(index.getIndexId(), olapTable.getPartitionNames());
                    }
                    try {
                        buildOrDeleteTableInvertedIndices(db, olapTable, indexSchemaMap,
                                              alterIndexes, invertedIndexOnPartitions, true);
                    } catch (Exception e) {
                        throw new DdlException(e.getMessage());
                    }
                }
            }

            LOG.info("finished modify table's meta for add or drop inverted index. table: {}, job: {}, is replay: {}",
                    olapTable.getName(), jobId, isReplay);
        } else {
            if (!isReplay) {
                TableAddOrDropColumnsInfo info = new TableAddOrDropColumnsInfo(rawSql, db.getId(), olapTable.getId(),
                        indexSchemaMap, indexes, jobId);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("logModifyTableAddOrDropColumns info:{}", info);
                }
                Env.getCurrentEnv().getEditLog().logModifyTableAddOrDropColumns(info);
                // Drop table column stats after light schema change finished.
                Env.getCurrentEnv().getAnalysisManager().dropStats(olapTable, null);
            }
            LOG.info("finished modify table's add or drop or modify columns. table: {}, job: {}, is replay: {}",
                    olapTable.getName(), jobId, isReplay);
        }
    }

    public void replayModifyTableLightSchemaChange(TableAddOrDropColumnsInfo info)
            throws MetaNotFoundException, AnalysisException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("info:{}", info);
        }
        long dbId = info.getDbId();
        long tableId = info.getTableId();
        Map<Long, LinkedList<Column>> indexSchemaMap = info.getIndexSchemaMap();
        List<Index> indexes = info.getIndexes();
        long jobId = info.getJobId();

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrMetaException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        olapTable.writeLock();
        try {
            modifyTableLightSchemaChange("", db, olapTable, indexSchemaMap, indexes, null, false, jobId, true);
        } catch (DdlException e) {
            // should not happen
            LOG.warn("failed to replay modify table add or drop or modify columns", e);
        } finally {
            olapTable.writeUnlock();
        }
    }

    public Map<Long, List<Column>> checkTable(Database db, OlapTable olapTable,
                                              Map<Long, LinkedList<Column>> indexSchemaMap) throws DdlException {
        Map<Long, List<Column>> changedIndexIdToSchema = Maps.newHashMap();
        // ATTN: DO NOT change any meta in this loop
        for (Long alterIndexId : indexSchemaMap.keySet()) {
            // Must get all columns including invisible columns.
            // Because in alter process, all columns must be considered.
            List<Column> alterSchema = indexSchemaMap.get(alterIndexId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("index[{}] is changed. start checking...", alterIndexId);
            }
            // 1. check order: a) has key; b) value after key
            boolean meetValue = false;
            boolean hasKey = false;
            for (Column column : alterSchema) {
                if (column.isKey() && meetValue) {
                    throw new DdlException(
                            "Invalid column order. value should be after key. index[" + olapTable.getIndexNameById(
                                    alterIndexId) + "]");
                }
                if (!column.isKey()) {
                    meetValue = true;
                } else {
                    hasKey = true;
                }
            }
            if (!hasKey && !olapTable.isDuplicateWithoutKey()) {
                throw new DdlException("No key column left. index[" + olapTable.getIndexNameById(alterIndexId) + "]");
            }

            // 2. check partition key
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (partitionInfo.getType() == PartitionType.RANGE || partitionInfo.getType() == PartitionType.LIST) {
                List<Column> partitionColumns = partitionInfo.getPartitionColumns();
                for (Column partitionCol : partitionColumns) {
                    boolean found = false;
                    for (Column alterColumn : alterSchema) {
                        if (alterColumn.nameEquals(partitionCol.getName(), true)) {
                            found = true;
                            break;
                        }
                    } // end for alterColum

                    if (!found && alterIndexId == olapTable.getBaseIndexId()) {
                        // 2.1 partition column cannot be deleted.
                        throw new DdlException(
                                "Partition column[" + partitionCol.getName() + "] cannot be dropped. index["
                                        + olapTable.getIndexNameById(alterIndexId) + "]");
                    }
                } // end for partitionColumns
            }

            // 3. check distribution key:
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            if (distributionInfo.getType() == DistributionInfoType.HASH) {
                List<Column> distributionColumns = ((HashDistributionInfo) distributionInfo).getDistributionColumns();
                for (Column distributionCol : distributionColumns) {
                    boolean found = false;
                    for (Column alterColumn : alterSchema) {
                        if (alterColumn.nameEquals(distributionCol.getName(), true)) {
                            found = true;
                            break;
                        }
                    } // end for alterColumns
                    if (!found && alterIndexId == olapTable.getBaseIndexId()) {
                        // 2.2 distribution column cannot be deleted.
                        throw new DdlException(
                                "Distribution column[" + distributionCol.getName() + "] cannot be dropped. index["
                                        + olapTable.getIndexNameById(alterIndexId) + "]");
                    }
                } // end for distributionCols
            }

            // 5. store the changed columns for edit log
            changedIndexIdToSchema.put(alterIndexId, alterSchema);

            if (LOG.isDebugEnabled()) {
                LOG.debug("schema change[{}-{}-{}] check pass.", db.getId(), olapTable.getId(), alterIndexId);
            }
        } // end for indices
        return changedIndexIdToSchema;
    }

    public void updateBaseIndexSchema(OlapTable olapTable, Map<Long, LinkedList<Column>> indexSchemaMap,
                                      List<Index> indexes) throws IOException {
        long baseIndexId = olapTable.getBaseIndexId();
        List<Long> indexIds = new ArrayList<Long>();
        indexIds.add(baseIndexId);
        indexIds.addAll(olapTable.getIndexIdListExceptBaseIndex());
        for (int i = 0; i < indexIds.size(); i++) {
            List<Column> indexSchema = indexSchemaMap.get(indexIds.get(i));
            MaterializedIndexMeta currentIndexMeta = olapTable.getIndexMetaByIndexId(indexIds.get(i));
            currentIndexMeta.setSchema(indexSchema);

            int currentSchemaVersion = currentIndexMeta.getSchemaVersion();
            int newSchemaVersion = currentSchemaVersion + 1;
            currentIndexMeta.setSchemaVersion(newSchemaVersion);

            //update max column unique id
            int maxColUniqueId = currentIndexMeta.getMaxColUniqueId();
            for (Column column : indexSchema) {
                if (column.getUniqueId() > maxColUniqueId) {
                    maxColUniqueId = column.getUniqueId();
                }
            }
            currentIndexMeta.setMaxColUniqueId(maxColUniqueId);
            currentIndexMeta.setIndexes(indexes);
        }
        olapTable.setIndexes(indexes);
        olapTable.rebuildFullSchema();
        olapTable.rebuildDistributionInfo();
    }

    public void replayModifyTableAddOrDropInvertedIndices(TableAddOrDropInvertedIndicesInfo info)
            throws MetaNotFoundException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("info:{}", info);
        }
        long dbId = info.getDbId();
        long tableId = info.getTableId();
        Map<Long, LinkedList<Column>> indexSchemaMap = info.getIndexSchemaMap();
        List<Index> newIndexes = info.getIndexes();
        List<Index> alterIndexes = info.getAlterInvertedIndexes();
        boolean isDropIndex = info.getIsDropInvertedIndex();
        long jobId = info.getJobId();

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrMetaException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        olapTable.writeLock();
        try {
            modifyTableLightSchemaChange("", db, olapTable, indexSchemaMap, newIndexes,
                                             alterIndexes, isDropIndex, jobId, true);
        } catch (UserException e) {
            // should not happen
            LOG.warn("failed to replay modify table add or drop indexes", e);
        } finally {
            olapTable.writeUnlock();
        }
    }

    public void buildOrDeleteTableInvertedIndices(Database db, OlapTable olapTable,
            Map<Long, LinkedList<Column>> indexSchemaMap, List<Index> alterIndexes,
            Map<Long, Set<String>> invertedIndexOnPartitions, boolean isDropOp) throws UserException {
        LOG.info("begin to build table's inverted index. table: {}", olapTable.getName());

        // for now table's state can only be NORMAL
        Preconditions.checkState(olapTable.getState() == OlapTableState.NORMAL, olapTable.getState().name());

        // remove the index which is not the base index, only base index can be built inverted index
        indexSchemaMap.entrySet().removeIf(entry -> !entry.getKey().equals(olapTable.getBaseIndexId()));
        // begin checking each table
        Map<Long, List<Column>> changedIndexIdToSchema = Maps.newHashMap();
        try {
            changedIndexIdToSchema = checkTable(db, olapTable, indexSchemaMap);
        } catch (DdlException e) {
            throw new DdlException("Table " + db.getFullName() + "." + olapTable.getName()
                                   + " check failed");
        }
        if (changedIndexIdToSchema.isEmpty() && alterIndexes.isEmpty()) {
            throw new DdlException("Nothing is changed. please check your alter stmt.");
        }

        try {
            long timeoutSecond = Config.alter_table_timeout_second;
            for (Map.Entry<Long, List<Column>> entry : changedIndexIdToSchema.entrySet()) {
                long originIndexId = entry.getKey();
                for (Partition partition : olapTable.getPartitions()) {
                    // create job
                    long jobId = Env.getCurrentEnv().getNextId();
                    IndexChangeJob indexChangeJob = new IndexChangeJob(
                            jobId, db.getId(), olapTable.getId(), olapTable.getName(), timeoutSecond * 1000);
                    indexChangeJob.setOriginIndexId(originIndexId);
                    indexChangeJob.setAlterInvertedIndexInfo(isDropOp, alterIndexes);
                    long partitionId = partition.getId();
                    String partitionName = partition.getName();
                    boolean found = false;
                    for (Set<String> partitions : invertedIndexOnPartitions.values()) {
                        if (partitions.contains(partitionName)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        continue;
                    }

                    if (hasIndexChangeJobOnPartition(originIndexId, db.getId(), olapTable.getId(),
                            partitionName, alterIndexes, isDropOp)) {
                        throw new DdlException("partition " + partitionName + " has been built specified index."
                                            + " please check your build stmt.");
                    }

                    indexChangeJob.setPartitionId(partitionId);
                    indexChangeJob.setPartitionName(partitionName);

                    addIndexChangeJob(indexChangeJob);

                    // write edit log
                    Env.getCurrentEnv().getEditLog().logIndexChangeJob(indexChangeJob);
                    LOG.info("finish create table's inverted index job. table: {}, partition: {}, job: {}",
                            olapTable.getName(), partitionName, jobId);
                } // end for partition
            } // end for index
        } catch (Exception e) {
            LOG.warn("Exception:", e);
            throw new UserException(e.getMessage());
        }
    }

    public boolean hasIndexChangeJobOnPartition(
            long originIndexId, long dbId, long tableId, String partitionName,
            List<Index> alterIndexes, boolean isDrop) {
        // TODO: this is temporary methods
        for (IndexChangeJob indexChangeJob : ImmutableList.copyOf(indexChangeJobs.values())) {
            if (indexChangeJob.getOriginIndexId() == originIndexId
                    && indexChangeJob.getDbId() == dbId
                    && indexChangeJob.getTableId() == tableId
                    && indexChangeJob.getPartitionName().equals(partitionName)
                    && indexChangeJob.hasSameAlterInvertedIndex(isDrop, alterIndexes)
                    && !indexChangeJob.isDone()) {
                // if JobState is done (CANCELLED or FINISHED), also allow user to create job again
                return true;
            }
        }
        return false;
    }

    public void replayIndexChangeJob(IndexChangeJob indexChangeJob) throws MetaNotFoundException {
        if (!indexChangeJob.isDone() && !runnableSchemaChangeJobV2.containsKey(indexChangeJob.getJobId())) {
            runnableIndexChangeJob.put(indexChangeJob.getJobId(), indexChangeJob);
        }
        indexChangeJobs.put(indexChangeJob.getJobId(), indexChangeJob);
        indexChangeJob.replay(indexChangeJob);
        if (indexChangeJob.isDone()) {
            runnableIndexChangeJob.remove(indexChangeJob.getJobId());
        }
    }

    public boolean updateBinlogConfig(Database db, OlapTable olapTable, List<AlterClause> alterClauses)
            throws DdlException, UserException {
        List<Partition> partitions = Lists.newArrayList();
        BinlogConfig oldBinlogConfig;
        BinlogConfig newBinlogConfig;

        olapTable.readLock();
        try {
            oldBinlogConfig = new BinlogConfig(olapTable.getBinlogConfig());
            newBinlogConfig = new BinlogConfig(oldBinlogConfig);
            partitions.addAll(olapTable.getPartitions());
        } catch (Exception e) {
            throw new DdlException(e.getMessage());
        } finally {
            olapTable.readUnlock();
        }

        for (AlterClause alterClause : alterClauses) {
            Map<String, String> properties = alterClause.getProperties();
            if (properties == null) {
                continue;
            }

            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE)) {
                boolean binlogEnable = Boolean.parseBoolean(properties.get(
                        PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE));
                if (binlogEnable != oldBinlogConfig.isEnable()) {
                    newBinlogConfig.setEnable(binlogEnable);
                }
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS)) {
                Long binlogTtlSeconds = Long.parseLong(properties.get(
                        PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS));
                if (binlogTtlSeconds != oldBinlogConfig.getTtlSeconds()) {
                    newBinlogConfig.setTtlSeconds(binlogTtlSeconds);
                }
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES)) {
                Long binlogMaxBytes = Long.parseLong(properties.get(
                        PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES));
                if (binlogMaxBytes != oldBinlogConfig.getMaxBytes()) {
                    newBinlogConfig.setMaxBytes(binlogMaxBytes);
                }
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS)) {
                Long binlogMaxHistoryNums = Long.parseLong(properties.get(
                        PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS));
                if (binlogMaxHistoryNums != oldBinlogConfig.getMaxHistoryNums()) {
                    newBinlogConfig.setMaxHistoryNums(binlogMaxHistoryNums);
                }
            }
        }

        boolean hasChanged = !newBinlogConfig.equals(oldBinlogConfig);
        if (!hasChanged) {
            LOG.info("table {} binlog config is same as the previous version, so nothing need to do",
                    olapTable.getName());
            return true;
        }

        // check db binlog config, if db binlog config is not same as table binlog config, throw exception
        BinlogConfig dbBinlogConfig;
        db.readLock();
        try {
            dbBinlogConfig = new BinlogConfig(db.getBinlogConfig());
        } finally {
            db.readUnlock();
        }
        boolean dbBinlogEnable = (dbBinlogConfig != null && dbBinlogConfig.isEnable());
        if (dbBinlogEnable && !newBinlogConfig.isEnable()) {
            throw new DdlException("db binlog is enable, but table binlog is disable");
        }

        LOG.info("begin to update table's binlog config. table: {}, old binlog: {}, new binlog: {}",
                olapTable.getName(), oldBinlogConfig, newBinlogConfig);


        for (Partition partition : partitions) {
            updatePartitionProperties(db, olapTable.getName(), partition.getName(), -1, -1,
                                                newBinlogConfig, null, null, -1, -1, -1);
        }

        olapTable.writeLockOrDdlException();
        try {
            Env.getCurrentEnv().updateBinlogConfig(db, olapTable, newBinlogConfig);
        } finally {
            olapTable.writeUnlock();
        }

        return false;
    }

    private void removeColumnWhenDropGeneratedColumn(Column dropColumn, Map<String, Column> nameToColumn) {
        GeneratedColumnInfo generatedColumnInfo = dropColumn.getGeneratedColumnInfo();
        if (generatedColumnInfo == null) {
            return;
        }
        String dropColName = dropColumn.getName();
        Expr expr = generatedColumnInfo.getExpr();
        Set<Expr> slotRefsInGeneratedExpr = new HashSet<>();
        expr.collect(e -> e instanceof SlotRef, slotRefsInGeneratedExpr);
        for (Expr slotRef : slotRefsInGeneratedExpr) {
            String name = ((SlotRef) slotRef).getColumnName();
            if (!nameToColumn.containsKey(name)) {
                continue;
            }
            Column c = nameToColumn.get(name);
            Set<String> sets = c.getGeneratedColumnsThatReferToThis();
            sets.remove(dropColName);
        }
    }

    private void checkOrder(List<Column> targetIndexSchema, List<String> orderedColNames) throws DdlException {
        Set<String> nameSet = new HashSet<>();
        for (Column column : targetIndexSchema) {
            if (column.isVisible() && null == column.getGeneratedColumnInfo()) {
                nameSet.add(column.getName());
            }
        }
        for (String colName : orderedColNames) {
            Column oneCol = null;
            for (Column column : targetIndexSchema) {
                if (column.getName().equalsIgnoreCase(colName) && column.isVisible()) {
                    oneCol = column;
                    break;
                }
            }
            if (oneCol == null) {
                throw new DdlException("Column[" + colName + "] not exists");
            }
            if (null == oneCol.getGeneratedColumnInfo()) {
                continue;
            }
            Expr expr = oneCol.getGeneratedColumnInfo().getExpr();
            Set<Expr> slotRefsInGeneratedExpr = new HashSet<>();
            expr.collect(e -> e instanceof SlotRef, slotRefsInGeneratedExpr);
            for (Expr slotRef : slotRefsInGeneratedExpr) {
                String slotName = ((SlotRef) slotRef).getColumnName();
                if (!nameSet.contains(slotName)) {
                    throw new DdlException("The specified column order is incorrect, `" + colName
                            + "` should come after `" + slotName
                            + "`, because both of them are generated columns, and `"
                            + colName + "` refers to `" + slotName + "`.");
                }
            }
            nameSet.add(colName);
        }
    }
}
