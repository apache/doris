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

package org.apache.doris.load;

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TFileFormatType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Load {
    private static final Logger LOG = LogManager.getLogger(Load.class);
    public static final String VERSION = "v1";

    // valid state change map
    private static final Map<JobState, Set<JobState>> STATE_CHANGE_MAP = Maps.newHashMap();

    // system dpp config
    public static DppConfig dppDefaultConfig = null;
    public static Map<String, DppConfig> clusterToDppConfig = Maps.newHashMap();

    // load job meta
    private Map<Long, LoadJob> idToLoadJob; // loadJobId to loadJob
    private Map<Long, List<LoadJob>> dbToLoadJobs; // db to loadJob list
    private Map<Long, Map<String, List<LoadJob>>> dbLabelToLoadJobs; // db label to loadJob list
    private Map<Long, LoadJob> idToPendingLoadJob; // loadJobId to pending loadJob
    private Map<Long, LoadJob> idToEtlLoadJob; // loadJobId to etl loadJob
    private Map<Long, LoadJob> idToLoadingLoadJob; // loadJobId to loading loadJob
    private Map<Long, LoadJob> idToQuorumFinishedLoadJob; // loadJobId to quorum finished loadJob
    private Set<Long> loadingPartitionIds; // loading partition id set
    // dbId -> set of (label, timestamp)
    private Map<Long, Map<String, Long>> dbToMiniLabels; // db to mini uncommitted label

    // lock for load job
    // lock is private and must use after db lock
    private ReentrantReadWriteLock lock;

    static {
        Set<JobState> pendingDestStates = Sets.newHashSet();
        pendingDestStates.add(JobState.ETL);
        pendingDestStates.add(JobState.CANCELLED);
        STATE_CHANGE_MAP.put(JobState.PENDING, pendingDestStates);

        Set<JobState> etlDestStates = Sets.newHashSet();
        etlDestStates.add(JobState.LOADING);
        etlDestStates.add(JobState.CANCELLED);
        STATE_CHANGE_MAP.put(JobState.ETL, etlDestStates);

        Set<JobState> loadingDestStates = Sets.newHashSet();
        loadingDestStates.add(JobState.FINISHED);
        loadingDestStates.add(JobState.QUORUM_FINISHED);
        loadingDestStates.add(JobState.CANCELLED);
        STATE_CHANGE_MAP.put(JobState.LOADING, loadingDestStates);

        Set<JobState> quorumFinishedDestStates = Sets.newHashSet();
        quorumFinishedDestStates.add(JobState.FINISHED);
        STATE_CHANGE_MAP.put(JobState.QUORUM_FINISHED, quorumFinishedDestStates);

        // system dpp config
        Gson gson = new Gson();
        try {
            Map<String, String> defaultConfig =
                    (HashMap<String, String>) gson.fromJson(Config.dpp_default_config_str, HashMap.class);
            dppDefaultConfig = DppConfig.create(defaultConfig);

            Map<String, Map<String, String>> clusterToConfig =
                    (HashMap<String, Map<String, String>>) gson.fromJson(Config.dpp_config_str, HashMap.class);
            for (Entry<String, Map<String, String>> entry : clusterToConfig.entrySet()) {
                String cluster = entry.getKey();
                DppConfig dppConfig = dppDefaultConfig.getCopiedDppConfig();
                dppConfig.update(DppConfig.create(entry.getValue()));
                dppConfig.check();

                clusterToDppConfig.put(cluster, dppConfig);
            }

            if (!clusterToDppConfig.containsKey(Config.dpp_default_cluster)) {
                throw new LoadException("Default cluster not exist");
            }
        } catch (Throwable e) {
            LOG.error("dpp default config ill-formed", e);
            System.exit(-1);
        }
    }

    public Load() {
        idToLoadJob = Maps.newHashMap();
        dbToLoadJobs = Maps.newHashMap();
        dbLabelToLoadJobs = Maps.newHashMap();
        idToPendingLoadJob = Maps.newLinkedHashMap();
        idToEtlLoadJob = Maps.newLinkedHashMap();
        idToLoadingLoadJob = Maps.newLinkedHashMap();
        idToQuorumFinishedLoadJob = Maps.newLinkedHashMap();
        loadingPartitionIds = Sets.newHashSet();
        dbToMiniLabels = Maps.newHashMap();
        lock = new ReentrantReadWriteLock(true);
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    /**
     * When doing schema change, there may have some 'shadow' columns, with prefix '__doris_shadow_' in
     * their names. These columns are invisible to user, but we need to generate data for these columns.
     * So we add column mappings for these column.
     * eg1:
     * base schema is (A, B, C), and B is under schema change, so there will be a shadow column: '__doris_shadow_B'
     * So the final column mapping should looks like: (A, B, C, __doris_shadow_B = substitute(B));
     */
    public static List<ImportColumnDesc> getSchemaChangeShadowColumnDesc(Table tbl, Map<String, Expr> columnExprMap) {
        List<ImportColumnDesc> shadowColumnDescs = Lists.newArrayList();
        for (Column column : tbl.getFullSchema()) {
            if (!column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
                continue;
            }

            String originCol = column.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX);
            if (columnExprMap.containsKey(originCol)) {
                Expr mappingExpr = columnExprMap.get(originCol);
                if (mappingExpr != null) {
                    /*
                     * eg:
                     * (A, C) SET (B = func(xx))
                     * ->
                     * (A, C) SET (B = func(xx), __doris_shadow_B = func(xx))
                     */
                    ImportColumnDesc importColumnDesc = new ImportColumnDesc(column.getName(), mappingExpr);
                    shadowColumnDescs.add(importColumnDesc);
                } else {
                    /*
                     * eg:
                     * (A, B, C)
                     * ->
                     * (A, B, C) SET (__doris_shadow_B = B)
                     */
                    SlotRef slot = new SlotRef(null, originCol);
                    slot.setType(column.getType());
                    ImportColumnDesc importColumnDesc = new ImportColumnDesc(column.getName(), slot);
                    shadowColumnDescs.add(importColumnDesc);
                }
            } else {
                /*
                 * There is a case that if user does not specify the related origin column, eg:
                 * COLUMNS (A, C), and B is not specified, but B is being modified
                 * so there is a shadow column '__doris_shadow_B'.
                 * We can not just add a mapping function "__doris_shadow_B = substitute(B)",
                 * because Doris can not find column B.
                 * In this case, __doris_shadow_B can use its default value, so no need to add it to column mapping
                 */
                // do nothing
            }
        }
        return shadowColumnDescs;
    }

    /*
     * used for spark load job
     * not init slot desc and analyze exprs
     */
    public static void initColumns(Table tbl, List<ImportColumnDesc> columnExprs,
            Map<String, Pair<String, List<String>>> columnToHadoopFunction) throws UserException {
        initColumns(tbl, columnExprs, columnToHadoopFunction, null, null, null, null, null, null, null, false, false);
    }

    /*
     * This function should be used for broker load v2 and stream load.
     * And it must be called in same db lock when planing.
     */
    public static void initColumns(Table tbl, LoadTaskInfo.ImportColumnDescs columnDescs,
            Map<String, Pair<String, List<String>>> columnToHadoopFunction, Map<String, Expr> exprsByName,
            Analyzer analyzer, TupleDescriptor srcTupleDesc, Map<String, SlotDescriptor> slotDescByName,
            List<Integer> srcSlotIds, TFileFormatType formatType, List<String> hiddenColumns, boolean isPartialUpdate)
            throws UserException {
        rewriteColumns(columnDescs);
        initColumns(tbl, columnDescs.descs, columnToHadoopFunction, exprsByName, analyzer, srcTupleDesc, slotDescByName,
                srcSlotIds, formatType, hiddenColumns, true, isPartialUpdate);
    }

    /*
     * This function will do followings:
     * 1. fill the column exprs if user does not specify any column or column mapping.
     * 2. For not specified columns, check if they have default value or they are auto-increment columns.
     * 3. Add any shadow columns if have.
     * 4. validate hadoop functions
     * 5. init slot descs and expr map for load plan
     */
    private static void initColumns(Table tbl, List<ImportColumnDesc> columnExprs,
            Map<String, Pair<String, List<String>>> columnToHadoopFunction, Map<String, Expr> exprsByName,
            Analyzer analyzer, TupleDescriptor srcTupleDesc, Map<String, SlotDescriptor> slotDescByName,
            List<Integer> srcSlotIds, TFileFormatType formatType, List<String> hiddenColumns,
            boolean needInitSlotAndAnalyzeExprs, boolean isPartialUpdate) throws UserException {
        // We make a copy of the columnExprs so that our subsequent changes
        // to the columnExprs will not affect the original columnExprs.
        // skip the mapping columns not exist in schema
        // eg: the origin column list is:
        //          (k1, k2, tmpk3 = k1 + k2, k3 = tmpk3)
        //     after calling rewriteColumns(), it will become
        //          (k1, k2, tmpk3 = k1 + k2, k3 = k1 + k2)
        //     so "tmpk3 = k1 + k2" is not needed anymore, we can skip it.
        List<ImportColumnDesc> copiedColumnExprs = new ArrayList<>();
        for (ImportColumnDesc importColumnDesc : columnExprs) {
            String mappingColumnName = importColumnDesc.getColumnName();
            if (importColumnDesc.isColumn() || tbl.getColumn(mappingColumnName) != null) {
                copiedColumnExprs.add(importColumnDesc);
            }
        }
        // check whether the OlapTable has sequenceCol
        boolean hasSequenceCol = false;
        if (tbl instanceof OlapTable && ((OlapTable) tbl).hasSequenceCol()) {
            hasSequenceCol = true;
        }

        // If user does not specify the file field names, generate it by using base schema of table.
        // So that the following process can be unified
        boolean specifyFileFieldNames = copiedColumnExprs.stream().anyMatch(p -> p.isColumn());
        if (!specifyFileFieldNames) {
            List<Column> columns = tbl.getBaseSchema(false);
            for (Column column : columns) {
                // columnExprs has sequence column, don't need to generate the sequence column
                if (hasSequenceCol && column.isSequenceColumn()) {
                    continue;
                }
                ImportColumnDesc columnDesc = null;
                if (formatType == TFileFormatType.FORMAT_JSON) {
                    columnDesc = new ImportColumnDesc(column.getName());
                } else {
                    columnDesc = new ImportColumnDesc(column.getName().toLowerCase());
                }
                LOG.debug("add base column {} to stream load task", column.getName());
                copiedColumnExprs.add(columnDesc);
            }
            if (hiddenColumns != null) {
                for (String columnName : hiddenColumns) {
                    Column column = tbl.getColumn(columnName);
                    if (column != null && !column.isVisible()) {
                        ImportColumnDesc columnDesc = new ImportColumnDesc(column.getName());
                        LOG.debug("add hidden column {} to stream load task", column.getName());
                        copiedColumnExprs.add(columnDesc);
                    }
                }
            }
        }
        // generate a map for checking easily
        Map<String, Expr> columnExprMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (ImportColumnDesc importColumnDesc : copiedColumnExprs) {
            columnExprMap.put(importColumnDesc.getColumnName(), importColumnDesc.getExpr());
        }
        HashMap<String, Type> colToType = new HashMap<>();

        // check default value and auto-increment column
        for (Column column : tbl.getBaseSchema()) {
            String columnName = column.getName();
            colToType.put(columnName, column.getType());
            if (columnExprMap.containsKey(columnName)) {
                continue;
            }
            if (column.getDefaultValue() != null) {
                exprsByName.put(column.getName(), column.getDefaultValueExpr());
                continue;
            }
            if (column.isAllowNull()) {
                exprsByName.put(column.getName(), NullLiteral.create(column.getType()));
                continue;
            }
            if (isPartialUpdate) {
                continue;
            }
            if (column.isAutoInc()) {
                continue;
            }
            throw new DdlException("Column has no default value. column: " + columnName);
        }

        // get shadow column desc when table schema change
        copiedColumnExprs.addAll(getSchemaChangeShadowColumnDesc(tbl, columnExprMap));

        // validate hadoop functions
        if (columnToHadoopFunction != null) {
            Map<String, String> columnNameMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (ImportColumnDesc importColumnDesc : copiedColumnExprs) {
                if (importColumnDesc.isColumn()) {
                    columnNameMap.put(importColumnDesc.getColumnName(), importColumnDesc.getColumnName());
                }
            }
            for (Entry<String, Pair<String, List<String>>> entry : columnToHadoopFunction.entrySet()) {
                String mappingColumnName = entry.getKey();
                Column mappingColumn = tbl.getColumn(mappingColumnName);
                if (mappingColumn == null) {
                    throw new DdlException("Mapping column is not in table. column: " + mappingColumnName);
                }
                Pair<String, List<String>> function = entry.getValue();
                try {
                    DataDescription.validateMappingFunction(function.first, function.second, columnNameMap,
                            mappingColumn, false);
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
            }
        }

        if (!needInitSlotAndAnalyzeExprs) {
            return;
        }
        Set<String> exprSrcSlotName = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ImportColumnDesc importColumnDesc : copiedColumnExprs) {
            if (importColumnDesc.isColumn()) {
                continue;
            }
            List<SlotRef> slots = Lists.newArrayList();
            importColumnDesc.getExpr().collect(SlotRef.class, slots);
            for (SlotRef slot : slots) {
                String slotColumnName = slot.getColumnName();
                exprSrcSlotName.add(slotColumnName);
            }
        }

        // init slot desc add expr map, also transform hadoop functions
        for (ImportColumnDesc importColumnDesc : copiedColumnExprs) {
            // make column name case match with real column name
            String columnName = importColumnDesc.getColumnName();
            Column tblColumn = tbl.getColumn(columnName);
            String realColName;
            if (tblColumn == null || tblColumn.getName() == null || importColumnDesc.getExpr() == null) {
                realColName = columnName;
            } else {
                realColName = tblColumn.getName();
            }
            if (importColumnDesc.getExpr() != null) {
                Expr expr = transformHadoopFunctionExpr(tbl, realColName, importColumnDesc.getExpr());
                exprsByName.put(realColName, expr);
            } else {
                SlotDescriptor slotDesc = analyzer.getDescTbl().addSlotDescriptor(srcTupleDesc);

                if (formatType == TFileFormatType.FORMAT_ARROW) {
                    slotDesc.setColumn(new Column(realColName, colToType.get(realColName)));
                } else {
                    // columns default be varchar type
                    slotDesc.setType(ScalarType.createType(PrimitiveType.VARCHAR));
                    slotDesc.setColumn(new Column(realColName, PrimitiveType.VARCHAR));
                }

                // ISSUE A: src slot should be nullable even if the column is not nullable.
                // because src slot is what we read from file, not represent to real column value.
                // If column is not nullable, error will be thrown when filling the dest slot,
                // which is not nullable.
                slotDesc.setIsNullable(true);

                slotDesc.setIsMaterialized(true);
                srcSlotIds.add(slotDesc.getId().asInt());
                slotDescByName.put(realColName, slotDesc);
            }
        }

        LOG.debug("plan srcTupleDesc {}", srcTupleDesc.toString());

        /*
         * The extension column of the materialized view is added to the expression evaluation of load
         * To avoid nested expressions. eg : column(a, tmp_c, c = expr(tmp_c)) ,
         * __doris_materialized_view_bitmap_union_c need be analyzed after exprsByName
         * So the columns of the materialized view are stored separately here
         */
        Map<String, Expr> mvDefineExpr = Maps.newHashMap();
        for (Column column : tbl.getFullSchema()) {
            if (column.getDefineExpr() != null) {
                mvDefineExpr.put(column.getName(), column.getDefineExpr());
            }
        }

        LOG.debug("slotDescByName: {}, exprsByName: {}, mvDefineExpr: {}", slotDescByName, exprsByName, mvDefineExpr);

        // in vectorized load, reanalyze exprs with castExpr type
        // otherwise analyze exprs with varchar type
        analyzeAllExprs(tbl, analyzer, exprsByName, mvDefineExpr, slotDescByName);
        LOG.debug("after init column, exprMap: {}", exprsByName);
    }

    private static Expr getExprFromDesc(Analyzer analyzer, SlotDescriptor slotDesc, SlotRef slot)
            throws AnalysisException {
        SlotRef newSlot = new SlotRef(slotDesc);
        newSlot.setType(slotDesc.getType());
        Expr rhs = newSlot;
        rhs = rhs.castTo(slot.getType());

        if (slot.getDesc() == null) {
            // shadow column
            return rhs;
        }

        if (newSlot.isNullable() && !slot.isNullable()) {
            rhs = new FunctionCallExpr("non_nullable", Lists.newArrayList(rhs));
            rhs.setType(slotDesc.getType());
            rhs.analyze(analyzer);
        } else if (!newSlot.isNullable() && slot.isNullable()) {
            rhs = new FunctionCallExpr("nullable", Lists.newArrayList(rhs));
            rhs.setType(slotDesc.getType());
            rhs.analyze(analyzer);
        }
        return rhs;
    }

    private static void analyzeAllExprs(Table tbl, Analyzer analyzer, Map<String, Expr> exprsByName,
            Map<String, Expr> mvDefineExpr, Map<String, SlotDescriptor> slotDescByName) throws UserException {
        // analyze all exprs
        for (Map.Entry<String, Expr> entry : exprsByName.entrySet()) {
            ExprSubstitutionMap smap = new ExprSubstitutionMap();
            List<SlotRef> slots = Lists.newArrayList();
            entry.getValue().collect(SlotRef.class, slots);
            for (SlotRef slot : slots) {
                SlotDescriptor slotDesc = slotDescByName.get(slot.getColumnName());
                if (slotDesc == null) {
                    if (entry.getKey() != null) {
                        if (entry.getKey().equalsIgnoreCase(Column.DELETE_SIGN)) {
                            throw new UserException("unknown reference column in DELETE ON clause:"
                                    + slot.getColumnName());
                        } else if (entry.getKey().equalsIgnoreCase(Column.SEQUENCE_COL)) {
                            throw new UserException("unknown reference column in ORDER BY clause:"
                                    + slot.getColumnName());
                        }
                    }
                    throw new UserException("unknown reference column, column=" + entry.getKey()
                            + ", reference=" + slot.getColumnName());
                }
                smap.getLhs().add(slot);
                smap.getRhs().add(new SlotRef(slotDesc));
            }
            Expr expr = entry.getValue().clone(smap);
            expr.analyze(analyzer);

            // check if contain aggregation
            List<FunctionCallExpr> funcs = Lists.newArrayList();
            expr.collect(FunctionCallExpr.class, funcs);
            for (FunctionCallExpr fn : funcs) {
                if (fn.isAggregateFunction()) {
                    throw new AnalysisException("Don't support aggregation function in load expression");
                }
            }

            // Array type do not support cast now
            Type exprReturnType = expr.getType();
            if (exprReturnType.isArrayType()) {
                Type schemaType = tbl.getColumn(entry.getKey()).getType();
                if (exprReturnType != schemaType) {
                    throw new AnalysisException("Don't support load from type:" + exprReturnType + " to type:"
                            + schemaType + " for column:" + entry.getKey());
                }
            }

            exprsByName.put(entry.getKey(), expr);
        }

        for (Map.Entry<String, Expr> entry : mvDefineExpr.entrySet()) {
            ExprSubstitutionMap smap = new ExprSubstitutionMap();
            List<SlotRef> slots = Lists.newArrayList();
            entry.getValue().collect(SlotRef.class, slots);
            for (SlotRef slot : slots) {
                if (slotDescByName.get(slot.getColumnName()) != null) {
                    smap.getLhs().add(slot);
                    smap.getRhs().add(getExprFromDesc(analyzer, slotDescByName.get(slot.getColumnName()), slot));
                } else if (exprsByName.get(slot.getColumnName()) != null) {
                    smap.getLhs().add(slot);
                    smap.getRhs().add(new CastExpr(tbl.getColumn(slot.getColumnName()).getType(),
                            exprsByName.get(slot.getColumnName())));
                } else {
                    if (entry.getKey().equalsIgnoreCase(Column.DELETE_SIGN)) {
                        throw new UserException("unknown reference column in DELETE ON clause:" + slot.getColumnName());
                    } else if (entry.getKey().equalsIgnoreCase(Column.SEQUENCE_COL)) {
                        throw new UserException("unknown reference column in ORDER BY clause:" + slot.getColumnName());
                    } else {
                        throw new UserException("unknown reference column, column=" + entry.getKey()
                                + ", reference=" + slot.getColumnName());
                    }
                }
            }
            Expr expr = entry.getValue().clone(smap);
            expr.analyze(analyzer);

            exprsByName.put(entry.getKey(), expr);
        }
    }

    public static void rewriteColumns(LoadTaskInfo.ImportColumnDescs columnDescs) {
        if (columnDescs.isColumnDescsRewrited) {
            return;
        }

        Map<String, Expr> derivativeColumns = new HashMap<>();
        // find and rewrite the derivative columns
        // e.g. (v1,v2=v1+1,v3=v2+1) --> (v1, v2=v1+1, v3=v1+1+1)
        // 1. find the derivative columns
        for (ImportColumnDesc importColumnDesc : columnDescs.descs) {
            if (!importColumnDesc.isColumn()) {
                if (importColumnDesc.getExpr() instanceof SlotRef) {
                    String columnName = ((SlotRef) importColumnDesc.getExpr()).getColumnName();
                    if (derivativeColumns.containsKey(columnName)) {
                        importColumnDesc.setExpr(derivativeColumns.get(columnName));
                    }
                } else {
                    recursiveRewrite(importColumnDesc.getExpr(), derivativeColumns);
                }
                derivativeColumns.put(importColumnDesc.getColumnName(), importColumnDesc.getExpr());
            }
        }

        columnDescs.isColumnDescsRewrited = true;
    }

    private static void recursiveRewrite(Expr expr, Map<String, Expr> derivativeColumns) {
        if (CollectionUtils.isEmpty(expr.getChildren())) {
            return;
        }
        for (int i = 0; i < expr.getChildren().size(); i++) {
            Expr e = expr.getChild(i);
            if (e instanceof SlotRef) {
                String columnName = ((SlotRef) e).getColumnName();
                if (derivativeColumns.containsKey(columnName)) {
                    expr.setChild(i, derivativeColumns.get(columnName));
                }
            } else {
                recursiveRewrite(e, derivativeColumns);
            }
        }
    }

    /**
     * This method is used to transform hadoop function.
     * The hadoop function includes: replace_value, strftime, time_format, alignment_timestamp, default_value, now.
     * It rewrites those function with real function name and param.
     * For the other function, the expr only go through this function and the origin expr is returned.
     *
     * @param columnName
     * @param originExpr
     * @return
     * @throws UserException
     */
    private static Expr transformHadoopFunctionExpr(Table tbl, String columnName, Expr originExpr)
            throws UserException {
        Column column = tbl.getColumn(columnName);
        if (column == null) {
            // the unknown column will be checked later.
            return originExpr;
        }

        // To compatible with older load version
        if (originExpr instanceof FunctionCallExpr) {
            FunctionCallExpr funcExpr = (FunctionCallExpr) originExpr;
            String funcName = funcExpr.getFnName().getFunction();

            if (funcName.equalsIgnoreCase("replace_value")) {
                List<Expr> exprs = Lists.newArrayList();
                SlotRef slotRef = new SlotRef(null, columnName);
                // We will convert this to IF(`col` != child0, `col`, child1),
                // because we need the if return type equal to `col`, we use NE

                /*
                 * We will convert this based on different cases:
                 * case 1: k1 = replace_value(null, anyval);
                 *     to: k1 = if (k1 is not null, k1, anyval);
                 *
                 * case 2: k1 = replace_value(anyval1, anyval2);
                 *     to: k1 = if (k1 is not null, if(k1 != anyval1, k1, anyval2), null);
                 */
                if (funcExpr.getChild(0) instanceof NullLiteral) {
                    // case 1
                    exprs.add(new IsNullPredicate(slotRef, true));
                    exprs.add(slotRef);
                    if (funcExpr.hasChild(1)) {
                        exprs.add(funcExpr.getChild(1));
                    } else {
                        if (column.getDefaultValue() != null) {
                            if (column.getDefaultValueExprDef() != null) {
                                exprs.add(column.getDefaultValueExpr());
                            } else {
                                exprs.add(new StringLiteral(column.getDefaultValue()));
                            }
                        } else {
                            if (column.isAllowNull()) {
                                exprs.add(NullLiteral.create(Type.VARCHAR));
                            } else {
                                throw new UserException("Column(" + columnName + ") has no default value.");
                            }
                        }
                    }
                } else {
                    // case 2
                    exprs.add(new IsNullPredicate(slotRef, true));
                    List<Expr> innerIfExprs = Lists.newArrayList();
                    innerIfExprs.add(new BinaryPredicate(BinaryPredicate.Operator.NE, slotRef, funcExpr.getChild(0)));
                    innerIfExprs.add(slotRef);
                    if (funcExpr.hasChild(1)) {
                        innerIfExprs.add(funcExpr.getChild(1));
                    } else {
                        if (column.getDefaultValue() != null) {
                            if (column.getDefaultValueExprDef() != null) {
                                innerIfExprs.add(column.getDefaultValueExpr());
                            } else {
                                innerIfExprs.add(new StringLiteral(column.getDefaultValue()));
                            }
                        } else {
                            if (column.isAllowNull()) {
                                innerIfExprs.add(NullLiteral.create(Type.VARCHAR));
                            } else {
                                throw new UserException("Column(" + columnName + ") has no default value.");
                            }
                        }
                    }
                    FunctionCallExpr innerIfFn = new FunctionCallExpr("if", innerIfExprs);
                    exprs.add(innerIfFn);
                    exprs.add(NullLiteral.create(Type.VARCHAR));
                }

                LOG.debug("replace_value expr: {}", exprs);
                FunctionCallExpr newFn = new FunctionCallExpr("if", exprs);
                return newFn;
            } else if (funcName.equalsIgnoreCase("strftime")) {
                // FROM_UNIXTIME(val)
                FunctionName fromUnixName = new FunctionName("FROM_UNIXTIME");
                List<Expr> fromUnixArgs = Lists.newArrayList(funcExpr.getChild(1));
                FunctionCallExpr fromUnixFunc = new FunctionCallExpr(
                        fromUnixName, new FunctionParams(false, fromUnixArgs));

                return fromUnixFunc;
            } else if (funcName.equalsIgnoreCase("time_format")) {
                // DATE_FORMAT(STR_TO_DATE(dt_str, dt_fmt))
                FunctionName strToDateName = new FunctionName("STR_TO_DATE");
                List<Expr> strToDateExprs = Lists.newArrayList(funcExpr.getChild(2), funcExpr.getChild(1));
                FunctionCallExpr strToDateFuncExpr = new FunctionCallExpr(
                        strToDateName, new FunctionParams(false, strToDateExprs));

                FunctionName dateFormatName = new FunctionName("DATE_FORMAT");
                List<Expr> dateFormatArgs = Lists.newArrayList(strToDateFuncExpr, funcExpr.getChild(0));
                FunctionCallExpr dateFormatFunc = new FunctionCallExpr(
                        dateFormatName, new FunctionParams(false, dateFormatArgs));

                return dateFormatFunc;
            } else if (funcName.equalsIgnoreCase("alignment_timestamp")) {
                /*
                 * change to:
                 * UNIX_TIMESTAMP(DATE_FORMAT(FROM_UNIXTIME(ts), "%Y-01-01 00:00:00"));
                 *
                 */

                // FROM_UNIXTIME
                FunctionName fromUnixName = new FunctionName("FROM_UNIXTIME");
                List<Expr> fromUnixArgs = Lists.newArrayList(funcExpr.getChild(1));
                FunctionCallExpr fromUnixFunc = new FunctionCallExpr(
                        fromUnixName, new FunctionParams(false, fromUnixArgs));

                // DATE_FORMAT
                StringLiteral precision = (StringLiteral) funcExpr.getChild(0);
                StringLiteral format;
                if (precision.getStringValue().equalsIgnoreCase("year")) {
                    format = new StringLiteral("%Y-01-01 00:00:00");
                } else if (precision.getStringValue().equalsIgnoreCase("month")) {
                    format = new StringLiteral("%Y-%m-01 00:00:00");
                } else if (precision.getStringValue().equalsIgnoreCase("day")) {
                    format = new StringLiteral("%Y-%m-%d 00:00:00");
                } else if (precision.getStringValue().equalsIgnoreCase("hour")) {
                    format = new StringLiteral("%Y-%m-%d %H:00:00");
                } else {
                    throw new UserException("Unknown precision(" + precision.getStringValue() + ")");
                }
                FunctionName dateFormatName = new FunctionName("DATE_FORMAT");
                List<Expr> dateFormatArgs = Lists.newArrayList(fromUnixFunc, format);
                FunctionCallExpr dateFormatFunc = new FunctionCallExpr(
                        dateFormatName, new FunctionParams(false, dateFormatArgs));

                // UNIX_TIMESTAMP
                FunctionName unixTimeName = new FunctionName("UNIX_TIMESTAMP");
                List<Expr> unixTimeArgs = Lists.newArrayList();
                unixTimeArgs.add(dateFormatFunc);
                FunctionCallExpr unixTimeFunc = new FunctionCallExpr(
                        unixTimeName, new FunctionParams(false, unixTimeArgs));

                return unixTimeFunc;
            } else if (funcName.equalsIgnoreCase("default_value")) {
                return funcExpr.getChild(0);
            } else if (funcName.equalsIgnoreCase("now")) {
                FunctionName nowFunctionName = new FunctionName("NOW");
                FunctionCallExpr newFunc = new FunctionCallExpr(nowFunctionName, new FunctionParams(null));
                return newFunc;
            } else if (funcName.equalsIgnoreCase("substitute")) {
                return funcExpr.getChild(0);
            }
        }
        return originExpr;
    }

    // return true if we truly register a mini load label
    // return false otherwise (eg: a retry request)
    public boolean registerMiniLabel(String fullDbName, String label, long timestamp) throws DdlException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(fullDbName);

        long dbId = db.getId();
        writeLock();
        try {
            if (unprotectIsLabelUsed(dbId, label, timestamp, true)) {
                // label is used and this is a retry request.
                // no need to do further operation, just return.
                return false;
            }

            Map<String, Long> miniLabels = null;
            if (dbToMiniLabels.containsKey(dbId)) {
                miniLabels = dbToMiniLabels.get(dbId);
            } else {
                miniLabels = Maps.newHashMap();
                dbToMiniLabels.put(dbId, miniLabels);
            }
            miniLabels.put(label, timestamp);

            return true;
        } finally {
            writeUnlock();
        }
    }

    public void deregisterMiniLabel(String fullDbName, String label) throws DdlException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(fullDbName);

        long dbId = db.getId();
        writeLock();
        try {
            if (!dbToMiniLabels.containsKey(dbId)) {
                return;
            }

            Map<String, Long> miniLabels = dbToMiniLabels.get(dbId);
            miniLabels.remove(label);
            if (miniLabels.isEmpty()) {
                dbToMiniLabels.remove(dbId);
            }
        } finally {
            writeUnlock();
        }
    }

    public boolean isUncommittedLabel(long dbId, String label) throws DdlException {
        readLock();
        try {
            if (dbToMiniLabels.containsKey(dbId)) {
                Map<String, Long> uncommittedLabels = dbToMiniLabels.get(dbId);
                return uncommittedLabels.containsKey(label);
            }
        } finally {
            readUnlock();
        }
        return false;
    }

    public boolean isLabelUsed(long dbId, String label) throws DdlException {
        readLock();
        try {
            return unprotectIsLabelUsed(dbId, label, -1, true);
        } finally {
            readUnlock();
        }
    }

    /*
     * 1. if label is already used, and this is not a retry request,
     *    throw exception ("Label already used")
     * 2. if label is already used, but this is a retry request,
     *    return true
     * 3. if label is not used, return false
     * 4. throw exception if encounter error.
     */
    private boolean unprotectIsLabelUsed(long dbId, String label, long timestamp, boolean checkMini)
            throws DdlException {
        // check dbLabelToLoadJobs
        if (dbLabelToLoadJobs.containsKey(dbId)) {
            Map<String, List<LoadJob>> labelToLoadJobs = dbLabelToLoadJobs.get(dbId);
            if (labelToLoadJobs.containsKey(label)) {
                List<LoadJob> labelLoadJobs = labelToLoadJobs.get(label);
                for (LoadJob oldJob : labelLoadJobs) {
                    JobState oldJobState = oldJob.getState();
                    if (oldJobState != JobState.CANCELLED) {
                        if (timestamp == -1) {
                            // timestamp == -1 is for compatibility
                            throw new LabelAlreadyUsedException(label);
                        } else {
                            if (timestamp == oldJob.getTimestamp()) {
                                // this timestamp is used to verify if this label check is a retry request from backend.
                                // if the timestamp in request is same as timestamp in existing load job,
                                // which means this load job is already submitted
                                LOG.info("get a retry request with label: {}, timestamp: {}. return ok",
                                        label, timestamp);
                                return true;
                            } else {
                                throw new LabelAlreadyUsedException(label);
                            }
                        }
                    }
                }
            }
        }

        // check dbToMiniLabel
        if (checkMini) {
            return checkMultiLabelUsed(dbId, label, timestamp);
        }

        return false;
    }

    private boolean checkMultiLabelUsed(long dbId, String label, long timestamp) throws DdlException {
        if (dbToMiniLabels.containsKey(dbId)) {
            Map<String, Long> uncommittedLabels = dbToMiniLabels.get(dbId);
            if (uncommittedLabels.containsKey(label)) {
                if (timestamp == -1) {
                    throw new LabelAlreadyUsedException(label);
                } else {
                    if (timestamp == uncommittedLabels.get(label)) {
                        // this timestamp is used to verify if this label check is a retry request from backend.
                        // if the timestamp in request is same as timestamp in existing load job,
                        // which means this load job is already submitted
                        LOG.info("get a retry mini load request with label: {}, timestamp: {}. return ok",
                                label, timestamp);
                        return true;
                    } else {
                        throw new LabelAlreadyUsedException(label);
                    }
                }
            }
        }
        return false;
    }

    public Map<Long, LoadJob> getIdToLoadJob() {
        return idToLoadJob;
    }

    public Map<Long, List<LoadJob>> getDbToLoadJobs() {
        return dbToLoadJobs;
    }

    public List<LoadJob> getLoadJobs(JobState jobState) {
        List<LoadJob> jobs = new ArrayList<LoadJob>();
        Collection<LoadJob> stateJobs = null;
        readLock();
        try {
            switch (jobState) {
                case PENDING:
                    stateJobs = idToPendingLoadJob.values();
                    break;
                case ETL:
                    stateJobs = idToEtlLoadJob.values();
                    break;
                case LOADING:
                    stateJobs = idToLoadingLoadJob.values();
                    break;
                case QUORUM_FINISHED:
                    stateJobs = idToQuorumFinishedLoadJob.values();
                    break;
                default:
                    break;
            }
            if (stateJobs != null) {
                jobs.addAll(stateJobs);
            }
        } finally {
            readUnlock();
        }
        return jobs;
    }

    public long getLoadJobNum(JobState jobState, long dbId) {
        readLock();
        try {
            List<LoadJob> loadJobs = this.dbToLoadJobs.get(dbId);
            if (loadJobs == null) {
                return 0;
            }

            int jobNum = 0;
            for (LoadJob job : loadJobs) {
                if (job.getState() == jobState) {
                    ++jobNum;
                }
            }
            return jobNum;
        } finally {
            readUnlock();
        }
    }

    public long getLoadJobNum(JobState jobState) {
        readLock();
        try {
            List<LoadJob> loadJobs = new ArrayList<>();
            for (Long dbId : dbToLoadJobs.keySet()) {
                if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                        Env.getCurrentEnv().getCatalogMgr().getDbNullable(dbId).getFullName(),
                        PrivPredicate.LOAD)) {
                    continue;
                }
                loadJobs.addAll(this.dbToLoadJobs.get(dbId));
            }

            int jobNum = 0;
            for (LoadJob job : loadJobs) {
                if (job.getState() == jobState) {
                    ++jobNum;
                }
            }
            return jobNum;
        } finally {
            readUnlock();
        }
    }

    public LoadJob getLoadJob(long jobId) {
        readLock();
        try {
            return idToLoadJob.get(jobId);
        } finally {
            readUnlock();
        }
    }

    public LinkedList<List<Comparable>> getAllLoadJobInfos() {
        LinkedList<List<Comparable>> loadJobInfos = new LinkedList<List<Comparable>>();
        readLock();
        try {
            List<LoadJob> loadJobs = new ArrayList<>();
            for (Long dbId : dbToLoadJobs.keySet()) {
                if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                        Env.getCurrentEnv().getCatalogMgr().getDbNullable(dbId).getFullName(),
                        PrivPredicate.LOAD)) {
                    continue;
                }

                loadJobs.addAll(this.dbToLoadJobs.get(dbId));
            }
            if (loadJobs.size() == 0) {
                return loadJobInfos;
            }

            long start = System.currentTimeMillis();
            LOG.debug("begin to get load job info, size: {}", loadJobs.size());

            for (LoadJob loadJob : loadJobs) {
                // filter first
                String dbName = Env.getCurrentEnv().getCatalogMgr().getDbNullable(loadJob.getDbId()).getFullName();
                // check auth
                Set<String> tableNames = loadJob.getTableNames();
                boolean auth = true;
                for (String tblName : tableNames) {
                    if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), dbName,
                            tblName, PrivPredicate.LOAD)) {
                        auth = false;
                        break;
                    }
                }
                if (!auth) {
                    continue;
                }

                loadJobInfos.add(composeJobInfoByLoadJob(loadJob));
            } // end for loadJobs

            LOG.debug("finished to get load job info, cost: {}", (System.currentTimeMillis() - start));
        } finally {
            readUnlock();
        }

        return loadJobInfos;
    }

    private List<Comparable> composeJobInfoByLoadJob(LoadJob loadJob) {
        List<Comparable> jobInfo = new ArrayList<Comparable>();

        // jobId
        jobInfo.add(loadJob.getId());
        // label
        jobInfo.add(loadJob.getLabel());
        // state
        jobInfo.add(loadJob.getState().name());

        // progress
        switch (loadJob.getState()) {
            case PENDING:
                jobInfo.add("ETL:0%; LOAD:0%");
                break;
            case ETL:
                jobInfo.add("ETL:" + loadJob.getProgress() + "%; LOAD:0%");
                break;
            case LOADING:
                jobInfo.add("ETL:100%; LOAD:" + loadJob.getProgress() + "%");
                break;
            case QUORUM_FINISHED:
            case FINISHED:
                jobInfo.add("ETL:100%; LOAD:100%");
                break;
            case CANCELLED:
            default:
                jobInfo.add("ETL:N/A; LOAD:N/A");
                break;
        }

        // type
        jobInfo.add(loadJob.getEtlJobType().name());

        // etl info
        EtlStatus status = loadJob.getEtlJobStatus();
        if (status == null || status.getState() == TEtlState.CANCELLED) {
            jobInfo.add(FeConstants.null_string);
        } else {
            Map<String, String> counters = status.getCounters();
            List<String> info = Lists.newArrayList();
            for (String key : counters.keySet()) {
                // XXX: internal etl job return all counters
                if (key.equalsIgnoreCase("HDFS bytes read")
                        || key.equalsIgnoreCase("Map input records")
                        || key.startsWith("dpp.")
                        || loadJob.getEtlJobType() == EtlJobType.MINI) {
                    info.add(key + "=" + counters.get(key));
                }
            } // end for counters
            if (info.isEmpty()) {
                jobInfo.add(FeConstants.null_string);
            } else {
                jobInfo.add(StringUtils.join(info, "; "));
            }
        }

        // task info
        jobInfo.add("cluster:" + loadJob.getHadoopCluster()
                + "; timeout(s):" + loadJob.getTimeoutSecond()
                + "; max_filter_ratio:" + loadJob.getMaxFilterRatio());

        // error msg
        if (loadJob.getState() == JobState.CANCELLED) {
            FailMsg failMsg = loadJob.getFailMsg();
            jobInfo.add("type:" + failMsg.getCancelType() + "; msg:" + failMsg.getMsg());
        } else {
            jobInfo.add(FeConstants.null_string);
        }

        // create time
        jobInfo.add(TimeUtils.longToTimeString(loadJob.getCreateTimeMs()));
        // etl start time
        jobInfo.add(TimeUtils.longToTimeString(loadJob.getEtlStartTimeMs()));
        // etl end time
        jobInfo.add(TimeUtils.longToTimeString(loadJob.getEtlFinishTimeMs()));
        // load start time
        jobInfo.add(TimeUtils.longToTimeString(loadJob.getLoadStartTimeMs()));
        // load end time
        jobInfo.add(TimeUtils.longToTimeString(loadJob.getLoadFinishTimeMs()));
        // tracking url
        jobInfo.add(status.getTrackingUrl());
        // job detail(not used for hadoop load, just return an empty string)
        jobInfo.add("");
        // transaction id
        jobInfo.add(loadJob.getTransactionId());
        // error tablets(not used for hadoop load, just return an empty string)
        jobInfo.add("");
        // user
        jobInfo.add(loadJob.getUser());
        // comment
        jobInfo.add(loadJob.getComment());

        return jobInfo;
    }

    public LinkedList<List<Comparable>> getLoadJobInfosByDb(long dbId, String dbName, String labelValue,
            boolean accurateMatch, Set<JobState> states) throws AnalysisException {
        LinkedList<List<Comparable>> loadJobInfos = new LinkedList<List<Comparable>>();
        readLock();
        try {
            List<LoadJob> loadJobs = this.dbToLoadJobs.get(dbId);
            if (loadJobs == null) {
                return loadJobInfos;
            }

            long start = System.currentTimeMillis();
            LOG.debug("begin to get load job info, size: {}", loadJobs.size());
            PatternMatcher matcher = null;
            if (labelValue != null && !accurateMatch) {
                matcher = PatternMatcherWrapper.createMysqlPattern(labelValue,
                        CaseSensibility.LABEL.getCaseSensibility());
            }

            for (LoadJob loadJob : loadJobs) {
                // filter first
                String label = loadJob.getLabel();
                JobState state = loadJob.getState();

                if (labelValue != null) {
                    if (accurateMatch) {
                        if (!label.equals(labelValue)) {
                            continue;
                        }
                    } else {
                        if (!matcher.match(label)) {
                            continue;
                        }
                    }
                }

                if (states != null) {
                    if (!states.contains(state)) {
                        continue;
                    }
                }

                // check auth
                Set<String> tableNames = loadJob.getTableNames();
                if (tableNames.isEmpty()) {
                    // forward compatibility
                    if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(), dbName,
                            PrivPredicate.LOAD)) {
                        continue;
                    }
                } else {
                    boolean auth = true;
                    for (String tblName : tableNames) {
                        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), dbName,
                                tblName, PrivPredicate.LOAD)) {
                            auth = false;
                            break;
                        }
                    }
                    if (!auth) {
                        continue;
                    }
                }

                loadJobInfos.add(composeJobInfoByLoadJob(loadJob));
            } // end for loadJobs

            LOG.debug("finished to get load job info, cost: {}", (System.currentTimeMillis() - start));
        } finally {
            readUnlock();
        }

        return loadJobInfos;
    }

    public long getLatestJobIdByLabel(long dbId, String labelValue) {
        long jobId = 0;
        readLock();
        try {
            List<LoadJob> loadJobs = this.dbToLoadJobs.get(dbId);
            if (loadJobs == null) {
                return 0;
            }

            for (LoadJob loadJob : loadJobs) {
                String label = loadJob.getLabel();

                if (labelValue != null) {
                    if (!label.equals(labelValue)) {
                        continue;
                    }
                }

                long currJobId = loadJob.getId();

                if (currJobId > jobId) {
                    jobId = currJobId;
                }
            }
        } finally {
            readUnlock();
        }

        return jobId;
    }

    public List<List<Comparable>> getLoadJobUnfinishedInfo(long jobId) {
        LinkedList<List<Comparable>> infos = new LinkedList<List<Comparable>>();
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();

        LoadJob loadJob = getLoadJob(jobId);
        if (loadJob == null
                || (loadJob.getState() != JobState.LOADING && loadJob.getState() != JobState.QUORUM_FINISHED)) {
            return infos;
        }

        long dbId = loadJob.getDbId();
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            return infos;
        }


        readLock();
        try {
            Map<Long, TabletLoadInfo> tabletMap = loadJob.getIdToTabletLoadInfo();
            for (long tabletId : tabletMap.keySet()) {
                TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                if (tabletMeta == null) {
                    // tablet may be dropped during loading
                    continue;
                }

                long tableId = tabletMeta.getTableId();

                OlapTable table = (OlapTable) db.getTableNullable(tableId);
                if (table == null) {
                    continue;
                }
                table.readLock();
                try {
                    long partitionId = tabletMeta.getPartitionId();
                    Partition partition = table.getPartition(partitionId);
                    if (partition == null) {
                        continue;
                    }

                    long indexId = tabletMeta.getIndexId();
                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index == null) {
                        continue;
                    }

                    Tablet tablet = index.getTablet(tabletId);
                    if (tablet == null) {
                        continue;
                    }

                    PartitionLoadInfo partitionLoadInfo = loadJob.getPartitionLoadInfo(tableId, partitionId);
                    long version = partitionLoadInfo.getVersion();

                    for (Replica replica : tablet.getReplicas()) {
                        if (replica.checkVersionCatchUp(version, false)) {
                            continue;
                        }

                        List<Comparable> info = Lists.newArrayList();
                        info.add(replica.getBackendId());
                        info.add(tabletId);
                        info.add(replica.getId());
                        info.add(replica.getVersion());
                        info.add(partitionId);
                        info.add(version);

                        infos.add(info);
                    }
                } finally {
                    table.readUnlock();
                }
            }
        } finally {
            readUnlock();
        }

        // sort by version, backendId
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(3, 0);
        Collections.sort(infos, comparator);

        return infos;
    }

    public static class JobInfo {
        public String dbName;
        public Set<String> tblNames = Sets.newHashSet();
        public String label;
        public JobState state;
        public String failMsg;
        public String trackingUrl;

        public JobInfo(String dbName, String label) {
            this.dbName = dbName;
            this.label = label;
        }
    }

    // Get job state
    // result saved in info
    public void getJobInfo(JobInfo info) throws DdlException, MetaNotFoundException {
        String fullDbName = info.dbName;
        info.dbName = fullDbName;
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbLabelToLoadJobs.get(db.getId());
            if (labelToLoadJobs == null) {
                throw new DdlException("No jobs belong to database(" + info.dbName + ")");
            }
            List<LoadJob> loadJobs = labelToLoadJobs.get(info.label);
            if (loadJobs == null) {
                throw new DdlException("Unknown job(" + info.label + ")");
            }
            // only the last one should be running
            LoadJob job = loadJobs.get(loadJobs.size() - 1);

            if (!job.getTableNames().isEmpty()) {
                info.tblNames.addAll(job.getTableNames());
            }

            info.state = job.getState();
            if (info.state == JobState.QUORUM_FINISHED) {
                info.state = JobState.FINISHED;
            }

            info.failMsg = job.getFailMsg().getMsg();
            info.trackingUrl = job.getEtlJobStatus().getTrackingUrl();
        } finally {
            readUnlock();
        }
    }

    public void replayClearRollupInfo(ReplicaPersistInfo info, Env env) throws MetaNotFoundException {
        Database db = env.getInternalCatalog().getDbOrMetaException(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(info.getTableId(), TableType.OLAP);
        olapTable.writeLock();
        try {
            Partition partition = olapTable.getPartition(info.getPartitionId());
            MaterializedIndex index = partition.getIndex(info.getIndexId());
            index.clearRollupIndexInfo();
        } finally {
            olapTable.writeUnlock();
        }
    }

    // remove all db jobs from dbToLoadJobs and dbLabelToLoadJobs
    // only remove finished or cancelled job from idToLoadJob
    // LoadChecker will update other state jobs to cancelled or finished,
    //     and they will be removed by removeOldLoadJobs periodically
    public void removeDbLoadJob(long dbId) {
        writeLock();
        try {
            if (dbToLoadJobs.containsKey(dbId)) {
                List<LoadJob> dbLoadJobs = dbToLoadJobs.remove(dbId);
                for (LoadJob job : dbLoadJobs) {
                    JobState state = job.getState();
                    if (state == JobState.CANCELLED || state == JobState.FINISHED) {
                        idToLoadJob.remove(job.getId());
                    }
                }
            }
            if (dbLabelToLoadJobs.containsKey(dbId)) {
                dbLabelToLoadJobs.remove(dbId);
            }
        } finally {
            writeUnlock();
        }
    }

    // Added by ljb. Remove old load jobs from idToLoadJob, dbToLoadJobs and dbLabelToLoadJobs
    // This function is called periodically. every Configure.label_clean_interval_second seconds
    public void removeOldLoadJobs() {
        long currentTimeMs = System.currentTimeMillis();

        writeLock();
        try {
            Iterator<Map.Entry<Long, LoadJob>> iter = idToLoadJob.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, LoadJob> entry = iter.next();
                LoadJob job = entry.getValue();
                if (job.isExpired(currentTimeMs)) {
                    long dbId = job.getDbId();
                    String label = job.getLabel();
                    // Remove job from idToLoadJob
                    iter.remove();

                    // Remove job from dbToLoadJobs
                    List<LoadJob> loadJobs = dbToLoadJobs.get(dbId);
                    if (loadJobs != null) {
                        loadJobs.remove(job);
                        if (loadJobs.size() == 0) {
                            dbToLoadJobs.remove(dbId);
                        }
                    }

                    // Remove job from dbLabelToLoadJobs
                    Map<String, List<LoadJob>> mapLabelToJobs = dbLabelToLoadJobs.get(dbId);
                    if (mapLabelToJobs != null) {
                        loadJobs = mapLabelToJobs.get(label);
                        if (loadJobs != null) {
                            loadJobs.remove(job);
                            if (loadJobs.isEmpty()) {
                                mapLabelToJobs.remove(label);
                                if (mapLabelToJobs.isEmpty()) {
                                    dbLabelToLoadJobs.remove(dbId);
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            writeUnlock();
        }
    }

    // clear dpp output and kill etl job
    public void clearJob(LoadJob job, JobState srcState) {
        JobState state = job.getState();
        if (state != JobState.CANCELLED && state != JobState.FINISHED) {
            LOG.warn("job state error. state: {}", state);
            return;
        }

        EtlJobType etlJobType = job.getEtlJobType();
        switch (etlJobType) {
            case HADOOP:
                DppScheduler dppScheduler = new DppScheduler(job.getHadoopDppConfig());
                // kill etl job
                if (state == JobState.CANCELLED && srcState == JobState.ETL) {
                    try {
                        dppScheduler.killEtlJob(job.getHadoopEtlJobId());
                    } catch (Exception e) {
                        LOG.warn("kill etl job error", e);
                    }
                }

                // delete all dirs related to job label, use "" instead of job.getEtlOutputDir()
                // hdfs://host:port/outputPath/dbId/loadLabel/
                DppConfig dppConfig = job.getHadoopDppConfig();
                String outputPath = DppScheduler.getEtlOutputPath(dppConfig.getFsDefaultName(),
                        dppConfig.getOutputPath(), job.getDbId(), job.getLabel(), "");
                try {
                    dppScheduler.deleteEtlOutputPath(outputPath);
                } catch (Exception e) {
                    LOG.warn("delete etl output path error", e);
                }
                break;
            case MINI:
                break;
            case INSERT:
                break;
            case BROKER:
                break;
            case DELETE:
                break;
            default:
                LOG.warn("unknown etl job type. type: {}, job id: {}", etlJobType.name(), job.getId());
                break;
        }
    }

    public boolean addLoadingPartitions(Set<Long> partitionIds) {
        writeLock();
        try {
            for (long partitionId : partitionIds) {
                if (loadingPartitionIds.contains(partitionId)) {
                    LOG.info("partition {} is loading", partitionId);
                    return false;
                }
            }
            loadingPartitionIds.addAll(partitionIds);
            return true;
        } finally {
            writeUnlock();
        }
    }

    public void removeLoadingPartitions(Set<Long> partitionIds) {
        writeLock();
        try {
            loadingPartitionIds.removeAll(partitionIds);
        } finally {
            writeUnlock();
        }
    }

    public boolean checkPartitionLoadFinished(long partitionId, List<LoadJob> quorumFinishedLoadJobs) {
        readLock();
        try {
            for (JobState state : JobState.values()) {
                if (state == JobState.FINISHED || state == JobState.CANCELLED) {
                    continue;
                }

                // we check PENDING / ETL / LOADING
                List<LoadJob> loadJobs = this.getLoadJobs(state);
                for (LoadJob loadJob : loadJobs) {
                    Preconditions.checkNotNull(loadJob.getIdToTableLoadInfo());
                    for (TableLoadInfo tableLoadInfo : loadJob.getIdToTableLoadInfo().values()) {
                        if (tableLoadInfo.getIdToPartitionLoadInfo().containsKey(partitionId)) {
                            if (state == JobState.QUORUM_FINISHED) {
                                if (quorumFinishedLoadJobs != null) {
                                    quorumFinishedLoadJobs.add(loadJob);
                                } else {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        }
                    }
                }
            }
            return true;
        } finally {
            readUnlock();
        }
    }
}
