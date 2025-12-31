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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DistributionDescriptor;
import org.apache.doris.nereids.trees.plans.commands.info.MTMVPartitionDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.SimpleColumnDefinition;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class MTMVPlanUtil {
    private static final Logger LOG = LogManager.getLogger(MTMVPlanUtil.class);
    // The rules should be disabled when generate MTMV cache
    // Because these rules may change the plan structure and cause the plan can not match the mv
    // this is mainly for final CBO phase rewrite, pre RBO phase does not need to consider, because
    // maintain tmp plan alone for rewrite when pre RBO rewrite
    public static final List<RuleType> DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE = ImmutableList.of(
            RuleType.COMPRESSED_MATERIALIZE_AGG,
            RuleType.COMPRESSED_MATERIALIZE_SORT,
            RuleType.ELIMINATE_CONST_JOIN_CONDITION,
            RuleType.CONSTANT_PROPAGATION,
            RuleType.ADD_DEFAULT_LIMIT,
            RuleType.ELIMINATE_JOIN_BY_FK,
            RuleType.ELIMINATE_JOIN_BY_UK,
            RuleType.ELIMINATE_GROUP_BY_KEY_BY_UNIFORM,
            RuleType.ELIMINATE_GROUP_BY,
            RuleType.SALT_JOIN
    );
    // The rules should be disabled when run MTMV task
    public static final List<RuleType> DISABLE_RULES_WHEN_RUN_MTMV_TASK = DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE;

    public static ConnectContext createMTMVContext(MTMV mtmv, List<RuleType> disableRules) {
        ConnectContext ctx = createBasicMvContext(null, disableRules, mtmv.getSessionVariables());
        Optional<String> workloadGroup = mtmv.getWorkloadGroup();
        if (workloadGroup.isPresent()) {
            ctx.getSessionVariable().setWorkloadGroup(workloadGroup.get());
        }
        // Set db&catalog to be used when creating materialized views to avoid SQL statements not writing the full path
        // After https://github.com/apache/doris/pull/36543,
        // After 1, this logic is no longer needed. This is to be compatible with older versions
        setCatalogAndDb(ctx, mtmv);
        return ctx;
    }

    public static ConnectContext createBasicMvContext(@Nullable ConnectContext parentContext,
            List<RuleType> disableRules, Map<String, String> sessionVariables) {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
        ctx.getState().reset();
        ctx.getState().setInternal(true);
        ctx.setThreadLocalInfo();
        ctx.getSessionVariable().setAffectQueryResultInPlanSessionVariables(sessionVariables);
        // Debug session variable should be disabled when refreshed
        ctx.getSessionVariable().skipDeletePredicate = false;
        ctx.getSessionVariable().skipDeleteBitmap = false;
        ctx.getSessionVariable().skipDeleteSign = false;
        ctx.getSessionVariable().skipStorageEngineMerge = false;
        ctx.getSessionVariable().showHiddenColumns = false;
        ctx.getSessionVariable().allowModifyMaterializedViewData = true;
        ctx.getSessionVariable().setDisableNereidsRules(
                disableRules.stream().map(RuleType::name).collect(Collectors.joining(",")));
        ctx.setStartTime();
        if (parentContext != null) {
            ctx.changeDefaultCatalog(parentContext.getDefaultCatalog());
            ctx.setDatabase(parentContext.getDatabase());
        }
        return ctx;
    }


    private static void setCatalogAndDb(ConnectContext ctx, MTMV mtmv) {
        EnvInfo envInfo = mtmv.getEnvInfo();
        if (envInfo == null) {
            return;
        }
        // switch catalog;
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(envInfo.getCtlId());
        // if catalog not exist, it may not have any impact, so there is no error and it will be returned directly
        if (catalog == null) {
            return;
        }
        ctx.changeDefaultCatalog(catalog.getName());
        // use db
        Optional<? extends DatabaseIf<? extends TableIf>> databaseIf = catalog.getDb(envInfo.getDbId());
        // if db not exist, it may not have any impact, so there is no error and it will be returned directly
        if (!databaseIf.isPresent()) {
            return;
        }
        ctx.setDatabase(databaseIf.get().getFullName());
    }

    public static MTMVRelation generateMTMVRelation(Set<TableIf> tablesInPlan, Set<TableIf> oneLevelTablesInPlan) {
        Set<BaseTableInfo> oneLevelTables = Sets.newHashSet();
        Set<BaseTableInfo> allLevelTables = Sets.newHashSet();
        Set<BaseTableInfo> oneLevelViews = Sets.newHashSet();
        Set<BaseTableInfo> allLevelViews = Sets.newHashSet();
        Set<BaseTableInfo> oneLevelTablesAndFromView = Sets.newHashSet();
        for (TableIf table : tablesInPlan) {
            BaseTableInfo baseTableInfo = new BaseTableInfo(table);
            if (table.getType() == TableType.VIEW) {
                allLevelViews.add(baseTableInfo);
            } else {
                oneLevelTablesAndFromView.add(baseTableInfo);
                allLevelTables.add(baseTableInfo);
                if (table instanceof MTMV) {
                    allLevelTables.addAll(((MTMV) table).getRelation().getBaseTables());
                }
            }
        }
        for (TableIf table : oneLevelTablesInPlan) {
            BaseTableInfo baseTableInfo = new BaseTableInfo(table);
            if (table.getType() == TableType.VIEW) {
                oneLevelViews.add(baseTableInfo);
            } else {
                oneLevelTables.add(baseTableInfo);
            }
        }
        return new MTMVRelation(allLevelTables, oneLevelTables, oneLevelTablesAndFromView, allLevelViews,
                oneLevelViews);
    }

    // return allLevelTables:oneLevelTables
    public static Pair<Set<TableIf>, Set<TableIf>> getBaseTableFromQuery(String querySql, ConnectContext ctx) {
        List<StatementBase> statements;
        try {
            statements = new NereidsParser().parseSQL(querySql);
        } catch (Exception e) {
            throw new ParseException("Nereids parse failed. " + e.getMessage());
        }
        StatementBase parsedStmt = statements.get(0);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
        StatementContext original = ctx.getStatementContext();
        try (StatementContext tempCtx = new StatementContext()) {
            ctx.setStatementContext(tempCtx);
            try {
                NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
                planner.planWithLock(logicalPlan, PhysicalProperties.ANY, ExplainLevel.ANALYZED_PLAN);
                return Pair.of(Sets.newHashSet(ctx.getStatementContext().getTables().values()),
                        Sets.newHashSet(ctx.getStatementContext().getOneLevelTables().values()));
            } finally {
                ctx.setStatementContext(original);
            }
        }
    }

    /**
     * Derive the MTMV columns based on the query statement.
     *
     * @param querySql
     * @param ctx
     * @param partitionCol partition column name of MTMV
     * @param distributionColumnNames distribution column names of MTMV
     * @param simpleColumnDefinitions Use custom column names if provided (non-empty);
     *         otherwise, auto-generate the column names.
     * @param properties properties of MTMV, it determines whether row storage needs to be generated based on this.
     * @return ColumnDefinitions of MTMV
     */
    public static List<ColumnDefinition> generateColumnsBySql(String querySql, ConnectContext ctx, String partitionCol,
            Set<String> distributionColumnNames, List<SimpleColumnDefinition> simpleColumnDefinitions,
            Map<String, String> properties) {
        List<StatementBase> statements;
        try {
            statements = new NereidsParser().parseSQL(querySql);
        } catch (Exception e) {
            throw new ParseException("Nereids parse failed. " + e.getMessage());
        }
        StatementBase parsedStmt = statements.get(0);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
        StatementContext original = ctx.getStatementContext();
        try (StatementContext tempCtx = new StatementContext()) {
            ctx.setStatementContext(tempCtx);
            try {
                NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
                Plan plan = planner.planWithLock(logicalPlan, PhysicalProperties.ANY, ExplainLevel.ANALYZED_PLAN);
                return generateColumns(plan, ctx, partitionCol, distributionColumnNames, simpleColumnDefinitions,
                        properties);
            } finally {
                ctx.setStatementContext(original);
            }
        }
    }

    /**
     * Derive the MTMV columns based on the analyzed plan.
     *
     * @param plan should be analyzed plan
     * @param ctx
     * @param partitionCol partition column name of MTMV
     * @param distributionColumnNames distribution column names of MTMV
     * @param simpleColumnDefinitions Use custom column names if provided (non-empty);
     *         otherwise, auto-generate the column names.
     * @param properties properties of MTMV, it determines whether row storage needs to be generated based on this.
     * @return ColumnDefinitions of MTMV
     */
    public static List<ColumnDefinition> generateColumns(Plan plan, ConnectContext ctx, String partitionCol,
            Set<String> distributionColumnNames, List<SimpleColumnDefinition> simpleColumnDefinitions,
            Map<String, String> properties) {
        List<ColumnDefinition> columns = Lists.newArrayList();
        List<Slot> slots = plan.getOutput();
        if (slots.isEmpty()) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException("table should contain at least one column");
        }
        if (!CollectionUtils.isEmpty(simpleColumnDefinitions) && simpleColumnDefinitions.size() != slots.size()) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "simpleColumnDefinitions size is not equal to the query's");
        }
        Set<String> colNames = Sets.newHashSet();
        for (int i = 0; i < slots.size(); i++) {
            String colName = CollectionUtils.isEmpty(simpleColumnDefinitions) ? slots.get(i).getName()
                    : simpleColumnDefinitions.get(i).getName();
            try {
                FeNameFormat.checkColumnName(colName);
            } catch (org.apache.doris.common.AnalysisException e) {
                throw new org.apache.doris.nereids.exceptions.AnalysisException(e.getMessage(), e);
            }
            if (colNames.contains(colName)) {
                throw new org.apache.doris.nereids.exceptions.AnalysisException("repeat cols:" + colName);
            } else {
                colNames.add(colName);
            }
            DataType dataType = getDataType(slots.get(i), i, ctx, partitionCol, distributionColumnNames);
            // If datatype is AggStateType, AggregateType should be generic, or column definition check will fail
            columns.add(new ColumnDefinition(
                    colName,
                    dataType,
                    false,
                    slots.get(i).getDataType() instanceof AggStateType ? AggregateType.GENERIC : null,
                    slots.get(i).nullable(),
                    Optional.empty(),
                    CollectionUtils.isEmpty(simpleColumnDefinitions) ? null
                            : simpleColumnDefinitions.get(i).getComment()));
        }
        // add a hidden column as row store
        if (properties != null) {
            try {
                boolean storeRowColumn =
                        PropertyAnalyzer.analyzeStoreRowColumn(Maps.newHashMap(properties));
                if (storeRowColumn) {
                    columns.add(ColumnDefinition.newRowStoreColumnDefinition(null));
                }
            } catch (Exception e) {
                throw new org.apache.doris.nereids.exceptions.AnalysisException(e.getMessage(), e.getCause());
            }
        }
        return columns;
    }

    /**
     * generate DataType by Slot
     *
     * @param s
     * @param i
     * @param ctx
     * @param partitionCol
     * @param distributionColumnNames
     * @return
     */
    public static DataType getDataType(Slot s, int i, ConnectContext ctx, String partitionCol,
            Set<String> distributionColumnNames) {
        DataType dataType = s.getDataType().conversion();
        // first column can not be TEXT, should transfer to varchar
        if (i == 0 && dataType.isStringType()) {
            dataType = VarcharType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH);
        } else {
            dataType = TypeCoercionUtils.replaceSpecifiedType(dataType,
                    NullType.class, TinyIntType.INSTANCE);
            dataType = TypeCoercionUtils.replaceSpecifiedType(dataType,
                    DecimalV2Type.class, DecimalV2Type.SYSTEM_DEFAULT);
            if (s.isColumnFromTable()) {
                // check if external table
                if ((!((SlotReference) s).getOriginalTable().isPresent()
                        || !((SlotReference) s).getOriginalTable().get().isManagedTable())) {
                    if (s.getName().equals(partitionCol) || distributionColumnNames.contains(s.getName())) {
                        // String type can not be used in partition/distributed column
                        // so we replace it to varchar
                        dataType = TypeCoercionUtils.replaceSpecifiedType(dataType,
                                CharacterType.class, VarcharType.MAX_VARCHAR_TYPE);
                    } else {
                        dataType = TypeCoercionUtils.replaceSpecifiedType(dataType,
                                CharacterType.class, StringType.INSTANCE);
                    }
                }
            } else {
                if (ctx.getSessionVariable().useMaxLengthOfVarcharInCtas) {
                    // The calculation of columns that are not from the original table will become VARCHAR(65533)
                    dataType = TypeCoercionUtils.replaceSpecifiedType(dataType,
                            VarcharType.class, VarcharType.MAX_VARCHAR_TYPE);
                    dataType = TypeCoercionUtils.replaceSpecifiedType(dataType,
                            CharType.class, VarcharType.MAX_VARCHAR_TYPE);
                }
            }
        }
        return dataType;
    }

    public static MTMVAnalyzeQueryInfo analyzeQueryWithSql(MTMV mtmv, ConnectContext ctx) throws UserException {
        String querySql = mtmv.getQuerySql();
        MTMVPartitionInfo mvPartitionInfo = mtmv.getMvPartitionInfo();
        MTMVPartitionDefinition mtmvPartitionDefinition = new MTMVPartitionDefinition();
        mtmvPartitionDefinition.setPartitionCol(mvPartitionInfo.getPartitionCol());
        mtmvPartitionDefinition.setPartitionType(mvPartitionInfo.getPartitionType());
        Expr expr = mvPartitionInfo.getExpr();
        if (expr != null) {
            mtmvPartitionDefinition.setFunctionCallExpression(new NereidsParser().parseExpression(expr.toSql()));
        }
        List<String> keys = mtmv.getBaseSchema().stream()
                .filter(Column::isKey)
                .map(Column::getName)
                .collect(Collectors.toList());
        List<StatementBase> statements;
        try {
            statements = new NereidsParser().parseSQL(querySql);
        } catch (Exception e) {
            throw new ParseException("Nereids parse failed. " + e.getMessage());
        }
        StatementBase parsedStmt = statements.get(0);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
        DistributionInfo defaultDistributionInfo = mtmv.getDefaultDistributionInfo();
        DistributionDescriptor distribution = new DistributionDescriptor(defaultDistributionInfo.getType().equals(
                DistributionInfoType.HASH), defaultDistributionInfo.getAutoBucket(),
                defaultDistributionInfo.getBucketNum(), Lists.newArrayList(mtmv.getDistributionColumnNames()));
        return analyzeQuery(ctx, mtmv.getMvProperties(), querySql, mtmvPartitionDefinition, distribution, null,
                mtmv.getTableProperty().getProperties(), keys, logicalPlan);
    }

    public static MTMVAnalyzeQueryInfo analyzeQuery(ConnectContext ctx, Map<String, String> mvProperties,
            String querySql,
            MTMVPartitionDefinition mvPartitionDefinition, DistributionDescriptor distribution,
            List<SimpleColumnDefinition> simpleColumnDefinitions, Map<String, String> properties, List<String> keys,
            LogicalPlan
                    logicalQuery) throws UserException {
        try (StatementContext statementContext = ctx.getStatementContext()) {
            NereidsPlanner planner = new NereidsPlanner(statementContext);
            // this is for expression column name infer when not use alias
            LogicalSink<Plan> logicalSink = new UnboundResultSink<>(logicalQuery);
            // Should not make table without data to empty relation when analyze the related table,
            // so add disable rules
            Set<String> tempDisableRules = ctx.getSessionVariable().getDisableNereidsRuleNames();
            ctx.getSessionVariable().setDisableNereidsRules(CreateMTMVInfo.MTMV_PLANER_DISABLE_RULES);
            statementContext.invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
            Plan plan;
            try {
                // must disable constant folding by be, because be constant folding may return wrong type
                ctx.getSessionVariable().setVarOnce(SessionVariable.ENABLE_FOLD_CONSTANT_BY_BE, "false");
                plan = planner.planWithLock(logicalSink, PhysicalProperties.ANY, ExplainLevel.ALL_PLAN);
            } finally {
                // after operate, roll back the disable rules
                ctx.getSessionVariable().setDisableNereidsRules(String.join(",", tempDisableRules));
                statementContext.invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
            }
            // can not contain Random function
            analyzeExpressions(planner.getAnalyzedPlan(), mvProperties);
            // can not contain partition or tablets
            boolean containTableQueryOperator = MaterializedViewUtils.containTableQueryOperator(
                    planner.getAnalyzedPlan());
            if (containTableQueryOperator) {
                throw new AnalysisException("can not contain invalid expression");
            }

            Set<TableIf> baseTables = Sets.newHashSet(statementContext.getTables().values());
            Set<TableIf> oneLevelTables = Sets.newHashSet(statementContext.getOneLevelTables().values());
            for (TableIf table : baseTables) {
                if (table.isTemporary()) {
                    throw new AnalysisException("do not support create materialized view on temporary table ("
                            + Util.getTempTableDisplayName(table.getName()) + ")");
                }
                if (table instanceof ExternalTable) {
                    ExternalTable externalTable = (ExternalTable) table;
                    if (externalTable.isView()) {
                        throw new AnalysisException("can not contain external VIEW");
                    }
                }
            }
            MTMVRelation relation = generateMTMVRelation(baseTables, oneLevelTables);
            MTMVPartitionInfo mvPartitionInfo = mvPartitionDefinition.analyzeAndTransferToMTMVPartitionInfo(planner);
            List<ColumnDefinition> columns = MTMVPlanUtil.generateColumns(plan, ctx, mvPartitionInfo.getPartitionCol(),
                    (distribution == null || CollectionUtils.isEmpty(distribution.getCols())) ? Sets.newHashSet()
                            : Sets.newHashSet(distribution.getCols()),
                    simpleColumnDefinitions, properties);
            analyzeKeys(keys, properties, columns);
            // analyze column
            final boolean finalEnableMergeOnWrite = false;
            Set<String> keysSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            keysSet.addAll(keys);
            validateColumns(columns, keysSet, finalEnableMergeOnWrite);
            return new MTMVAnalyzeQueryInfo(columns, mvPartitionInfo, relation);
        }
    }

    /**
     * validate column name
     */
    private static void validateColumns(List<ColumnDefinition> columns, Set<String> keysSet,
            boolean finalEnableMergeOnWrite) throws UserException {
        Set<String> colSets = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ColumnDefinition col : columns) {
            if (!colSets.add(col.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col.getName());
            }
            if (col.getType().isVarBinaryType()) {
                throw new AnalysisException("MTMV do not support varbinary type : " + col.getName());
            }
            col.validate(true, keysSet, Sets.newHashSet(), finalEnableMergeOnWrite, KeysType.DUP_KEYS);
        }
    }

    private static void analyzeKeys(List<String> keys, Map<String, String> properties, List<ColumnDefinition> columns) {
        boolean enableDuplicateWithoutKeysByDefault = false;
        try {
            if (properties != null) {
                enableDuplicateWithoutKeysByDefault =
                        PropertyAnalyzer.analyzeEnableDuplicateWithoutKeysByDefault(properties);
            }
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
        if (keys.isEmpty() && !enableDuplicateWithoutKeysByDefault) {
            keys = Lists.newArrayList();
            int keyLength = 0;
            for (ColumnDefinition column : columns) {
                DataType type = column.getType();
                Type catalogType = column.getType().toCatalogDataType();
                keyLength += catalogType.getIndexSize();
                if (keys.size() >= FeConstants.shortkey_max_column_count
                        || keyLength > FeConstants.shortkey_maxsize_bytes) {
                    if (keys.isEmpty() && type.isStringLikeType()) {
                        keys.add(column.getName());
                        column.setIsKey(true);
                    }
                    break;
                }
                if (column.getAggType() != null) {
                    break;
                }
                if (!catalogType.couldBeShortKey()) {
                    break;
                }
                keys.add(column.getName());
                column.setIsKey(true);
                if (type.isVarcharType()) {
                    break;
                }
            }
        }
    }

    private static void analyzeExpressions(Plan plan, Map<String, String> mvProperties) {
        boolean enableNondeterministicFunction = Boolean.parseBoolean(
                mvProperties.get(PropertyAnalyzer.PROPERTIES_ENABLE_NONDETERMINISTIC_FUNCTION));
        if (enableNondeterministicFunction) {
            return;
        }
        List<Expression> functionCollectResult = MaterializedViewUtils.extractNondeterministicFunction(plan);
        if (!CollectionUtils.isEmpty(functionCollectResult)) {
            throw new AnalysisException(String.format(
                    "can not contain nonDeterministic expression, the expression is %s. "
                            + "Should add 'enable_nondeterministic_function'  = 'true' property "
                            + "when create materialized view if you know the property real meaning entirely",
                    functionCollectResult.stream().map(Expression::toString).collect(Collectors.joining(","))));
        }
    }

    public static void ensureMTMVQueryUsable(MTMV mtmv, ConnectContext ctx) throws JobException {
        MTMVAnalyzeQueryInfo mtmvAnalyzedQueryInfo;
        try {
            mtmvAnalyzedQueryInfo = MTMVPlanUtil.analyzeQueryWithSql(mtmv, ctx);
        } catch (Exception e) {
            throw new JobException(e.getMessage(), e);
        }
        checkColumnIfChange(mtmv, mtmvAnalyzedQueryInfo.getColumnDefinitions());
        checkMTMVPartitionInfo(mtmv, mtmvAnalyzedQueryInfo.getMvPartitionInfo());
    }

    private static void checkMTMVPartitionInfo(MTMV mtmv, MTMVPartitionInfo analyzedMvPartitionInfo)
            throws JobException {
        MTMVPartitionInfo originalMvPartitionInfo = mtmv.getMvPartitionInfo();
        if (!checkMTMVPartitionInfoLike(originalMvPartitionInfo, analyzedMvPartitionInfo)) {
            throw new JobException("async materialized view partition info changed, analyzed: %s, original: %s",
                    analyzedMvPartitionInfo.toInfoString(), originalMvPartitionInfo.toInfoString());
        }
    }

    private static boolean checkMTMVPartitionInfoLike(MTMVPartitionInfo originalMvPartitionInfo,
            MTMVPartitionInfo analyzedMvPartitionInfo) {
        if (!originalMvPartitionInfo.getPartitionType().equals(analyzedMvPartitionInfo.getPartitionType())) {
            return false;
        }
        if (originalMvPartitionInfo.getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return true;
        }
        // because old version only support one pct table, so can not use equal
        if (!analyzedMvPartitionInfo.getPctInfos().containsAll(originalMvPartitionInfo.getPctInfos())) {
            return false;
        }
        if (originalMvPartitionInfo.getPartitionType() == MTMVPartitionType.EXPR) {
            try {
                MTMVPartitionExprService originalExprService = MTMVPartitionExprFactory.getExprService(
                        originalMvPartitionInfo.getExpr());
                MTMVPartitionExprService analyzedExprService = MTMVPartitionExprFactory.getExprService(
                        analyzedMvPartitionInfo.getExpr());
                return originalExprService.equals(analyzedExprService);
            } catch (org.apache.doris.common.AnalysisException e) {
                LOG.warn(e);
                return false;
            }
        }
        return true;
    }

    private static void checkColumnIfChange(MTMV mtmv, List<ColumnDefinition> analyzedColumnDefinitions)
            throws JobException {
        List<Column> analyzedColumns = analyzedColumnDefinitions.stream()
                .map(ColumnDefinition::translateToCatalogStyle)
                .collect(Collectors.toList());
        List<Column> originalColumns = mtmv.getBaseSchema(true);
        if (analyzedColumns.size() != originalColumns.size()) {
            throw new JobException(String.format(
                    "column length not equals, please check whether columns of base table have changed, "
                            + "original length is: %s, current length is: %s",
                    originalColumns.size(), analyzedColumns.size()));
        }
        for (int i = 0; i < originalColumns.size(); i++) {
            if (!isTypeLike(originalColumns.get(i).getType(), analyzedColumns.get(i).getType())) {
                throw new JobException(String.format(
                        "column type not same, please check whether columns of base table have changed, "
                                + "column name is: %s, original type is: %s, current type is: %s",
                        originalColumns.get(i).getName(), originalColumns.get(i).getType().toSql(),
                        analyzedColumns.get(i).getType().toSql()));
            }
        }
    }

    private static boolean isTypeLike(Type type, Type typeOther) {
        if (type.isStringType()) {
            return typeOther.isStringType();
        } else {
            return type.equals(typeOther);
        }
    }
}
