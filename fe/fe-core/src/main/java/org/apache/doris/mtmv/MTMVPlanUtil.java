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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.SimpleColumnDefinition;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class MTMVPlanUtil {

    public static ConnectContext createMTMVContext(MTMV mtmv) {
        ConnectContext ctx = createBasicMvContext(null);
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

    public static ConnectContext createBasicMvContext(@Nullable ConnectContext parentContext) {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
        ctx.getState().reset();
        ctx.getState().setInternal(true);
        ctx.setThreadLocalInfo();
        // Debug session variable should be disabled when refreshed
        ctx.getSessionVariable().skipDeletePredicate = false;
        ctx.getSessionVariable().skipDeleteBitmap = false;
        ctx.getSessionVariable().skipDeleteSign = false;
        ctx.getSessionVariable().skipStorageEngineMerge = false;
        ctx.getSessionVariable().showHiddenColumns = false;
        ctx.getSessionVariable().allowModifyMaterializedViewData = true;
        // Rules disabled during materialized view plan generation. These rules can cause significant plan changes,
        // which may affect transparent query rewriting by mv
        List<RuleType> disableRules = Arrays.asList(
                RuleType.COMPRESSED_MATERIALIZE_AGG,
                RuleType.COMPRESSED_MATERIALIZE_SORT,
                RuleType.ELIMINATE_CONST_JOIN_CONDITION,
                RuleType.CONSTANT_PROPAGATION,
                RuleType.ADD_DEFAULT_LIMIT,
                RuleType.ELIMINATE_GROUP_BY
        );
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

    public static MTMVRelation generateMTMVRelation(Set<TableIf> tablesInPlan, ConnectContext ctx) {
        Set<BaseTableInfo> oneLevelTables = Sets.newHashSet();
        Set<BaseTableInfo> allLevelTables = Sets.newHashSet();
        Set<BaseTableInfo> oneLevelViews = Sets.newHashSet();
        for (TableIf table : tablesInPlan) {
            BaseTableInfo baseTableInfo = new BaseTableInfo(table);
            if (table.getType() == TableType.VIEW) {
                // TODO reopen it after we support mv on view
                // oneLevelViews.add(baseTableInfo);
            } else {
                oneLevelTables.add(baseTableInfo);
                allLevelTables.add(baseTableInfo);
                if (table instanceof MTMV) {
                    allLevelTables.addAll(((MTMV) table).getRelation().getBaseTables());
                }
            }
        }
        return new MTMVRelation(allLevelTables, oneLevelTables, oneLevelViews);
    }

    public static Set<TableIf> getBaseTableFromQuery(String querySql, ConnectContext ctx) {
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
                return Sets.newHashSet(ctx.getStatementContext().getTables().values());
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
}
