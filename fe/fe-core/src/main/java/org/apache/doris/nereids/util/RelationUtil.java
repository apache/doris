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

package org.apache.doris.nereids.util;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.systable.SysTable;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.parser.Origin;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * relation util
 */
public class RelationUtil {
    private static final String SYNC_MV_PLANER_DISABLE_RULES = "OLAP_SCAN_PARTITION_PRUNE, PRUNE_EMPTY_PARTITION, "
            + "ELIMINATE_GROUP_BY_KEY_BY_UNIFORM, HAVING_TO_FILTER, ELIMINATE_GROUP_BY, SIMPLIFY_AGG_GROUP_BY, "
            + "MERGE_PERCENTILE_TO_ARRAY, VARIANT_SUB_PATH_PRUNING, INFER_PREDICATES, INFER_AGG_NOT_NULL, "
            + "INFER_SET_OPERATOR_DISTINCT, INFER_FILTER_NOT_NULL, INFER_JOIN_NOT_NULL, PUSH_DOWN_MAX_MIN_FILTER, "
            + "ELIMINATE_SORT, ELIMINATE_AGGREGATE, ELIMINATE_LIMIT, ELIMINATE_SEMI_JOIN, ELIMINATE_NOT_NULL, "
            + "ELIMINATE_JOIN_BY_UK, ELIMINATE_JOIN_BY_FK, ELIMINATE_GROUP_BY_KEY, ELIMINATE_GROUP_BY_KEY_BY_UNIFORM, "
            + "ELIMINATE_FILTER_GROUP_BY_KEY";

    /**
     * get table qualifier
     */
    public static List<String> getQualifierName(ConnectContext context, List<String> nameParts) {
        switch (nameParts.size()) {
            case 1: { // table
                // Use current database name from catalog.
                String tableName = nameParts.get(0);
                CatalogIf catalogIf = context.getCurrentCatalog();
                if (catalogIf == null) {
                    throw new IllegalStateException(
                            "Current catalog is not set. default catalog is: " + context.getDefaultCatalog());
                }
                String catalogName = catalogIf.getName();
                String dbName = context.getDatabase();
                if (Strings.isNullOrEmpty(dbName)) {
                    throw new IllegalStateException("Current database is not set.");
                }
                return ImmutableList.of(catalogName, dbName, tableName);
            }
            case 2: { // db.table
                // Use database name from table name parts.
                CatalogIf catalogIf = context.getCurrentCatalog();
                if (catalogIf == null) {
                    throw new IllegalStateException("Current catalog is not set.");
                }
                String catalogName = catalogIf.getName();
                // if the relation is view, nameParts.get(0) is dbName.
                String dbName = nameParts.get(0);
                String tableName = nameParts.get(1);
                return ImmutableList.of(catalogName, dbName, tableName);
            }
            case 3: { // catalog.db.table
                // Use catalog and database name from name parts.
                String catalogName = nameParts.get(0);
                String dbName = nameParts.get(1);
                String tableName = nameParts.get(2);
                return ImmutableList.of(catalogName, dbName, tableName);
            }
            default:
                throw new IllegalStateException("Table name [" + java.lang.String
                        .join(".", nameParts) + "] is invalid.");
        }
    }

    /**
     * get table
     */
    public static TableIf getTable(List<String> qualifierName, Env env, Optional<UnboundRelation> unboundRelation) {
        return getDbAndTable(qualifierName, env, unboundRelation).second;
    }

    /**
     * get database and table
     */
    public static Pair<DatabaseIf<?>, TableIf> getDbAndTable(
            List<String> qualifierName, Env env, Optional<UnboundRelation> unboundRelation) {
        String catalogName = qualifierName.get(0);
        String dbName = qualifierName.get(1);
        String tableName = qualifierName.get(2);
        CatalogIf<?> catalog = env.getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            throw new AnalysisException(java.lang.String.format("Catalog %s does not exist.", catalogName));
        }
        Optional<Origin> origin = unboundRelation.flatMap(AbstractTreeNode::getOrigin);
        try {
            DatabaseIf<TableIf> db = catalog.getDbOrException(dbName, s -> new AnalysisException(
                    "Database [" + dbName + "] does not exist."
                            + (origin.map(loc -> "(" + loc + ")").orElse("")))
            );
            Pair<String, String> tableNameWithSysTableName
                    = SysTable.getTableNameWithSysTableName(tableName);
            TableIf tbl = db.getTableOrException(tableNameWithSysTableName.first,
                    s -> new AnalysisException(
                            "Table [" + tableName + "] does not exist in database [" + dbName + "]."
                                    + (origin.map(loc -> "(" + loc + ")").orElse("")))
            );
            Optional<TableValuedFunction> sysTable = tbl.getSysTableFunction(catalogName, dbName, tableName);
            if (!Strings.isNullOrEmpty(tableNameWithSysTableName.second) && !sysTable.isPresent()) {
                throw new AnalysisException("Unknown sys table '" + tableName + "'");
            }
            return Pair.of(db, tbl);
        } catch (Throwable e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
    }

    /**
     * get mv used column names of base table
     */
    public static Set<String> getMvUsedColumnNames(MaterializedIndexMeta meta) {
        Set<String> columns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        if (meta.getDefineStmt() != null) {
            // get the original create mv sql
            String createMvSql = meta.getDefineStmt().originStmt;
            Optional<String> querySql = new NereidsParser().parseForSyncMv(createMvSql);
            if (querySql.isPresent()) {
                LogicalPlan unboundMvPlan = new NereidsParser().parseSingle(querySql.get());
                ConnectContext connectContext = ConnectContext.get();
                StatementContext statementContext = new StatementContext(connectContext,
                        new OriginStatement(querySql.get(), 0));
                NereidsPlanner planner = new NereidsPlanner(statementContext);
                if (statementContext.getConnectContext().getStatementContext() == null) {
                    statementContext.getConnectContext().setStatementContext(statementContext);
                }
                Set<String> tempDisableRules = connectContext.getSessionVariable().getDisableNereidsRuleNames();
                connectContext.getSessionVariable().setDisableNereidsRules(SYNC_MV_PLANER_DISABLE_RULES);
                connectContext.getStatementContext().invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
                LogicalPlan logicalPlan;
                try {
                    // disable constant fold
                    connectContext.getSessionVariable().setVarOnce(SessionVariable.DEBUG_SKIP_FOLD_CONSTANT, "true");
                    planner.planWithLock(unboundMvPlan, PhysicalProperties.ANY,
                            ExplainCommand.ExplainLevel.REWRITTEN_PLAN);
                    logicalPlan = (LogicalPlan) planner.getCascadesContext().getRewritePlan();
                } finally {
                    // after operate, roll back the disable rules
                    connectContext.getSessionVariable().setDisableNereidsRules(String.join(",", tempDisableRules));
                    connectContext.getStatementContext().invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
                }
                Map<Boolean, List<Object>> partitionedPlan = logicalPlan
                        .collect(plan -> true)
                        .stream()
                        .collect(Collectors.partitioningBy(
                                plan -> plan instanceof LogicalProject
                                        && ((LogicalProject<?>) plan).child() instanceof LogicalCatalogRelation
                        ));
                List<Object> projects = partitionedPlan.get(true);
                if (projects.isEmpty()) {
                    // for scan
                    partitionedPlan.get(false)
                            .stream()
                            .filter(plan -> plan instanceof LogicalCatalogRelation)
                            .map(plan -> (LogicalCatalogRelation) plan)
                            .forEach(plan -> columns.addAll(logicalPlan.getOutput().stream().map(Slot::getName).collect(
                                    Collectors.toList())));
                } else {
                    // for projects
                    projects
                            .stream()
                            .map(plan -> (LogicalProject<?>) plan)
                            .forEach(plan -> columns.addAll(plan.getInputSlots().stream().map(Slot::getName).collect(
                                    Collectors.toList())));
                }
            } else {
                throw new AnalysisException(String.format("can't parse %s ", createMvSql));
            }
        } else {
            // no define stmt, means mv created by add rollup. assume schema change can handle such case
            // columns.addAll(meta.getSchema(false).stream().map(Column::getName).collect(Collectors.toList()));
        }
        return columns;
    }
}

