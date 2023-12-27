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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.CreateMTMVStmt;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.ListPartitionDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.View;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mtmv.EnvInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils.RelatedTableInfo;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.visitor.NondeterministicFunctionCollector;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector.TableCollectorContext;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * MTMV info in creating MTMV.
 */
public class CreateMTMVInfo {
    public static final Logger LOG = LogManager.getLogger(CreateMTMVInfo.class);

    private final boolean ifNotExists;
    private final TableNameInfo mvName;
    private List<String> keys;
    private final String comment;
    private final DistributionDescriptor distribution;
    private Map<String, String> properties;
    private Map<String, String> mvProperties = Maps.newHashMap();

    private final LogicalPlan logicalQuery;
    private final String querySql;
    private final MTMVRefreshInfo refreshInfo;
    private final List<ColumnDefinition> columns = Lists.newArrayList();
    private final List<SimpleColumnDefinition> simpleColumnDefinitions;
    private final EnvInfo envInfo;
    private final MTMVPartitionInfo mvPartitionInfo;
    private PartitionDesc partitionDesc;
    private MTMVRelation relation;

    /**
     * constructor for create MTMV
     */
    public CreateMTMVInfo(boolean ifNotExists, TableNameInfo mvName,
            List<String> keys, String comment,
            DistributionDescriptor distribution, Map<String, String> properties,
            LogicalPlan logicalQuery, String querySql,
            MTMVRefreshInfo refreshInfo,
            List<SimpleColumnDefinition> simpleColumnDefinitions,
            MTMVPartitionInfo mvPartitionInfo) {
        this.ifNotExists = Objects.requireNonNull(ifNotExists, "require ifNotExists object");
        this.mvName = Objects.requireNonNull(mvName, "require mvName object");
        this.keys = Utils.copyRequiredList(keys);
        this.comment = comment;
        this.distribution = Objects.requireNonNull(distribution, "require distribution object");
        this.properties = Objects.requireNonNull(properties, "require properties object");
        this.logicalQuery = Objects.requireNonNull(logicalQuery, "require logicalQuery object");
        this.querySql = Objects.requireNonNull(querySql, "require querySql object");
        this.refreshInfo = Objects.requireNonNull(refreshInfo, "require refreshInfo object");
        this.simpleColumnDefinitions = Objects
                .requireNonNull(simpleColumnDefinitions, "require simpleColumnDefinitions object");
        this.envInfo = new EnvInfo(ConnectContext.get().getCurrentCatalog().getId(),
                ConnectContext.get().getCurrentDbId());
        this.mvPartitionInfo = Objects
                .requireNonNull(mvPartitionInfo, "require mtmvPartitionInfo object");
    }

    /**
     * analyze create table info
     */
    public void analyze(ConnectContext ctx) {
        // analyze table name
        mvName.analyze(ctx);
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), mvName.getDb(),
                mvName.getTbl(), PrivPredicate.CREATE)) {
            String message = ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("CREATE",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    mvName.getDb() + ": " + mvName.getTbl());
            throw new AnalysisException(message);
        }
        analyzeProperties();
        analyzeQuery(ctx);
        // analyze column
        final boolean finalEnableMergeOnWrite = false;
        Set<String> keysSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        keysSet.addAll(keys);
        columns.forEach(c -> c.validate(true, keysSet, finalEnableMergeOnWrite, KeysType.DUP_KEYS));

        if (distribution == null) {
            throw new AnalysisException("Create MTMV should contain distribution desc");
        }

        if (properties == null) {
            properties = Maps.newHashMap();
        }

        // analyze distribute
        Map<String, ColumnDefinition> columnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        columns.forEach(c -> columnMap.put(c.getName(), c));
        distribution.updateCols(columns.get(0).getName());
        distribution.validate(columnMap, KeysType.DUP_KEYS);
        refreshInfo.validate();

        analyzeProperties();
    }

    private void analyzeProperties() {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD)) {
            String gracePeriod = properties.get(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD);
            try {
                Long.parseLong(gracePeriod);
            } catch (NumberFormatException e) {
                throw new AnalysisException(
                        "valid grace_period: " + properties.get(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD));
            }
            mvProperties.put(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD, gracePeriod);
            properties.remove(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REFRESH_PARTITION_NUM)) {
            String refreshPartitionNum = properties.get(PropertyAnalyzer.PROPERTIES_REFRESH_PARTITION_NUM);
            try {
                Integer.parseInt(refreshPartitionNum);
            } catch (NumberFormatException e) {
                throw new AnalysisException(
                        "valid refresh_partition_num: " + properties
                                .get(PropertyAnalyzer.PROPERTIES_REFRESH_PARTITION_NUM));
            }
            mvProperties.put(PropertyAnalyzer.PROPERTIES_REFRESH_PARTITION_NUM, refreshPartitionNum);
            properties.remove(PropertyAnalyzer.PROPERTIES_REFRESH_PARTITION_NUM);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)) {
            String excludedTriggerTables = properties.get(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES);
            mvProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, excludedTriggerTables);
            properties.remove(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES);
        }
    }

    /**
     * analyzeQuery
     */
    public void analyzeQuery(ConnectContext ctx) {
        // create table as select
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        // this is for expression column name infer when not use alias
        LogicalSink<Plan> logicalSink = new UnboundResultSink<>(logicalQuery);
        Plan plan = planner.plan(logicalSink, PhysicalProperties.ANY, ExplainLevel.ALL_PLAN);
        if (plan.anyMatch(node -> node instanceof OneRowRelation)) {
            throw new AnalysisException("at least contain one table");
        }
        // can not contain VIEW or MTMV
        analyzeBaseTables(planner.getAnalyzedPlan());
        // can not contain Random function
        analyzeExpressions(planner.getAnalyzedPlan());
        // can not contain partition or tablets
        boolean containTableQueryOperator = MaterializedViewUtils.containTableQueryOperator(planner.getAnalyzedPlan());
        if (containTableQueryOperator) {
            throw new AnalysisException("can not contain invalid expression");
        }
        getRelation(planner);
        getColumns(plan);
        analyzePartition(planner);
    }

    private void getRelation(NereidsPlanner planner) {
        Plan plan = planner.plan(logicalQuery, PhysicalProperties.ANY, ExplainLevel.NONE);
        this.relation = MTMVPlanUtil.generateMTMVRelation(plan);
    }

    private void analyzePartition(NereidsPlanner planner) {
        if (mvPartitionInfo.getPartitionType() == MTMVPartitionType.FOLLOW_BASE_TABLE) {
            Plan mvRewrittenPlan =
                    planner.plan(logicalQuery, PhysicalProperties.ANY, ExplainLevel.REWRITTEN_PLAN);
            Optional<RelatedTableInfo> relatedTableInfo = MaterializedViewUtils
                    .getRelatedTableInfo(mvPartitionInfo.getPartitionCol(), mvRewrittenPlan);
            if (!relatedTableInfo.isPresent() || !relatedTableInfo.get().isPctPossible()) {
                throw new AnalysisException("Unable to find a suitable base table for partitioning");
            }
            TableIf followTable = null;
            try {
                followTable = MTMVUtil.getTable(relatedTableInfo.get().getTableInfo());
            } catch (org.apache.doris.common.AnalysisException e) {
                throw new AnalysisException(e.getMessage(), e);
            }
            if (!(followTable instanceof OlapTable)) {
                throw new AnalysisException("base table for partitioning only can be OlapTable.");
            }
            Set<String> partitionColumnNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            try {
                partitionColumnNames.addAll(((OlapTable) followTable).getPartitionColumnNames());
            } catch (DdlException e) {
                throw new AnalysisException(e.getMessage(), e);
            }

            if (!partitionColumnNames.contains(relatedTableInfo.get().getColumn())) {
                throw new AnalysisException("error related column: " + relatedTableInfo.get().getColumn());
            }
            if (partitionColumnNames.size() != 1) {
                throw new AnalysisException("base table for partitioning only support single column.");
            }
            mvPartitionInfo.setRelatedTable(relatedTableInfo.get().getTableInfo());
            mvPartitionInfo.setRelatedCol(relatedTableInfo.get().getColumn());
            partitionDesc = generatePartitionDesc((OlapTable) followTable);
        }
    }

    private PartitionDesc generatePartitionDesc(OlapTable relatedTable) {
        PartitionType type = relatedTable.getPartitionInfo().getType();
        try {
            if (type == PartitionType.RANGE) {
                return new RangePartitionDesc(Lists.newArrayList(mvPartitionInfo.getPartitionCol()),
                        Lists.newArrayList());
            } else if (type == PartitionType.LIST) {
                return new ListPartitionDesc(Lists.newArrayList(mvPartitionInfo.getPartitionCol()),
                        Lists.newArrayList());
            } else {
                return null;
            }
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException("can not generate partitionDesc", e);
        }
    }

    private void analyzeBaseTables(Plan plan) {
        TableCollectorContext collectorContext =
                new TableCollector.TableCollectorContext(Sets.newHashSet(TableType.MATERIALIZED_VIEW));
        plan.accept(TableCollector.INSTANCE, collectorContext);
        List<TableIf> collectedTables = collectorContext.getCollectedTables();
        if (!CollectionUtils.isEmpty(collectedTables)) {
            throw new AnalysisException("can not contain MATERIALIZED_VIEW");
        }

        List<Object> subQuerys = plan.collectToList(node -> node instanceof LogicalSubQueryAlias);
        for (Object subquery : subQuerys) {
            List<String> qualifier = ((LogicalSubQueryAlias) subquery).getQualifier();
            if (!CollectionUtils.isEmpty(qualifier) && qualifier.size() == 3) {
                try {
                    TableIf table = Env.getCurrentEnv().getCatalogMgr()
                            .getCatalogOrAnalysisException(qualifier.get(0))
                            .getDbOrAnalysisException(qualifier.get(1)).getTableOrAnalysisException(qualifier.get(2));
                    if (table instanceof View) {
                        throw new AnalysisException("can not contain VIEW");
                    }
                } catch (org.apache.doris.common.AnalysisException e) {
                    LOG.warn("can not get table, ", e);
                }
            }
        }
    }

    private void analyzeExpressions(Plan plan) {
        List<TreeNode<Expression>> functionCollectResult = new ArrayList<>();
        plan.accept(NondeterministicFunctionCollector.INSTANCE, functionCollectResult);
        if (!CollectionUtils.isEmpty(functionCollectResult)) {
            throw new AnalysisException("can not contain invalid expression");
        }
    }

    private void getColumns(Plan plan) {
        List<Slot> slots = plan.getOutput();
        if (slots.isEmpty()) {
            throw new AnalysisException("table should contain at least one column");
        }
        if (!CollectionUtils.isEmpty(simpleColumnDefinitions) && simpleColumnDefinitions.size() != slots.size()) {
            throw new AnalysisException("simpleColumnDefinitions size is not equal to the query's");
        }
        Set<String> colNames = Sets.newHashSet();
        for (int i = 0; i < slots.size(); i++) {
            String colName = CollectionUtils.isEmpty(simpleColumnDefinitions) ? slots.get(i).getName()
                    : simpleColumnDefinitions.get(i).getName();
            try {
                FeNameFormat.checkColumnName(colName);
            } catch (org.apache.doris.common.AnalysisException e) {
                throw new AnalysisException(e.getMessage());
            }
            if (colNames.contains(colName)) {
                throw new AnalysisException("repeat cols:" + colName);
            } else {
                colNames.add(colName);
            }
            columns.add(new ColumnDefinition(
                    colName, slots.get(i).getDataType(), slots.get(i).nullable(),
                    CollectionUtils.isEmpty(simpleColumnDefinitions) ? null
                            : simpleColumnDefinitions.get(i).getComment()));
        }
    }

    /**
     * translate to catalog CreateMultiTableMaterializedViewStmt
     */
    public CreateMTMVStmt translateToLegacyStmt() {
        TableName tableName = mvName.transferToTableName();
        KeysDesc keysDesc = new KeysDesc(KeysType.DUP_KEYS, keys);
        List<Column> catalogColumns = columns.stream()
                .map(ColumnDefinition::translateToCatalogStyle)
                .collect(Collectors.toList());
        return new CreateMTMVStmt(ifNotExists, tableName, catalogColumns, refreshInfo, keysDesc,
                distribution.translateToCatalogStyle(), properties, mvProperties, querySql, comment, envInfo,
                partitionDesc, mvPartitionInfo, relation);
    }

}
