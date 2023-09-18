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

import org.apache.doris.analysis.CreateMultiTableMaterializedViewStmt;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.MVRefreshInfo.BuildMode;
import org.apache.doris.analysis.MVRefreshInfo.RefreshMethod;
import org.apache.doris.analysis.MVRefreshTriggerInfo;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * MTMV info in creating MTMV.
 */
public class CreateMTMVInfo {
    private final boolean ifNotExists;
    private String dbName;
    private final String tableName;
    private List<String> keys;
    private final String comment;
    private final DistributionDescriptor distribution;
    private Map<String, String> properties;

    private final LogicalPlan logicalQuery;
    private final String querySql;
    private final String originSql;

    private final BuildMode buildMode;
    private final RefreshMethod refreshMethod;
    private final MVRefreshTriggerInfo refreshTriggerInfo;
    private final List<TableIf> baseTables = Lists.newArrayList();
    private final List<ColumnDefinition> columns = Lists.newArrayList();

    /**
     * constructor for create MTMV
     */
    public CreateMTMVInfo(boolean ifNotExists, String dbName, String tableName,
            List<String> keys, String comment,
            DistributionDescriptor distribution, Map<String, String> properties,
            LogicalPlan logicalQuery, String querySql, String originSql,
            BuildMode buildMode, RefreshMethod refreshMethod,
            MVRefreshTriggerInfo refreshTriggerInfo) {
        this.ifNotExists = Objects.requireNonNull(ifNotExists, "require ifNotExists object");
        this.dbName = dbName;
        this.tableName = Objects.requireNonNull(tableName, "require tableName object");
        this.keys = Utils.copyRequiredList(keys);
        this.comment = comment;
        this.distribution = Objects.requireNonNull(distribution, "require distribution object");
        this.properties = properties;
        this.logicalQuery = Objects.requireNonNull(logicalQuery, "require logicalQuery object");
        this.querySql = Objects.requireNonNull(querySql, "require querySql object");
        this.originSql = Objects.requireNonNull(originSql, "require originSql object");
        this.buildMode = Objects.requireNonNull(buildMode, "require buildMode object");
        this.refreshMethod = Objects.requireNonNull(refreshMethod, "require refreshMethod object");
        this.refreshTriggerInfo = Objects.requireNonNull(refreshTriggerInfo, "require refreshTriggerInfo object");
    }

    /**
     * analyze create table info
     */
    public void analyze(ConnectContext ctx) {
        analyzeQuery(ctx);
        // pre-block in some cases.
        if (columns.isEmpty()) {
            throw new AnalysisException("table should contain at least one column");
        }
        // analyze column
        final boolean finalEnableMergeOnWrite = false;
        Set<String> keysSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        keysSet.addAll(keys);
        columns.forEach(c -> c.validate(keysSet, finalEnableMergeOnWrite, KeysType.DUP_KEYS));

        if (distribution == null) {
            throw new AnalysisException("Create MTMV should contain distribution desc");
        }

        if (properties == null) {
            properties = Maps.newHashMap();
        }

        // analyze table name
        if (dbName == null) {
            dbName = ClusterNamespace.getFullName(ctx.getClusterName(), ctx.getDatabase());
        } else {
            dbName = ClusterNamespace.getFullName(ctx.getClusterName(), dbName);
        }
        // analyze distribute
        Map<String, ColumnDefinition> columnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        columns.forEach(c -> columnMap.put(c.getName(), c));
        distribution.updateCols(columns.get(0).getName());
        distribution.validate(columnMap, KeysType.DUP_KEYS);
        // analyze
        refreshTriggerInfo.validate();
    }

    public void analyzeQuery(ConnectContext ctx) {
        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        getColumns(planner);
        getBaseTables(planner);
    }

    private void getBaseTables(NereidsPlanner planner) {
        for (ScanNode scanNode : planner.getScanNodes()) {
            baseTables.add(scanNode.getTupleDesc().getTable());
        }
    }

    private void getColumns(NereidsPlanner planner) {
        Set<String> colNames = Sets.newHashSet();
        List<Slot> slots = planner.getOptimizedPlan().getOutput();
        for (Slot slot : slots) {
            try {
                FeNameFormat.checkColumnName(slot.getName());
            } catch (org.apache.doris.common.AnalysisException e) {
                throw new AnalysisException(e.getMessage() + ", please use alis.");
            }
            if (colNames.contains(slot.getName())) {
                throw new AnalysisException("repeat cols:" + slot.getName());
            } else {
                colNames.add(slot.getName());
            }
            columns.add(new ColumnDefinition(slot.getName(), slot.getDataType(), true));
        }
    }

    /**
     * translate to catalog CreateMultiTableMaterializedViewStmt
     */
    public CreateMultiTableMaterializedViewStmt translateToLegacyStmt() {
        TableName tableName = new TableName(Env.getCurrentEnv().getCurrentCatalog().getName(), dbName, this.tableName);
        KeysDesc keysDesc = new KeysDesc(KeysType.DUP_KEYS, keys);
        List<Column> catalogColumns = columns.stream()
                .map(ColumnDefinition::translateToCatalogStyle)
                .collect(Collectors.toList());
        return new CreateMultiTableMaterializedViewStmt(ifNotExists, tableName, catalogColumns, buildMode,
                refreshMethod,
                keysDesc,
                distribution.translateToCatalogStyle(), properties, querySql, refreshTriggerInfo, baseTables,
                originSql, comment);
    }
}
