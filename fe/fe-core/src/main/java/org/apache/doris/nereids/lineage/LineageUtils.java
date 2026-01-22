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

package org.apache.doris.nereids.lineage;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.InlineTable;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Utility methods for lineage event construction and filtering.
 */
public final class LineageUtils {

    private LineageUtils() {
    }

    /**
     * Check whether the parsed statement matches the current command type.
     *
     * @param executor statement executor containing parsed statement
     * @param currentCommand current command class
     * @return true if parsed command matches current command
     */
    public static boolean isSameParsedCommand(StmtExecutor executor, Class<? extends Command> currentCommand) {
        if (executor == null || currentCommand == null) {
            return false;
        }
        StatementBase parsedStmt = executor.getParsedStmt();
        if (!(parsedStmt instanceof LogicalPlanAdapter)) {
            return false;
        }
        Plan parsedPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
        if (!(parsedPlan instanceof Command)) {
            return false;
        }
        return parsedPlan.getClass().equals(currentCommand);
    }

    /**
     * Build a lineage event and compute lineage info if lineage plugins are enabled.
     *
     * @param plan the plan to extract lineage from
     * @param sourceCommand the command type for the event
     * @param ctx connect context holding query metadata
     * @param executor statement executor for query text
     */
    public static LineageEvent buildLineageEvent(Plan plan, Class<? extends Command> sourceCommand,
            ConnectContext ctx, StmtExecutor executor) {
        if (plan == null || ctx == null) {
            return null;
        }
        LineageInfo lineageInfo = LineageInfoExtractor.extractLineageInfo(plan);
        String queryId = ctx.queryId() == null ? "" : DebugUtil.printId(ctx.queryId());
        String queryText = executor == null ? "" : executor.getOriginStmtInString();
        String user = ctx.getQualifiedUser();
        String database = ctx.getDatabase() == null ? "" : ctx.getDatabase();
        String catalog = ctx.getDefaultCatalog() == null ? "" : ctx.getDefaultCatalog();
        long timestampMs = System.currentTimeMillis();
        long durationMs = ctx.getStartTime() > 0 ? (timestampMs - ctx.getStartTime()) : 0;
        LineageContext context = new LineageContext(sourceCommand, queryId, queryText, user, database,
                timestampMs, durationMs);
        context.setCatalog(catalog);
        context.setExternalCatalogProperties(collectExternalCatalogProperties(lineageInfo));
        lineageInfo.setContext(context);
        return new LineageEvent(lineageInfo);
    }

    /**
     * Submit lineage event if lineage plugins are enabled and command matches parsed statement.
     *
     * @param executor statement executor containing parsed statement
     * @param lineagePlan optional lineage plan to use instead of current plan
     * @param currentPlan current logical plan
     * @param currentHandleClass current command class
     */
    public static void submitLineageEventIfNeeded(StmtExecutor executor, Optional<Plan> lineagePlan,
                                            LogicalPlan currentPlan,
                                            Class<? extends Command> currentHandleClass) {
        if (!LineageUtils.isSameParsedCommand(executor, currentHandleClass)) {
            return;
        }
        if (Config.activate_lineage_plugin == null || Config.activate_lineage_plugin.length == 0) {
            return;
        }
        Plan plan = lineagePlan.orElse(currentPlan);
        if (shouldSkipLineage(plan)) {
            return;
        }
        LineageEvent lineageEvent = LineageUtils.buildLineageEvent(plan, currentHandleClass,
                executor.getContext(), executor);
        if (lineageEvent != null) {
            Env.getCurrentEnv().getLineageEventProcessor().submitLineageEvent(lineageEvent);
        }
    }

    public static boolean shouldSkipLineage(Plan plan) {
        if (isValuesOnly(plan)) {
            return true;
        }
        return isInternalSchemaTarget(plan);
    }

    private static boolean isValuesOnly(Plan plan) {
        if (plan.containsType(LogicalCatalogRelation.class)) {
            return false;
        }
        return plan.containsType(InlineTable.class)
                || plan.containsType(LogicalUnion.class)
                || plan.containsType(LogicalOneRowRelation.class);
    }

    private static boolean isInternalSchemaTarget(Plan plan) {
        Optional<LogicalTableSink> sink = plan.collectFirst(node -> node instanceof LogicalTableSink);
        if (!sink.isPresent()) {
            return false;
        }
        TableIf targetTable = sink.get().getTargetTable();
        if (targetTable == null || targetTable.getDatabase() == null
                || targetTable.getDatabase().getCatalog() == null) {
            return false;
        }
        String catalogName = targetTable.getDatabase().getCatalog().getName();
        String dbName = targetTable.getDatabase().getFullName();
        return InternalCatalog.INTERNAL_CATALOG_NAME.equalsIgnoreCase(catalogName)
                && FeConstants.INTERNAL_DB_NAME.equalsIgnoreCase(dbName);
    }

    private static Map<String, Map<String, String>> collectExternalCatalogProperties(LineageInfo lineageInfo) {
        if (lineageInfo == null) {
            return Collections.emptyMap();
        }
        Set<TableIf> tables = new HashSet<>();
        if (lineageInfo.getTableLineageSet() != null) {
            tables.addAll(lineageInfo.getTableLineageSet());
        }
        if (lineageInfo.getTargetTable() != null) {
            tables.add(lineageInfo.getTargetTable());
        }
        if (tables.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Map<String, String>> externalCatalogs = new LinkedHashMap<>();
        for (TableIf table : tables) {
            if (table == null || table.getDatabase() == null || table.getDatabase().getCatalog() == null) {
                continue;
            }
            CatalogIf<?> catalog = table.getDatabase().getCatalog();
            if (catalog.isInternalCatalog()) {
                continue;
            }
            String catalogName = catalog.getName();
            if (externalCatalogs.containsKey(catalogName)) {
                continue;
            }
            Map<String, String> properties = new LinkedHashMap<>();
            if (catalog.getType() != null) {
                properties.put("type", catalog.getType());
            }
            properties.putAll(sanitizeCatalogProperties(catalog));
            externalCatalogs.put(catalogName, properties);
        }
        return externalCatalogs;
    }

    private static Map<String, String> sanitizeCatalogProperties(CatalogIf<?> catalog) {
        Map<String, String> sanitized = new LinkedHashMap<>();
        if (catalog == null || catalog.getProperties() == null) {
            return sanitized;
        }
        for (Map.Entry<String, String> entry : catalog.getProperties().entrySet()) {
            String key = entry.getKey();
            if (key == null) {
                continue;
            }
            if (PrintableMap.HIDDEN_KEY.contains(key) || PrintableMap.SENSITIVE_KEY.contains(key)) {
                continue;
            }
            if (catalog instanceof ExternalCatalog && ExternalCatalog.HIDDEN_PROPERTIES.contains(key)) {
                continue;
            }
            sanitized.put(key, entry.getValue());
        }
        return sanitized;
    }
}
