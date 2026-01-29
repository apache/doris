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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    public static final Logger LOG = LogManager.getLogger(LineageUtils.class);
    private static final String EMPTY_STRING = "";
    private static final String CATALOG_TYPE_KEY = "type";
    private static final int NO_PLUGINS = 0;
    private static final long UNKNOWN_START_TIME_MS = 0L;

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
     * Build lineage info and compute lineage context if lineage plugins are enabled.
     *
     * @param plan the plan to extract lineage from
     * @param sourceCommand the command type for the event
     * @param ctx connect context holding query metadata
     * @param executor statement executor for query text
     */
    public static LineageInfo buildLineageInfo(Plan plan, Class<? extends Command> sourceCommand,
                                               ConnectContext ctx, StmtExecutor executor) {
        if (plan == null || ctx == null) {
            return null;
        }
        long startNs = 0L;
        if (LOG.isDebugEnabled()) {
            startNs = System.nanoTime();
        }
        long eventTimeMs = System.currentTimeMillis();
        long durationMs = LineageContext.UNKNOWN_DURATION_MS;
        long startTimeMs = ctx.getStartTime();
        if (startTimeMs > UNKNOWN_START_TIME_MS) {
            long elapsed = eventTimeMs - startTimeMs;
            durationMs = elapsed >= 0 ? elapsed : LineageContext.UNKNOWN_DURATION_MS;
        }
        LineageInfo lineageInfo = LineageInfoExtractor.extractLineageInfo(plan);
        LineageContext context = buildLineageContext(sourceCommand, ctx, executor, eventTimeMs, durationMs);
        String catalog = safeString(ctx.getDefaultCatalog());
        context.setCatalog(catalog);
        context.setExternalCatalogProperties(collectExternalCatalogProperties(lineageInfo));
        lineageInfo.setContext(context);
        if (LOG.isDebugEnabled()) {
            Map<?, ?> directMap = lineageInfo.getDirectLineageMap();
            Object indirectMap = lineageInfo.getInDirectLineageMapByDataset();
            Object tableLineage = lineageInfo.getTableLineageSet();
            Object targetColumns = lineageInfo.getTargetColumns();
            String targetTable = lineageInfo.getTargetTable() == null
                    ? "null"
                    : lineageInfo.getTargetTable().getName();
            int externalCatalogs = context.getExternalCatalogProperties() == null
                    ? 0
                    : context.getExternalCatalogProperties().size();
            long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;
            LOG.debug("Lineage info built: plan={}, targetTable={}, targetColumns={}, directMap={},"
                            + " indirectMap={}, tableLineage={}, externalCatalogs={}, elapsedMs={}",
                    plan.getClass().getSimpleName(), targetTable, targetColumns, directMap, indirectMap,
                    tableLineage, externalCatalogs, elapsedMs);
        }
        return lineageInfo;
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
        if (!isLineagePluginConfigured()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Skip lineage: no plugin configured");
            }
            return;
        }
        if (!LineageUtils.isSameParsedCommand(executor, currentHandleClass)) {
            if (LOG.isDebugEnabled()) {
                String parsedCommand = executor == null || executor.getParsedStmt() == null
                        ? "null"
                        : executor.getParsedStmt().getClass().getSimpleName();
                LOG.debug("Skip lineage: parsed command mismatch, parsed={}, current={}",
                        parsedCommand, currentHandleClass == null ? "null" : currentHandleClass.getSimpleName());
            }
            return;
        }
        Plan plan = lineagePlan.orElse(currentPlan);
        if (plan == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Skip lineage: plan is null");
            }
            return;
        }
        boolean valuesOnly = isValuesOnly(plan);
        boolean internalTarget = !valuesOnly && isInternalSchemaTarget(plan);
        if (shouldSkipLineage(plan)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Skip lineage: valuesOnly={}, internalSchemaTarget={}, plan={}",
                        valuesOnly, internalTarget, plan.getClass().getSimpleName());
            }
            return;
        }
        try {
            LineageInfo lineageInfo = LineageUtils.buildLineageInfo(plan, currentHandleClass,
                    executor.getContext(), executor);
            if (lineageInfo != null) {
                if (LOG.isDebugEnabled()) {
                    LineageContext context = lineageInfo.getContext();
                    LOG.debug("Submit lineage: queryId={}, plan={}",
                            context == null ? "" : context.getQueryId(),
                            plan.getClass().getSimpleName());
                }
                Env.getCurrentEnv().getLineageEventProcessor().submitLineageEvent(lineageInfo);
            }
        } catch (Exception e) {
            // Log and ignore exceptions during lineage processing to avoid impacting query execution
            LOG.error("Failed to submit lineage event", e);
        }
    }

    public static boolean shouldSkipLineage(Plan plan) {
        return plan == null || isValuesOnly(plan) || isInternalSchemaTarget(plan);
    }

    private static boolean isValuesOnly(Plan plan) {
        if (plan.containsType(LogicalCatalogRelation.class)) {
            return false;
        }
        return plan.containsType(InlineTable.class, LogicalUnion.class, LogicalOneRowRelation.class);
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
        Set<TableIf> tableLineageSet = lineageInfo.getTableLineageSet();
        TableIf targetTable = lineageInfo.getTargetTable();
        int tableCount = (tableLineageSet == null ? 0 : tableLineageSet.size()) + (targetTable == null ? 0 : 1);
        Set<TableIf> tables = new HashSet<>(Math.max(tableCount, 1));
        if (tableLineageSet != null) {
            tables.addAll(tableLineageSet);
        }
        if (targetTable != null) {
            tables.add(targetTable);
        }
        if (tables.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Map<String, String>> externalCatalogs = new LinkedHashMap<>();
        for (TableIf table : tables) {
            CatalogIf<?> catalog = getCatalog(table);
            if (catalog == null) {
                continue;
            }
            if (catalog.isInternalCatalog()) {
                continue;
            }
            String catalogName = catalog.getName();
            if (externalCatalogs.containsKey(catalogName)) {
                continue;
            }
            Map<String, String> properties = new LinkedHashMap<>();
            if (catalog.getType() != null) {
                properties.put(CATALOG_TYPE_KEY, catalog.getType());
            }
            properties.putAll(sanitizeCatalogProperties(catalog));
            externalCatalogs.put(catalogName, properties);
        }
        return externalCatalogs;
    }

    private static Map<String, String> sanitizeCatalogProperties(CatalogIf<?> catalog) {
        if (catalog == null || catalog.getProperties() == null) {
            return Collections.emptyMap();
        }
        Map<String, String> sanitized = new LinkedHashMap<>(catalog.getProperties().size());
        if (catalog.getProperties().isEmpty()) {
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

    private static LineageContext buildLineageContext(Class<? extends Command> sourceCommand, ConnectContext ctx,
            StmtExecutor executor, long timestampMs, long durationMs) {
        String queryId = ctx.queryId() == null ? EMPTY_STRING : DebugUtil.printId(ctx.queryId());
        String queryText = executor == null ? EMPTY_STRING : executor.getOriginStmtInString();
        Map<String, String> connectAttributes = ctx.getConnectAttributes();
        String scheduleInfo = connectAttributes == null ? EMPTY_STRING : connectAttributes.get("scheduleInfo");
        if (scheduleInfo != null && !scheduleInfo.isEmpty()) {
            queryText = "/* scheduleInfo=" + scheduleInfo + " */ " + queryText;
        }
        String user = safeString(ctx.getQualifiedUser());
        String database = safeString(ctx.getDatabase());
        LineageContext lineageContext =
                new LineageContext(sourceCommand, queryId, queryText, user, database, timestampMs, durationMs);
        lineageContext.setClientIp(safeString(ctx.getClientIP()));
        lineageContext.setState(ctx.getState() == null ? EMPTY_STRING : ctx.getState().getStateType().name());
        return lineageContext;
    }

    private static boolean isLineagePluginConfigured() {
        return Config.activate_lineage_plugin != null
                && Config.activate_lineage_plugin.length != NO_PLUGINS;
    }

    private static String safeString(String value) {
        return value == null ? EMPTY_STRING : value;
    }

    private static CatalogIf<?> getCatalog(TableIf table) {
        if (table == null || table.getDatabase() == null) {
            return null;
        }
        return table.getDatabase().getCatalog();
    }
}
