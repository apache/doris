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

package org.apache.doris.plugin.lineage;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DigitalVersion;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.lineage.LineageInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.apache.doris.plugin.PluginMgr;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DataworksLineagePlugin extends AbstractLineagePlugin {

    private static final Logger LOG = LogManager.getLogger(DataworksLineagePlugin.class);
    private static final Logger LINEAGE_LOG = LogManager.getLogger("lineage.dataworks");
    private static final String EDGE_TYPE_PROJECTION = "PROJECTION";
    private static final String EDGE_TYPE_PREDICATE = "PREDICATE";

    private final PluginInfo pluginInfo;

    public DataworksLineagePlugin() {
        pluginInfo = new PluginInfo(PluginMgr.BUILTIN_PLUGIN_PREFIX + "DataworksLineage", PluginType.LINEAGE,
                "builtin dataworks lineage plugin", DigitalVersion.fromString("4.0.0"),
                DigitalVersion.fromString("1.8.31"), DataworksLineagePlugin.class.getName(),
                null, null);
    }

    public PluginInfo getPluginInfo() {
        return pluginInfo;
    }

    @Override
    public String getName() {
        return "dataworks";
    }

    @Override
    public boolean eventFilter() {
        if (!isPluginEnabled()) {
            return false;
        }
        return !getEnabledScopes().isEmpty();
    }

    @Override
    public boolean exec(LineageInfo lineageInfo) {
        if (lineageInfo == null || !eventFilter()) {
            return false;
        }
        if (lineageInfo.getTargetTable() == null) {
            LOG.warn("skip dataworks lineage event: target table is null");
            return false;
        }
        Set<String> scopes = getEnabledScopes();
        boolean includeTable = scopes.contains("table");
        boolean includeColumn = scopes.contains("column");
        if (!includeTable && !includeColumn) {
            return false;
        }

        DataworksLineageEvent event = buildEvent(lineageInfo, includeTable, includeColumn);
        LINEAGE_LOG.info(GsonUtils.GSON.toJson(event));
        return true;
    }

    private boolean isPluginEnabled() {
        String[] plugins = Config.activate_lineage_plugin;
        if (plugins == null || plugins.length == 0) {
            return false;
        }
        for (String plugin : plugins) {
            if (getName().equalsIgnoreCase(Strings.nullToEmpty(plugin).trim())) {
                return true;
            }
        }
        return false;
    }

    private Set<String> getEnabledScopes() {
        if (Strings.isNullOrEmpty(Config.lineage_dataworks_enabled_scope)) {
            return java.util.Collections.emptySet();
        }
        return Arrays.stream(Config.lineage_dataworks_enabled_scope.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
    }

    private DataworksLineageEvent buildEvent(LineageInfo lineageInfo, boolean includeTable, boolean includeColumn) {
        DataworksLineageEvent event = new DataworksLineageEvent();
        event.database = lineageInfo.getDatabase();
        event.duration = lineageInfo.getDurationMs();
        event.engine = "doris";
        event.hash = lineageInfo.getQueryId();
        event.jobIds = java.util.Collections.emptyList();
        event.queryText = lineageInfo.getQueryText();
        event.timestamp = lineageInfo.getTimestampMs();
        event.user = lineageInfo.getUser();
        event.version = DigitalVersion.CURRENT_DORIS_VERSION.toString();
        if (includeTable) {
            event.tableLineages = buildTableLineages(lineageInfo);
        }
        if (includeColumn) {
            event.columnLineages = buildColumnLineages(lineageInfo);
        }
        return event;
    }

    private List<TableLineage> buildTableLineages(LineageInfo lineageInfo) {
        TableIf targetTable = lineageInfo.getTargetTable();
        CatalogDbRef dest = CatalogDbRef.fromTable(targetTable);
        List<TableLineage> results = new ArrayList<>();
        for (TableIf sourceTable : lineageInfo.getTableLineageSet()) {
            CatalogDbRef src = CatalogDbRef.fromTable(sourceTable);
            TableLineage lineage = new TableLineage();
            lineage.destCatalog = dest == null ? null : dest.catalogName;
            lineage.destDatabase = dest == null ? null : dest.dbName;
            lineage.destTable = targetTable.getName();
            lineage.srcCatalog = src == null ? null : src.catalogName;
            lineage.srcDatabase = src == null ? null : src.dbName;
            lineage.srcTable = sourceTable.getName();
            results.add(lineage);
        }
        return results;
    }

    private List<ColumnLineage> buildColumnLineages(LineageInfo lineageInfo) {
        Map<SlotReference, SetMultimap<LineageInfo.DirectLineageType, Expression>> directMap =
                lineageInfo.getDirectLineageMap();
        Map<SlotReference, SetMultimap<LineageInfo.IndirectLineageType, Expression>> indirectMap =
                lineageInfo.getInDirectLineageMap();
        TableIf targetTable = lineageInfo.getTargetTable();
        CatalogDbRef dest = CatalogDbRef.fromTable(targetTable);
        List<Slot> targetColumns = lineageInfo.getTargetColumns();

        List<ColumnLineage> results = new ArrayList<>();
        for (Map.Entry<SlotReference, SetMultimap<LineageInfo.DirectLineageType, Expression>> entry
                : directMap.entrySet()) {
            SlotReference outputSlot = entry.getKey();
            String targetColumn = resolveTargetColumnName(outputSlot, targetColumns);
            ColumnRef targetRef = new ColumnRef(targetTable.getName(), targetColumn);

            List<Expression> directExprs = Lists.newArrayList(entry.getValue().values());
            List<SlotReference> directSources = collectSourceSlots(directExprs);
            if (!directSources.isEmpty()) {
                ColumnLineage lineage = new ColumnLineage();
                lineage.edgeType = EDGE_TYPE_PROJECTION;
                lineage.expression = buildDirectExpression(directExprs);
                lineage.targets = java.util.Collections.singletonList(targetRef);
                lineage.sources = toColumnRefs(directSources);
                applyCatalogDb(lineage, dest, commonCatalogDb(directSources));
                results.add(lineage);
            }

            SetMultimap<LineageInfo.IndirectLineageType, Expression> indirectExprs = indirectMap.get(outputSlot);
            if (indirectExprs == null || indirectExprs.isEmpty()) {
                continue;
            }
            for (Map.Entry<LineageInfo.IndirectLineageType, Expression> indirectEntry : indirectExprs.entries()) {
                Expression expr = indirectEntry.getValue();
                List<SlotReference> indirectSources = collectSourceSlots(expr);
                if (indirectSources.isEmpty()) {
                    continue;
                }
                ColumnLineage lineage = new ColumnLineage();
                lineage.edgeType = EDGE_TYPE_PREDICATE;
                lineage.expression = expr.toSql();
                lineage.targets = java.util.Collections.singletonList(targetRef);
                lineage.sources = toColumnRefs(indirectSources);
                applyCatalogDb(lineage, dest, commonCatalogDb(indirectSources));
                results.add(lineage);
            }
        }
        return results;
    }

    private String resolveTargetColumnName(SlotReference outputSlot, List<Slot> targetColumns) {
        String name = outputSlot.getName();
        if (targetColumns == null) {
            return name;
        }
        for (Slot column : targetColumns) {
            if (column.getName().equalsIgnoreCase(name)) {
                return column.getName();
            }
        }
        return name;
    }

    private String buildDirectExpression(List<Expression> exprs) {
        if (exprs.isEmpty()) {
            return "";
        }
        if (exprs.size() == 1 && exprs.get(0) instanceof SlotReference) {
            return "";
        }
        return exprs.stream().map(Expression::toSql).collect(Collectors.joining("; "));
    }

    private List<SlotReference> collectSourceSlots(Expression expr) {
        return collectSourceSlots(java.util.Collections.singletonList(expr));
    }

    private List<SlotReference> collectSourceSlots(List<Expression> expressions) {
        LinkedHashSet<SlotReference> slots = new LinkedHashSet<>();
        for (Expression expr : expressions) {
            for (Object slotExpr : expr.collectToList(SlotReference.class::isInstance)) {
                slots.add((SlotReference) slotExpr);
            }
        }
        return new ArrayList<>(slots);
    }

    private List<ColumnRef> toColumnRefs(List<SlotReference> slots) {
        List<ColumnRef> refs = new ArrayList<>();
        for (SlotReference slot : slots) {
            String tableName = null;
            TableIf table = slot.getOriginalTable().orElse(slot.getOneLevelTable().orElse(null));
            if (table != null) {
                tableName = table.getName();
            } else if (!slot.getQualifier().isEmpty()) {
                tableName = slot.getQualifier().get(slot.getQualifier().size() - 1);
            }
            if (tableName == null) {
                tableName = "";
            }
            refs.add(new ColumnRef(tableName, slot.getName()));
        }
        return refs;
    }

    private CatalogDbRef commonCatalogDb(List<SlotReference> slots) {
        CatalogDbRef common = null;
        for (SlotReference slot : slots) {
            TableIf table = slot.getOriginalTable().orElse(slot.getOneLevelTable().orElse(null));
            if (table == null) {
                continue;
            }
            CatalogDbRef current = CatalogDbRef.fromTable(table);
            if (current == null) {
                continue;
            }
            if (common == null) {
                common = current;
            } else if (!common.equals(current)) {
                return null;
            }
        }
        return common;
    }

    private void applyCatalogDb(ColumnLineage lineage, CatalogDbRef dest, CatalogDbRef src) {
        if (dest != null) {
            lineage.destCatalog = dest.catalogName;
            lineage.destDatabase = dest.dbName;
        }
        if (src != null) {
            lineage.srcCatalog = src.catalogName;
            lineage.srcDatabase = src.dbName;
        }
    }

    private static class CatalogDbRef {
        private final String catalogName;
        private final String dbName;

        private CatalogDbRef(String catalogName, String dbName) {
            this.catalogName = catalogName;
            this.dbName = dbName;
        }

        static CatalogDbRef fromTable(TableIf table) {
            if (table == null || table.getDatabase() == null || table.getDatabase().getCatalog() == null) {
                return null;
            }
            String catalogName = table.getDatabase().getCatalog().getName();
            if (InternalCatalog.INTERNAL_CATALOG_NAME.equals(catalogName)) {
                catalogName = null;
            }
            String dbName = table.getDatabase().getFullName();
            return new CatalogDbRef(catalogName, dbName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CatalogDbRef)) {
                return false;
            }
            CatalogDbRef that = (CatalogDbRef) o;
            return Objects.equals(catalogName, that.catalogName) && Objects.equals(dbName, that.dbName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogName, dbName);
        }
    }

    private static class DataworksLineageEvent {
        @SerializedName("columnLineages")
        public List<ColumnLineage> columnLineages;
        @SerializedName("database")
        public String database;
        @SerializedName("duration")
        public long duration;
        @SerializedName("engine")
        public String engine;
        @SerializedName("hash")
        public String hash;
        @SerializedName("jobIds")
        public List<String> jobIds;
        @SerializedName("queryText")
        public String queryText;
        @SerializedName("tableLineages")
        public List<TableLineage> tableLineages;
        @SerializedName("timestamp")
        public long timestamp;
        @SerializedName("user")
        public String user;
        @SerializedName("version")
        public String version;
    }

    private static class ColumnLineage {
        @SerializedName("destCatalog")
        public String destCatalog;
        @SerializedName("destDatabase")
        public String destDatabase;
        @SerializedName("srcCatalog")
        public String srcCatalog;
        @SerializedName("srcDatabase")
        public String srcDatabase;
        @SerializedName("edgeType")
        public String edgeType;
        @SerializedName("expression")
        public String expression;
        @SerializedName("sources")
        public List<ColumnRef> sources;
        @SerializedName("targets")
        public List<ColumnRef> targets;
    }

    private static class ColumnRef {
        @SerializedName("column")
        public String column;
        @SerializedName("table")
        public String table;

        ColumnRef(String table, String column) {
            this.table = table;
            this.column = column;
        }
    }

    private static class TableLineage {
        @SerializedName("destCatalog")
        public String destCatalog;
        @SerializedName("destDatabase")
        public String destDatabase;
        @SerializedName("destTable")
        public String destTable;
        @SerializedName("srcCatalog")
        public String srcCatalog;
        @SerializedName("srcDatabase")
        public String srcDatabase;
        @SerializedName("srcTable")
        public String srcTable;
    }
}
