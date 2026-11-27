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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.plugin.PluginDrivenSysExternalTable;
import org.apache.doris.datasource.systable.SysTableResolver;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Named-argument facade for a connector's native incremental changes relation. */
public class TableChangesTableValuedFunction extends TableValuedFunctionIf {
    public static final String NAME = "table_changes";
    private static final Pattern IDENTIFIER = Pattern.compile(
            "\\s*(?:`((?:[^`]|``)+)`|([\\p{L}\\p{N}_$]+))\\s*");
    private static final Set<String> BOUNDS = Set.of(
            "start_version", "start_timestamp", "end_version", "end_timestamp");
    private final List<String> tableName;
    private final TableScanParams scanParams;

    public TableChangesTableValuedFunction(Map<String, String> arguments, ConnectContext ctx) {
        Map<String, String> params = new LinkedHashMap<>();
        arguments.forEach((key, value) -> {
            if (value == null || value.isBlank() || params.putIfAbsent(key.toLowerCase(Locale.ROOT), value) != null) {
                throw new AnalysisException("Duplicate or empty table_changes parameter: " + key);
            }
        });
        List<String> parts = splitIdentifier(params.remove("table"));
        String catalog = params.remove("catalog");
        String database = params.remove("database");
        String db = params.remove("db");
        if ((database != null && db != null) || (parts.size() == 3 && catalog != null)
                || (parts.size() >= 2 && (database != null || db != null))) {
            throw new AnalysisException("Conflicting table_changes catalog or database qualifiers");
        }
        if (parts.size() == 1 && (database != null || db != null || catalog != null)) {
            parts.add(0, database != null ? database : db != null ? db : ctx.getDatabase());
        }
        if (catalog != null) {
            parts.add(0, catalog);
        }
        tableName = RelationUtil.getQualifierName(ctx, parts);
        for (String key : params.keySet()) {
            if (!BOUNDS.contains(key)) {
                throw new AnalysisException("Unknown table_changes parameter: " + key);
            }
        }
        if (params.containsKey("start_version") == params.containsKey("start_timestamp")
                || (params.containsKey("end_version") && params.containsKey("end_timestamp"))) {
            throw new AnalysisException("table_changes requires one start boundary and at most one end boundary");
        }
        scanParams = new TableScanParams(TableScanParams.INCREMENTAL_READ, Map.copyOf(params), List.of());
    }

    private static List<String> splitIdentifier(String text) {
        List<String> parts = new ArrayList<>();
        if (text == null) {
            throw new AnalysisException("table_changes requires a table parameter");
        }
        Matcher matcher = IDENTIFIER.matcher(text);
        while (parts.size() < 3 && matcher.lookingAt()) {
            parts.add(matcher.group(1) != null ? matcher.group(1).replace("``", "`") : matcher.group(2));
            int end = matcher.end();
            if (end == text.length()) {
                return parts;
            }
            if (text.charAt(end) != '.') {
                break;
            }
            matcher.region(end + 1, text.length());
        }
        throw new AnalysisException("Invalid table_changes table identifier: " + text);
    }

    public List<String> baseName() {
        return tableName;
    }

    public List<String> changesName() {
        return List.of(tableName.get(0), tableName.get(1), tableName.get(2) + "$changes");
    }

    public TableScanParams scanParams() {
        return scanParams;
    }

    @Override
    public String getTableName() {
        return NAME;
    }

    @Override
    public void checkAuth(ConnectContext ctx) {
        if (!ctx.getEnv().getAccessManager().checkTblPriv(ctx, tableName.get(0), tableName.get(1),
                tableName.get(2), PrivPredicate.SELECT)) {
            throw new AnalysisException("SELECT command denied on " + String.join(".", tableName));
        }
    }

    @Override
    public List<Column> getTableColumns() {
        ConnectContext ctx = ConnectContext.get();
        checkAuth(ctx);
        TableIf base = RelationUtil.getTable(tableName, ctx.getEnv(), Optional.empty());
        PluginDrivenSysExternalTable view = SysTableResolver.resolveForPlan(base, tableName.get(0),
                tableName.get(1), changesName().get(2)).filter(SysTableResolver.SysTablePlan::isNative)
                .map(SysTableResolver.SysTablePlan::getSysExternalTable)
                .filter(PluginDrivenSysExternalTable.class::isInstance).map(PluginDrivenSysExternalTable.class::cast)
                .filter(table -> table.selectorSupported(Optional.of(scanParams)))
                .orElseThrow(() -> new AnalysisException("Table does not support incremental $changes: " + tableName));
        return view.getFullSchemaAt(Optional.empty(), Optional.of(scanParams));
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        throw new AnalysisException("table_changes requires Nereids native file scan planning");
    }
}
