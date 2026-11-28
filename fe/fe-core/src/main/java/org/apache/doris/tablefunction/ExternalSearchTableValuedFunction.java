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

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.search.ConnectorSearch;
import org.apache.doris.connector.spi.search.ConnectorSearchFunction;
import org.apache.doris.connector.spi.search.ConnectorSearchLazyMaterialization;
import org.apache.doris.connector.spi.search.ConnectorSearchOps;
import org.apache.doris.connector.spi.search.ConnectorSearchRequest;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.connector.converter.ConnectorColumnConverter;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenMetadata;
import org.apache.doris.datasource.scan.PluginDrivenScanNode;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A search table function over a table of a plugin catalog, such as
 * {@code vector_search("table" = "ctl.db.tbl", ...)}: the engine's side of {@link ConnectorSearchOps}.
 *
 * <p>The engine reads only the {@code table} argument: it checks the user's SELECT privilege on that table and hands
 * every other argument to the connector of the table's catalog, which validates them and pins what the search reads.
 * The relation's columns and the scan come from the connector's {@link ConnectorSearch}; the merge of the ranges'
 * candidates, the WHERE written on the function below it and the lazy fetch of columns after it are engine rules
 * that recognize this class.</p>
 *
 * <p>A search reads the table as a relation of its own, so the engine's protections of a table scan do not reach
 * it: the column privileges a scan checks for the columns it reads, and the row policies and data masks it applies.
 * The arguments name columns the engine cannot see (the searched column, a {@code filter}), so the user must be
 * allowed to read every column of the table, and a search is refused while a row policy or a data mask applies to
 * the user on the table, rather than return what they hide.</p>
 */
public abstract class ExternalSearchTableValuedFunction extends TableValuedFunctionIf {
    public static final String TABLE = "table";

    private static final String FULLY_QUALIFIED_TABLE_NAME_ERROR =
            "'table' must be a fully qualified catalog.database.table name";

    private final ConnectorSearchFunction function;
    private final String sourceTableName;
    private final PluginDrivenExternalCatalog catalog;
    private final ConnectorSearch search;
    private final List<Column> columns;

    /**
     * @param deferredArguments the arguments a prepared statement leaves to parameters, while it is prepared;
     *         empty otherwise
     */
    protected ExternalSearchTableValuedFunction(ConnectorSearchFunction function, Map<String, String> properties,
            Set<String> deferredArguments) throws AnalysisException {
        this.function = function;
        String functionName = function.getFunctionName();
        Map<String, String> arguments = normalizeArguments(properties, functionName);
        // The parser refuses a parameter in place of 'table', so a PREPARE never defers it.
        Preconditions.checkArgument(!deferredArguments.contains(TABLE), "%s() deferred 'table'", functionName);
        String table = arguments.remove(TABLE);
        if (table == null || table.trim().isEmpty()) {
            throw new AnalysisException("Missing required " + functionName + "() property 'table'");
        }
        List<String> names = parseTableName(table.trim());
        this.sourceTableName = String.join(".", names);
        this.catalog = findSearchCatalog(names.get(0), functionName);
        // Before the table is looked up, which may ask a remote metastore.
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, catalog.getConnector());
        if (!(metadata instanceof ConnectorSearchOps)) {
            throw new AnalysisException(unsupportedCatalog(catalog.getName(), functionName));
        }
        PluginDrivenExternalTable sourceTable = findTable(catalog, names);
        checkSelectPrivilege(catalog.getName(), sourceTable);
        ConnectorTableHandle tableHandle = sourceTable.resolveConnectorTableHandle(session, metadata)
                .orElseThrow(() -> new AnalysisException("Table '" + sourceTableName + "' does not exist"));
        try {
            this.search = ((ConnectorSearchOps) metadata).prepareSearch(session, tableHandle,
                    new ConnectorSearchRequest(function, arguments, deferredArguments));
        } catch (DorisConnectorException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        List<Column> searchColumns = checkColumns(
                ConnectorColumnConverter.convertColumns(search.getColumns()), function, sourceTableName);
        // The table's columns as of what the search pinned: all but the result column (ConnectorSearch#getColumns).
        Set<String> tableColumns = new LinkedHashSet<>();
        for (Column column : searchColumns.subList(0, searchColumns.size() - 1)) {
            tableColumns.add(column.getName());
        }
        checkColumnPrivileges(catalog.getName(), sourceTable, tableColumns);
        refuseDataPolicies(catalog.getName(), sourceTable, tableColumns, functionName, sourceTableName);
        this.columns = Collections.unmodifiableList(searchColumns);
    }

    public final ConnectorSearchFunction getFunction() {
        return function;
    }

    /** The column the engine merges the ranges' candidates by, the last of the relation's columns. */
    public final String getResultColumn() {
        return function.getResultColumn();
    }

    public final long getTopK() {
        return search.getTopK();
    }

    public final long getOffset() {
        return search.getOffset();
    }

    public final ConnectorSearchLazyMaterialization getLazyMaterialization() {
        return search.getLazyMaterialization();
    }

    @Override
    public final String getTableName() {
        return getClass().getSimpleName() + "<" + sourceTableName + ">";
    }

    @Override
    public final List<Column> getTableColumns() {
        return columns;
    }

    @Override
    public final ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        return new PluginDrivenScanNode(id, desc, false, sv,
                ScanContext.builder().clusterName(sv.resolveCloudClusterName())
                        .beExecVersion(ConnectContext.get().getStatementContext().getBeExecVersion())
                        .build(),
                catalog.getConnector(), catalog.buildConnectorSession(), search.getScanHandle());
    }

    /** The arguments by their lower-case names; two that differ only in case are refused. */
    private static Map<String, String> normalizeArguments(Map<String, String> properties, String functionName)
            throws AnalysisException {
        Map<String, String> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (normalized.put(key, entry.getValue()) != null) {
                throw new AnalysisException("Duplicate " + functionName + "() property '" + key + "'");
            }
        }
        return normalized;
    }

    private static List<String> parseTableName(String value) throws AnalysisException {
        Expression expression;
        try {
            expression = new NereidsParser().parseExpression(value);
        } catch (ParseException e) {
            throw new AnalysisException(FULLY_QUALIFIED_TABLE_NAME_ERROR, e);
        }
        if (!(expression instanceof UnboundSlot) || ((UnboundSlot) expression).getNameParts().size() != 3) {
            throw new AnalysisException(FULLY_QUALIFIED_TABLE_NAME_ERROR);
        }
        return ((UnboundSlot) expression).getNameParts();
    }

    /**
     * SELECT on the table found, by its own names as a scan of it checks them: the lookup may match the names as
     * written case-insensitively, so the written names are not the table's.
     */
    private static void checkSelectPrivilege(String catalogName, PluginDrivenExternalTable table)
            throws AnalysisException {
        ConnectContext context = ConnectContext.get();
        String dbName = table.getDatabase().getFullName();
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(context, catalogName, dbName, table.getName(),
                PrivPredicate.SELECT)) {
            throw new AnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("SELECT",
                    context.getQualifiedUser(), context.getRemoteIP(), dbName + ": " + table.getName()));
        }
    }

    /**
     * SELECT on every column of the table, as a scan checks the columns it reads: the search may read any of them,
     * through the arguments the connector interprets.
     */
    private static void checkColumnPrivileges(String catalogName, PluginDrivenExternalTable table,
            Set<String> columns) throws AnalysisException {
        try {
            Env.getCurrentEnv().getAccessManager().checkColumnsPriv(ConnectContext.get(), catalogName,
                    table.getDatabase().getFullName(), table.getName(), columns, PrivPredicate.SELECT);
        } catch (UserException e) {
            throw new AnalysisException(e.getDetailMessage(), e);
        }
    }

    /**
     * Refuses the search while a row policy or a data mask applies to the user on the table: a scan of the table
     * applies them, and the search can not. The literal root and admin accounts are subject to neither, as in
     * {@code LogicalCheckPolicy#findPolicy}.
     */
    private static void refuseDataPolicies(String catalogName, PluginDrivenExternalTable table, Set<String> columns,
            String functionName, String sourceTableName) throws AnalysisException {
        UserIdentity user = ConnectContext.get().getCurrentUserIdentity();
        if (user.isSystemUser()) {
            return;
        }
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        String dbName = table.getDatabase().getFullName();
        if (!accessManager.evalRowFilterPolicies(user, catalogName, dbName, table.getName()).isEmpty()) {
            throw new AnalysisException(functionName + "() is not supported on table '" + sourceTableName
                    + "' while row policies apply to it for the current user");
        }
        if (!accessManager.evalDataMaskPolicies(user, catalogName, dbName, table.getName(), columns).isEmpty()) {
            throw new AnalysisException(functionName + "() is not supported on table '" + sourceTableName
                    + "' while data masking policies apply to it for the current user");
        }
    }

    private static PluginDrivenExternalCatalog findSearchCatalog(String catalogName, String functionName)
            throws AnalysisException {
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            throw new AnalysisException("Catalog '" + catalogName + "' does not exist");
        }
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            throw new AnalysisException(unsupportedCatalog(catalogName, functionName));
        }
        return (PluginDrivenExternalCatalog) catalog;
    }

    private static PluginDrivenExternalTable findTable(PluginDrivenExternalCatalog catalog, List<String> names)
            throws AnalysisException {
        TableIf table = catalog.getDbOrAnalysisException(names.get(1)).getTableOrAnalysisException(names.get(2));
        Preconditions.checkState(table instanceof PluginDrivenExternalTable,
                "plugin catalog %s returned a %s", catalog.getName(), table.getClass().getName());
        return (PluginDrivenExternalTable) table;
    }

    private static String unsupportedCatalog(String catalogName, String functionName) {
        return "Catalog '" + catalogName + "' does not support " + functionName + "()";
    }

    /**
     * Checks what the relation's columns must hold for the engine's rules, whichever connector returned them: the
     * function's FLOAT result column comes last, names are unique as Doris resolves them (case-insensitively), and
     * no table column takes the prefix of the row-id column that lazy materialization adds.
     */
    private static List<Column> checkColumns(List<Column> columns, ConnectorSearchFunction function,
            String sourceTableName) throws AnalysisException {
        Preconditions.checkState(!columns.isEmpty()
                        && columns.get(columns.size() - 1).getName().equals(function.getResultColumn())
                        && columns.get(columns.size() - 1).getType().getPrimitiveType() == PrimitiveType.FLOAT,
                "the connector of %s did not end the columns of %s() with FLOAT %s",
                sourceTableName, function.getFunctionName(), function.getResultColumn());
        Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Column column : columns) {
            if (!names.add(column.getName())) {
                throw new AnalysisException("Table '" + sourceTableName + "' has a column named '" + column.getName()
                        + "' twice under case-insensitive matching, or one that takes the name of "
                        + function.getFunctionName() + "()'s result column '" + function.getResultColumn() + "'");
            }
            if (column.getName().startsWith(Column.GLOBAL_ROWID_COL)) {
                throw new AnalysisException("Table '" + sourceTableName + "' contains column '" + column.getName()
                        + "' using reserved Doris internal column prefix '" + Column.GLOBAL_ROWID_COL + "'");
            }
        }
        return columns;
    }
}
