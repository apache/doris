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

package org.apache.doris.datasource.jdbc.source;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.ExprToExternalSqlVisitor;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalFunctionRules;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.datasource.jdbc.JdbcNameUtil;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TOdbcTableType;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * JdbcScanNode extends FileQueryScanNode to integrate JDBC scanning into
 * the unified FileScanner framework (FORMAT_JNI path).
 *
 * <p>This replaces the old ExternalScanNode-based JDBC_SCAN_NODE with:
 * <pre>
 *   FE: JdbcScanNode(extends FileQueryScanNode) → TFileScanRange(FORMAT_JNI)
 *   BE: FileScanOperatorX → FileScanner → JdbcJniReader → JniConnector → JdbcJniScanner
 * </pre>
 *
 * <p>All predicate push-down logic (createJdbcFilters, getJdbcQueryStr, etc.)
 * is preserved from the original implementation.
 */
public class JdbcScanNode extends FileQueryScanNode {

    private final List<String> columns = new ArrayList<String>();
    private final List<String> filters = new ArrayList<String>();
    private final List<Expr> pushedDownConjuncts = new ArrayList<>();
    private String tableName;
    private TOdbcTableType jdbcType;
    private String graphQueryString = "";
    private boolean isTableValuedFunction = false;
    private String query = "";

    // For normal JDBC external table scan
    private JdbcExternalTable extTable;
    // For TVF scan, connection info comes from catalog directly
    private JdbcExternalCatalog tvfCatalog;
    private long catalogId;

    // Accessor interfaces to abstract connection info source (external table vs TVF catalog)
    private String jdbcUrl;
    private String jdbcUser;
    private String jdbcPasswd;
    private String driverClass;
    private String driverUrl;
    private String checkSum;
    private int connectionPoolMinSize;
    private int connectionPoolMaxSize;
    private int connectionPoolMaxWaitTime;
    private int connectionPoolMaxLifeTime;
    private boolean connectionPoolKeepAlive;
    private ExternalFunctionRules externalFunctionRules;

    public JdbcScanNode(PlanNodeId id, TupleDescriptor desc, ScanContext scanContext) {
        super(id, desc, "JdbcScanNode", scanContext, false,
                ConnectContext.get() != null ? ConnectContext.get().getSessionVariable() : new SessionVariable());
        extTable = (JdbcExternalTable) (desc.getTable());
        jdbcType = extTable.getJdbcTableType();
        tableName = extTable.getProperRemoteFullTableName(jdbcType);
        catalogId = extTable.getCatalogId();
        populateConnectionFromExtTable(extTable);
    }

    public JdbcScanNode(PlanNodeId id, TupleDescriptor desc, JdbcExternalCatalog catalog, String query,
            ScanContext scanContext) {
        super(id, desc, "JdbcScanNode", scanContext, false,
                ConnectContext.get() != null ? ConnectContext.get().getSessionVariable() : new SessionVariable());
        this.isTableValuedFunction = true;
        this.query = query;
        this.tvfCatalog = catalog;
        this.jdbcType = JdbcExternalTable.parseJdbcType(catalog.getDatabaseTypeName());
        this.tableName = desc.getTable().getName();
        this.catalogId = catalog.getId();
        populateConnectionFromCatalog(catalog);
    }

    private void populateConnectionFromExtTable(JdbcExternalTable table) {
        this.jdbcUrl = table.getJdbcUrl();
        this.jdbcUser = table.getJdbcUser();
        this.jdbcPasswd = table.getJdbcPasswd();
        this.driverClass = table.getDriverClass();
        this.driverUrl = table.getDriverUrl();
        this.checkSum = table.getCheckSum();
        this.connectionPoolMinSize = table.getConnectionPoolMinSize();
        this.connectionPoolMaxSize = table.getConnectionPoolMaxSize();
        this.connectionPoolMaxWaitTime = table.getConnectionPoolMaxWaitTime();
        this.connectionPoolMaxLifeTime = table.getConnectionPoolMaxLifeTime();
        this.connectionPoolKeepAlive = table.isConnectionPoolKeepAlive();
        this.externalFunctionRules = table.getExternalFunctionRules();
    }

    private void populateConnectionFromCatalog(JdbcExternalCatalog catalog) {
        this.jdbcUrl = catalog.getJdbcUrl();
        this.jdbcUser = catalog.getJdbcUser();
        this.jdbcPasswd = catalog.getJdbcPasswd();
        this.driverClass = catalog.getDriverClass();
        this.driverUrl = catalog.getDriverUrl();
        this.checkSum = catalog.getCheckSum();
        this.connectionPoolMinSize = catalog.getConnectionPoolMinSize();
        this.connectionPoolMaxSize = catalog.getConnectionPoolMaxSize();
        this.connectionPoolMaxWaitTime = catalog.getConnectionPoolMaxWaitTime();
        this.connectionPoolMaxLifeTime = catalog.getConnectionPoolMaxLifeTime();
        this.connectionPoolKeepAlive = catalog.isConnectionPoolKeepAlive();
        this.externalFunctionRules = catalog.getFunctionRules();
    }

    // ========= FileQueryScanNode abstract method implementations =========

    @Override
    public TFileFormatType getFileFormatType() {
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    public List<String> getPathPartitionKeys() {
        // JDBC has no file path partitions
        return Collections.emptyList();
    }

    @Override
    public TableIf getTargetTable() {
        return desc.getTable();
    }

    @Override
    protected Map<String, String> getLocationProperties() {
        // JDBC does not need storage location properties
        return Collections.emptyMap();
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        // JDBC always produces a single split — the query cannot be partitioned
        createJdbcColumns();
        createJdbcFilters();

        String querySql = isTableValuedFunction ? query : getJdbcQueryStr();

        JdbcSplit split = new JdbcSplit(
                querySql,
                jdbcUrl,
                jdbcUser,
                jdbcPasswd,
                driverClass,
                driverUrl,
                checkSum,
                catalogId,
                jdbcType,
                connectionPoolMinSize,
                connectionPoolMaxSize,
                connectionPoolMaxWaitTime,
                connectionPoolMaxLifeTime,
                connectionPoolKeepAlive
        );

        List<Split> splits = new ArrayList<>();
        splits.add(split);
        return splits;
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (!(split instanceof JdbcSplit)) {
            return;
        }
        JdbcSplit jdbcSplit = (JdbcSplit) split;

        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType("jdbc");

        // Build JDBC params map — these are passed to JdbcJniScanner via JniConnector
        Map<String, String> jdbcParams = new HashMap<>();
        jdbcParams.put("jdbc_url", jdbcSplit.getJdbcUrl());
        jdbcParams.put("jdbc_user", jdbcSplit.getJdbcUser());
        jdbcParams.put("jdbc_password", jdbcSplit.getJdbcPassword());
        jdbcParams.put("jdbc_driver_class", jdbcSplit.getDriverClass());
        jdbcParams.put("jdbc_driver_url", jdbcSplit.getDriverUrl());
        jdbcParams.put("jdbc_driver_checksum", jdbcSplit.getDriverChecksum());
        jdbcParams.put("query_sql", jdbcSplit.getQuerySql());
        jdbcParams.put("catalog_id", String.valueOf(jdbcSplit.getCatalogId()));
        jdbcParams.put("table_type", jdbcSplit.getTableType().name());
        jdbcParams.put("connection_pool_min_size",
                String.valueOf(jdbcSplit.getConnectionPoolMinSize()));
        jdbcParams.put("connection_pool_max_size",
                String.valueOf(jdbcSplit.getConnectionPoolMaxSize()));
        jdbcParams.put("connection_pool_max_wait_time",
                String.valueOf(jdbcSplit.getConnectionPoolMaxWaitTime()));
        jdbcParams.put("connection_pool_max_life_time",
                String.valueOf(jdbcSplit.getConnectionPoolMaxLifeTime()));
        jdbcParams.put("connection_pool_keep_alive",
                jdbcSplit.isConnectionPoolKeepAlive() ? "true" : "false");

        tableFormatFileDesc.setJdbcParams(jdbcParams);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    // ========= JDBC-specific query generation (preserved from original) =========

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();
    }

    @Override
    protected void convertPredicate() {
        // Predicate push-down is handled in getSplits() via createJdbcFilters()
        // Nothing needed here since JDBC manages its own filter push-down
    }

    @Override
    public int getNumInstances() {
        // JDBC always uses a single instance — no parallelism at the data source
        return 1;
    }

    private void createJdbcFilters() {
        if (conjuncts.isEmpty()) {
            return;
        }

        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr.collectList(conjuncts, SlotRef.class, slotRefs);
        ExprSubstitutionMap sMap = new ExprSubstitutionMap();
        for (SlotRef slotRef : slotRefs) {
            SlotRef slotRef1 = (SlotRef) slotRef.clone();
            slotRef1.setTableNameInfoToNull();
            slotRef1.setLabel(JdbcNameUtil.properNameWithRemoteName(jdbcType, slotRef1.getColumnName()));
            sMap.put(slotRef, slotRef1);
        }

        ArrayList<Expr> conjunctsList = Expr.cloneList(conjuncts, sMap);
        List<String> errors = Lists.newArrayList();
        List<Expr> pushDownConjuncts = collectConjunctsToPushDown(conjunctsList, errors,
                externalFunctionRules);

        for (Expr individualConjunct : pushDownConjuncts) {
            String filter = conjunctExprToString(jdbcType, individualConjunct, desc.getTable());
            filters.add(filter);
            pushedDownConjuncts.add(individualConjunct);
        }
    }

    private List<Expr> collectConjunctsToPushDown(List<Expr> conjunctsList, List<String> errors,
            ExternalFunctionRules functionRules) {
        List<Expr> pushDownConjuncts = new ArrayList<>();
        for (Expr p : conjunctsList) {
            if (shouldPushDownConjunct(jdbcType, p)) {
                List<Expr> individualConjuncts = p.getConjuncts();
                for (Expr individualConjunct : individualConjuncts) {
                    Expr newp = JdbcFunctionPushDownRule.processFunctions(jdbcType, individualConjunct, errors,
                            functionRules);
                    if (!errors.isEmpty()) {
                        errors.clear();
                        continue;
                    }
                    pushDownConjuncts.add(newp);
                }
            }
        }
        return pushDownConjuncts;
    }

    private void createJdbcColumns() {
        columns.clear();
        for (SlotDescriptor slot : desc.getSlots()) {
            Column col = slot.getColumn();
            if (extTable != null) {
                columns.add(extTable.getProperRemoteColumnName(jdbcType, col.getName()));
            } else {
                columns.add(JdbcNameUtil.databaseProperName(jdbcType, col.getName()));
            }
        }
        if (columns.isEmpty()) {
            columns.add("*");
        }
    }

    private boolean shouldPushDownLimit() {
        return limit != -1 && conjuncts.size() == pushedDownConjuncts.size();
    }

    private String getJdbcQueryStr() {
        StringBuilder sql = new StringBuilder("SELECT ");

        // Oracle use the where clause to do top n
        if (shouldPushDownLimit() && (jdbcType == TOdbcTableType.ORACLE
                || jdbcType == TOdbcTableType.OCEANBASE_ORACLE)) {
            filters.add("ROWNUM <= " + limit);
        }

        // MSSQL use select top to do top n
        if (shouldPushDownLimit() && jdbcType == TOdbcTableType.SQLSERVER) {
            sql.append("TOP " + limit + " ");
        }

        sql.append(Joiner.on(", ").join(columns));
        sql.append(" FROM ").append(tableName);

        if (!filters.isEmpty()) {
            sql.append(" WHERE (");
            sql.append(Joiner.on(") AND (").join(filters));
            sql.append(")");
        }

        // Other DataBase use limit do top n
        if (shouldPushDownLimit()
                && (jdbcType == TOdbcTableType.MYSQL
                || jdbcType == TOdbcTableType.POSTGRESQL
                || jdbcType == TOdbcTableType.MONGODB
                || jdbcType == TOdbcTableType.CLICKHOUSE
                || jdbcType == TOdbcTableType.SAP_HANA
                || jdbcType == TOdbcTableType.TRINO
                || jdbcType == TOdbcTableType.PRESTO
                || jdbcType == TOdbcTableType.OCEANBASE
                || jdbcType == TOdbcTableType.GBASE)) {
            sql.append(" LIMIT ").append(limit);
        }

        if (jdbcType == TOdbcTableType.CLICKHOUSE
                && ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable().jdbcClickhouseQueryFinal) {
            sql.append(" SETTINGS final = 1");
        }
        return sql.toString();
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        if (isTableValuedFunction) {
            output.append(prefix).append("TABLE VALUE FUNCTION\n");
            output.append(prefix).append("CATALOG ID: ").append(catalogId).append("\n");
            output.append(prefix).append("QUERY: ").append(query).append("\n");
        } else {
            output.append(prefix).append("CATALOG ID: ").append(catalogId).append("\n");
            output.append(prefix).append("TABLE: ").append(tableName).append("\n");
            if (detailLevel == TExplainLevel.BRIEF) {
                return output.toString();
            }
            output.append(prefix).append("QUERY: ").append(getJdbcQueryStr()).append("\n");
            if (!conjuncts.isEmpty()) {
                Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
                output.append(prefix).append("PREDICATES: ")
                        .append(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)).append("\n");
            }
        }
        if (useTopnFilter()) {
            String topnFilterSources = String.join(",",
                    topnFilterSortNodes.stream()
                            .map(node -> node.getId().asInt() + "").collect(Collectors.toList()));
            output.append(prefix).append("TOPN OPT:").append(topnFilterSources).append("\n");
        }
        return output.toString();
    }

    @Override
    public void finalizeForNereids() throws UserException {
        // FileQueryScanNode.doFinalize() calls convertPredicate() + createScanRangeLocations()
        // JDBC-specific predicate preparation (createJdbcColumns/Filters) happens in getSplits()
        doFinalize();
    }

    // ========= Static helper methods for predicate push-down =========

    private static boolean shouldPushDownConjunct(TOdbcTableType tableType, Expr expr) {
        // Prevent pushing down expressions with NullLiteral to Oracle
        if (ConnectContext.get() != null
                && !ConnectContext.get().getSessionVariable().enableJdbcOracleNullPredicatePushDown
                && containsNullLiteral(expr)
                && tableType.equals(TOdbcTableType.ORACLE)) {
            return false;
        }

        // Prevent pushing down cast expressions if ConnectContext is null or cast pushdown is disabled
        if (ConnectContext.get() == null || !ConnectContext.get()
                .getSessionVariable().enableJdbcCastPredicatePushDown) {
            if (containsCastExpr(expr)) {
                return false;
            }
        }

        if (containsFunctionCallExpr(expr)) {
            if (tableType.equals(TOdbcTableType.MYSQL) || tableType.equals(TOdbcTableType.CLICKHOUSE)
                    || tableType.equals(TOdbcTableType.ORACLE)) {
                if (ConnectContext.get() != null) {
                    return ConnectContext.get().getSessionVariable().enableExtFuncPredPushdown;
                } else {
                    return true;
                }
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    private static boolean containsFunctionCallExpr(Expr expr) {
        List<FunctionCallExpr> fnExprList = Lists.newArrayList();
        expr.collect(FunctionCallExpr.class, fnExprList);
        return !fnExprList.isEmpty();
    }

    public static String conjunctExprToString(TOdbcTableType tableType, Expr expr, TableIf tbl) {
        if (expr == null) {
            return "";
        }
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
            if (compoundPredicate.getOp() == Operator.NOT) {
                String childString = conjunctExprToString(tableType, compoundPredicate.getChild(0), tbl);
                return "(NOT " + childString + ")";
            } else {
                String leftString = conjunctExprToString(tableType, compoundPredicate.getChild(0), tbl);
                String rightString = "";
                if (compoundPredicate.getChildren().size() > 1) {
                    rightString = conjunctExprToString(tableType, compoundPredicate.getChild(1), tbl);
                }
                String opString = compoundPredicate.getOp().toString();
                return "(" + leftString + " " + opString + " " + rightString + ")";
            }
        }

        if (expr.contains(DateLiteral.class) && expr instanceof BinaryPredicate) {
            ArrayList<Expr> children = expr.getChildren();
            ToSqlParams params = new ToSqlParams(false, true, TableType.JDBC_EXTERNAL_TABLE, tbl);
            String filter = children.get(0).accept(ExprToExternalSqlVisitor.INSTANCE, params);
            filter += " " + ((BinaryPredicate) expr).getOp().toString() + " ";

            if (tableType.equals(TOdbcTableType.ORACLE)) {
                filter += handleOracleDateFormat(children.get(1), tbl);
            } else if (tableType.equals(TOdbcTableType.TRINO) || tableType.equals(TOdbcTableType.PRESTO)) {
                filter += handleTrinoDateFormat(children.get(1), tbl);
            } else if (tableType.equals(TOdbcTableType.SQLSERVER)) {
                filter += handleSQLServerDateFormat(children.get(1), tbl);
            } else {
                filter += children.get(1).accept(ExprToExternalSqlVisitor.INSTANCE, params);
            }

            return filter;
        }

        if (expr.contains(DateLiteral.class) && expr instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) expr;
            Expr leftChild = inPredicate.getChild(0);
            ToSqlParams params = new ToSqlParams(false, true, TableType.JDBC_EXTERNAL_TABLE, tbl);
            String filter = leftChild.accept(ExprToExternalSqlVisitor.INSTANCE, params);

            if (inPredicate.isNotIn()) {
                filter += " NOT";
            }
            filter += " IN (";

            List<String> inItemStrings = new ArrayList<>();
            for (int i = 1; i < inPredicate.getChildren().size(); i++) {
                Expr inItem = inPredicate.getChild(i);
                if (tableType.equals(TOdbcTableType.ORACLE)) {
                    inItemStrings.add(handleOracleDateFormat(inItem, tbl));
                } else if (tableType.equals(TOdbcTableType.TRINO) || tableType.equals(TOdbcTableType.PRESTO)) {
                    inItemStrings.add(handleTrinoDateFormat(inItem, tbl));
                } else if (tableType.equals(TOdbcTableType.SQLSERVER)) {
                    inItemStrings.add(handleSQLServerDateFormat(inItem, tbl));
                } else {
                    inItemStrings.add(inItem.accept(ExprToExternalSqlVisitor.INSTANCE, params));
                }
            }

            filter += String.join(", ", inItemStrings);
            filter += ")";

            return filter;
        }

        // Only for old planner
        if (expr.contains(BoolLiteral.class) && "1".equals(expr.getStringValue()) && expr.getChildren().isEmpty()) {
            return "1 = 1";
        }

        return expr.accept(ExprToExternalSqlVisitor.INSTANCE,
                new ToSqlParams(false, true, TableType.JDBC_EXTERNAL_TABLE, tbl));
    }

    private static String handleOracleDateFormat(Expr expr, TableIf tbl) {
        if (expr.isConstant()
                && (expr.getType().isDatetime() || expr.getType().isDatetimeV2())) {
            String dateStr = expr.getStringValue();
            // Check if the date string contains milliseconds/microseconds
            if (dateStr.contains(".")) {
                // For Oracle, we need to use to_timestamp for fractional seconds
                // Extract date part and fractional seconds part
                String formatModel = getString(dateStr);
                return "to_timestamp('" + dateStr + "', '" + formatModel + "')";
            }
            // Regular datetime without fractional seconds
            return "to_date('" + dateStr + "', 'yyyy-mm-dd hh24:mi:ss')";
        }
        return expr.accept(ExprToExternalSqlVisitor.INSTANCE,
                new ToSqlParams(false, true, TableType.JDBC_EXTERNAL_TABLE, tbl));
    }

    @NotNull
    private static String getString(String dateStr) {
        String[] parts = dateStr.split("\\.");
        String fractionPart = parts[1];
        // Determine the format model based on the length of fractional seconds
        String formatModel;
        if (fractionPart.length() <= 3) {
            // Milliseconds (up to 3 digits)
            formatModel = "yyyy-mm-dd hh24:mi:ss.FF3";
        } else {
            // Microseconds (up to 6 digits)
            formatModel = "yyyy-mm-dd hh24:mi:ss.FF6";
        }
        return formatModel;
    }

    private static String handleTrinoDateFormat(Expr expr, TableIf tbl) {
        if (expr.isConstant()) {
            if (expr.getType().isDate() || expr.getType().isDateV2()) {
                return "date '" + expr.getStringValue() + "'";
            } else if (expr.getType().isDatetime() || expr.getType().isDatetimeV2()) {
                return "timestamp '" + expr.getStringValue() + "'";
            }
        }
        return expr.accept(ExprToExternalSqlVisitor.INSTANCE,
                new ToSqlParams(false, true, TableType.JDBC_EXTERNAL_TABLE, tbl));
    }

    private static String handleSQLServerDateFormat(Expr expr, TableIf tbl) {
        if (expr.isConstant()) {
            if (expr.getType().isDatetime() || expr.getType().isDatetimeV2()) {
                // Use CONVERT with style 121 (ODBC canonical: yyyy-mm-dd hh:mi:ss.mmm)
                // which is language-independent and handles fractional seconds
                return "CONVERT(DATETIME, '" + expr.getStringValue() + "', 121)";
            } else if (expr.getType().isDate() || expr.getType().isDateV2()) {
                // Use CONVERT with style 23 (ISO8601: yyyy-mm-dd)
                // which is language-independent
                return "CONVERT(DATE, '" + expr.getStringValue() + "', 23)";
            }
        }
        return expr.accept(ExprToExternalSqlVisitor.INSTANCE,
                new ToSqlParams(false, true, TableType.JDBC_EXTERNAL_TABLE, tbl));
    }

    private static boolean containsNullLiteral(Expr expr) {
        List<NullLiteral> nullExprList = Lists.newArrayList();
        expr.collect(NullLiteral.class, nullExprList);
        return !nullExprList.isEmpty();
    }

    private static boolean containsCastExpr(Expr expr) {
        List<CastExpr> castExprList = Lists.newArrayList();
        expr.collect(CastExpr.class, castExprList);
        return !castExprList.isEmpty();
    }
}
