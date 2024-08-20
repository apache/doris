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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalScanNode;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsRecursiveDerive;
import org.apache.doris.statistics.query.StatsDelta;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TJdbcScanNode;
import org.apache.doris.thrift.TOdbcTableType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JdbcScanNode extends ExternalScanNode {
    private static final Logger LOG = LogManager.getLogger(JdbcScanNode.class);

    private final List<String> columns = new ArrayList<String>();
    private final List<String> filters = new ArrayList<String>();
    private final List<Expr> pushedDownConjuncts = new ArrayList<>();
    private String tableName;
    private TOdbcTableType jdbcType;
    private String graphQueryString = "";
    private boolean isTableValuedFunction = false;
    private String query = "";

    private JdbcTable tbl;

    public JdbcScanNode(PlanNodeId id, TupleDescriptor desc, boolean isJdbcExternalTable) {
        super(id, desc, "JdbcScanNode", StatisticalType.JDBC_SCAN_NODE, false);
        if (isJdbcExternalTable) {
            JdbcExternalTable jdbcExternalTable = (JdbcExternalTable) (desc.getTable());
            tbl = jdbcExternalTable.getJdbcTable();
        } else {
            tbl = (JdbcTable) (desc.getTable());
        }
        jdbcType = tbl.getJdbcTableType();
        tableName = tbl.getProperRemoteFullTableName(jdbcType);
    }

    public JdbcScanNode(PlanNodeId id, TupleDescriptor desc, boolean isTableValuedFunction, String query) {
        super(id, desc, "JdbcScanNode", StatisticalType.JDBC_SCAN_NODE, false);
        this.isTableValuedFunction = isTableValuedFunction;
        this.query = query;
        tbl = (JdbcTable) desc.getTable();
        jdbcType = tbl.getJdbcTableType();
        tableName = tbl.getExternalTableName();
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
    }

    /**
     * Used for Nereids. Should NOT use this function in anywhere else.
     */
    @Override
    public void init() throws UserException {
        super.init();
        numNodes = numNodes <= 0 ? 1 : numNodes;
        StatsRecursiveDerive.getStatsRecursiveDerive().statsRecursiveDerive(this);
        cardinality = (long) statsDeriveResult.getRowCount();
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
            slotRef1.setTblName(null);
            slotRef1.setLabel(JdbcTable.properNameWithRemoteName(jdbcType, slotRef1.getColumnName()));
            sMap.put(slotRef, slotRef1);
        }

        ArrayList<Expr> conjunctsList = Expr.cloneList(conjuncts, sMap);
        List<String> errors = Lists.newArrayList();
        List<Expr> pushDownConjuncts = collectConjunctsToPushDown(conjunctsList, errors);

        for (Expr individualConjunct : pushDownConjuncts) {
            String filter = conjunctExprToString(jdbcType, individualConjunct, tbl);
            filters.add(filter);
            pushedDownConjuncts.add(individualConjunct);
        }
    }

    private List<Expr> collectConjunctsToPushDown(List<Expr> conjunctsList, List<String> errors) {
        List<Expr> pushDownConjuncts = new ArrayList<>();
        for (Expr p : conjunctsList) {
            if (shouldPushDownConjunct(jdbcType, p)) {
                List<Expr> individualConjuncts = p.getConjuncts();
                for (Expr individualConjunct : individualConjuncts) {
                    Expr newp = JdbcFunctionPushDownRule.processFunctions(jdbcType, individualConjunct, errors);
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
            if (!slot.isMaterialized()) {
                continue;
            }
            Column col = slot.getColumn();
            columns.add(tbl.getProperRemoteColumnName(jdbcType, col.getName()));
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
                || jdbcType == TOdbcTableType.OCEANBASE)) {
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
            output.append(prefix).append("QUERY: ").append(query).append("\n");
        } else {
            output.append(prefix).append("TABLE: ").append(tableName).append("\n");
            if (detailLevel == TExplainLevel.BRIEF) {
                return output.toString();
            }
            output.append(prefix).append("QUERY: ").append(getJdbcQueryStr()).append("\n");
            if (!conjuncts.isEmpty()) {
                Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
                output.append(prefix).append("PREDICATES: ").append(expr.toSql()).append("\n");
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
    public void finalize(Analyzer analyzer) throws UserException {
        // Convert predicates to Jdbc columns and filters.
        createJdbcColumns();
        createJdbcFilters();
        createScanRangeLocations();
    }

    @Override
    public void finalizeForNereids() throws UserException {
        createJdbcColumns();
        createJdbcFilters();
        createScanRangeLocations();
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        scanRangeLocations = Lists.newArrayList(createSingleScanRangeLocations(backendPolicy));
    }

    @Override
    public void computeStats(Analyzer analyzer) throws UserException {
        super.computeStats(analyzer);
        // even if current node scan has no data,at least on backend will be assigned when the fragment actually execute
        numNodes = numNodes <= 0 ? 1 : numNodes;

        StatsRecursiveDerive.getStatsRecursiveDerive().statsRecursiveDerive(this);
        cardinality = (long) statsDeriveResult.getRowCount();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.JDBC_SCAN_NODE;
        msg.jdbc_scan_node = new TJdbcScanNode();
        msg.jdbc_scan_node.setTupleId(desc.getId().asInt());
        msg.jdbc_scan_node.setTableName(tableName);
        if (isTableValuedFunction) {
            msg.jdbc_scan_node.setQueryString(query);
        } else {
            msg.jdbc_scan_node.setQueryString(getJdbcQueryStr());
        }
        msg.jdbc_scan_node.setTableType(jdbcType);
        super.toThrift(msg);
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        return helper.addValue(super.debugString()).toString();
    }

    @Override
    public int getNumInstances() {
        return ConnectContext.get().getSessionVariable().getParallelExecInstanceNum();
    }

    @Override
    public StatsDelta genStatsDelta() throws AnalysisException {
        return new StatsDelta(Env.getCurrentEnv().getCurrentCatalog().getId(),
                Env.getCurrentEnv().getCurrentCatalog().getDbOrAnalysisException(tbl.getQualifiedDbName()).getId(),
                tbl.getId(), -1L);
    }

    private static boolean shouldPushDownConjunct(TOdbcTableType tableType, Expr expr) {
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
        if (expr.contains(DateLiteral.class) && expr instanceof BinaryPredicate) {
            ArrayList<Expr> children = expr.getChildren();
            String filter = children.get(0).toExternalSql(TableType.JDBC_EXTERNAL_TABLE, tbl);
            filter += " " + ((BinaryPredicate) expr).getOp().toString() + " ";

            if (tableType.equals(TOdbcTableType.ORACLE)) {
                filter += handleOracleDateFormat(children.get(1), tbl);
            } else if (tableType.equals(TOdbcTableType.TRINO) || tableType.equals(TOdbcTableType.PRESTO)) {
                filter += handleTrinoDateFormat(children.get(1), tbl);
            } else {
                filter += children.get(1).toExternalSql(TableType.JDBC_EXTERNAL_TABLE, tbl);
            }

            return filter;
        }

        // Only for old planner
        if (expr.contains(BoolLiteral.class) && "1".equals(expr.getStringValue()) && expr.getChildren().isEmpty()) {
            return "1 = 1";
        }

        return expr.toExternalSql(TableType.JDBC_EXTERNAL_TABLE, tbl);
    }

    private static String handleOracleDateFormat(Expr expr, TableIf tbl) {
        if (expr.isConstant()
                && (expr.getType().isDatetime() || expr.getType().isDatetimeV2())) {
            return "to_date('" + expr.getStringValue() + "', 'yyyy-mm-dd hh24:mi:ss')";
        }
        return expr.toExternalSql(TableType.JDBC_EXTERNAL_TABLE, tbl);
    }

    private static String handleTrinoDateFormat(Expr expr, TableIf tbl) {
        if (expr.isConstant()) {
            if (expr.getType().isDate() || expr.getType().isDateV2()) {
                return "date '" + expr.getStringValue() + "'";
            } else if (expr.getType().isDatetime() || expr.getType().isDatetimeV2()) {
                return "timestamp '" + expr.getStringValue() + "'";
            }
        }
        return expr.toExternalSql(TableType.JDBC_EXTERNAL_TABLE, tbl);
    }
}
