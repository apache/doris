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

package org.apache.doris.planner.external.jdbc;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.external.JdbcExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.ExternalScanNode;
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
import java.util.Set;

public class JdbcScanNode extends ExternalScanNode {
    private static final Logger LOG = LogManager.getLogger(JdbcScanNode.class);

    private final List<String> columns = new ArrayList<String>();
    private final List<String> filters = new ArrayList<String>();
    private String tableName;
    private TOdbcTableType jdbcType;
    private String graphQueryString = "";

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
        tableName = tbl.getProperRealFullTableName(jdbcType);
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        getGraphQueryString();
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

    private boolean isNebula() {
        return jdbcType == TOdbcTableType.NEBULA;
    }

    private void getGraphQueryString() {
        if (!isNebula()) {
            return;
        }
        for (Expr expr : conjuncts) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
            if ("g".equals(functionCallExpr.getFnName().getFunction())) {
                graphQueryString = functionCallExpr.getChild(0).getStringValue();
                break;
            }
        }
        // clean conjusts cause graph sannnode no need conjuncts
        conjuncts = Lists.newArrayList();
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
            slotRef1.setLabel(JdbcTable.properNameWithRealName(jdbcType, slotRef1.getColumnName()));
            sMap.put(slotRef, slotRef1);
        }

        ArrayList<Expr> conjunctsList = Expr.cloneList(conjuncts, sMap);
        List<String> errors = Lists.newArrayList();
        List<Expr> pushDownConjuncts = collectConjunctsToPushDown(conjunctsList, errors);

        for (Expr individualConjunct : pushDownConjuncts) {
            String filter = conjunctExprToString(jdbcType, individualConjunct);
            filters.add(filter);
            conjuncts.remove(individualConjunct);
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
            columns.add(JdbcTable.databaseProperName(jdbcType, col.getName()));
        }
        if (0 == columns.size()) {
            columns.add("*");
        }
    }

    private boolean shouldPushDownLimit() {
        return limit != -1 && conjuncts.isEmpty();
    }

    private String getJdbcQueryStr() {
        if (isNebula()) {
            return graphQueryString;
        }
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
                && ConnectContext.get().getSessionVariable().jdbcClickhouseQueryFinal) {
            sql.append(" SETTINGS final = 1");
        }
        return sql.toString();
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(tableName).append("\n");
        if (detailLevel == TExplainLevel.BRIEF) {
            return output.toString();
        }
        output.append(prefix).append("QUERY: ").append(getJdbcQueryStr()).append("\n");
        if (!conjuncts.isEmpty()) {
            Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
            output.append(prefix).append("PREDICATES: ").append(expr.toSql()).append("\n");
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
    public void updateRequiredSlots(PlanTranslatorContext context, Set<SlotId> requiredByProjectSlotIdSet)
            throws UserException {
        createJdbcColumns();
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
        msg.jdbc_scan_node.setQueryString(getJdbcQueryStr());
        msg.jdbc_scan_node.setTableType(jdbcType);
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        return helper.addValue(super.debugString()).toString();
    }

    @Override
    public int getNumInstances() {
        return ConnectContext.get().getSessionVariable().getEnablePipelineEngine()
                ? ConnectContext.get().getSessionVariable().getParallelExecInstanceNum() : 1;
    }

    @Override
    public StatsDelta genStatsDelta() throws AnalysisException {
        return new StatsDelta(Env.getCurrentEnv().getCurrentCatalog().getId(),
                Env.getCurrentEnv().getCurrentCatalog().getDbOrAnalysisException(tbl.getQualifiedDbName()).getId(),
                tbl.getId(), -1L);
    }

    private static boolean shouldPushDownConjunct(TOdbcTableType tableType, Expr expr) {
        if (containsFunctionCallExpr(expr)) {
            if (tableType.equals(TOdbcTableType.MYSQL) || tableType.equals(TOdbcTableType.CLICKHOUSE)) {
                return Config.enable_func_pushdown;
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

    public static String conjunctExprToString(TOdbcTableType tableType, Expr expr) {
        if (expr instanceof CompoundPredicate) {
            StringBuilder result = new StringBuilder();
            CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
            for (Expr child : compoundPredicate.getChildren()) {
                result.append(conjunctExprToString(tableType, child));
                result.append(" ").append(compoundPredicate.getOp().toString()).append(" ");
            }
            // Remove the last operator
            result.setLength(result.length() - compoundPredicate.getOp().toString().length() - 2);
            return result.toString();
        }

        if (expr.contains(DateLiteral.class) && expr instanceof BinaryPredicate) {
            ArrayList<Expr> children = expr.getChildren();
            String filter = children.get(0).toMySql();
            filter += " " + ((BinaryPredicate) expr).getOp().toString() + " ";

            if (tableType.equals(TOdbcTableType.ORACLE)) {
                filter += handleOracleDateFormat(children.get(1));
            } else if (tableType.equals(TOdbcTableType.TRINO) || tableType.equals(TOdbcTableType.PRESTO)) {
                filter += handleTrinoDateFormat(children.get(1));
            } else {
                filter += children.get(1).toMySql();
            }

            return filter;
        }

        if (expr.contains(SlotRef.class) && expr instanceof BinaryPredicate) {
            ArrayList<Expr> children = expr.getChildren();
            String filter;
            if (children.get(0) instanceof SlotRef) {
                filter = "`" + children.get(0).toMySql() + "`";
            } else {
                filter = children.get(0).toMySql();
            }
            filter += " " + ((BinaryPredicate) expr).getOp().toString() + " ";
            filter += children.get(1).toMySql();
            return filter;
        }

        // only for old planner
        if (expr.contains(BoolLiteral.class) && "1".equals(expr.getStringValue()) && expr.getChildren().isEmpty()) {
            return "1 = 1";
        }

        return expr.toMySql();
    }

    private static String handleOracleDateFormat(Expr expr) {
        if (expr.isConstant()
                && (expr.getType().isDatetime() || expr.getType().isDatetimeV2())) {
            return "to_date('" + expr.getStringValue() + "', 'yyyy-mm-dd hh24:mi:ss')";
        }
        return expr.toMySql();
    }

    private static String handleTrinoDateFormat(Expr expr) {
        if (expr.isConstant()) {
            if (expr.getType().isDate() || expr.getType().isDateV2()) {
                return "date '" + expr.getStringValue() + "'";
            } else if (expr.getType().isDatetime() || expr.getType().isDatetimeV2()) {
                return "timestamp '" + expr.getStringValue() + "'";
            }
        }
        return expr.toMySql();
    }
}
