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

package org.apache.doris.datasource.odbc.source;

import org.apache.doris.analysis.Analyzer;
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
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalScanNode;
import org.apache.doris.datasource.jdbc.source.JdbcScanNode;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsRecursiveDerive;
import org.apache.doris.statistics.query.StatsDelta;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TOdbcScanNode;
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
import java.util.stream.Collectors;

/**
 * Full scan of an ODBC table.
 */
public class OdbcScanNode extends ExternalScanNode {
    private static final Logger LOG = LogManager.getLogger(OdbcScanNode.class);

    private final List<String> columns = new ArrayList<String>();
    private final List<String> filters = new ArrayList<String>();
    private String tblName;
    private String connectString;
    private TOdbcTableType odbcType;

    private OdbcTable tbl;

    /**
     * Constructs node to scan given data files of table 'tbl'.
     */
    public OdbcScanNode(PlanNodeId id, TupleDescriptor desc, OdbcTable tbl) {
        super(id, desc, "SCAN ODBC", StatisticalType.ODBC_SCAN_NODE, false);
        connectString = tbl.getConnectString();
        odbcType = tbl.getOdbcTableType();
        tblName = JdbcTable.databaseProperName(odbcType, tbl.getOdbcTableName());
        this.tbl = tbl;
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

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        return helper.addValue(super.debugString()).toString();
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        // Convert predicates to Odbc columns and filters.
        createOdbcColumns();
        createOdbcFilters();
        createScanRangeLocations();
    }

    @Override
    public void finalizeForNereids() throws UserException {
        createOdbcColumns();
        createOdbcFilters();
        createScanRangeLocations();
    }

    @Override
    public void updateRequiredSlots(PlanTranslatorContext context, Set<SlotId> requiredByProjectSlotIdSet)
            throws UserException {
        createOdbcColumns();
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        scanRangeLocations = Lists.newArrayList(createSingleScanRangeLocations(backendPolicy));
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(tblName).append("\n");
        output.append(prefix).append("TABLE TYPE: ").append(odbcType.toString()).append("\n");
        if (detailLevel == TExplainLevel.BRIEF) {
            return output.toString();
        }
        output.append(prefix).append("QUERY: ").append(getOdbcQueryStr()).append("\n");
        if (!conjuncts.isEmpty()) {
            Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
            output.append(prefix).append("PREDICATES: ").append(expr.toSql()).append("\n");
        }
        if (useTopnFilter()) {
            String topnFilterSources = String.join(",",
                    topnFilterSortNodes.stream()
                            .map(node -> node.getId().asInt() + "").collect(Collectors.toList()));
            output.append(prefix).append("TOPN OPT:").append(topnFilterSources).append("\n");
        }
        return output.toString();
    }

    // only all conjuncts be pushed down as filter, we can
    // push down limit operation to ODBC table
    private boolean shouldPushDownLimit() {
        return limit != -1 && conjuncts.isEmpty();
    }

    private String getOdbcQueryStr() {
        StringBuilder sql = new StringBuilder("SELECT ");

        // Oracle use the where clause to do top n
        if (shouldPushDownLimit() && odbcType == TOdbcTableType.ORACLE) {
            filters.add("ROWNUM <= " + limit);
        }

        // MSSQL use select top to do top n
        if (shouldPushDownLimit() && odbcType == TOdbcTableType.SQLSERVER) {
            sql.append("TOP " + limit + " ");
        }

        sql.append(Joiner.on(", ").join(columns));
        sql.append(" FROM ").append(tblName);

        if (!filters.isEmpty()) {
            sql.append(" WHERE (");
            sql.append(Joiner.on(") AND (").join(filters));
            sql.append(")");
        }

        // Other DataBase use limit do top n
        if (shouldPushDownLimit()
                && (odbcType == TOdbcTableType.MYSQL
                || odbcType == TOdbcTableType.POSTGRESQL
                || odbcType == TOdbcTableType.MONGODB)) {
            sql.append(" LIMIT ").append(limit);
        }

        return sql.toString();
    }

    private void createOdbcColumns() {
        columns.clear();
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            Column col = slot.getColumn();
            columns.add(JdbcTable.databaseProperName(odbcType, col.getName()));
        }
        // this happens when count(*)
        if (columns.isEmpty()) {
            columns.add("*");
        }
    }

    // We convert predicates of the form <slotref> op <constant> to Odbc filters
    private void createOdbcFilters() {
        if (conjuncts.isEmpty()) {
            return;
        }
        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr.collectList(conjuncts, SlotRef.class, slotRefs);
        ExprSubstitutionMap sMap = new ExprSubstitutionMap();
        for (SlotRef slotRef : slotRefs) {
            SlotRef tmpRef = (SlotRef) slotRef.clone();
            tmpRef.setTblName(null);
            tmpRef.setLabel(JdbcTable.databaseProperName(odbcType, tmpRef.getColumnName()));
            sMap.put(slotRef, tmpRef);
        }
        ArrayList<Expr> odbcConjuncts = Expr.cloneList(conjuncts, sMap);
        for (Expr p : odbcConjuncts) {
            if (shouldPushDownConjunct(odbcType, p)) {
                String filter = JdbcScanNode.conjunctExprToString(odbcType, p, tbl);
                filters.add(filter);
                conjuncts.remove(p);
            }
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.ODBC_SCAN_NODE;

        TOdbcScanNode odbcScanNode = new TOdbcScanNode();
        odbcScanNode.setTupleId(desc.getId().asInt());
        odbcScanNode.setTableName(tblName);
        odbcScanNode.setConnectString(connectString);
        odbcScanNode.setQueryString(getOdbcQueryStr());

        msg.odbc_scan_node = odbcScanNode;
        super.toThrift(msg);
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
    public StatsDelta genStatsDelta() throws AnalysisException {
        return new StatsDelta(Env.getCurrentEnv().getCurrentCatalog().getId(),
                Env.getCurrentEnv().getCurrentCatalog().getDbOrAnalysisException(tbl.getQualifiedDbName()).getId(),
                tbl.getId(), -1L);
    }

    @Override
    public int getNumInstances() {
        return ConnectContext.get().getSessionVariable().getParallelExecInstanceNum();
    }

    public static boolean shouldPushDownConjunct(TOdbcTableType tableType, Expr expr) {
        if (!tableType.equals(TOdbcTableType.MYSQL)) {
            List<FunctionCallExpr> fnExprList = Lists.newArrayList();
            expr.collect(FunctionCallExpr.class, fnExprList);
            if (!fnExprList.isEmpty()) {
                return false;
            }
        }
        if (ConnectContext.get() != null) {
            return ConnectContext.get().getSessionVariable().enableExtFuncPredPushdown;
        } else {
            return true;
        }
    }
}
