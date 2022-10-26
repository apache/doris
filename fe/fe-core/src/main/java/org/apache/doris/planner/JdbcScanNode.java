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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.common.UserException;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsRecursiveDerive;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TJdbcScanNode;
import org.apache.doris.thrift.TOdbcTableType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class JdbcScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(JdbcScanNode.class);

    private final List<String> columns = new ArrayList<String>();
    private final List<String> filters = new ArrayList<String>();
    private String tableName;
    private TOdbcTableType jdbcType;

    public JdbcScanNode(PlanNodeId id, TupleDescriptor desc, JdbcTable tbl) {
        super(id, desc, "SCAN JDBC", StatisticalType.JDBC_SCAN_NODE);
        jdbcType = tbl.getJdbcTableType();
        tableName = OdbcTable.databaseProperName(jdbcType, tbl.getJdbcTable());
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        computeStats(analyzer);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return null;
    }

    private void createJdbcFilters(Analyzer analyzer) {
        if (conjuncts.isEmpty()) {
            return;
        }

        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr.collectList(conjuncts, SlotRef.class, slotRefs);
        ExprSubstitutionMap sMap = new ExprSubstitutionMap();
        for (SlotRef slotRef : slotRefs) {
            SlotRef slotRef1 = (SlotRef) slotRef.clone();
            slotRef1.setTblName(null);
            slotRef1.setLabel(OdbcTable.databaseProperName(jdbcType, slotRef1.getColumnName()));
            sMap.put(slotRef, slotRef1);
        }

        ArrayList<Expr> conjunctsList = Expr.cloneList(conjuncts, sMap);
        for (Expr p : conjunctsList) {
            if (OdbcScanNode.shouldPushDownConjunct(jdbcType, p)) {
                String filter = p.toMySql();
                filters.add(filter);
                conjuncts.remove(p);
            }
        }
    }

    private void createJdbcColumns(Analyzer analyzer) {
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            Column col = slot.getColumn();
            columns.add(OdbcTable.databaseProperName(jdbcType, col.getName()));
        }
        if (0 == columns.size()) {
            columns.add("*");
        }
    }

    private boolean shouldPushDownLimit() {
        return limit != -1 && conjuncts.isEmpty();
    }

    private String getJdbcQueryStr() {
        StringBuilder sql = new StringBuilder("SELECT ");

        // Oracle use the where clause to do top n
        if (shouldPushDownLimit() && jdbcType == TOdbcTableType.ORACLE) {
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
                || jdbcType == TOdbcTableType.MONGODB)) {
            sql.append(" LIMIT ").append(limit);
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
        return output.toString();
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        // Convert predicates to Jdbc columns and filters.
        createJdbcColumns(analyzer);
        createJdbcFilters(analyzer);
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
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        return helper.addValue(super.debugString()).toString();
    }

    @Override
    public int getNumInstances() {
        return 1;
    }
}
