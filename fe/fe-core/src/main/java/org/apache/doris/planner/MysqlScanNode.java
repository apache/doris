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
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TMySQLScanNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRangeLocations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Full scan of an MySQL table.
 */
public class MysqlScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(MysqlScanNode.class);

    private final List<String> columns = new ArrayList<String>();
    private final List<String> filters = new ArrayList<String>();
    private String tblName;

    /**
     * Constructs node to scan given data files of table 'tbl'.
     */
    public MysqlScanNode(PlanNodeId id, TupleDescriptor desc, MysqlTable tbl) {
        super(id, desc, "SCAN MYSQL");
        tblName = "`" + tbl.getMysqlTableName() + "`";
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        computeStats(analyzer);
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        return helper.addValue(super.debugString()).toString();
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        // Convert predicates to MySQL columns and filters.
        createMySQLColumns(analyzer);
        createMySQLFilters(analyzer);
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(tblName).append("\n");
        if (detailLevel == TExplainLevel.BRIEF) {
            return output.toString();
        }
        output.append(prefix).append("Query: ").append(getMysqlQueryStr()).append("\n");
        return output.toString();
    }

    private String getMysqlQueryStr() {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(Joiner.on(", ").join(columns));
        sql.append(" FROM ").append(tblName);

        if (!filters.isEmpty()) {
            sql.append(" WHERE (");
            sql.append(Joiner.on(") AND (").join(filters));
            sql.append(")");
        }

        if (limit != -1) {
            sql.append(" LIMIT " + limit);
        }

        return sql.toString();
    }

    private void createMySQLColumns(Analyzer analyzer) {
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            Column col = slot.getColumn();
            columns.add("`" + col.getName() + "`");
        }
        // this happens when count(*)
        if (0 == columns.size()) {
            columns.add("*");
        }
    }

    // We convert predicates of the form <slotref> op <constant> to MySQL filters
    private void createMySQLFilters(Analyzer analyzer) {
        if (conjuncts.isEmpty()) {
            return;

        }
        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr.collectList(conjuncts, SlotRef.class, slotRefs);
        ExprSubstitutionMap sMap = new ExprSubstitutionMap();
        for (SlotRef slotRef : slotRefs) {
            SlotRef tmpRef = (SlotRef) slotRef.clone();
            tmpRef.setTblName(null);

            sMap.put(slotRef, tmpRef);
        }
        ArrayList<Expr> mysqlConjuncts = Expr.cloneList(conjuncts, sMap);
        for (Expr p : mysqlConjuncts) {
            filters.add(p.toMySql());
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.MYSQL_SCAN_NODE;
        msg.mysql_scan_node = new TMySQLScanNode(desc.getId().asInt(), tblName, columns, filters);
    }

    /**
     * We query MySQL Meta to get request's data location
     * extra result info will pass to backend ScanNode
     */
    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return null;
    }

    @Override
    public int getNumInstances() {
        return 1;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
        // even if current node scan has no data,at least on backend will be assigned when the fragment actually execute
        numNodes = numNodes <= 0 ? 1 : numNodes;
        // this is just to avoid mysql scan node's cardinality being -1. So that we can calculate the join cost
        // normally.
        // We assume that the data volume of all mysql tables is very small, so set cardinality directly to 1.
        cardinality = cardinality == -1 ? 1 : cardinality;
    }
}
