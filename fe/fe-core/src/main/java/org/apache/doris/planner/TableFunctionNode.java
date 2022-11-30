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
import org.apache.doris.analysis.LateralViewRef;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsRecursiveDerive;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TTableFunctionNode;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TableFunctionNode extends PlanNode {
    private List<LateralViewRef> lateralViewRefs;
    private ArrayList<Expr> fnCallExprList;
    private List<TupleId> lateralViewTupleIds;

    // The output slot ids of TableFunctionNode
    // Only the slot whose id is in this list will be output by TableFunctionNode
    private List<SlotId> outputSlotIds = Lists.newArrayList();

    protected TableFunctionNode(PlanNodeId id, PlanNode inputNode, List<LateralViewRef> lateralViewRefs) {
        super(id, "TABLE FUNCTION NODE", StatisticalType.TABLE_FUNCTION_NODE);
        tupleIds.addAll(inputNode.getTupleIds());
        tblRefIds.addAll(inputNode.getTupleIds());
        tblRefIds.addAll(inputNode.getTblRefIds());
        lateralViewTupleIds = lateralViewRefs.stream().map(e -> e.getDesc().getId())
                .collect(Collectors.toList());
        tupleIds.addAll(lateralViewTupleIds);
        tblRefIds.addAll(lateralViewTupleIds);
        children.add(inputNode);
        this.lateralViewRefs = lateralViewRefs;
    }

    /**
     * This function is mainly used to calculate @outputSlotIds.
     * After the PlanNode executes the @fnCallExpr,
     * it needs to perform projection operation.
     * This function is used to calculate which columns should be projected.
     * The slot belongs to outputSlotIds should be retained after the projection is completed.
     * Slots in selectItems and unassigned predicates should be projected.
     * <p>
     * Case1: The slot belongs to selectItems. The outputSlotIds should include it.
     * For example:
     * Query: select k1, v1 from table lateral view explode_split(v1, ",") t1 as c1;
     * The outputSlots: [k1, v1, c1]
     * <p>
     * Case2: The slot belongs to where clause and the predicate has not been assigned.
     * Query: select k1 from table a lateral view explode_split(v1, ",") t1 as c1, table b where a.v1=b.v1;
     * The outputSlots: [a.k1, a.v1, t1.c1]
     * <p>
     * Case3: The slot neither is part of the unassigned predicate, nor appears in the selectItems.
     * Query: select k1 from table a lateral view explode_split(v1, ",") t1 as c1;
     * The outputSlots: [k1, c1]
     */
    // TODO(ml): Unified to projectplanner
    public void projectSlots(Analyzer analyzer, SelectStmt selectStmt) throws AnalysisException {
        // TODO(ml): Support project calculations that include aggregation and sorting in select stmt
        if ((selectStmt.hasAggInfo() || selectStmt.getSortInfo() != null || selectStmt.hasAnalyticInfo())
                && selectStmt.hasInlineView()) {
            // The query must be rewritten like TableFunctionPlanTest.aggColumnInOuterQuery()
            throw new AnalysisException("Please treat the query containing the lateral view as a inline view"
                    + "and extract your aggregation/sort/window functions to the outer query."
                    + "For example select sum(a) from (select a from table lateral view xxx) tmp1");
        }
        Set<SlotRef> outputSlotRef = Sets.newHashSet();
        // case1
        List<Expr> baseTblResultExprs = selectStmt.getResultExprs();
        for (Expr resultExpr : baseTblResultExprs) {
            // find all slotRef bound by tupleIds in resultExpr
            resultExpr.getSlotRefsBoundByTupleIds(tupleIds, outputSlotRef);

            // For vec engine while lateral view involves subquery
            Expr dst = outputSmap.get(resultExpr);
            if (dst != null) {
                dst.getSlotRefsBoundByTupleIds(tupleIds, outputSlotRef);
            }
        }
        // case2
        List<Expr> remainConjuncts = analyzer.getRemainConjuncts(tupleIds);
        for (Expr expr : remainConjuncts) {
            expr.getSlotRefsBoundByTupleIds(tupleIds, outputSlotRef);

            // For vec engine while lateral view involves subquery
            Expr dst = outputSmap.get(expr);
            if (dst != null) {
                dst.getSlotRefsBoundByTupleIds(tupleIds, outputSlotRef);
            }
        }
        // set output slot ids
        for (SlotRef slotRef : outputSlotRef) {
            outputSlotIds.add(slotRef.getSlotId());
        }
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        fnCallExprList = new ArrayList<>(lateralViewRefs.stream().map(e -> e.getFnExpr()).collect(Collectors.toList()));
        /*
        When the expression of the lateral view involves the column of the subquery,
        the column needs to be rewritten as the real column in the subquery through childrenSmap.
        Example:
          select e1 from (select a from t1) tmp1 lateral view explode_split(a, ",") tmp2 as e1
          Slot 'a' is originally linked to tuple 'tmp1'. <tmp1.a>
          But tmp1 is just a virtual and unreal inline view tuple.
          So we need to push down 'a' and hang it on the real tuple 't1'. <t1.a>
         */
        outputSmap = getCombinedChildSmap();
        fnCallExprList = Expr.substituteList(fnCallExprList, outputSmap, analyzer, false);
        // end

        computeStats(analyzer);
    }

    @Override
    protected void computeStats(Analyzer analyzer) throws UserException {
        super.computeStats(analyzer);

        StatsRecursiveDerive.getStatsRecursiveDerive().statsRecursiveDerive(this);
        cardinality = (long) statsDeriveResult.getRowCount();
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("table function: ");
        for (Expr fnExpr : fnCallExprList) {
            output.append(fnExpr.toSql()).append(" ");
        }
        output.append("\n");

        output.append(prefix).append("lateral view tuple id: ");
        for (TupleId tupleId : lateralViewTupleIds) {
            output.append(tupleId.asInt()).append(" ");
        }
        output.append("\n");

        if (detailLevel == TExplainLevel.BRIEF) {
            output.append(prefix).append(String.format("cardinality=%,d", cardinality)).append("\n");
            return output.toString();
        }

        output.append(prefix).append("output slot id: ");
        for (SlotId slotId : outputSlotIds) {
            output.append(slotId.asInt()).append(" ");
        }
        output.append("\n");

        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(
                    getExplainString(conjuncts)).append("\n");
        }
        output.append(prefix).append(String.format("cardinality=%,d", cardinality)).append("\n");
        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.TABLE_FUNCTION_NODE;
        msg.table_function_node = new TTableFunctionNode();
        msg.table_function_node.setFnCallExprList(Expr.treesToThrift(fnCallExprList));
        for (SlotId slotId : outputSlotIds) {
            msg.table_function_node.addToOutputSlotIds(slotId.asInt());
        }
    }
}
