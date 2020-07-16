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
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TQueryOptions;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The planner is responsible for turning parse trees into plan fragments that can be shipped off to backends for
 * execution.
 */
public class Planner {
    private static final Logger LOG = LogManager.getLogger(Planner.class);

    private boolean isBlockQuery = false;

    private ArrayList<PlanFragment> fragments = Lists.newArrayList();

    private PlannerContext plannerContext;
    private SingleNodePlanner singleNodePlanner;
    private DistributedPlanner distributedPlanner;

    public boolean isBlockQuery() {
        return isBlockQuery;
    }

    public List<PlanFragment> getFragments() {
        return fragments;
    }

    public List<ScanNode> getScanNodes() {
        if (singleNodePlanner == null) {
            return Lists.newArrayList();
        }
        return singleNodePlanner.getScanNodes();
    }

    public void plan(StatementBase queryStmt, Analyzer analyzer, TQueryOptions queryOptions)
            throws UserException {
        createPlanFragments(queryStmt, analyzer, queryOptions);
    }

    /**
     */
    private void setResultExprScale(Analyzer analyzer, ArrayList<Expr> outputExprs) {
        for (TupleDescriptor tupleDesc : analyzer.getDescTbl().getTupleDescs()) {
            for (SlotDescriptor slotDesc : tupleDesc.getSlots()) {
                for (Expr expr : outputExprs) {
                    List<SlotId> slotList = Lists.newArrayList();
                    expr.getIds(null, slotList);
                    if (PrimitiveType.DECIMAL != expr.getType().getPrimitiveType() && 
                            PrimitiveType.DECIMALV2 != expr.getType().getPrimitiveType()) {
                        continue;
                            }

                    if (PrimitiveType.DECIMAL != slotDesc.getType().getPrimitiveType() &&
                            PrimitiveType.DECIMALV2 != slotDesc.getType().getPrimitiveType()) {
                        continue;
                            }

                    if (slotList.contains(slotDesc.getId()) && null != slotDesc.getColumn()) {
                        // TODO output scale
                        // int outputScale = slotDesc.getColumn().getType().getScale();
                        int outputScale = 10;
                        if (outputScale >= 0) {
                            if (outputScale > expr.getOutputScale()) {
                                expr.setOutputScale(outputScale);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Return combined explain string for all plan fragments.
     */
    public String getExplainString(List<PlanFragment> fragments, TExplainLevel explainLevel) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < fragments.size(); ++i) {
            PlanFragment fragment = fragments.get(i);
            if (i > 0) {
                // a blank line between plan fragments
                str.append("\n");
            }
            str.append("PLAN FRAGMENT " + i + "\n");
            str.append(fragment.getExplainString(explainLevel));
        }
        if (explainLevel == TExplainLevel.VERBOSE) {
            str.append(plannerContext.getRootAnalyzer().getDescTbl().getExplainString());
        }
        return str.toString();
    }

    /**
     * Create plan fragments for an analyzed statement, given a set of execution options. The fragments are returned in
     * a list such that element i of that list can only consume output of the following fragments j > i.
     */
    public void createPlanFragments(StatementBase statement, Analyzer analyzer, TQueryOptions queryOptions)
            throws UserException {
        QueryStmt queryStmt;
        if (statement instanceof InsertStmt) {
            queryStmt = ((InsertStmt) statement).getQueryStmt();
        } else {
            queryStmt = (QueryStmt) statement;
        }

        plannerContext = new PlannerContext(analyzer, queryStmt, queryOptions, statement);
        singleNodePlanner = new SingleNodePlanner(plannerContext);
        PlanNode singleNodePlan = singleNodePlanner.createSingleNodePlan();

        if (statement instanceof InsertStmt) {
            InsertStmt insertStmt = (InsertStmt) statement;
            insertStmt.prepareExpressions();
        }

        // TODO chenhao16 , no used materialization work
        // compute referenced slots before calling computeMemLayout()
        //analyzer.markRefdSlots(analyzer, singleNodePlan, resultExprs, null);

        setResultExprScale(analyzer, queryStmt.getResultExprs());

        // compute mem layout *before* finalize(); finalize() may reference
        // TupleDescriptor.avgSerializedSize
        analyzer.getDescTbl().computeMemLayout();
        singleNodePlan.finalize(analyzer);
        // materialized view selector
        singleNodePlanner.selectMaterializedView(queryStmt, analyzer);
        if (queryOptions.num_nodes == 1) {
            // single-node execution; we're almost done
            singleNodePlan = addUnassignedConjuncts(analyzer, singleNodePlan);
            fragments.add(new PlanFragment(plannerContext.getNextFragmentId(), singleNodePlan,
                    DataPartition.UNPARTITIONED));
        } else {
            // all select query are unpartitioned.
            distributedPlanner = new DistributedPlanner(plannerContext);
            fragments = distributedPlanner.createPlanFragments(singleNodePlan);
        }

        // Optimize the transfer of query statistic when query does't contain limit.
        PlanFragment rootFragment = fragments.get(fragments.size() - 1);
        QueryStatisticsTransferOptimizer queryStatisticTransferOptimizer = new QueryStatisticsTransferOptimizer(rootFragment);
        queryStatisticTransferOptimizer.optimizeQueryStatisticsTransfer();

        if (statement instanceof InsertStmt) {
            InsertStmt insertStmt = (InsertStmt) statement;
            rootFragment = distributedPlanner.createInsertFragment(rootFragment, insertStmt, fragments);
            rootFragment.setSink(insertStmt.getDataSink());
            insertStmt.complete();
            ArrayList<Expr> exprs = ((InsertStmt) statement).getResultExprs();
            List<Expr> resExprs = Expr.substituteList(
                    exprs, rootFragment.getPlanRoot().getOutputSmap(), analyzer, true);
            rootFragment.setOutputExprs(resExprs);
        } else {
            List<Expr> resExprs = Expr.substituteList(queryStmt.getBaseTblResultExprs(),
                    rootFragment.getPlanRoot().getOutputSmap(), analyzer, false);
            rootFragment.setOutputExprs(resExprs);
        }
        // rootFragment.setOutputExprs(exprs);
        LOG.debug("finalize plan fragments");
        for (PlanFragment fragment : fragments) {
            fragment.finalize(analyzer, !queryOptions.allow_unsupported_formats);
        }
        Collections.reverse(fragments);

        setOutfileSink(queryStmt);

        if (queryStmt instanceof SelectStmt) {
            SelectStmt selectStmt = (SelectStmt) queryStmt;
            if (queryStmt.getSortInfo() != null || selectStmt.getAggInfo() != null) {
                isBlockQuery = true;
                LOG.debug("this is block query");
            } else {
                isBlockQuery = false;
                LOG.debug("this isn't block query");
            }
        }
    }

    // if query stmt has OUTFILE clause, set info into ResultSink.
    // this should be done after fragments are generated.
    private void setOutfileSink(QueryStmt queryStmt) {
        if (!queryStmt.hasOutFileClause()) {
            return;
        }
        PlanFragment topFragment = fragments.get(0);
        if (!(topFragment.getSink() instanceof ResultSink)) {
            return;
        }

        ResultSink resultSink = (ResultSink) topFragment.getSink();
        resultSink.setOutfileInfo(queryStmt.getOutFileClause());
    }

    /**
     * If there are unassigned conjuncts, returns a SelectNode on top of root that evaluate those conjuncts; otherwise
     * returns root unchanged.
     */
    private PlanNode addUnassignedConjuncts(Analyzer analyzer, PlanNode root)
            throws UserException {
        Preconditions.checkNotNull(root);
        // List<Expr> conjuncts = analyzer.getUnassignedConjuncts(root.getTupleIds());

        List<Expr> conjuncts = analyzer.getUnassignedConjuncts(root);
        if (conjuncts.isEmpty()) {
            return root;
        }
        // evaluate conjuncts in SelectNode
        SelectNode selectNode = new SelectNode(plannerContext.getNextNodeId(), root, conjuncts);
        selectNode.init(analyzer);
        Preconditions.checkState(selectNode.hasValidStats());
        return selectNode;
    }

    private static class QueryStatisticsTransferOptimizer {
        private final PlanFragment root;
        
        public QueryStatisticsTransferOptimizer(PlanFragment root) {
            Preconditions.checkNotNull(root);
            this.root = root;
        }

        public void optimizeQueryStatisticsTransfer() {
            optimizeQueryStatisticsTransfer(root, null);
        }

        private void optimizeQueryStatisticsTransfer(PlanFragment fragment, PlanFragment parent) {
            if (parent != null && hasLimit(parent.getPlanRoot(), fragment.getPlanRoot())) {
                fragment.setTransferQueryStatisticsWithEveryBatch(true);
            }
            for (PlanFragment child : fragment.getChildren()) {
                optimizeQueryStatisticsTransfer(child, fragment);
            }
        }

        // Check whether leaf node contains limit.
        private boolean hasLimit(PlanNode ancestor, PlanNode successor) {
            final List<PlanNode> exchangeNodes = Lists.newArrayList();
            collectExchangeNode(ancestor, exchangeNodes);
            for (PlanNode leaf : exchangeNodes) {
                if (leaf.getChild(0) == successor
                        && leaf.hasLimit()) {
                    return true;
                }
            }
            return false;
        }

        private void collectExchangeNode(PlanNode planNode, List<PlanNode> exchangeNodes) {
            if (planNode instanceof ExchangeNode) {
                exchangeNodes.add(planNode);
            }

            for (PlanNode child : planNode.getChildren()) {
                if (child instanceof ExchangeNode) {
                    exchangeNodes.add(child);
                } else {
                    collectExchangeNode(child, exchangeNodes);
                }
            }
        }
    }
}
