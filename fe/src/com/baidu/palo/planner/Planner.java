// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.planner;

import com.baidu.palo.analysis.AnalyticInfo;
import com.baidu.palo.analysis.Analyzer;
import com.baidu.palo.analysis.Expr;
import com.baidu.palo.analysis.InsertStmt;
import com.baidu.palo.analysis.QueryStmt;
import com.baidu.palo.analysis.SelectStmt;
import com.baidu.palo.analysis.SlotDescriptor;
import com.baidu.palo.analysis.SlotId;
import com.baidu.palo.analysis.StatementBase;
import com.baidu.palo.analysis.TupleDescriptor;
import com.baidu.palo.analysis.TupleId;
import com.baidu.palo.catalog.PrimitiveType;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.NotImplementedException;
import com.baidu.palo.thrift.TExplainLevel;
import com.baidu.palo.thrift.TQueryOptions;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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
        return singleNodePlanner.getScanNodes();
    }

    public void plan(StatementBase queryStmt, Analyzer analyzer, TQueryOptions queryOptions)
            throws NotImplementedException, InternalException, AnalysisException {
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
                    if (PrimitiveType.DECIMAL == expr.getType().getPrimitiveType()
                            && slotList.contains(slotDesc.getId())
                            && PrimitiveType.DECIMAL == slotDesc.getType().getPrimitiveType()
                            && null != slotDesc.getColumn()) {
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
        return str.toString();
    }

    /**
     * Create plan fragments for an analyzed statement, given a set of execution options. The fragments are returned in
     * a list such that element i of that list can only consume output of the following fragments j > i.
     */
    public void createPlanFragments(StatementBase statment, Analyzer analyzer, TQueryOptions queryOptions)
            throws NotImplementedException, InternalException, AnalysisException {
        QueryStmt queryStmt;
        if (statment instanceof InsertStmt) {
            queryStmt = ((InsertStmt) statment).getQueryStmt();
        } else {
            queryStmt = (QueryStmt) statment;
        }

        plannerContext = new PlannerContext(analyzer, queryStmt, queryOptions, statment);
        singleNodePlanner = new SingleNodePlanner(plannerContext);
        PlanNode singleNodePlan = singleNodePlanner.createSingleNodePlan();

        List<Expr> resultExprs = queryStmt.getResultExprs();
        if (statment instanceof InsertStmt) {
            InsertStmt insertStmt = (InsertStmt) statment;
            if (insertStmt.getOlapTuple() != null) {
                singleNodePlan = new OlapRewriteNode(plannerContext.getNextNodeId(), singleNodePlan, insertStmt);
                singleNodePlan.init(analyzer);
                resultExprs = insertStmt.getResultExprs();
            }
        }

        // compute referenced slots before calling computeMemLayout()
        analyzer.markRefdSlots(analyzer, singleNodePlan, resultExprs, null);

        setResultExprScale(analyzer, queryStmt.getResultExprs());

        // compute mem layout *before* finalize(); finalize() may reference
        // TupleDescriptor.avgSerializedSize
        analyzer.getDescTbl().computeMemLayout();
        singleNodePlan.finalize(analyzer);
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

        PlanFragment rootFragment = fragments.get(fragments.size() - 1);
        if (statment instanceof InsertStmt) {
            InsertStmt insertStmt = (InsertStmt) statment;
            rootFragment = distributedPlanner.createInsertFragment(rootFragment, insertStmt, fragments);
            rootFragment.setSink(insertStmt.createDataSink());
            ArrayList<Expr> exprs = ((InsertStmt) statment).getResultExprs();
            List<Expr> resExprs = Expr.substituteList(
                    exprs, rootFragment.getPlanRoot().getOutputSmap(), analyzer, false);
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

    /**
     * If there are unassigned conjuncts, returns a SelectNode on top of root that evaluate those conjuncts; otherwise
     * returns root unchanged.
     */
    private PlanNode addUnassignedConjuncts(Analyzer analyzer, PlanNode root)
            throws InternalException {
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
}
