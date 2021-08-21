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
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TMergeNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.List;

/**
 * Node that merges the results of its child plans by materializing
 * the corresponding result exprs.
 * If no result exprs are specified for a child, it simply passes on the child's
 * results.
 */
public class MergeNode extends PlanNode {
    private final static Logger LOG = LogManager.getLogger(MergeNode.class);

    // Expr lists corresponding to the input query stmts.
    // The ith resultExprList belongs to the ith child.
    // All exprs are resolved to base tables.
    protected  List<List<Expr>> resultExprLists = Lists.newArrayList();

    // Expr lists that originate from constant select stmts.
    // We keep them separate from the regular expr lists to avoid null children.
    protected  List<List<Expr>> constExprLists = Lists.newArrayList();

    // Output tuple materialized by this node.
    protected final List<TupleDescriptor> tupleDescs = Lists.newArrayList();

    protected final TupleId tupleId;

    protected MergeNode(PlanNodeId id, MergeNode node) {
        super(id, node, "MERGE");
        this.tupleId = node.tupleId;
    }

    public void addConstExprList(List<Expr> exprs) {
        constExprLists.add(exprs);
    }

    public void addChild(PlanNode node, List<Expr> resultExprs) {
        addChild(node);
        resultExprLists.add(resultExprs);
        if (resultExprs != null) {
            // if we're materializing output, we can only do that into a single
            // output tuple
            Preconditions.checkState(tupleIds.size() == 1);
        }
    }

    /**
     * This must be called *after* addChild()/addConstExprList() because it recomputes
     * both of them.
     * The MergeNode doesn't need an smap: like a ScanNode, it materializes an "original"
     * tuple id
     */
    @Override
    public void init(Analyzer analyzer) throws UserException {
        assignConjuncts(analyzer);
        //computeMemLayout(analyzer);
        computeStats(analyzer);
        Preconditions.checkState(resultExprLists.size() == getChildren().size());

        // drop resultExprs/constExprs that aren't getting materialized (= where the
        // corresponding output slot isn't being materialized)
        List<SlotDescriptor> slots = analyzer.getDescTbl().getTupleDesc(tupleId).getSlots();
        List<List<Expr>> newResultExprLists = Lists.newArrayList();
        //
        //    for (int i = 0; i < resultExprLists.size(); ++i) {
        //    List<Expr> exprList = resultExprLists.get(i);
        //    List<Expr> newExprList = Lists.newArrayList();
        //    for (int j = 0; j < exprList.size(); ++j) {
        //        if (slots.get(j).isMaterialized()) newExprList.add(exprList.get(j));
        //    }
        //    newResultExprLists.add(newExprList);
        //    newResultExprLists.add(
        //            Expr.substituteList(newExprList, getChild(i).getOutputSmap(), analyzer, true));
        //   }
        //    resultExprLists = newResultExprLists;
        //
        Preconditions.checkState(resultExprLists.size() == getChildren().size());

        List<List<Expr>> newConstExprLists = Lists.newArrayList();
        for (List<Expr> exprList: constExprLists) {
            List<Expr> newExprList = Lists.newArrayList();
            for (int i = 0; i < exprList.size(); ++i) {
                if (slots.get(i).isMaterialized()) newExprList.add(exprList.get(i));
            }
            newConstExprLists.add(newExprList);
        }
        constExprLists = newConstExprLists;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
        if (!analyzer.safeIsEnableJoinReorderBasedCost()) {
            return;
        }
        cardinality = constExprLists.size();
        for (PlanNode child : children) {
            // ignore missing child cardinality info in the hope it won't matter enough
            // to change the planning outcome
            if (child.cardinality > 0) {
                cardinality += child.cardinality;
            }
        }
        applyConjunctsSelectivity();
        capCardinalityAtLimit();
        if (LOG.isDebugEnabled()) {
            LOG.debug("stats Merge: cardinality={}", Long.toString(cardinality));
        }
    }

    @Override
    protected void computeOldCardinality() {
        cardinality = constExprLists.size();
        for (PlanNode child : children) {
            // ignore missing child cardinality info in the hope it won't matter enough
            // to change the planning outcome
            if (child.cardinality > 0) {
                cardinality += child.cardinality;
            }
        }
        LOG.debug("stats Merge: cardinality={}", Long.toString(cardinality));
    }

    public List<List<Expr>> getResultExprLists() {
        return resultExprLists;
    }

    public List<List<Expr>> getConstExprLists() {
        return constExprLists;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        List<List<TExpr>> texprLists = Lists.newArrayList();
        List<List<TExpr>> constTexprLists = Lists.newArrayList();
        for (List<Expr> exprList : resultExprLists) {
            if (exprList != null) {
                texprLists.add(Expr.treesToThrift(exprList));
            }
        }
        for (List<Expr> constTexprList : constExprLists) {
            constTexprLists.add(Expr.treesToThrift(constTexprList));
        }
        msg.merge_node = new TMergeNode(tupleId.asInt(), texprLists, constTexprLists);
        msg.node_type = TPlanNodeType.MERGE_NODE;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        if (detailLevel == TExplainLevel.BRIEF) {
            return "";
        }
        StringBuilder output = new StringBuilder();
        // A MergeNode may have predicates if a union is used inside an inline view,
        // and the enclosing select stmt has predicates referring to the inline view.
        if (!conjuncts.isEmpty()) {
            output.append(prefix + "predicates: " + getExplainString(conjuncts) + "\n");
        }
        if (constExprLists.size() > 0) {
            output.append(prefix + "merging " + constExprLists.size() + " SELECT CONSTANT\n");
        }
        return output.toString();
    }

    @Override
    public void getMaterializedIds(Analyzer analyzer, List<SlotId> ids) {
        super.getMaterializedIds(analyzer, ids);

        for (List<Expr> resultExprs : resultExprLists) {
            Expr.getIds(resultExprs, null, ids);
        }

        // for now, also mark all of our output slots as materialized
        // TODO: fix this, it's not really necessary, but it breaks the logic
        // in MergeNode (c++)
        for (TupleId tupleId : tupleIds) {
            TupleDescriptor tupleDesc = analyzer.getTupleDesc(tupleId);
            for (SlotDescriptor slotDesc : tupleDesc.getSlots()) {
                ids.add(slotDesc.getId());
            }
        }
    }

    @Override
    public int getNumInstances() {
        return 1;
    }
}
