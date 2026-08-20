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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TRecCTENode;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public class RecursiveCteNode extends PlanNode {
    private String cteName;
    private boolean isUnionAll;
    private List<List<Expr>> materializedResultExprLists = Lists.newArrayList();
    private TRecCTENode tRecCTENode;

    public RecursiveCteNode(PlanNodeId id, TupleId tupleId, String cteName, boolean isUnionAll) {
        super(id, tupleId.asList(), "RECURSIVE_CTE");
        this.cteName = cteName;
        this.isUnionAll = isUnionAll;
    }

    public boolean isUnionAll() {
        return isUnionAll;
    }

    public void setMaterializedResultExprLists(List<List<Expr>> exprs) {
        this.materializedResultExprLists = exprs;
    }

    public List<List<Expr>> getMaterializedResultExprLists() {
        return materializedResultExprLists;
    }

    public void settRecCTENode(TRecCTENode tRecCTENode) {
        this.tRecCTENode = tRecCTENode;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.REC_CTE_NODE;
        msg.rec_cte_node = tRecCTENode;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("Recursive Cte: ").append(cteName).append("\n");
        output.append(prefix).append("isUnionAll: ").append(isUnionAll).append("\n");
        if (!conjuncts.isEmpty()) {
            Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
            output.append(prefix).append("PREDICATES: ")
                    .append(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)).append("\n");
        }
        return output.toString();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", cteName)
                .add("id", getId().asInt())
                .add("tid", tupleIds.get(0).asInt())
                .add("isUnionAll", isUnionAll).toString();
    }

    @Override
    public boolean isSerialNode() {
        // Mirror BE's RecCTESourceOperatorX::is_serial_operator() which always returns true:
        // the recursive driver runs sequentially in one task, so downstream consumers must see
        // RecursiveCteNode as serial too.  Without this, FE planner leaves the producer
        // fragment with parallel=N senders but only one actually emits data — the cross-
        // fragment Exchange receiver expects N senders done and hangs waiting on the other
        // N-1.  Marking serial here lets AddLocalExchange#addLocalExchangeForFragment wrap
        // the root with a PASSTHROUGH LE that fans the serial RecCte output out to N
        // parallel sinks, matching BE-native _plan_local_exchange behaviour.
        return true;
    }

    @Override
    public Pair<PlanNode, LocalExchangeType> enforceAndDeriveLocalExchange(
            PlanTranslatorContext translatorContext, PlanNode parent, LocalExchangeTypeRequire parentRequire) {
        // Recurse into children to give them a chance to plan local exchanges below
        // themselves, but never insert one *directly* under RecursiveCteNode:
        //   - ThriftPlansBuilder locates the recursive sender fragment via
        //     `getChild(1).getChild(0).getFragment()`; a LocalExchangeNode wrapper
        //     would shift that path off the cross-fragment ExchangeNode and pull the
        //     wrong fragment into `fragmentsToReset`.
        //   - BE's RecCTESourceOperatorX wires the anchor / recursive side pipelines
        //     directly against the Exchange children (pipeline_fragment_context.cpp
        //     REC_CTE_NODE handling); injecting an extra LE pipeline between them
        //     mis-routes the rerun signal and crashes BE during execution.
        // Both issues are pure shape mismatches — RecursiveCteNode's children are
        // already the cross-fragment ExchangeNode receivers, which BE drives serially
        // itself, so no FE-side fan-out is needed here.
        ArrayList<PlanNode> newChildren = Lists.newArrayList();
        for (int i = 0; i < children.size(); i++) {
            PlanNode child = children.get(i);
            Pair<PlanNode, LocalExchangeType> childOutput = child.enforceAndDeriveLocalExchange(
                    translatorContext, this, LocalExchangeTypeRequire.noRequire());
            newChildren.add(childOutput.first);
        }
        this.children = newChildren;
        return Pair.of(this, LocalExchangeType.NOOP);
    }

    @Override
    protected boolean shouldResetSerialFlagForChild(int childIndex) {
        return true;
    }
}
