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
import org.apache.doris.analysis.TupleId;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExceptNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TIntersectNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TUnionNode;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Node that merges the results of its child plans, Normally, this is done by
 * materializing the corresponding result exprs into a new tuple. However, if
 * a child has an identical tuple layout as the output of the set operation node, and
 * the child only has naked SlotRefs as result exprs, then the child is marked
 * as 'passthrough'. The rows of passthrough children are directly returned by
 * the set operation node, instead of materializing the child's result exprs into new
 * tuples.
 */
public abstract class SetOperationNode extends PlanNode {
    private static final Logger LOG = LoggerFactory.getLogger(SetOperationNode.class);

    // List of set operation result exprs of the originating SetOperationStmt. Used for
    // determining passthrough-compatibility of children.
    protected List<Expr> setOpResultExprs;

    // Materialized result/const exprs corresponding to materialized slots.
    // Set in finalize() and substituted against the corresponding child's output smap.
    protected List<List<Expr>> materializedResultExprLists = Lists.newArrayList();
    protected List<List<Expr>> materializedConstExprLists = Lists.newArrayList();

    // Indicates if this UnionNode is inside a subplan.
    protected boolean isInSubplan;

    // Index of the first non-passthrough child.
    protected int firstMaterializedChildIdx;

    protected final TupleId tupleId;

    private boolean isColocate = false;

    protected SetOperationNode(PlanNodeId id, TupleId tupleId, String planNodeName, StatisticalType statisticalType) {
        super(id, tupleId.asList(), planNodeName, statisticalType);
        this.setOpResultExprs = Lists.newArrayList();
        this.tupleId = tupleId;
        this.isInSubplan = false;
    }

    protected SetOperationNode(PlanNodeId id, TupleId tupleId, String planNodeName,
                               List<Expr> setOpResultExprs, boolean isInSubplan, StatisticalType statisticalType) {
        super(id, tupleId.asList(), planNodeName, statisticalType);
        this.setOpResultExprs = setOpResultExprs;
        this.tupleId = tupleId;
        this.isInSubplan = isInSubplan;
    }

    protected SetOperationNode(PlanNodeId id, TupleId tupleId, String planNodeName) {
        super(id, tupleId.asList(), planNodeName, StatisticalType.SET_OPERATION_NODE);
        this.setOpResultExprs = Lists.newArrayList();
        this.tupleId = tupleId;
        this.isInSubplan = false;
    }

    public void setMaterializedConstExprLists(List<List<Expr>> exprs) {
        this.materializedConstExprLists = exprs;
    }

    public void setMaterializedResultExprLists(List<List<Expr>> exprs) {
        this.materializedResultExprLists = exprs;
    }

    public List<List<Expr>> getMaterializedResultExprLists() {
        return materializedResultExprLists;
    }

    public void setColocate(boolean colocate) {
        this.isColocate = colocate;
    }

    protected void toThrift(TPlanNode msg, TPlanNodeType nodeType) {
        Preconditions.checkState(materializedResultExprLists.size() == children.size());
        List<List<TExpr>> texprLists = Lists.newArrayList();
        for (List<Expr> exprList : materializedResultExprLists) {
            texprLists.add(Expr.treesToThrift(exprList));
        }
        List<List<TExpr>> constTexprLists = Lists.newArrayList();
        for (List<Expr> constTexprList : materializedConstExprLists) {
            constTexprLists.add(Expr.treesToThrift(constTexprList));
        }
        Preconditions.checkState(firstMaterializedChildIdx <= children.size());
        switch (nodeType) {
            case UNION_NODE:
                msg.union_node = new TUnionNode(
                        tupleId.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx);
                msg.node_type = TPlanNodeType.UNION_NODE;
                break;
            case INTERSECT_NODE:
                msg.intersect_node = new TIntersectNode(
                        tupleId.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx);
                msg.intersect_node.setIsColocate(isColocate);
                msg.node_type = TPlanNodeType.INTERSECT_NODE;
                break;
            case EXCEPT_NODE:
                msg.except_node = new TExceptNode(
                        tupleId.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx);
                msg.except_node.setIsColocate(isColocate);
                msg.node_type = TPlanNodeType.EXCEPT_NODE;
                break;
            default:
                LOG.error("Node type: " + nodeType + " is invalid.");
                break;
        }
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        if (detailLevel == TExplainLevel.BRIEF) {
            return "";
        }

        StringBuilder output = new StringBuilder();
        // A SetOperationNode may have predicates if a union is set operation inside an inline view,
        // and the enclosing select stmt has predicates referring to the inline view.
        if (CollectionUtils.isNotEmpty(conjuncts)) {
            output.append(prefix).append("predicates: ").append(getExplainString(conjuncts)).append("\n");
        }
        if (CollectionUtils.isNotEmpty(materializedConstExprLists)) {
            output.append(prefix).append("constant exprs: ").append("\n");
            for (List<Expr> exprs : materializedConstExprLists) {
                output.append(prefix).append("    ").append(exprs.stream().map(Expr::toSql)
                        .collect(Collectors.joining(" | "))).append("\n");
            }
        }
        if (detailLevel == TExplainLevel.VERBOSE) {
            if (CollectionUtils.isNotEmpty(materializedResultExprLists)) {
                output.append(prefix).append("child exprs: ").append("\n");
                for (List<Expr> exprs : materializedResultExprLists) {
                    output.append(prefix).append("    ").append(exprs.stream().map(Expr::toSql)
                            .collect(Collectors.joining(" | "))).append("\n");
                }
            }
            List<String> passThroughNodeIds = Lists.newArrayList();
            for (int i = 0; i < firstMaterializedChildIdx; ++i) {
                passThroughNodeIds.add(children.get(i).getId().toString());
            }
            if (!passThroughNodeIds.isEmpty()) {
                String result = prefix + "pass-through-operands: ";
                if (passThroughNodeIds.size() == children.size()) {
                    output.append(result).append("all\n");
                } else {
                    output.append(result).append(Joiner.on(",").join(passThroughNodeIds)).append("\n");
                }
            }
        }
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        int numInstances = 0;
        for (PlanNode child : children) {
            numInstances += child.getNumInstances();
        }
        numInstances = Math.max(1, numInstances);
        return numInstances;
    }
}
