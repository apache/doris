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

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.thrift.TBucketedAggregationNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Bucketed hash aggregation node.
 *
 * Fuses two-phase aggregation (local + global) into a single BE operator for single-BE deployments.
 * Produces a BUCKETED_AGGREGATION_NODE in the Thrift plan, which the BE maps to
 * BucketedAggSinkOperatorX / BucketedAggSourceOperatorX.
 */
public class BucketedAggregationNode extends PlanNode {
    private final AggregateInfo aggInfo;
    private final boolean needsFinalize;

    public BucketedAggregationNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo,
            boolean needsFinalize) {
        super(id, aggInfo.getOutputTupleId().asList(), "BUCKETED AGGREGATE");
        this.aggInfo = aggInfo;
        this.needsFinalize = needsFinalize;
        this.children.add(input);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.BUCKETED_AGGREGATION_NODE;

        List<TExpr> aggregateFunctions = Lists.newArrayList();
        for (FunctionCallExpr e : aggInfo.getMaterializedAggregateExprs()) {
            aggregateFunctions.add(ExprToThriftVisitor.treeToThrift(e));
        }

        List<TExpr> groupingExprs = Lists.newArrayList();
        if (aggInfo.getGroupingExprs() != null) {
            groupingExprs = ExprToThriftVisitor.treesToThrift(aggInfo.getGroupingExprs());
        }

        TBucketedAggregationNode bucketedAggNode = new TBucketedAggregationNode();
        bucketedAggNode.setGroupingExprs(groupingExprs);
        bucketedAggNode.setAggregateFunctions(aggregateFunctions);
        bucketedAggNode.setTupleId(aggInfo.getOutputTupleId().asInt());
        bucketedAggNode.setNeedFinalize(needsFinalize);

        msg.bucketed_agg_node = bucketedAggNode;
    }

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(detailPrefix).append("BUCKETED").append("\n");

        if (detailLevel == TExplainLevel.BRIEF) {
            output.append(detailPrefix).append(String.format("cardinality=%,d", cardinality)).append("\n");
            return output.toString();
        }

        if (aggInfo.getAggregateExprs() != null && aggInfo.getMaterializedAggregateExprs().size() > 0) {
            output.append(detailPrefix).append("output: ")
                    .append(getExplainString(aggInfo.getMaterializedAggregateExprs())).append("\n");
        }
        output.append(detailPrefix).append("group by: ")
                .append(getExplainString(aggInfo.getGroupingExprs()))
                .append("\n");
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("having: ").append(getExplainString(conjuncts)).append("\n");
        }
        output.append(detailPrefix).append(String.format("cardinality=%,d", cardinality)).append("\n");
        return output.toString();
    }

    @Override
    public boolean isSerialOperator() {
        // Bucketed agg handles group-by keys only (no without-key in initial version),
        // so it's never a serial operator.
        return false;
    }
}
