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
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.nereids.trees.plans.PartitionTopnPhase;
import org.apache.doris.nereids.trees.plans.WindowFuncType;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPartTopNPhase;
import org.apache.doris.thrift.TPartitionSortNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TSortInfo;
import org.apache.doris.thrift.TopNAlgorithm;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 * PartitionSortNode.
 * PartitionSortNode is only used in the Nereids.
 */
public class PartitionSortNode extends PlanNode {
    private final WindowFuncType function;
    private final List<Expr> partitionExprs;
    private final SortInfo info;
    private final boolean hasGlobalLimit;
    private final long partitionLimit;
    private final PartitionTopnPhase phase;

    /**
     * Constructor.
     */
    public PartitionSortNode(PlanNodeId id, PlanNode input, WindowFuncType function, List<Expr> partitionExprs,
            SortInfo info, boolean hasGlobalLimit, long partitionLimit, PartitionTopnPhase phase) {
        super(id, "PartitionTopN", StatisticalType.PARTITION_TOPN_NODE);
        Preconditions.checkArgument(info.getOrderingExprs().size() == info.getIsAscOrder().size());
        this.function = function;
        this.partitionExprs = partitionExprs;
        this.info = info;
        this.hasGlobalLimit = hasGlobalLimit;
        this.partitionLimit = partitionLimit;
        this.phase = phase;
        this.tupleIds.addAll(Lists.newArrayList(info.getSortTupleDescriptor().getId()));
        this.tblRefIds.addAll(Lists.newArrayList(info.getSortTupleDescriptor().getId()));
        this.nullableTupleIds.addAll(input.getNullableTupleIds());
        this.children.add(input);
    }

    public SortInfo getSortInfo() {
        return info;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        if (detailLevel == TExplainLevel.BRIEF) {
            return "";
        }

        StringBuilder output = new StringBuilder();

        // Add the function name.
        String funcName;
        if (function == WindowFuncType.ROW_NUMBER) {
            funcName = "row_number";
        } else if (function == WindowFuncType.RANK) {
            funcName = "rank";
        } else {
            funcName = "dense_rank";
        }
        output.append(prefix).append("functions: ").append(funcName).append("\n");

        // Add the partition expr.
        List<String> strings = Lists.newArrayList();
        if (!partitionExprs.isEmpty()) {
            output.append(prefix).append("partition by: ");

            for (Expr partitionExpr : partitionExprs) {
                strings.add(partitionExpr.toSql());
            }

            output.append(Joiner.on(", ").join(strings));
            output.append("\n");
        }

        // Add the order by.
        output.append(prefix).append("order by: ");
        Iterator<Expr> expr = info.getOrderingExprs().iterator();
        Iterator<Boolean> isAsc = info.getIsAscOrder().iterator();
        boolean start = true;
        while (expr.hasNext()) {
            if (start) {
                start = false;
            } else {
                output.append(", ");
            }
            output.append(expr.next().toSql()).append(" ");
            output.append(isAsc.next() ? "ASC" : "DESC");
        }
        output.append("\n");

        // Add the limit information;
        output.append(prefix).append("has global limit: ").append(hasGlobalLimit).append("\n");
        output.append(prefix).append("partition limit: ").append(partitionLimit).append("\n");

        // mark partition topn phase
        output.append(prefix).append("partition topn phase: ").append(phase).append("\n");

        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.PARTITION_SORT_NODE;

        TSortInfo sortInfo = info.toThrift();
        Preconditions.checkState(tupleIds.size() == 1, "Incorrect size for tupleIds in PartitionSortNode");

        TopNAlgorithm topNAlgorithm;
        if (function == WindowFuncType.ROW_NUMBER) {
            topNAlgorithm = TopNAlgorithm.ROW_NUMBER;
        } else if (function == WindowFuncType.RANK) {
            topNAlgorithm = TopNAlgorithm.RANK;
        } else {
            topNAlgorithm = TopNAlgorithm.DENSE_RANK;
        }

        TPartTopNPhase pTopNPhase;
        if (phase == PartitionTopnPhase.ONE_PHASE_GLOBAL_PTOPN) {
            pTopNPhase = TPartTopNPhase.ONE_PHASE_GLOBAL;
        } else if (phase == PartitionTopnPhase.TWO_PHASE_LOCAL_PTOPN) {
            pTopNPhase = TPartTopNPhase.TWO_PHASE_LOCAL;
        } else if (phase == PartitionTopnPhase.TWO_PHASE_GLOBAL_PTOPN) {
            pTopNPhase = TPartTopNPhase.TWO_PHASE_GLOBAL;
        } else {
            pTopNPhase = TPartTopNPhase.UNKNOWN;
        }

        TPartitionSortNode partitionSortNode = new TPartitionSortNode();
        partitionSortNode.setTopNAlgorithm(topNAlgorithm);
        partitionSortNode.setPartitionExprs(Expr.treesToThrift(partitionExprs));
        partitionSortNode.setSortInfo(sortInfo);
        partitionSortNode.setHasGlobalLimit(hasGlobalLimit);
        partitionSortNode.setPartitionInnerLimit(partitionLimit);
        partitionSortNode.setPtopnPhase(pTopNPhase);
        msg.partition_sort_node = partitionSortNode;
    }
}
