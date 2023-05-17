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
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.nereids.types.WindowFuncType;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPartitionSortNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TSortInfo;
import org.apache.doris.thrift.TopNAlgorithm;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * PartitionSortNode.
 * PartitionSortNode is only used in the Nereids.
 */
public class PartitionSortNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(PartitionSortNode.class);
    private List<Expr> resolvedTupleExprs;
    private final WindowFuncType function;
    private final List<Expr> partitionExprs;
    private final SortInfo info;
    private final boolean hasGlobalLimit;
    private final long partitionLimit;

    private boolean isUnusedExprRemoved = false;
    private ArrayList<Boolean> nullabilityChangedFlags = Lists.newArrayList();

    /**
     * Constructor.
     */
    public PartitionSortNode(PlanNodeId id, PlanNode input, WindowFuncType function, List<Expr> partitionExprs,
                             SortInfo info, boolean hasGlobalLimit, long partitionLimit) {
        super(id, "PartitionTopN", StatisticalType.PARTITION_TOPN_MODE);
        this.function = function;
        this.partitionExprs = partitionExprs;
        this.info = info;
        this.hasGlobalLimit = hasGlobalLimit;
        this.partitionLimit = partitionLimit;
        this.tupleIds.addAll(Lists.newArrayList(info.getSortTupleDescriptor().getId()));
        this.tblRefIds.addAll(Lists.newArrayList(info.getSortTupleDescriptor().getId()));
        this.nullableTupleIds.addAll(input.getNullableTupleIds());
        this.children.add(input);
        Preconditions.checkArgument(info.getOrderingExprs().size() == info.getIsAscOrder().size());
    }

    public SortInfo getSortInfo() {
        return info;
    }

    @Override
    public void getMaterializedIds(Analyzer analyzer, List<SlotId> ids) {
        super.getMaterializedIds(analyzer, ids);
        Expr.getIds(info.getOrderingExprs(), null, ids);
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

        return output.toString();
    }

    private void removeUnusedExprs() {
        if (!isUnusedExprRemoved) {
            if (resolvedTupleExprs != null) {
                List<SlotDescriptor> slotDescriptorList = this.info.getSortTupleDescriptor().getSlots();
                for (int i = slotDescriptorList.size() - 1; i >= 0; i--) {
                    if (!slotDescriptorList.get(i).isMaterialized()) {
                        resolvedTupleExprs.remove(i);
                        nullabilityChangedFlags.remove(i);
                    }
                }
            }
            isUnusedExprRemoved = true;
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.PARTITION_SORT_NODE;

        TSortInfo sortInfo = info.toThrift();
        Preconditions.checkState(tupleIds.size() == 1, "Incorrect size for tupleIds in PartitionSortNode");
        removeUnusedExprs();
        if (resolvedTupleExprs != null) {
            sortInfo.setSortTupleSlotExprs(Expr.treesToThrift(resolvedTupleExprs));
            // FIXME this is a bottom line solution for wrong nullability of resolvedTupleExprs
            // remove the following line after nereids online
            sortInfo.setSlotExprsNullabilityChangedFlags(nullabilityChangedFlags);
        }

        TopNAlgorithm topNAlgorithm;
        if (function == WindowFuncType.ROW_NUMBER) {
            topNAlgorithm = TopNAlgorithm.ROW_NUMBER;
        } else if (function == WindowFuncType.RANK) {
            topNAlgorithm = TopNAlgorithm.RANK;
        } else {
            topNAlgorithm = TopNAlgorithm.DENSE_RANK;
        }

        TPartitionSortNode partitionSortNode = new TPartitionSortNode();
        partitionSortNode.setTopNAlgorithm(topNAlgorithm);
        partitionSortNode.setPartitionExprs(Expr.treesToThrift(partitionExprs));
        partitionSortNode.setSortInfo(sortInfo);
        partitionSortNode.setHasGlobalLimit(hasGlobalLimit);
        partitionSortNode.setPartitionInnerLimit(partitionLimit);
        msg.partition_sort_node = partitionSortNode;
    }

    @Override
    public Set<SlotId> computeInputSlotIds(Analyzer analyzer) throws NotImplementedException {
        removeUnusedExprs();
        List<Expr> materializedTupleExprs = new ArrayList<>(resolvedTupleExprs);
        List<SlotId> result = Lists.newArrayList();
        Expr.getIds(materializedTupleExprs, null, result);
        return new HashSet<>(result);
    }

    /**
     * Supplement the information needed by be for the partition sort node.
     */
    public void finalizeForNereids(TupleDescriptor tupleDescriptor,
                                   List<Expr> outputList, List<Expr> orderingExpr) {
        resolvedTupleExprs = Lists.newArrayList();
        // TODO: should fix the duplicate order by exprs in nereids code later
        for (Expr order : orderingExpr) {
            if (!resolvedTupleExprs.contains(order)) {
                resolvedTupleExprs.add(order);
            }
        }
        for (Expr output : outputList) {
            if (!resolvedTupleExprs.contains(output)) {
                resolvedTupleExprs.add(output);
            }
        }
        info.setSortTupleDesc(tupleDescriptor);
        info.setSortTupleSlotExprs(resolvedTupleExprs);

        nullabilityChangedFlags.clear();
        for (int i = 0; i < resolvedTupleExprs.size(); i++) {
            nullabilityChangedFlags.add(false);
        }
    }
}
