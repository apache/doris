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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/SortNode.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.common.Pair;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TSortAlgorithm;
import org.apache.doris.thrift.TSortInfo;
import org.apache.doris.thrift.TSortNode;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;

/**
 * Sorting.
 * Attention, in some corner case, we need enable projection planner to promise the correctness of the Plan.
 * Please refer to this regression test:regression-test/suites/query/aggregate/aggregate_count1.groovy.
 */
public class SortNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(SortNode.class);
    // info_.sortTupleSlotExprs_ substituted with the outputSmap_ for materialized slots in init().
    List<Expr> resolvedTupleExprs;
    private final SortInfo info;
    private final boolean  useTopN;
    private boolean useTopnOpt = false;
    private boolean useTwoPhaseReadOpt;
    private boolean hasRuntimePredicate = false;

    // If mergeByexchange is set to true, the sort information is pushed to the
    // exchange node, and the sort node is used for the ORDER BY .
    private boolean mergeByexchange = false;

    private boolean useLocalMerge = false;

    // if true, the output of this node feeds an AnalyticNode
    private boolean isAnalyticSort;
    private boolean isColocate = false;
    private DataPartition inputPartition;
    TSortAlgorithm algorithm;
    private long fullSortMaxBufferedBytes = -1;

    private boolean isUnusedExprRemoved = false;

    // topn filter target: ScanNode id + slot desc
    private List<Pair<Integer, Integer>> topnFilterTargets;

    /**
     * Constructor.
     */
    public SortNode(PlanNodeId id, PlanNode input, SortInfo info, boolean useTopN, long offset) {
        super(id, useTopN ? "TOP-N" : "SORT", StatisticalType.SORT_NODE);
        this.info = info;
        this.useTopN = useTopN;
        this.tupleIds.addAll(Lists.newArrayList(info.getSortTupleDescriptor().getId()));
        this.tblRefIds.addAll(Lists.newArrayList(info.getSortTupleDescriptor().getId()));
        this.nullableTupleIds.addAll(input.getNullableTupleIds());
        this.children.add(input);
        this.offset = offset;
        Preconditions.checkArgument(info.getOrderingExprs().size() == info.getIsAscOrder().size());
        updateSortAlgorithm();
    }

    public SortNode(PlanNodeId id, PlanNode input, SortInfo info, boolean useTopN) {
        this(id, input, info, useTopN, 0);
    }

    private void updateSortAlgorithm() {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && !connectContext.getSessionVariable().forceSortAlgorithm.isEmpty()) {
            String algo = connectContext.getSessionVariable().forceSortAlgorithm;
            if (algo.equals("heap")) {
                algorithm = TSortAlgorithm.HEAP_SORT;
            } else if (algo.equals("topn")) {
                algorithm = TSortAlgorithm.TOPN_SORT;
            } else {
                algorithm = TSortAlgorithm.FULL_SORT;
            }
        } else {
            if (limit <= 0) {
                algorithm = TSortAlgorithm.FULL_SORT;
            } else if (hasRuntimePredicate || useTwoPhaseReadOpt) {
                algorithm = TSortAlgorithm.HEAP_SORT;
            } else {
                if (limit + offset < 50000) {
                    algorithm = TSortAlgorithm.HEAP_SORT;
                } else if (limit + offset < 20000000) {
                    algorithm = TSortAlgorithm.FULL_SORT;
                } else {
                    algorithm = TSortAlgorithm.TOPN_SORT;
                }
            }
        }

        if (connectContext != null && connectContext.getSessionVariable().fullSortMaxBufferedBytes > 0) {
            fullSortMaxBufferedBytes = connectContext.getSessionVariable().fullSortMaxBufferedBytes;
        }
    }

    public void setIsAnalyticSort(boolean v) {
        isAnalyticSort = v;
    }

    public SortInfo getSortInfo() {
        return info;
    }

    public void setMergeByExchange() {
        this.mergeByexchange = true;
        // mergeByexchange = true means that the sort data will be merged once at the
        // exchange node
        // If enable_local_merge = true at the same time, it can be merged once before
        // the exchange node
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && connectContext.getSessionVariable().getEnableLocalMergeSort()
                && this.mergeByexchange) {
            this.useLocalMerge = true;
        } else {
            this.useLocalMerge = false;
        }
    }

    public void setUseTopnOpt(boolean useTopnOpt) {
        this.useTopnOpt = useTopnOpt;
    }

    public void setUseTwoPhaseReadOpt(boolean useTwoPhaseReadOpt) {
        this.useTwoPhaseReadOpt = useTwoPhaseReadOpt;
        updateSortAlgorithm();
    }

    public List<Expr> getResolvedTupleExprs() {
        return resolvedTupleExprs;
    }

    @Override
    public void setCompactData(boolean on) {
        this.compactData = on;
    }

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        if (detailLevel == TExplainLevel.BRIEF) {
            return "";
        }

        StringBuilder output = new StringBuilder();
        output.append(detailPrefix).append("order by: ");
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

        if (useTopnOpt) {
            output.append(detailPrefix + "TOPN filter targets: ").append(topnFilterTargets).append("\n");
        }
        if (useTwoPhaseReadOpt) {
            output.append(detailPrefix + "OPT TWO PHASE\n");
        }

        output.append(detailPrefix + "algorithm: ");
        if (algorithm == TSortAlgorithm.HEAP_SORT) {
            output.append("heap sort\n");
        } else if (algorithm == TSortAlgorithm.TOPN_SORT) {
            output.append("topn sort\n");
        } else {
            output.append("full sort\n");
        }

        if (useLocalMerge) {
            output.append(detailPrefix + "local merge sort\n");
        }
        if (mergeByexchange) {
            output.append(detailPrefix + "merge by exchange\n");
        }
        output.append(detailPrefix).append("offset: ").append(offset).append("\n");
        return output.toString();
    }

    private void removeUnusedExprs() {
        if (!isUnusedExprRemoved) {
            if (resolvedTupleExprs != null) {
                List<SlotDescriptor> slotDescriptorList = this.info.getSortTupleDescriptor().getSlots();
                for (int i = slotDescriptorList.size() - 1; i >= 0; i--) {
                    if (!slotDescriptorList.get(i).isMaterialized()) {
                        resolvedTupleExprs.remove(i);
                    }
                }
            }
            isUnusedExprRemoved = true;
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.SORT_NODE;

        TSortInfo sortInfo = info.toThrift();
        Preconditions.checkState(tupleIds.size() == 1, "Incorrect size for tupleIds in SortNode");
        removeUnusedExprs();
        if (resolvedTupleExprs != null) {
            sortInfo.setSortTupleSlotExprs(Expr.treesToThrift(resolvedTupleExprs));
        }
        TSortNode sortNode = new TSortNode(sortInfo, useTopN);

        msg.sort_node = sortNode;
        msg.sort_node.setOffset(offset);
        msg.sort_node.setUseTopnOpt(useTopnOpt);
        msg.sort_node.setMergeByExchange(this.mergeByexchange);
        msg.sort_node.setUseLocalMerge(this.useLocalMerge);
        msg.sort_node.setIsAnalyticSort(isAnalyticSort);
        msg.sort_node.setIsColocate(isColocate);

        msg.sort_node.setAlgorithm(algorithm);
        if (fullSortMaxBufferedBytes > 0) {
            msg.sort_node.setFullSortMaxBufferedBytes(fullSortMaxBufferedBytes);
        }
    }

    @Override
    protected String debugString() {
        List<String> strings = Lists.newArrayList();
        for (Boolean isAsc : info.getIsAscOrder()) {
            strings.add(isAsc ? "a" : "d");
        }
        return MoreObjects.toStringHelper(this).add("ordering_exprs",
                Expr.debugString(info.getOrderingExprs())).add("is_asc",
                "[" + Joiner.on(" ").join(strings) + "]").addValue(super.debugString()).toString();
    }

    // If it's analytic sort or not merged by a followed exchange node, it must output the global ordered data.
    @Override
    public boolean isSerialOperator() {
        return !isAnalyticSort && !mergeByexchange;
    }

    public void setColocate(boolean colocate) {
        isColocate = colocate;
    }

    public void setHasRuntimePredicate() {
        this.hasRuntimePredicate = true;
        updateSortAlgorithm();
    }

    @Override
    public void setLimit(long limit) {
        super.setLimit(limit);
        updateSortAlgorithm();
    }

    public void setOffset(long offset) {
        super.setOffset(offset);
        updateSortAlgorithm();
    }

    public void setTopnFilterTargets(
            List<Pair<Integer, Integer>> topnFilterTargets) {
        this.topnFilterTargets = topnFilterTargets;
    }
}
