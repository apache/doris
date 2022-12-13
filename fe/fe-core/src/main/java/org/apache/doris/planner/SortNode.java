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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.UserException;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsRecursiveDerive;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TSortInfo;
import org.apache.doris.thrift.TSortNode;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
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

    private boolean  isDefaultLimit;
    private long offset;
    // if true, the output of this node feeds an AnalyticNode
    private boolean isAnalyticSort;
    private DataPartition inputPartition;

    private boolean isUnusedExprRemoved = false;

    private ArrayList<Boolean> nullabilityChangedFlags = Lists.newArrayList();

    /**
     * Constructor.
     */
    public SortNode(PlanNodeId id, PlanNode input, SortInfo info, boolean useTopN,
            boolean isDefaultLimit, long offset) {
        super(id, useTopN ? "TOP-N" : "SORT", StatisticalType.SORT_NODE);
        this.info = info;
        this.useTopN = useTopN;
        this.isDefaultLimit = isDefaultLimit;
        this.tupleIds.addAll(Lists.newArrayList(info.getSortTupleDescriptor().getId()));
        this.tblRefIds.addAll(Lists.newArrayList(info.getSortTupleDescriptor().getId()));
        this.nullableTupleIds.addAll(input.getNullableTupleIds());
        this.children.add(input);
        this.offset = offset;
        Preconditions.checkArgument(info.getOrderingExprs().size() == info.getIsAscOrder().size());
    }

    public SortNode(PlanNodeId id, PlanNode input, SortInfo info, boolean useTopN) {
        this(id, input, info, useTopN, true, 0);
    }

    /**
     * Clone 'inputSortNode' for distributed Top-N.
     */
    public SortNode(PlanNodeId id, SortNode inputSortNode, PlanNode child) {
        super(id, inputSortNode, inputSortNode.useTopN ? "TOP-N" : "SORT", StatisticalType.SORT_NODE);
        this.info = inputSortNode.info;
        this.useTopN = inputSortNode.useTopN;
        this.isDefaultLimit = inputSortNode.isDefaultLimit;
        this.children.add(child);
        this.offset = inputSortNode.offset;
    }

    /**
     * set isDefaultLimit when translate PhysicalLimit
     * @param defaultLimit
     */
    public void setDefaultLimit(boolean defaultLimit) {
        isDefaultLimit = defaultLimit;
    }

    public void setIsAnalyticSort(boolean v) {
        isAnalyticSort = v;
    }

    public boolean isAnalyticSort() {
        return isAnalyticSort;
    }

    public DataPartition getInputPartition() {
        return inputPartition;
    }

    public void setInputPartition(DataPartition inputPartition) {
        this.inputPartition = inputPartition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public SortInfo getSortInfo() {
        return info;
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
        output.append(detailPrefix).append("offset: ").append(offset).append("\n");
        return output.toString();
    }

    @Override
    protected void computeStats(Analyzer analyzer) throws UserException {
        super.computeStats(analyzer);
        if (!analyzer.safeIsEnableJoinReorderBasedCost()) {
            return;
        }

        StatsRecursiveDerive.getStatsRecursiveDerive().statsRecursiveDerive(this);
        cardinality = (long) statsDeriveResult.getRowCount();

        if (LOG.isDebugEnabled()) {
            LOG.debug("stats Sort: cardinality=" + cardinality);
        }
    }

    @Override
    protected void computeOldCardinality() {
        cardinality = getChild(0).cardinality;
        if (hasLimit()) {
            if (cardinality == -1) {
                cardinality = limit;
            } else {
                cardinality = Math.min(cardinality, limit);
            }
        }
        LOG.debug("stats Sort: cardinality=" + Double.toString(cardinality));
    }

    public void init(Analyzer analyzer) throws UserException {
        // Compute the memory layout for the generated tuple.
        computeStats(analyzer);
        // createDefaultSmap(analyzer);
        // // populate resolvedTupleExprs and outputSmap_
        // List<SlotDescriptor> sortTupleSlots = info.getSortTupleDescriptor().getSlots();
        // List<Expr> slotExprs = info.getSortTupleSlotExprs_();
        // Preconditions.checkState(sortTupleSlots.size() == slotExprs.size());

        // populate resolvedTupleExprs_ and outputSmap_
        List<SlotDescriptor> sortTupleSlots = info.getSortTupleDescriptor().getSlots();
        List<Expr> slotExprs = info.getSortTupleSlotExprs();
        Preconditions.checkState(sortTupleSlots.size() == slotExprs.size());

        resolvedTupleExprs = Lists.newArrayList();
        outputSmap = new ExprSubstitutionMap();

        for (int i = 0; i < slotExprs.size(); ++i) {
            resolvedTupleExprs.add(slotExprs.get(i));
            outputSmap.put(slotExprs.get(i), new SlotRef(sortTupleSlots.get(i)));
            nullabilityChangedFlags.add(slotExprs.get(i).isNullable());
        }

        ExprSubstitutionMap childSmap = getCombinedChildSmap();
        resolvedTupleExprs = Expr.substituteList(resolvedTupleExprs, childSmap, analyzer, false);

        for (int i = 0; i < resolvedTupleExprs.size(); ++i) {
            nullabilityChangedFlags.set(i, nullabilityChangedFlags.get(i) ^ resolvedTupleExprs.get(i).isNullable());
        }

        // Remap the ordering exprs to the tuple materialized by this sort node. The mapping
        // is a composition of the childSmap and the outputSmap_ because the child node may
        // have also remapped its input (e.g., as in a series of (sort->analytic)* nodes).
        // Parent nodes have to do the same so set the composition as the outputSmap_.
        outputSmap = ExprSubstitutionMap.compose(childSmap, outputSmap, analyzer);
        info.substituteOrderingExprs(outputSmap, analyzer);

        if (LOG.isDebugEnabled()) {
            LOG.debug("sort id " + tupleIds.get(0).toString() + " smap: "
                    + outputSmap.debugString());
            LOG.debug("sort input exprs: " + Expr.debugString(resolvedTupleExprs));
        }
    }

    @Override
    public void getMaterializedIds(Analyzer analyzer, List<SlotId> ids) {
        super.getMaterializedIds(analyzer, ids);
        Expr.getIds(info.getOrderingExprs(), null, ids);
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
        msg.node_type = TPlanNodeType.SORT_NODE;

        TSortInfo sortInfo = info.toThrift();
        Preconditions.checkState(tupleIds.size() == 1, "Incorrect size for tupleIds in SortNode");
        removeUnusedExprs();
        if (resolvedTupleExprs != null) {
            sortInfo.setSortTupleSlotExprs(Expr.treesToThrift(resolvedTupleExprs));
            // FIXME this is a bottom line solution for wrong nullability of resolvedTupleExprs
            // remove the following line after nereids online
            sortInfo.setSlotExprsNullabilityChangedFlags(nullabilityChangedFlags);
        }
        TSortNode sortNode = new TSortNode(sortInfo, useTopN);

        msg.sort_node = sortNode;
        msg.sort_node.setOffset(offset);
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

    @Override
    public int getNumInstances() {
        return children.get(0).getNumInstances();
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
     * Supplement the information needed by be for the sort node.
     * TODO: currently we only process slotref, so when order key is a + 1, we will failed.
     */
    public void finalizeForNereids(TupleDescriptor tupleDescriptor,
            List<Expr> outputList, List<Expr> orderingExpr) {
        resolvedTupleExprs = Lists.newArrayList(orderingExpr);
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
