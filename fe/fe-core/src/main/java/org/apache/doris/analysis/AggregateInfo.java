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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/AggregateInfo.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.planner.DataPartition;
import org.apache.doris.thrift.TPartitionType;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates all the information needed to compute the aggregate functions of a single
 * Select block, including a possible 2nd phase aggregation step for DISTINCT aggregate
 * functions and merge aggregation steps needed for distributed execution.
 *
 * The latter requires a tree structure of AggregateInfo objects which express the
 * original aggregate computations as well as the necessary merging aggregate
 * computations.
 * TODO: get rid of this by transforming
 *   SELECT COUNT(DISTINCT a, b, ..) GROUP BY x, y, ...
 * into an equivalent query with a inline view:
 *   SELECT COUNT(*) FROM (SELECT DISTINCT a, b, ..., x, y, ...) GROUP BY x, y, ...
 *
 * The tree structure looks as follows:
 * <pre>
 * - for non-distinct aggregation:
 *   - aggInfo: contains the original aggregation functions and grouping exprs
 *   - aggInfo.mergeAggInfo: contains the merging aggregation functions (grouping
 *     exprs are identical)
 * - for distinct aggregation (for an explanation of the phases, see
 *   SelectStmt.createDistinctAggInfo()):
 *   - aggInfo: contains the phase 1 aggregate functions and grouping exprs
 *   - aggInfo.2ndPhaseDistinctAggInfo: contains the phase 2 aggregate functions and
 *     grouping exprs
 *   - aggInfo.mergeAggInfo: contains the merging aggregate functions for the phase 1
 *     computation (grouping exprs are identical)
 *   - aggInfo.2ndPhaseDistinctAggInfo.mergeAggInfo: contains the merging aggregate
 *     functions for the phase 2 computation (grouping exprs are identical)
 * </pre>
 * In general, merging aggregate computations are idempotent; in other words,
 * aggInfo.mergeAggInfo == aggInfo.mergeAggInfo.mergeAggInfo.
 *
 * TODO: move the merge construction logic from SelectStmt into AggregateInfo
 * TODO: Add query tests for aggregation with intermediate tuples with num_nodes=1.
 */
public final class AggregateInfo extends AggregateInfoBase {
    private static final Logger LOG = LogManager.getLogger(AggregateInfo.class);

    public enum AggPhase {
        FIRST,
        FIRST_MERGE,
        SECOND,
        SECOND_MERGE;

        public boolean isMerge() {
            return this == FIRST_MERGE || this == SECOND_MERGE;
        }
    }

    // created by createMergeAggInfo()
    private AggregateInfo mergeAggInfo;

    // created by createDistinctAggInfo()
    private AggregateInfo secondPhaseDistinctAggInfo;

    private final AggPhase aggPhase;

    // Map from all grouping and aggregate exprs to a SlotRef referencing the corresp. slot
    // in the intermediate tuple. Identical to outputTupleSmap_ if no aggregateExpr has an
    // output type that is different from its intermediate type.
    protected ExprSubstitutionMap intermediateTupleSmap = new ExprSubstitutionMap();

    // Map from all grouping and aggregate exprs to a SlotRef referencing the corresp. slot
    // in the output tuple.
    protected ExprSubstitutionMap outputTupleSmap = new ExprSubstitutionMap();

    // Map from slots of outputTupleSmap_ to the corresponding slot in
    // intermediateTupleSmap_.
    protected ExprSubstitutionMap outputToIntermediateTupleSmap =
            new ExprSubstitutionMap();

    // if set, a subset of groupingExprs_; set and used during planning
    private List<Expr> partitionExprs;

    private boolean isUsingSetForDistinct;

    // the multi distinct's begin pos  and end pos in groupby exprs
    private ArrayList<Integer> firstIdx = Lists.newArrayList();
    private ArrayList<Integer> lastIdx = Lists.newArrayList();

    // C'tor creates copies of groupingExprs and aggExprs.
    private AggregateInfo(ArrayList<Expr> groupingExprs,
                          ArrayList<FunctionCallExpr> aggExprs, AggPhase aggPhase)  {
        this(groupingExprs, aggExprs, aggPhase, false);
    }

    private AggregateInfo(ArrayList<Expr> groupingExprs,
                          ArrayList<FunctionCallExpr> aggExprs, AggPhase aggPhase, boolean isUsingSetForDistinct)  {
        super(groupingExprs, aggExprs);
        this.aggPhase = aggPhase;
        this.isUsingSetForDistinct = isUsingSetForDistinct;
    }

    /**
     * C'tor for cloning.
     */
    private AggregateInfo(AggregateInfo other) {
        super(other);
        if (other.mergeAggInfo != null) {
            mergeAggInfo = other.mergeAggInfo.clone();
        }
        if (other.secondPhaseDistinctAggInfo != null) {
            secondPhaseDistinctAggInfo = other.secondPhaseDistinctAggInfo.clone();
        }
        aggPhase = other.aggPhase;
        outputTupleSmap = other.outputTupleSmap.clone();
        if (other.requiresIntermediateTuple()) {
            intermediateTupleSmap = other.intermediateTupleSmap.clone();
        } else {
            Preconditions.checkState(other.intermediateTupleDesc == other.outputTupleDesc);
            intermediateTupleSmap = outputTupleSmap;
        }
        partitionExprs =
                (other.partitionExprs != null) ? Expr.cloneList(other.partitionExprs) : null;
    }

    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    public void setPartitionExprs(List<Expr> exprs) {
        partitionExprs = exprs;
    }

    /**
     * Used by new optimizer.
     */
    public static AggregateInfo create(
            ArrayList<Expr> groupingExprs, ArrayList<FunctionCallExpr> aggExprs, List<Integer> aggExprIds,
            boolean isPartialAgg, TupleDescriptor tupleDesc, TupleDescriptor intermediateTupleDesc, AggPhase phase) {
        AggregateInfo result = new AggregateInfo(groupingExprs, aggExprs, phase);
        result.outputTupleDesc = tupleDesc;
        result.intermediateTupleDesc = intermediateTupleDesc;
        int aggExprSize = result.getAggregateExprs().size();
        for (int i = 0; i < aggExprSize; i++) {
            result.materializedSlots.add(i);
            String label = (isPartialAgg ? "partial_" : "")
                    + aggExprs.get(i).toSql() + "[#" + aggExprIds.get(i) + "]";
            result.materializedSlotLabels.add(label);
        }
        return result;
    }

    public ArrayList<FunctionCallExpr> getMaterializedAggregateExprs() {
        ArrayList<FunctionCallExpr> result = Lists.newArrayList();
        for (Integer i : materializedSlots) {
            result.add(aggregateExprs.get(i));
        }
        return result;
    }

    public boolean isMerge() {
        return aggPhase.isMerge();
    }

    public boolean isFirstPhase() {
        return aggPhase == AggPhase.FIRST;
    }

    public boolean isDistinctAgg() {
        return secondPhaseDistinctAggInfo != null;
    }

    public void updateMaterializedSlots() {
        // why output and intermediate may have different materialized slots?
        // because some slot is materialized by materializeSrcExpr method directly
        // in that case, only output slots is materialized
        // assume output tuple has correct materialized information
        // we update intermediate tuple and materializedSlots based on output tuple
        materializedSlots.clear();
        ArrayList<SlotDescriptor> outputSlots = outputTupleDesc.getSlots();
        int groupingExprNum = groupingExprs != null ? groupingExprs.size() : 0;
        Preconditions.checkState(groupingExprNum <= outputSlots.size());
        for (int i = groupingExprNum; i < outputSlots.size(); ++i) {
            if (outputSlots.get(i).isMaterialized()) {
                materializedSlots.add(i - groupingExprNum);
            }
        }

        ArrayList<SlotDescriptor> intermediateSlots = intermediateTupleDesc.getSlots();
        Preconditions.checkState(intermediateSlots.size() == outputSlots.size());
        for (int i = 0; i < outputSlots.size(); ++i) {
            intermediateSlots.get(i).setIsMaterialized(outputSlots.get(i).isMaterialized());
        }
        intermediateTupleDesc.computeStatAndMemLayout();
    }

    /**
     * Returns DataPartition derived from grouping exprs.
     * Returns unpartitioned spec if no grouping.
     * TODO: this won't work when we start supporting range partitions,
     * because we could derive both hash and order-based partitions
     */
    public DataPartition getPartition() {
        if (groupingExprs.isEmpty()) {
            return DataPartition.UNPARTITIONED;
        } else {
            return new DataPartition(TPartitionType.HASH_PARTITIONED, groupingExprs);
        }
    }

    public String debugString() {
        StringBuilder out = new StringBuilder(super.debugString());
        out.append(MoreObjects.toStringHelper(this)
                .add("phase", aggPhase)
                .add("intermediate_smap", intermediateTupleSmap.debugString())
                .add("output_smap", outputTupleSmap.debugString())
                .toString());
        if (mergeAggInfo != this && mergeAggInfo != null) {
            out.append("\nmergeAggInfo:\n" + mergeAggInfo.debugString());
        }
        if (secondPhaseDistinctAggInfo != null) {
            out.append("\nsecondPhaseDistinctAggInfo:\n"
                    + secondPhaseDistinctAggInfo.debugString());
        }
        return out.toString();
    }

    @Override
    protected String tupleDebugName() {
        return "agg-tuple";
    }

    @Override
    public AggregateInfo clone() {
        return new AggregateInfo(this);
    }

    public List<Expr> getInputPartitionExprs() {
        return partitionExprs != null ? partitionExprs : groupingExprs;
    }

}
