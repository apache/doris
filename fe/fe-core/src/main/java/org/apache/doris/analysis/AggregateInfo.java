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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public final class AggregateInfo {

    // For aggregations: All unique grouping expressions from a select block.
    // For analytics: Empty.
    protected ArrayList<Expr> groupingExprs;

    // For aggregations: All unique aggregate expressions from a select block.
    // For analytics: The results of AnalyticExpr.getFnCall() for the unique
    // AnalyticExprs of a select block.
    protected ArrayList<FunctionCallExpr> aggregateExprs;

    // The tuple into which the final output of the aggregation is materialized.
    // Contains groupingExprs.size() + aggregateExprs.size() slots, the first of which
    // contain the values of the grouping exprs, followed by slots into which the
    // aggregateExprs' finalize() symbol write its result, i.e., slots of the aggregate
    // functions' output types.
    protected TupleDescriptor outputTupleDesc;

    // For aggregation: indices into aggregate exprs for that need to be materialized
    // For analytics: indices into the analytic exprs and their corresponding aggregate
    // exprs that need to be materialized.
    // Populated in materializeRequiredSlots() which must be implemented by subclasses.
    protected ArrayList<Integer> materializedSlots = Lists.newArrayList();
    protected List<String> materializedSlotLabels = Lists.newArrayList();

    public enum AggPhase {
        FIRST,
        FIRST_MERGE,
        SECOND,
        SECOND_MERGE;

        public boolean isMerge() {
            return this == FIRST_MERGE || this == SECOND_MERGE;
        }
    }

    private final AggPhase aggPhase;

    // C'tor creates copies of groupingExprs and aggExprs.
    private AggregateInfo(ArrayList<Expr> groupingExprs,
                          ArrayList<FunctionCallExpr> aggExprs, AggPhase aggPhase)  {
        Preconditions.checkState(groupingExprs != null || aggExprs != null);
        this.groupingExprs =
                groupingExprs != null ? Expr.cloneList(groupingExprs) : new ArrayList<Expr>();
        aggregateExprs =
                aggExprs != null ? Expr.cloneList(aggExprs) : new ArrayList<FunctionCallExpr>();
        this.aggPhase = aggPhase;
    }

    /**
     * C'tor for cloning.
     */
    private AggregateInfo(AggregateInfo other) {
        groupingExprs =
                (other.groupingExprs != null) ? Expr.cloneList(other.groupingExprs) : null;
        aggregateExprs =
                (other.aggregateExprs != null) ? Expr.cloneList(other.aggregateExprs) : null;
        outputTupleDesc = other.outputTupleDesc;
        materializedSlots = Lists.newArrayList(other.materializedSlots);
        materializedSlotLabels = Lists.newArrayList(other.materializedSlotLabels);
        aggPhase = other.aggPhase;
    }

    /**
     * Used by new optimizer.
     */
    public static AggregateInfo create(
            ArrayList<Expr> groupingExprs, ArrayList<FunctionCallExpr> aggExprs, List<Integer> aggExprIds,
            boolean isPartialAgg, TupleDescriptor tupleDesc, AggPhase phase) {
        AggregateInfo result = new AggregateInfo(groupingExprs, aggExprs, phase);
        result.outputTupleDesc = tupleDesc;
        int aggExprSize = result.getAggregateExprs().size();
        for (int i = 0; i < aggExprSize; i++) {
            result.materializedSlots.add(i);
            String label = (isPartialAgg ? "partial_" : "")
                    + aggExprs.get(i).toSql() + "[#" + aggExprIds.get(i) + "]";
            result.materializedSlotLabels.add(label);
        }
        return result;
    }

    public ArrayList<Expr> getGroupingExprs() {
        return groupingExprs;
    }

    public ArrayList<FunctionCallExpr> getAggregateExprs() {
        return aggregateExprs;
    }

    public TupleDescriptor getOutputTupleDesc() {
        return outputTupleDesc;
    }

    public TupleId getOutputTupleId() {
        return outputTupleDesc.getId();
    }

    public List<String> getMaterializedAggregateExprLabels() {
        return Lists.newArrayList(materializedSlotLabels);
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
            materializedSlots.add(i - groupingExprNum);
        }
    }

    @Override
    public AggregateInfo clone() {
        return new AggregateInfo(this);
    }

}
