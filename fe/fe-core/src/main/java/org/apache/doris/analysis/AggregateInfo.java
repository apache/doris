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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

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

    private final AggPhase aggPhase;

    // C'tor creates copies of groupingExprs and aggExprs.
    private AggregateInfo(ArrayList<Expr> groupingExprs,
                          ArrayList<FunctionCallExpr> aggExprs, AggPhase aggPhase)  {
        super(groupingExprs, aggExprs);
        this.aggPhase = aggPhase;
    }

    /**
     * C'tor for cloning.
     */
    private AggregateInfo(AggregateInfo other) {
        super(other);
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
            if (outputSlots.get(i).isMaterialized()) {
                materializedSlots.add(i - groupingExprNum);
            }
        }
    }

    public String debugString() {
        StringBuilder out = new StringBuilder(super.debugString());
        out.append(MoreObjects.toStringHelper(this)
                .add("phase", aggPhase)
                .toString());
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

}
