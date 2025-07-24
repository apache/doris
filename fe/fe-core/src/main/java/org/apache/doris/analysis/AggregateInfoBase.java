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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/AggregateInfoBase.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Type;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for AggregateInfo and AnalyticInfo containing the intermediate and output
 * tuple descriptors as well as their smaps for evaluating aggregate functions.
 */
public abstract class AggregateInfoBase {
    private static final Logger LOG =
            LoggerFactory.getLogger(AggregateInfoBase.class);

    // For aggregations: All unique grouping expressions from a select block.
    // For analytics: Empty.
    protected ArrayList<Expr> groupingExprs;

    // For aggregations: All unique aggregate expressions from a select block.
    // For analytics: The results of AnalyticExpr.getFnCall() for the unique
    // AnalyticExprs of a select block.
    protected ArrayList<FunctionCallExpr> aggregateExprs;

    // The tuple into which the intermediate output of an aggregation is materialized.
    // Contains groupingExprs.size() + aggregateExprs.size() slots, the first of which
    // contain the values of the grouping exprs, followed by slots into which the
    // aggregateExprs' update()/merge() symbols materialize their output, i.e., slots
    // of the aggregate functions' intermediate types.
    // Identical to outputTupleDesc_ if no aggregateExpr has an output type that is
    // different from its intermediate type.
    protected TupleDescriptor intermediateTupleDesc;

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

    protected AggregateInfoBase(ArrayList<Expr> groupingExprs,
                                ArrayList<FunctionCallExpr> aggExprs)  {
        Preconditions.checkState(groupingExprs != null || aggExprs != null);
        this.groupingExprs =
                groupingExprs != null ? Expr.cloneList(groupingExprs) : new ArrayList<Expr>();
        Preconditions.checkState(aggExprs != null || !(this instanceof AnalyticInfo));
        aggregateExprs =
                aggExprs != null ? Expr.cloneList(aggExprs) : new ArrayList<FunctionCallExpr>();
    }

    /**
     * C'tor for cloning.
     */
    protected AggregateInfoBase(AggregateInfoBase other) {
        groupingExprs =
                (other.groupingExprs != null) ? Expr.cloneList(other.groupingExprs) : null;
        aggregateExprs =
                (other.aggregateExprs != null) ? Expr.cloneList(other.aggregateExprs) : null;
        intermediateTupleDesc = other.intermediateTupleDesc;
        outputTupleDesc = other.outputTupleDesc;
        materializedSlots = Lists.newArrayList(other.materializedSlots);
        materializedSlotLabels = Lists.newArrayList(other.materializedSlotLabels);
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

    public TupleDescriptor getIntermediateTupleDesc() {
        return intermediateTupleDesc;
    }

    public TupleId getIntermediateTupleId() {
        return intermediateTupleDesc.getId();
    }

    public TupleId getOutputTupleId() {
        return outputTupleDesc.getId();
    }

    public List<String> getMaterializedAggregateExprLabels() {
        return Lists.newArrayList(materializedSlotLabels);
    }

    public boolean requiresIntermediateTuple() {
        Preconditions.checkNotNull(intermediateTupleDesc);
        Preconditions.checkNotNull(outputTupleDesc);
        return intermediateTupleDesc != outputTupleDesc;
    }

    /**
     * Returns true if evaluating the given aggregate exprs requires an intermediate tuple,
     * i.e., whether one of the aggregate functions has an intermediate type different from
     * its output type.
     */
    public static <T extends Expr> boolean requiresIntermediateTuple(List<T> aggExprs) {
        for (Expr aggExpr : aggExprs) {
            Type intermediateType = ((AggregateFunction) aggExpr.fn).getIntermediateType();
            if (intermediateType != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * output tuple maybe different from intermediate when noGrouping and fn null mode
     * is depend on argument
     */
    public static <T extends Expr> boolean requiresIntermediateTuple(List<T> aggExprs, boolean noGrouping) {
        for (Expr aggExpr : aggExprs) {
            Type intermediateType = ((AggregateFunction) aggExpr.fn).getIntermediateType();
            if (intermediateType != null) {
                return true;
            }
            if (noGrouping && aggExpr.fn.getNullableMode().equals(Function.NullableMode.DEPEND_ON_ARGUMENT)) {
                return true;
            }
        }
        return false;
    }

    public String debugString() {
        StringBuilder out = new StringBuilder();
        out.append(MoreObjects.toStringHelper(this)
                .add("grouping_exprs", Expr.debugString(groupingExprs))
                .add("aggregate_exprs", Expr.debugString(aggregateExprs))
                .add("intermediate_tuple", (intermediateTupleDesc == null)
                        ? "null" : intermediateTupleDesc.debugString())
                .add("output_tuple", (outputTupleDesc == null)
                        ? "null" : outputTupleDesc.debugString())
                .toString());
        return out.toString();
    }

    protected abstract String tupleDebugName();
}
