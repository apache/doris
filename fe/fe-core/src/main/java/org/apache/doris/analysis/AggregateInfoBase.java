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
    private final static Logger LOG =
            LoggerFactory.getLogger(AggregateInfoBase.class);

    // For aggregations: All unique grouping expressions from a select block.
    // For analytics: Empty.
    protected ArrayList<Expr> groupingExprs_;

    // For aggregations: All unique aggregate expressions from a select block.
    // For analytics: The results of AnalyticExpr.getFnCall() for the unique
    // AnalyticExprs of a select block.
    protected ArrayList<FunctionCallExpr> aggregateExprs_;

    // The tuple into which the intermediate output of an aggregation is materialized.
    // Contains groupingExprs.size() + aggregateExprs.size() slots, the first of which
    // contain the values of the grouping exprs, followed by slots into which the
    // aggregateExprs' update()/merge() symbols materialize their output, i.e., slots
    // of the aggregate functions' intermediate types.
    // Identical to outputTupleDesc_ if no aggregateExpr has an output type that is
    // different from its intermediate type.
    protected TupleDescriptor intermediateTupleDesc_;

    // The tuple into which the final output of the aggregation is materialized.
    // Contains groupingExprs.size() + aggregateExprs.size() slots, the first of which
    // contain the values of the grouping exprs, followed by slots into which the
    // aggregateExprs' finalize() symbol write its result, i.e., slots of the aggregate
    // functions' output types.
    protected TupleDescriptor outputTupleDesc_;

    // For aggregation: indices into aggregate exprs for that need to be materialized
    // For analytics: indices into the analytic exprs and their corresponding aggregate
    // exprs that need to be materialized.
    // Populated in materializeRequiredSlots() which must be implemented by subclasses.
    protected ArrayList<Integer> materializedSlots_ = Lists.newArrayList();

    protected AggregateInfoBase(ArrayList<Expr> groupingExprs,
                                ArrayList<FunctionCallExpr> aggExprs)  {
        Preconditions.checkState(groupingExprs != null || aggExprs != null);
        groupingExprs_ =
                groupingExprs != null ? Expr.cloneList(groupingExprs) : new ArrayList<Expr>();
        Preconditions.checkState(aggExprs != null || !(this instanceof AnalyticInfo));
        aggregateExprs_ =
                aggExprs != null ? Expr.cloneList(aggExprs) : new ArrayList<FunctionCallExpr>();
    }

    /**
     * C'tor for cloning.
     */
    protected AggregateInfoBase(AggregateInfoBase other) {
        groupingExprs_ =
                (other.groupingExprs_ != null) ? Expr.cloneList(other.groupingExprs_) : null;
        aggregateExprs_ =
                (other.aggregateExprs_ != null) ? Expr.cloneList(other.aggregateExprs_) : null;
        intermediateTupleDesc_ = other.intermediateTupleDesc_;
        outputTupleDesc_ = other.outputTupleDesc_;
        materializedSlots_ = Lists.newArrayList(other.materializedSlots_);
    }

    /**
     * Creates the intermediate and output tuple descriptors. If no agg expr has an
     * intermediate type different from its output type, then only the output tuple
     * descriptor is created and the intermediate tuple is set to the output tuple.
     */
    protected void createTupleDescs(Analyzer analyzer) {
        // Create the intermediate tuple desc first, so that the tuple ids are increasing
        // from bottom to top in the plan tree.
        intermediateTupleDesc_ = createTupleDesc(analyzer, false);
        if (requiresIntermediateTuple(aggregateExprs_, groupingExprs_.size() == 0)) {
            outputTupleDesc_ = createTupleDesc(analyzer, true);
        } else {
            outputTupleDesc_ = intermediateTupleDesc_;
        }
    }

    /**
     * Returns a tuple descriptor for the aggregation/analytic's intermediate or final
     * result, depending on whether isOutputTuple is true or false.
     * Also updates the appropriate substitution map, and creates and registers auxiliary
     * equality predicates between the grouping slots and the grouping exprs.
     */
    private TupleDescriptor createTupleDesc(Analyzer analyzer, boolean isOutputTuple) {
        TupleDescriptor result =
                analyzer.getDescTbl().createTupleDescriptor(
                        tupleDebugName() + (isOutputTuple ? "-out" : "-intermed"));
        List<Expr> exprs = Lists.newArrayListWithCapacity(
                groupingExprs_.size() + aggregateExprs_.size());
        exprs.addAll(groupingExprs_);
        exprs.addAll(aggregateExprs_);

        int aggregateExprStartIndex = groupingExprs_.size();
        // if agg is grouping set, so we should set all groupingExpr unless last groupingExpr
        // must set be be nullable
        boolean isGroupingSet = !groupingExprs_.isEmpty() &&
                groupingExprs_.get(groupingExprs_.size() - 1) instanceof VirtualSlotRef;

        for (int i = 0; i < exprs.size(); ++i) {
            Expr expr = exprs.get(i);
            SlotDescriptor slotDesc = analyzer.addSlotDescriptor(result);
            slotDesc.initFromExpr(expr);
            // Not change the nullable of slot desc when is not grouping set id
            if (isGroupingSet && i < aggregateExprStartIndex - 1 && !(expr instanceof VirtualSlotRef)) {
                slotDesc.setIsNullable(true);
            }
            if (i < aggregateExprStartIndex) {
                // register equivalence between grouping slot and grouping expr;
                // do this only when the grouping expr isn't a constant, otherwise
                // it'll simply show up as a gratuitous HAVING predicate
                // (which would actually be incorrect if the constant happens to be NULL)
                if (!expr.isConstant()) {
                    analyzer.createAuxEquivPredicate(new SlotRef(slotDesc), expr.clone());
                }
            } else {
                Preconditions.checkArgument(expr instanceof FunctionCallExpr);
                FunctionCallExpr aggExpr = (FunctionCallExpr)expr;
                if (aggExpr.isMergeAggFn()) {
                    slotDesc.setLabel(aggExpr.getChild(0).toSql());
                    slotDesc.setSourceExpr(aggExpr.getChild(0));
                } else {
                    slotDesc.setLabel(aggExpr.toSql());
                    slotDesc.setSourceExpr(aggExpr);
                }

                if (isOutputTuple && aggExpr.getFn().getNullableMode().equals(Function.NullableMode.DEPEND_ON_ARGUMENT) &&
                        groupingExprs_.size() == 0) {
                    slotDesc.setIsNullable(true);
                }

                if (!isOutputTuple) {
                    Type intermediateType = ((AggregateFunction)aggExpr.fn).getIntermediateType();
                    if (intermediateType != null) {
                        // Use the output type as intermediate if the function has a wildcard decimal.
                        if (!intermediateType.isWildcardDecimal()) {
                            slotDesc.setType(intermediateType);
                        } else {
                            Preconditions.checkState(expr.getType().isDecimalV2());
                        }
                    }
                }
            }
        }

        if (LOG.isTraceEnabled()) {
            String prefix = (isOutputTuple ? "result " : "intermediate ");
            LOG.trace(prefix + " tuple=" + result.debugString());
        }
        return result;
    }

    /**
     * Marks the slots required for evaluating an Analytic/AggregateInfo by
     * resolving the materialized aggregate/analytic exprs against smap,
     * and then marking their slots.
     */
    public abstract void materializeRequiredSlots(Analyzer analyzer,
                                                  ExprSubstitutionMap smap);

    public ArrayList<Expr> getGroupingExprs() { return groupingExprs_; }
    public ArrayList<FunctionCallExpr> getAggregateExprs() { return aggregateExprs_; }
    public TupleDescriptor getOutputTupleDesc() { return outputTupleDesc_; }
    public TupleDescriptor getIntermediateTupleDesc() { return intermediateTupleDesc_; }
    public TupleId getIntermediateTupleId() { return intermediateTupleDesc_.getId(); }
    public TupleId getOutputTupleId() { return outputTupleDesc_.getId(); }
    public boolean requiresIntermediateTuple() {
        Preconditions.checkNotNull(intermediateTupleDesc_);
        Preconditions.checkNotNull(outputTupleDesc_);
        return intermediateTupleDesc_ != outputTupleDesc_;
    }

    /**
     * Returns true if evaluating the given aggregate exprs requires an intermediate tuple,
     * i.e., whether one of the aggregate functions has an intermediate type different from
     * its output type.
     */
    public static <T extends Expr> boolean requiresIntermediateTuple(List<T> aggExprs) {
        for (Expr aggExpr: aggExprs) {
            Type intermediateType = ((AggregateFunction) aggExpr.fn).getIntermediateType();
            if (intermediateType != null) return true;
        }
        return false;
    }

    /**
     * output tuple maybe different from intermediate when noGrouping and fn null mode
     * is depend on argument
     */
    public static <T extends Expr> boolean requiresIntermediateTuple(List<T> aggExprs, boolean noGrouping) {
        for (Expr aggExpr: aggExprs) {
            Type intermediateType = ((AggregateFunction) aggExpr.fn).getIntermediateType();
            if (intermediateType != null) return true;
            if (noGrouping && ((AggregateFunction) aggExpr.fn).getNullableMode().equals(Function.NullableMode.DEPEND_ON_ARGUMENT)) {
                return true;
            }
        }
        return false;
    }

    public String debugString() {
        StringBuilder out = new StringBuilder();
        out.append(MoreObjects.toStringHelper(this)
                .add("grouping_exprs", Expr.debugString(groupingExprs_))
                .add("aggregate_exprs", Expr.debugString(aggregateExprs_))
                .add("intermediate_tuple", (intermediateTupleDesc_ == null)
                        ? "null" : intermediateTupleDesc_.debugString())
                .add("output_tuple", (outputTupleDesc_ == null)
                        ? "null" : outputTupleDesc_.debugString())
                .toString());
        return out.toString();
    }

    protected abstract String tupleDebugName();
}
