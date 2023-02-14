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
import java.util.HashMap;
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

    /**
     * Creates the intermediate and output tuple descriptors. If no agg expr has an
     * intermediate type different from its output type, then only the output tuple
     * descriptor is created and the intermediate tuple is set to the output tuple.
     */
    protected void createTupleDescs(Analyzer analyzer) {
        // Create the intermediate tuple desc first, so that the tuple ids are increasing
        // from bottom to top in the plan tree.
        intermediateTupleDesc = createTupleDesc(analyzer, false);
        if (requiresIntermediateTuple(aggregateExprs, groupingExprs.size() == 0)) {
            outputTupleDesc = createTupleDesc(analyzer, true);
            // save the output and intermediate slots info into global desc table
            // after creaing the plan, we can call materializeIntermediateSlots method
            // to set the materialized info to intermediate slots based on output slots.
            ArrayList<SlotDescriptor> outputSlots = outputTupleDesc.getSlots();
            ArrayList<SlotDescriptor> intermediateSlots = intermediateTupleDesc.getSlots();
            HashMap<SlotDescriptor, SlotDescriptor> mapping = new HashMap<>();
            for (int i = 0; i < outputSlots.size(); ++i) {
                mapping.put(outputSlots.get(i), intermediateSlots.get(i));
            }
            analyzer.getDescTbl().addSlotMappingInfo(mapping);
        } else {
            outputTupleDesc = intermediateTupleDesc;
        }
    }

    /**
     * Returns a tuple descriptor for the aggregation/analytic's intermediate or final
     * result, depending on whether isOutputTuple is true or false.
     * Also updates the appropriate substitution map, and creates and registers auxiliary
     * equality predicates between the grouping slots and the grouping exprs.
     */
    protected TupleDescriptor createTupleDesc(Analyzer analyzer, boolean isOutputTuple) {
        TupleDescriptor result =
                analyzer.getDescTbl().createTupleDescriptor(
                        tupleDebugName() + (isOutputTuple ? "-out" : "-intermed"));
        List<Expr> exprs = Lists.newArrayListWithCapacity(
                groupingExprs.size() + aggregateExprs.size());
        exprs.addAll(groupingExprs);
        exprs.addAll(aggregateExprs);

        int aggregateExprStartIndex = groupingExprs.size();
        // if agg is grouping set, so we should set all groupingExpr unless last groupingExpr
        // must set be be nullable
        boolean isGroupingSet = !groupingExprs.isEmpty()
                && groupingExprs.get(groupingExprs.size() - 1) instanceof VirtualSlotRef;

        // the agg node may output slots from child outer join node
        // to make the agg node create the output tuple desc correctly, we need change the slots' to nullable
        // from all outer join nullable side temporarily
        // after create the output tuple we need revert the change by call analyzer.changeSlotsToNotNullable(slots)
        List<SlotDescriptor> slots = analyzer.changeSlotToNullableOfOuterJoinedTuples();
        for (int i = 0; i < exprs.size(); ++i) {
            Expr expr = exprs.get(i);
            SlotDescriptor slotDesc = analyzer.addSlotDescriptor(result);
            slotDesc.initFromExpr(expr);
            if (expr instanceof SlotRef) {
                if (((SlotRef) expr).getColumn() != null) {
                    slotDesc.setColumn(((SlotRef) expr).getColumn());
                }
            }
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
                FunctionCallExpr aggExpr = (FunctionCallExpr) expr;
                if (aggExpr.isMergeAggFn()) {
                    slotDesc.setLabel(aggExpr.getChild(0).toSql());
                    slotDesc.setSourceExpr(aggExpr.getChild(0));
                } else {
                    slotDesc.setLabel(aggExpr.toSql());
                    slotDesc.setSourceExpr(aggExpr);
                }

                if (isOutputTuple && aggExpr.getFn().getNullableMode().equals(Function.NullableMode.DEPEND_ON_ARGUMENT)
                        && groupingExprs.size() == 0) {
                    slotDesc.setIsNullable(true);
                }

                if (!isOutputTuple) {
                    Type intermediateType = ((AggregateFunction) aggExpr.fn).getIntermediateType();
                    if (intermediateType != null) {
                        // Use the output type as intermediate if the function has a wildcard decimal.
                        if (!intermediateType.isWildcardDecimal()) {
                            slotDesc.setType(intermediateType);
                        } else {
                            Preconditions.checkState(expr.getType().isDecimalV2() || expr.getType().isDecimalV3());
                        }
                    }
                }
            }
        }
        analyzer.changeSlotsToNotNullable(slots);

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
