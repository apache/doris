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

package org.apache.doris.analysis;

import org.apache.doris.common.TreeNode;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

/**
 * Encapsulates all the information needed to compute ORDER BY
 * This doesn't contain aliases or positional exprs.
 * TODO: reorganize this completely, this doesn't really encapsulate anything; this
 * should move into planner/ and encapsulate the implementation of the sort of a
 * particular input row (materialize all row slots)
 */
public class SortInfo {
    private final static Logger LOG = LogManager.getLogger(SortInfo.class);
    // All ordering exprs with cost greater than this will be materialized. Since we don't
    // currently have any information about actual function costs, this value is intended to
    // ensure that all expensive functions will be materialized while still leaving simple
    // operations unmaterialized, for example 'SlotRef + SlotRef' should have a cost below
    // this threshold.
    // TODO: rethink this when we have a better cost model.
    private static final float SORT_MATERIALIZATION_COST_THRESHOLD = Expr.FUNCTION_CALL_COST;

    private List<Expr> orderingExprs_;
    private final List<Boolean> isAscOrder_;
    // True if "NULLS FIRST", false if "NULLS LAST", null if not specified.
    private final List<Boolean> nullsFirstParams_;
    // Subset of ordering exprs that are materialized. Populated in
    // createMaterializedOrderExprs(), used for EXPLAIN output.
    private List<Expr> materializedOrderingExprs_;
    // The single tuple that is materialized, sorted, and output by a sort operator
    // (i.e. SortNode or TopNNode)
    private TupleDescriptor sortTupleDesc_;
    // Input expressions materialized into sortTupleDesc_. One expr per slot in
    // sortTupleDesc_.
    private List<Expr> sortTupleSlotExprs_;

    public SortInfo(List<Expr> orderingExprs, List<Boolean> isAscOrder,
                    List<Boolean> nullsFirstParams) {
        Preconditions.checkArgument(orderingExprs.size() == isAscOrder.size());
        Preconditions.checkArgument(orderingExprs.size() == nullsFirstParams.size());
        orderingExprs_ = orderingExprs;
        isAscOrder_ = isAscOrder;
        nullsFirstParams_ = nullsFirstParams;
        materializedOrderingExprs_ = Lists.newArrayList();
    }

    /**
     * C'tor for cloning.
     */
    private SortInfo(SortInfo other) {
        orderingExprs_ = Expr.cloneList(other.orderingExprs_);
        isAscOrder_ = Lists.newArrayList(other.isAscOrder_);
        nullsFirstParams_ = Lists.newArrayList(other.nullsFirstParams_);
        materializedOrderingExprs_ = Expr.cloneList(other.materializedOrderingExprs_);
        sortTupleDesc_ = other.sortTupleDesc_;
        if (other.sortTupleSlotExprs_ != null) {
            sortTupleSlotExprs_ = Expr.cloneList(other.sortTupleSlotExprs_);
        }
    }

    /**
     * Sets sortTupleDesc_, which is the internal row representation to be materialized and
     * sorted. The source exprs of the slots in sortTupleDesc_ are changed to those in
     * tupleSlotExprs.
     */
    public void setMaterializedTupleInfo(
            TupleDescriptor tupleDesc, List<Expr> tupleSlotExprs) {
        Preconditions.checkState(tupleDesc.getSlots().size() == tupleSlotExprs.size());
        sortTupleDesc_ = tupleDesc;
        sortTupleSlotExprs_ = tupleSlotExprs;
        for (int i = 0; i < sortTupleDesc_.getSlots().size(); ++i) {
            SlotDescriptor slotDesc = sortTupleDesc_.getSlots().get(i);
            slotDesc.setSourceExpr(sortTupleSlotExprs_.get(i));
        }
    }

    public List<Expr> getOrderingExprs() { return orderingExprs_; }
    public List<Boolean> getIsAscOrder() { return isAscOrder_; }
    public List<Boolean> getNullsFirstParams() { return nullsFirstParams_; }
    public List<Expr> getMaterializedOrderingExprs() { return materializedOrderingExprs_; }
    public List<Expr> getSortTupleSlotExprs() { return sortTupleSlotExprs_; }
    public TupleDescriptor getSortTupleDescriptor() { return sortTupleDesc_; }

    /**
     * Gets the list of booleans indicating whether nulls come first or last, independent
     * of asc/desc.
     */
    public List<Boolean> getNullsFirst() {
        Preconditions.checkState(orderingExprs_.size() == nullsFirstParams_.size());
        List<Boolean> nullsFirst = Lists.newArrayList();
        for (int i = 0; i < orderingExprs_.size(); ++i) {
            nullsFirst.add(OrderByElement.nullsFirst(nullsFirstParams_.get(i),
                    isAscOrder_.get(i)));
        }
        return nullsFirst;
    }

    /**
     * Materializes the slots in sortTupleDesc_ referenced in the ordering exprs.
     * Materializes the slots referenced by the corresponding sortTupleSlotExpr after
     * applying the 'smap'.
     */
    public void materializeRequiredSlots(Analyzer analyzer, ExprSubstitutionMap smap) {
        Preconditions.checkNotNull(sortTupleDesc_);
        Preconditions.checkNotNull(sortTupleSlotExprs_);
        Preconditions.checkState(sortTupleDesc_.isMaterialized());
        analyzer.materializeSlots(orderingExprs_);
        List<SlotDescriptor> sortTupleSlotDescs = sortTupleDesc_.getSlots();
        List<Expr> materializedExprs = Lists.newArrayList();
        for (int i = 0; i < sortTupleSlotDescs.size(); ++i) {
            if (sortTupleSlotDescs.get(i).isMaterialized()) {
                materializedExprs.add(sortTupleSlotExprs_.get(i));
            }
        }
        List<Expr> substMaterializedExprs =
                Expr.substituteList(materializedExprs, smap, analyzer, false);
        analyzer.materializeSlots(substMaterializedExprs);
    }

    public void substituteOrderingExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
        orderingExprs_ = Expr.substituteList(orderingExprs_, smap, analyzer, false);
    }

    /**
     * Asserts that all ordering exprs are bound by the sort tuple.
     */
    public void checkConsistency() {
        for (Expr orderingExpr: orderingExprs_) {
            Preconditions.checkState(orderingExpr.isBound(sortTupleDesc_.getId()));
        }
    }

    @Override
    public SortInfo clone() { return new SortInfo(this); }

    /**
     * Create a tuple descriptor for the single tuple that is materialized, sorted, and
     * output by the sort node. Materializes slots required by 'resultExprs' as well as
     * non-deterministic and expensive order by exprs. The materialized exprs are
     * substituted with slot refs into the new tuple. This simplifies the sorting logic for
     * total and top-n sorts. The substitution map is returned.
     */
    public ExprSubstitutionMap createSortTupleInfo(
            List<Expr> resultExprs, Analyzer analyzer) {
        // The descriptor for the tuples on which the sort operates.
        TupleDescriptor sortTupleDesc = analyzer.getDescTbl().createTupleDescriptor("sort");
        sortTupleDesc.setIsMaterialized(true);
        List<Expr> sortTupleExprs = Lists.newArrayList();

        // substOrderBy is a mapping from exprs evaluated on the sort input that get
        // materialized into the sort tuple to their corresponding SlotRefs in the sort tuple.
        // The following exprs are materialized:
        // 1. Ordering exprs that we chose to materialize
        // 2. SlotRefs against the sort input contained in the result and ordering exprs after
        // substituting the materialized ordering exprs.

        // Case 1:
        ExprSubstitutionMap substOrderBy =
                createMaterializedOrderExprs(sortTupleDesc, analyzer);
        sortTupleExprs.addAll(substOrderBy.getLhs());

        // Case 2: SlotRefs in the result and ordering exprs after substituting the
        // materialized ordering exprs.
        Set<SlotRef> sourceSlots = Sets.newHashSet();
        TreeNode.collect(Expr.substituteList(resultExprs, substOrderBy, analyzer, false),
                Predicates.instanceOf(SlotRef.class), sourceSlots);
        TreeNode.collect(Expr.substituteList(orderingExprs_, substOrderBy, analyzer, false),
                Predicates.instanceOf(SlotRef.class), sourceSlots);
        for (SlotRef origSlotRef: sourceSlots) {
            // Don't rematerialize slots that are already in the sort tuple.
            if (origSlotRef.getDesc().getParent().getId() != sortTupleDesc.getId()) {
                SlotDescriptor origSlotDesc = origSlotRef.getDesc();
                SlotDescriptor materializedDesc =
                        analyzer.copySlotDescriptor(origSlotDesc, sortTupleDesc);
                SlotRef cloneRef = new SlotRef(materializedDesc);
                substOrderBy.put(origSlotRef, cloneRef);
                sortTupleExprs.add(origSlotRef);
            }
        }

        // The ordering exprs are evaluated against the sort tuple, so they must reflect the
        // materialization decision above.
        substituteOrderingExprs(substOrderBy, analyzer);

        // Update the tuple descriptor used to materialize the input of the sort.
        setMaterializedTupleInfo(sortTupleDesc, sortTupleExprs);

        return substOrderBy;
    }

    /**
     * Materialize ordering exprs by creating slots for them in 'sortTupleDesc' if they:
     * - contain a non-deterministic expr
     * - contain a UDF (since we don't know if they're deterministic)
     * - are more expensive than a cost threshold
     * - don't have a cost set
     *
     * Populates 'materializedOrderingExprs_' and returns a mapping from the original
     * ordering exprs to the new SlotRefs. It is expected that this smap will be passed into
     * substituteOrderingExprs() by the caller.
     */
    public ExprSubstitutionMap createMaterializedOrderExprs(
            TupleDescriptor sortTupleDesc, Analyzer analyzer) {
        ExprSubstitutionMap substOrderBy = new ExprSubstitutionMap();
        for (Expr origOrderingExpr : orderingExprs_) {
            // TODO(zc): support materialized order exprs
            // if (!origOrderingExpr.hasCost()
            //         || origOrderingExpr.getCost() > SORT_MATERIALIZATION_COST_THRESHOLD
            //         || origOrderingExpr.contains(Expr.IS_NONDETERMINISTIC_BUILTIN_FN_PREDICATE)
            //         || origOrderingExpr.contains(Expr.IS_UDF_PREDICATE)) {
            //     SlotDescriptor materializedDesc = analyzer.addSlotDescriptor(sortTupleDesc);
            //     materializedDesc.initFromExpr(origOrderingExpr);
            //     materializedDesc.setIsMaterialized(true);
            //     SlotRef materializedRef = new SlotRef(materializedDesc);
            //     substOrderBy.put(origOrderingExpr, materializedRef);
            //     materializedOrderingExprs_.add(origOrderingExpr);
            // }
            {
                SlotDescriptor materializedDesc = analyzer.addSlotDescriptor(sortTupleDesc);
                materializedDesc.initFromExpr(origOrderingExpr);
                materializedDesc.setIsMaterialized(true);
                SlotRef materializedRef = new SlotRef(materializedDesc);
                substOrderBy.put(origOrderingExpr, materializedRef);
                materializedOrderingExprs_.add(origOrderingExpr);
            }
        }
        return substOrderBy;
    }
}

