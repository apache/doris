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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/SortInfo.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.common.TreeNode;
import org.apache.doris.thrift.TSortInfo;

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
    private static final Logger LOG = LogManager.getLogger(SortInfo.class);
    // All ordering exprs with cost greater than this will be materialized. Since we don't
    // currently have any information about actual function costs, this value is intended to
    // ensure that all expensive functions will be materialized while still leaving simple
    // operations unmaterialized, for example 'SlotRef + SlotRef' should have a cost below
    // this threshold.
    // TODO: rethink this when we have a better cost model.
    private static final float SORT_MATERIALIZATION_COST_THRESHOLD = Expr.FUNCTION_CALL_COST;

    private List<Expr> orderingExprs;
    private List<Expr> origOrderingExprs;
    private final List<Boolean> isAscOrder;
    // True if "NULLS FIRST", false if "NULLS LAST", null if not specified.
    private final List<Boolean> nullsFirstParams;
    // Subset of ordering exprs that are materialized. Populated in
    // createMaterializedOrderExprs(), used for EXPLAIN output.
    private List<Expr> materializedOrderingExprs;
    // The single tuple that is materialized, sorted, and output by a sort operator
    // (i.e. SortNode or TopNNode)
    private TupleDescriptor sortTupleDesc;
    // Input expressions materialized into sortTupleDesc_. One expr per slot in
    // sortTupleDesc_.
    private List<Expr> sortTupleSlotExprs;
    private boolean useTwoPhaseRead = false;

    public SortInfo(List<Expr> orderingExprs, List<Boolean> isAscOrder,
                    List<Boolean> nullsFirstParams) {
        Preconditions.checkArgument(orderingExprs.size() == isAscOrder.size());
        Preconditions.checkArgument(orderingExprs.size() == nullsFirstParams.size());
        this.orderingExprs = orderingExprs;
        this.isAscOrder = isAscOrder;
        this.nullsFirstParams = nullsFirstParams;
        materializedOrderingExprs = Lists.newArrayList();
    }

    /**
     * Used by new optimizer.
     */
    public SortInfo(List<Expr> orderingExprs,
                    List<Boolean> isAscOrder,
                    List<Boolean> nullsFirstParams,
                    TupleDescriptor sortTupleDesc) {
        this.orderingExprs = orderingExprs;
        this.isAscOrder = isAscOrder;
        this.nullsFirstParams = nullsFirstParams;
        this.sortTupleDesc = sortTupleDesc;
    }

    /**
     * C'tor for cloning.
     */
    private SortInfo(SortInfo other) {
        orderingExprs = Expr.cloneList(other.orderingExprs);
        isAscOrder = Lists.newArrayList(other.isAscOrder);
        nullsFirstParams = Lists.newArrayList(other.nullsFirstParams);
        materializedOrderingExprs = Expr.cloneList(other.materializedOrderingExprs);
        sortTupleDesc = other.sortTupleDesc;
        if (other.sortTupleSlotExprs != null) {
            sortTupleSlotExprs = Expr.cloneList(other.sortTupleSlotExprs);
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
        sortTupleDesc = tupleDesc;
        sortTupleSlotExprs = tupleSlotExprs;
        for (int i = 0; i < sortTupleDesc.getSlots().size(); ++i) {
            SlotDescriptor slotDesc = sortTupleDesc.getSlots().get(i);
            slotDesc.setSourceExpr(sortTupleSlotExprs.get(i));
        }
    }

    public List<Expr> getOrderingExprs() {
        return orderingExprs;
    }

    public List<Expr> getOrigOrderingExprs() {
        return origOrderingExprs;
    }

    public List<Boolean> getIsAscOrder() {
        return isAscOrder;
    }

    public List<Boolean> getNullsFirstParams() {
        return nullsFirstParams;
    }

    public List<Expr> getMaterializedOrderingExprs() {
        return materializedOrderingExprs;
    }

    public void addMaterializedOrderingExpr(Expr expr) {
        if (materializedOrderingExprs == null) {
            materializedOrderingExprs = Lists.newArrayList();
        }
        materializedOrderingExprs.add(expr);
    }

    public List<Expr> getSortTupleSlotExprs() {
        return sortTupleSlotExprs;
    }

    public void setSortTupleSlotExprs(List<Expr> sortTupleSlotExprs) {
        this.sortTupleSlotExprs = sortTupleSlotExprs;
    }

    public void setSortTupleDesc(TupleDescriptor tupleDesc) {
        sortTupleDesc = tupleDesc;
    }

    public void setUseTwoPhaseRead() {
        useTwoPhaseRead = true;
    }

    public boolean useTwoPhaseRead() {
        return useTwoPhaseRead;
    }

    public TupleDescriptor getSortTupleDescriptor() {
        return sortTupleDesc;
    }

    /**
     * Gets the list of booleans indicating whether nulls come first or last, independent
     * of asc/desc.
     */
    public List<Boolean> getNullsFirst() {
        Preconditions.checkState(orderingExprs.size() == nullsFirstParams.size());
        List<Boolean> nullsFirst = Lists.newArrayList();
        for (int i = 0; i < orderingExprs.size(); ++i) {
            nullsFirst.add(OrderByElement.nullsFirst(nullsFirstParams.get(i),
                    isAscOrder.get(i)));
        }
        return nullsFirst;
    }

    /**
     * Materializes the slots in sortTupleDesc_ referenced in the ordering exprs.
     * Materializes the slots referenced by the corresponding sortTupleSlotExpr after
     * applying the 'smap'.
     */
    public void materializeRequiredSlots(Analyzer analyzer, ExprSubstitutionMap smap) {
        Preconditions.checkNotNull(sortTupleDesc);
        Preconditions.checkNotNull(sortTupleSlotExprs);
        Preconditions.checkState(sortTupleDesc.isMaterialized());
        analyzer.materializeSlots(orderingExprs);
        List<SlotDescriptor> sortTupleSlotDescs = sortTupleDesc.getSlots();
        List<Expr> materializedExprs = Lists.newArrayList();
        for (int i = 0; i < sortTupleSlotDescs.size(); ++i) {
            if (sortTupleSlotDescs.get(i).isMaterialized()) {
                materializedExprs.add(sortTupleSlotExprs.get(i));
            }
        }
        List<Expr> substMaterializedExprs =
                Expr.substituteList(materializedExprs, smap, analyzer, false);
        analyzer.materializeSlots(substMaterializedExprs);
    }

    public void substituteOrderingExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
        orderingExprs = Expr.substituteList(orderingExprs, smap, analyzer, false);
    }

    /**
     * Asserts that all ordering exprs are bound by the sort tuple.
     */
    public void checkConsistency() {
        for (Expr orderingExpr : orderingExprs) {
            Preconditions.checkState(orderingExpr.isBound(sortTupleDesc.getId()));
        }
    }

    @Override
    public SortInfo clone() {
        return new SortInfo(this);
    }

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
        TreeNode.collect(Expr.substituteList(orderingExprs, substOrderBy, analyzer, false),
                Predicates.instanceOf(SlotRef.class), sourceSlots);
        for (SlotRef origSlotRef : sourceSlots) {
            // Don't rematerialize slots that are already in the sort tuple.
            if (origSlotRef.getDesc().getParent().getId() != sortTupleDesc.getId()) {
                SlotDescriptor origSlotDesc = origSlotRef.getDesc();
                SlotDescriptor materializedDesc =
                        analyzer.copySlotDescriptor(origSlotDesc, sortTupleDesc);
                // set to nullable if the origSlot is outer joined
                if (analyzer.isOuterJoined(origSlotDesc.getParent().getId())) {
                    materializedDesc.setIsNullable(true);
                }
                SlotRef cloneRef = new SlotRef(materializedDesc);
                substOrderBy.put(origSlotRef, cloneRef);
                sortTupleExprs.add(origSlotRef);
            }
        }

        // backup before substitute orderingExprs
        origOrderingExprs = orderingExprs;

        // The ordering exprs are evaluated against the sort tuple, so they must reflect the
        // materialization decision above.
        substituteOrderingExprs(substOrderBy, analyzer);

        // Update the tuple descriptor used to materialize the input of the sort.
        setMaterializedTupleInfo(sortTupleDesc, sortTupleExprs);
        if (LOG.isDebugEnabled()) {
            LOG.debug("sortTupleDesc {}", sortTupleDesc);
        }

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
        // the sort node exprs may come from the child outer join node
        // we need change the slots to nullable from all outer join nullable side temporarily
        // then the sort node expr would have correct nullable info
        // after create the output tuple we need revert the change by call analyzer.changeSlotsToNotNullable(slots)
        List<SlotDescriptor> slots = analyzer.changeSlotToNullableOfOuterJoinedTuples();
        ExprSubstitutionMap substOrderBy = new ExprSubstitutionMap();
        for (Expr origOrderingExpr : orderingExprs) {
            SlotDescriptor materializedDesc = analyzer.addSlotDescriptor(sortTupleDesc);
            materializedDesc.initFromExpr(origOrderingExpr);
            materializedDesc.setIsMaterialized(true);
            SlotRef origSlotRef = origOrderingExpr.getSrcSlotRef();
            if (LOG.isDebugEnabled()) {
                LOG.debug("origOrderingExpr {}", origOrderingExpr);
            }
            if (origSlotRef != null) {
                // need do this for two phase read of topn query optimization
                // check https://github.com/apache/doris/pull/15642 for detail
                materializedDesc.setSrcColumn(origSlotRef.getColumn());
            }
            SlotRef materializedRef = new SlotRef(materializedDesc);
            substOrderBy.put(origOrderingExpr, materializedRef);
            materializedOrderingExprs.add(origOrderingExpr);
        }
        analyzer.changeSlotsToNotNullable(slots);
        return substOrderBy;
    }

    /**
     * Convert the sort info to TSortInfo.
     */
    public TSortInfo toThrift() {
        TSortInfo sortInfo = new TSortInfo(
                Expr.treesToThrift(orderingExprs),
                isAscOrder,
                nullsFirstParams);
        if (useTwoPhaseRead) {
            sortInfo.setUseTwoPhaseRead(true);
        }
        return sortInfo;
    }
}
