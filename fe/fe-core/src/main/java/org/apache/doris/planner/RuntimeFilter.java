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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BitmapFilterPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.analysis.TupleIsNullPredicate;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterDesc;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Representation of a runtime filter. A runtime filter is generated from
 * an equi-join predicate of the form <lhs_expr> = <rhs_expr>, where lhs_expr is the
 * expr on which the filter is applied and must be bound by a single tuple id from
 * the left plan subtree of the associated join node, while rhs_expr is the expr on
 * which the filter is built and can be bound by any number of tuple ids from the
 * right plan subtree. Every runtime filter must record the join node that constructs
 * the filter and the scan nodes that apply the filter (destination nodes).
 */
public final class RuntimeFilter {
    private static final Logger LOG = LogManager.getLogger(RuntimeFilter.class);

    // Identifier of the filter (unique within a query)
    private final RuntimeFilterId id;
    // Join node that builds the filter
    private final PlanNode builderNode;
    // Expr (rhs of join predicate) on which the filter is built
    private final Expr srcExpr;
    // The position of expr in the join condition
    private final int exprOrder;
    // Expr (lhs of join predicate) from which the targetExprs_ are generated.
    private final List<Expr> origTargetExprs;
    // Runtime filter targets
    private final List<RuntimeFilterTarget> targets = new ArrayList<>();
    // Slots from base table tuples that have value transfer from the slots
    // of 'origTargetExpr'. The slots are grouped by tuple id.
    private final List<Map<TupleId, List<SlotId>>> targetSlotsByTid;
    // If true, the join node building this filter is executed using a broadcast join;
    // set in the DistributedPlanner.createHashJoinFragment()
    private boolean isBroadcastJoin;
    // Estimate of the number of distinct values that will be inserted into this filter,
    // globally across all instances of the source node. Used to compute an optimal size
    // for the filter. A value of -1 means no estimate is available, and default filter
    // parameters should be used.
    private long ndvEstimate = -1;
    // Size of the filter (in Bytes). Should be greater than zero for bloom filters.
    private long filterSizeBytes = 0;

    private long expectFilterSizeBytes = 0;
    // If true, the filter is produced by a broadcast join and there is at least one
    // destination scan node which is in the same fragment as the join; set in
    // DistributedPlanner.createHashJoinFragment().
    private boolean hasLocalTargets = false;
    // If true, there is at least one destination scan node which is not in the same
    // fragment as the join that produced the filter; set in
    // DistributedPlanner.createHashJoinFragment().
    private boolean hasRemoteTargets = false;
    // If set, indicates that the filter can't be assigned to another scan node.
    // Once set, it can't be unset.
    private boolean finalized = false;
    // The type of filter to build.
    private TRuntimeFilterType runtimeFilterType;

    private boolean bitmapFilterNotIn = false;

    private TMinMaxRuntimeFilterType tMinMaxRuntimeFilterType;

    private boolean bloomFilterSizeCalculatedByNdv = false;

    /**
     * Internal representation of a runtime filter target.
     */
    public static class RuntimeFilterTarget {
        // Scan node that applies the filter
        public ScanNode node;
        // Expr on which the filter is applied
        public Expr expr;
        // Indicates if 'expr' is bound only by partition columns
        public final boolean isBoundByKeyColumns;
        // Indicates if 'node' is in the same fragment as the join that produces the filter
        public final boolean isLocalTarget;

        public RuntimeFilterTarget(ScanNode targetNode, Expr targetExpr,
                                   boolean isBoundByKeyColumns, boolean isLocalTarget) {
            Preconditions.checkState(targetExpr.isBoundByTupleIds(targetNode.getTupleIds())
                    || targetNode instanceof CTEScanNode);
            this.node = targetNode;
            this.expr = targetExpr;
            this.isBoundByKeyColumns = isBoundByKeyColumns;
            this.isLocalTarget = isLocalTarget;
        }

        @Override
        public String toString() {
            return "Target Id: " + node.getId() + " "
                    + "Target expr: " + expr.debugString() + " "
                    + "Is only Bound By Key: " + isBoundByKeyColumns
                    + "Is local: " + isLocalTarget;
        }
    }

    private RuntimeFilter(RuntimeFilterId filterId, PlanNode filterSrcNode, Expr srcExpr, int exprOrder,
                          List<Expr> origTargetExprs, List<Map<TupleId, List<SlotId>>> targetSlots,
                          TRuntimeFilterType type,
                          RuntimeFilterGenerator.FilterSizeLimits filterSizeLimits, long buildSizeNdv,
                          TMinMaxRuntimeFilterType tMinMaxRuntimeFilterType) {
        this.id = filterId;
        this.builderNode = filterSrcNode;
        this.srcExpr = srcExpr;
        this.exprOrder = exprOrder;
        this.origTargetExprs = ImmutableList.copyOf(origTargetExprs);
        this.targetSlotsByTid = ImmutableList.copyOf(targetSlots);
        this.runtimeFilterType = type;
        this.ndvEstimate = buildSizeNdv;
        this.tMinMaxRuntimeFilterType = tMinMaxRuntimeFilterType;
        computeNdvEstimate();
        calculateFilterSize(filterSizeLimits);
    }

    private RuntimeFilter(RuntimeFilterId filterId, PlanNode filterSrcNode, Expr srcExpr, int exprOrder,
            List<Expr> origTargetExprs, List<Map<TupleId, List<SlotId>>> targetSlots, TRuntimeFilterType type,
            RuntimeFilterGenerator.FilterSizeLimits filterSizeLimits, long buildSizeNdv) {
        this(filterId, filterSrcNode, srcExpr, exprOrder, origTargetExprs,
                targetSlots, type, filterSizeLimits, buildSizeNdv, TMinMaxRuntimeFilterType.MIN_MAX);
    }

    // only for nereids planner
    public static RuntimeFilter fromNereidsRuntimeFilter(
            org.apache.doris.nereids.trees.plans.physical.RuntimeFilter nereidsFilter,
            JoinNodeBase node, Expr srcExpr, List<Expr> origTargetExprs,
            List<Map<TupleId, List<SlotId>>> targetSlots,
            RuntimeFilterGenerator.FilterSizeLimits filterSizeLimits) {
        return new RuntimeFilter(nereidsFilter.getId(), node, srcExpr, nereidsFilter.getExprOrder(), origTargetExprs,
                targetSlots, nereidsFilter.getType(), filterSizeLimits, nereidsFilter.getBuildSideNdv(),
                nereidsFilter.gettMinMaxType());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RuntimeFilter)) {
            return false;
        }
        return ((RuntimeFilter) obj).id.equals(id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public void markFinalized() {
        finalized = true;
    }

    public boolean isFinalized() {
        return finalized;
    }

    public void setBitmapFilterNotIn(boolean bitmapFilterNotIn) {
        this.bitmapFilterNotIn = bitmapFilterNotIn;
    }

    /**
     * Serializes a runtime filter to Thrift.
     */
    public TRuntimeFilterDesc toThrift() {
        TRuntimeFilterDesc tFilter = new TRuntimeFilterDesc();
        tFilter.setFilterId(id.asInt());
        tFilter.setSrcExpr(srcExpr.treeToThrift());
        tFilter.setExprOrder(exprOrder);
        tFilter.setIsBroadcastJoin(isBroadcastJoin);
        tFilter.setHasLocalTargets(hasLocalTargets);
        tFilter.setHasRemoteTargets(hasRemoteTargets);
        for (RuntimeFilterTarget target : targets) {
            tFilter.putToPlanIdToTargetExpr(target.node.getId().asInt(), target.expr.treeToThrift());
        }
        tFilter.setType(runtimeFilterType);
        tFilter.setBloomFilterSizeBytes(filterSizeBytes);
        if (runtimeFilterType.equals(TRuntimeFilterType.BITMAP)) {
            tFilter.setBitmapTargetExpr(targets.get(0).expr.treeToThrift());
            tFilter.setBitmapFilterNotIn(bitmapFilterNotIn);
        }
        if (runtimeFilterType.equals(TRuntimeFilterType.MIN_MAX)) {
            tFilter.setMinMaxType(tMinMaxRuntimeFilterType);
        }
        tFilter.setOptRemoteRf(hasRemoteTargets);
        tFilter.setBloomFilterSizeCalculatedByNdv(bloomFilterSizeCalculatedByNdv);
        if (builderNode instanceof HashJoinNode) {
            HashJoinNode join = (HashJoinNode) builderNode;
            BinaryPredicate eq = join.getEqJoinConjuncts().get(exprOrder);
            if (eq.getOp().equals(BinaryPredicate.Operator.EQ_FOR_NULL)) {
                tFilter.setNullAware(true);
            } else {
                tFilter.setNullAware(false);
            }
        }
        tFilter.setSyncFilterSize(ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable().enableSyncRuntimeFilterSize());
        return tFilter;
    }

    public List<RuntimeFilterTarget> getTargets() {
        return targets;
    }

    public boolean hasTargets() {
        return !targets.isEmpty();
    }

    public Expr getSrcExpr() {
        return srcExpr;
    }

    public List<Expr> getOrigTargetExprs() {
        return origTargetExprs;
    }

    public List<Map<TupleId, List<SlotId>>> getTargetSlots() {
        return targetSlotsByTid;
    }

    public RuntimeFilterId getFilterId() {
        return id;
    }

    public TRuntimeFilterType getType() {
        return runtimeFilterType;
    }

    public String getTypeDesc() {
        String desc = runtimeFilterType.toString().toLowerCase();
        if (runtimeFilterType == TRuntimeFilterType.MIN_MAX) {
            if (tMinMaxRuntimeFilterType == TMinMaxRuntimeFilterType.MIN) {
                desc = "min";
            } else if (tMinMaxRuntimeFilterType == TMinMaxRuntimeFilterType.MAX) {
                desc = "max";
            }
        }
        return desc;
    }

    public void setType(TRuntimeFilterType type) {
        runtimeFilterType = type;
    }

    public boolean hasRemoteTargets() {
        return hasRemoteTargets;
    }

    public PlanNode getBuilderNode() {
        return builderNode;
    }

    /**
     * Static function to create a RuntimeFilter from 'joinPredicate' that is assigned
     * to the join node 'filterSrcNode'. Returns an instance of RuntimeFilter
     * or null if a runtime filter cannot be generated from the specified predicate.
     */
    public static RuntimeFilter create(IdGenerator<RuntimeFilterId> idGen, Analyzer analyzer, Expr joinPredicate,
            int exprOrder, HashJoinNode filterSrcNode, TRuntimeFilterType type,
            RuntimeFilterGenerator.FilterSizeLimits filterSizeLimits, HashSet<TupleId> tupleHasConjuncts) {
        Preconditions.checkNotNull(idGen);
        Preconditions.checkNotNull(joinPredicate);
        Preconditions.checkNotNull(filterSrcNode);
        // Only consider binary equality predicates and not contain Null-safe equals.
        // The predicate could not be pushed down when there is Null-safe equal operator. Because the runtimeFilter
        // will filter the null value in child[0] while it is needed in the Null-safe equal join.
        // For example: select * from a join b where a.id<=>b.id
        // the null value in table a should be return by scan node instead of filtering it by runtimeFilter.
        if (!Predicate.isUnNullSafeEquivalencePredicate(joinPredicate)) {
            return null;
        }

        BinaryPredicate normalizedJoinConjunct =
                SingleNodePlanner.getNormalizedEqPred(joinPredicate,
                        filterSrcNode.getChild(0).getTupleIds(),
                        filterSrcNode.getChild(1).getTupleIds(), analyzer);
        if (normalizedJoinConjunct == null) {
            return null;
        }

        // Ensure that the target expr does not contain TupleIsNull predicates as these
        // can't be evaluated at a scan node.
        Expr targetExpr =
                TupleIsNullPredicate.unwrapExpr(normalizedJoinConjunct.getChild(0).clone());
        Expr srcExpr = normalizedJoinConjunct.getChild(1);

        Type srcType = srcExpr.getType();
        if (srcType.equals(ScalarType.HLL) || srcType.equals(ScalarType.BITMAP) || srcType.equals(ScalarType.BOOLEAN)) {
            return null;
        }

        targetExpr = targetExpr.getRealSlotRef();
        Map<TupleId, List<SlotId>> targetSlots = getTargetSlots(analyzer, targetExpr, filterSrcNode.getChild(0));
        Preconditions.checkNotNull(targetSlots);
        if (targetSlots.isEmpty()) {
            return null;
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Generating runtime filter from predicate " + joinPredicate);
        }
        if (ConnectContext.get().getSessionVariable().isEnableRuntimeFilterPrune()) {
            if (srcExpr instanceof SlotRef) {
                if (!tupleHasConjuncts.contains(((SlotRef) srcExpr).getDesc().getParent().getId())) {
                    // src tuple has no conjunct, don't create runtime filter
                    return null;
                } else {
                    // runtime filter itself is a valid conjunct, add all the target tuple ids
                    for (TupleId tupleId : targetSlots.keySet()) {
                        tupleHasConjuncts.add(tupleId);
                    }
                }
            }
        }

        return new RuntimeFilter(idGen.getNextId(), filterSrcNode, srcExpr, exprOrder,
                ImmutableList.of(targetExpr), ImmutableList.of(targetSlots), type, filterSizeLimits, -1L);
    }

    public static RuntimeFilter create(IdGenerator<RuntimeFilterId> idGen, Analyzer analyzer, Expr joinPredicate,
            int exprOrder, NestedLoopJoinNode filterSrcNode, TRuntimeFilterType type,
            RuntimeFilterGenerator.FilterSizeLimits filterSizeLimits) {
        Preconditions.checkNotNull(idGen);
        Preconditions.checkNotNull(joinPredicate);
        Preconditions.checkNotNull(filterSrcNode);

        if (type.equals(TRuntimeFilterType.BITMAP)) {
            if (!(joinPredicate instanceof BitmapFilterPredicate)) {
                return null;
            }

            Expr targetExpr = Expr.getFirstBoundChild(joinPredicate, filterSrcNode.getChild(0).getTupleIds());
            Expr srcExpr = Expr.getFirstBoundChild(joinPredicate, filterSrcNode.getChild(1).getTupleIds());
            if (targetExpr == null || srcExpr == null) {
                return null;
            }

            Type srcType = srcExpr.getType();
            if (!srcType.equals(ScalarType.BITMAP)) {
                return null;
            }

            Map<TupleId, List<SlotId>> targetSlots = getTargetSlots(analyzer, targetExpr, filterSrcNode.getChild(0));
            Preconditions.checkNotNull(targetSlots);
            if (targetSlots.isEmpty()) {
                return null;
            }
            while (targetExpr instanceof CastExpr && targetExpr.getChild(0).getType().isIntegerType()) {
                targetExpr = targetExpr.getChild(0);
            }

            RuntimeFilter runtimeFilter =
                    new RuntimeFilter(idGen.getNextId(), filterSrcNode, srcExpr, exprOrder,
                            ImmutableList.of(targetExpr), ImmutableList.of(targetSlots),
                            type, filterSizeLimits, -1L);
            runtimeFilter.setBitmapFilterNotIn(((BitmapFilterPredicate) joinPredicate).isNotIn());
            return runtimeFilter;
        }
        return null;
    }

    /**
     * Returns the ids of base table tuple slots on which a runtime filter expr can be
     * applied. Due to the existence of equivalence classes, a filter expr may be
     * applicable at multiple scan nodes. The returned slot ids are grouped by tuple id.
     * Returns an empty collection if the filter expr cannot be applied at a base table
     * or if applying the filter might lead to incorrect results.
     * Returns the slot id of the base table expected to use this target expr.
     */
    private static Map<TupleId, List<SlotId>> getTargetSlots(Analyzer analyzer, Expr expr, PlanNode root) {
        // 'expr' is not a SlotRef and may contain multiple SlotRefs
        List<TupleId> tids = new ArrayList<>();
        List<SlotId> sids = new ArrayList<>();
        expr.getIds(tids, sids);

        /*
          If the target expression evaluates to a non-NULL value for outer-join non-matches, then assigning the
          filter below the nullable side of an outer join may produce incorrect query results.
          This check is conservative but correct to keep the code simple. In particular, it would otherwise be
          difficult to identify incorrect runtime filter assignments through outer-joined inline views because
          the 'expr' has already been fully resolved.
          TODO(zxy) We rely on the value-transfer graph to check whether 'expr' could potentially be assigned
           below an outer-joined inline view.

          Queries with the following characteristics may produce wrong results due to an incorrectly assigned
          runtime filter:
               1）The query has an outer join
               2）A scan on the nullable side of that outer join has a runtime filter with a NULL-checking
                 expression such as COALESCE/IFNULL/CASE
               3）The latter point imples that there is another join above the outer join with a NULL-checking
                 expression in it's join condition

           Reproduction:
               TPC-DS 1T Benchmarks test
               "
                   select count(*) from store t1 left outer join store t2 on t1.s_store_sk = t2.s_store_sk
                   where coalesce(t2.s_store_sk + 100, 100) in (select ifnull(100, s_store_sk) from store);

                   select count(*) from store t1 left outer join store t2 on t1.s_store_sk = t2.s_store_sk
                   where case when t2.s_store_sk is NULL then 100 else t2.s_store_sk end
                   in (select ifnull(100, s_store_sk) from store limit 10);
               "
               We expect a count of 0. A count of 1024 is incorrect.
               Query plan:
                   |   4:HASH JOIN
                   |   |  join op: LEFT SEMI JOIN (BROADCAST)
                   |   |  equal join conjunct: coalesce(`t2`.`s_store_sk` + 100, 100) = ifnull(100, `s_store_sk`)
                   |   |  runtime filters: RF000[in] <- ifnull(100, `s_store_sk`)
                   |   |  cardinality=1002
                   |   |----7:EXCHANGE
                   |   3:HASH JOIN
                   |   |  join op: LEFT OUTER JOIN
                   |   |  equal join conjunct: `t1`.`s_store_sk` = `t2`.`s_store_sk`
                   |   |----1:OlapScanNode
                   |   |       TABLE: store
                   |   |       runtime filters: RF000[in] -> coalesce(`t2`.`s_store_sk` + 100, 100)
                   |   0:OlapScanNode
                   |      TABLE: store
               Explanation:
                   RF000 filters out all rows in scan 01.
                   In join 03 there are no join matches since the right-hand is empty. All rows from the right-hand
                   side are nulled.
                   The join condition in join 04 now satisfies all input rows because every "t2.id" is NULL,
                   so after the COALESCE() the join condition becomes 100 = 100.
         */
        if (analyzer.hasOuterJoinedValueTransferTarget(sids)) {
            // Do not push down when contains NULL-checking expression COALESCE/IFNULL/CASE
            // TODO(zxy) Returns true if 'p' evaluates to true when all its referenced slots are NULL, returns false
            //  otherwise. Throws if backend expression evaluation fails.
            if (expr.isContainsFunction("COALESCE") || expr.isContainsFunction("IFNULL")
                    || expr.isContainsClass("org.apache.doris.analysis.CaseExpr")) {
                return Collections.emptyMap();
            }
        }

        Map<TupleId, List<SlotId>> slotsByTid = new HashMap<>();
        // We need to iterate over all the slots of 'expr' and check if they have
        // equivalent slots that are bound by the same base table tuple(s).
        for (SlotId slotId : sids) {
            Map<TupleId, List<SlotId>> currSlotsByTid = getBaseTblEquivSlots(analyzer, slotId);
            if (currSlotsByTid.isEmpty()) {
                return Collections.emptyMap();
            }
            if (slotsByTid.isEmpty()) {
                slotsByTid.putAll(currSlotsByTid);
                continue;
            }

            // Compute the intersection between tuple ids from 'slotsByTid' and
            // 'currSlotsByTid'. If the intersection is empty, an empty collection
            // is returned.
            Iterator<Map.Entry<TupleId, List<SlotId>>> iter = slotsByTid.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<TupleId, List<SlotId>> entry = iter.next();
                List<SlotId> slotIds = currSlotsByTid.get(entry.getKey());
                // Take the intersection of the tuple ids of all slots in expr to
                // form <tupleid, slotid> and return.
                // A.a + B.b = C.c, when the tuple IDs of the two slots A.a and B.b are different, at this
                // time cannot be pushed down, so remove. If you can get A.a and transferd to B.a, then
                // the tuple IDs of A.a and B.b have intersection B, So target expr is available, the tuple
                // ID of this intersection is the scan node that is expected to use this runtime fitler
                if (slotIds == null) {
                    iter.remove();
                } else {
                    entry.getValue().addAll(slotIds);
                }
            }
            if (slotsByTid.isEmpty()) {
                return Collections.emptyMap();
            }
        }

        // rf shouldn't push down through any analytic node
        // remove the slots if there is any analytic node in the middle
        Map<TupleId, List<SlotId>> result = new HashMap<>();
        for (Map.Entry<TupleId, List<SlotId>> entry : slotsByTid.entrySet()) {
            Pair<Boolean, Boolean> isValid =
                    hasAnalyticNodeInSearchPath(entry.getKey(), root, false);
            if (isValid.first && !isValid.second) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    /**
     * deep first search the child having the corresponding tupleId
     * and record if meets any analytic node during the search
     * Returns Pair.first -> find a child's tupleId is id, Pair.second -> if met any analytic node during the search
     */
    private static Pair<Boolean, Boolean> hasAnalyticNodeInSearchPath(TupleId id, PlanNode parent,
            boolean hasAnalyticParent) {
        if (parent.getTupleIds().contains(id)) {
            return Pair.of(true, hasAnalyticParent);
        } else {
            for (PlanNode child : parent.getChildren()) {
                Pair<Boolean, Boolean> result = hasAnalyticNodeInSearchPath(id, child,
                        hasAnalyticParent || parent instanceof AnalyticEvalNode);
                if (result.first) {
                    return result;
                }
            }
        }
        return Pair.of(false, false);
    }

    /**
     * Static function that returns the ids of slots bound by base table tuples for which
     * there is a value transfer from 'srcSid'. The slots are grouped by tuple id.
     * That is, srcSid can be calculated from the <tuple id, slot id> of the base table.
     */
    private static Map<TupleId, List<SlotId>> getBaseTblEquivSlots(Analyzer analyzer,
                                                                   SlotId srcSid) {
        Map<TupleId, List<SlotId>> slotsByTid = new HashMap<>();
        for (SlotId targetSid : analyzer.getValueTransferTargets(srcSid)) {
            TupleDescriptor tupleDesc = analyzer.getSlotDesc(targetSid).getParent();
            if (tupleDesc.getTable() == null) {
                continue;
            }
            List<SlotId> sids = slotsByTid.computeIfAbsent(tupleDesc.getId(), k -> new ArrayList<>());
            sids.add(targetSid);
        }
        return slotsByTid;
    }

    public Expr getTargetExpr(PlanNodeId targetPlanNodeId) {
        for (RuntimeFilterTarget target : targets) {
            if (target.node.getId() != targetPlanNodeId) {
                continue;
            }
            return target.expr;
        }
        return null;
    }

    /**
     * Estimates the selectivity of a runtime filter as the cardinality of the
     * associated source join node over the cardinality of that join node's left
     * child.
     */
    public double getSelectivity() {
        if (builderNode.getCardinality() == -1
                || builderNode.getChild(0).getCardinality() == -1
                || builderNode.getChild(0).getCardinality() == 0) {
            return -1;
        }
        return builderNode.getCardinality() / (double) builderNode.getChild(0).getCardinality();
    }

    public void addTarget(RuntimeFilterTarget target) {
        targets.add(target);
    }

    public void setIsBroadcast(boolean isBroadcast) {
        isBroadcastJoin = isBroadcast;
    }

    public boolean isBroadcast() {
        return isBroadcastJoin;
    }

    public void computeNdvEstimate() {
        if (ndvEstimate < 0) {
            ndvEstimate = builderNode.getChild(1).getCardinalityAfterFilter();
        }
    }

    public void extractTargetsPosition() {
        Preconditions.checkNotNull(builderNode.getFragment());
        Preconditions.checkState(hasTargets());
        for (RuntimeFilterTarget target : targets) {
            Preconditions.checkNotNull(target.node.getFragment());
            hasLocalTargets = hasLocalTargets || target.isLocalTarget;
            hasRemoteTargets = hasRemoteTargets || !target.isLocalTarget;
        }
    }

    /**
     * Sets the filter size (in bytes) required for a bloom filter to achieve the
     * configured maximum false-positive rate based on the expected NDV. Also bounds the
     * filter size between the max and minimum filter sizes supplied to it by
     * 'filterSizeLimits'.
     * Considering that the `IN` filter may be converted to the `Bloom FIlter` when crossing fragments,
     * the bloom filter size is always calculated.
     */
    public void calculateFilterSize(RuntimeFilterGenerator.FilterSizeLimits filterSizeLimits) {
        if (ndvEstimate == -1) {
            filterSizeBytes = filterSizeLimits.defaultVal;
            return;
        }
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        if (sessionVariable.useRuntimeFilterDefaultSize) {
            filterSizeBytes = filterSizeLimits.defaultVal;
            return;
        }
        filterSizeBytes = expectRuntimeFilterSize(ndvEstimate);
        expectFilterSizeBytes = filterSizeBytes;
        filterSizeBytes = Math.max(filterSizeBytes, filterSizeLimits.minVal);
        filterSizeBytes = Math.min(filterSizeBytes, filterSizeLimits.maxVal);
    }

    public static long expectRuntimeFilterSize(long ndv) {
        double fpp = FeConstants.default_bloom_filter_fpp;
        int logFilterSize = getMinLogSpaceForBloomFilter(ndv, fpp);
        return 1L << logFilterSize;
    }

    /**
     * Returns the log (base 2) of the minimum number of bytes we need for a Bloom
     * filter with 'ndv' unique elements and a false positive probability of less
     * than 'fpp'.
     */
    public static int getMinLogSpaceForBloomFilter(long ndv, double fpp) {
        if (0 == ndv) {
            return 0;
        }
        double k = 8; // BUCKET_WORDS
        // m is the number of bits we would need to get the fpp specified
        double m = -k * ndv / Math.log(1 - Math.pow(fpp, 1.0 / k));

        // Handle case where ndv == 1 => ceil(log2(m/8)) < 0.
        return Math.max(0, (int) (Math.ceil(Math.log(m / 8) / Math.log(2))));
    }

    /**
     * Assigns this runtime filter to the corresponding plan nodes.
     */
    public void assignToPlanNodes() {
        Preconditions.checkState(hasTargets(), this.toString() + " has no target");
        builderNode.addRuntimeFilter(this);
        builderNode.fragment.setBuilderRuntimeFilterIds(getFilterId());
        for (RuntimeFilterTarget target : targets) {
            target.node.addRuntimeFilter(this);
            // fragment is expected to use this filter id
            target.node.fragment.setTargetRuntimeFilterIds(this.id);
        }
    }

    public void registerToPlan(Analyzer analyzer) {
        PlanNode node = getBuilderNode();
        if (node instanceof HashJoinNode) {
            setIsBroadcast(((HashJoinNode) node).getDistributionMode() == HashJoinNode.DistributionMode.BROADCAST);
        } else {
            setIsBroadcast(true);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Runtime filter: " + debugString());
        }
        assignToPlanNodes();
        analyzer.putAssignedRuntimeFilter(this);
    }

    public long getFilterSizeBytes() {
        return filterSizeBytes;
    }

    public long getEstimateNdv() {
        return ndvEstimate;
    }

    public String debugString() {
        return "FilterID: " + id + " "
                +      "Source: " + builderNode.getId() + " "
                +      "SrcExpr: " + getSrcExpr().debugString() + " "
                +      "Target(s): "
                +      Joiner.on(", ").join(targets) + " "
                + "Selectivity: " + getSelectivity();
    }


    public long getExpectFilterSizeBytes() {
        return expectFilterSizeBytes;
    }

    public String getExplainString(boolean isBuildNode, boolean isBrief, PlanNodeId targetNodeId) {
        StringBuilder filterStr = new StringBuilder();
        filterStr.append(getFilterId());
        if (!isBrief) {
            filterStr.append("[");
            filterStr.append(getTypeDesc());
            filterStr.append("]");
            if (isBuildNode) {
                filterStr.append(" <- ");
                filterStr.append(getSrcExpr().toSql());
                filterStr.append("(").append(getEstimateNdv()).append("/")
                        .append(getExpectFilterSizeBytes()).append("/")
                        .append(getFilterSizeBytes()).append(")");
            } else {
                filterStr.append(" -> ");
                filterStr.append(getTargetExpr(targetNodeId).toSql());
            }
        }
        return filterStr.toString();
    }


    public boolean isBloomFilterSizeCalculatedByNdv() {
        return bloomFilterSizeCalculatedByNdv;
    }

    public void setBloomFilterSizeCalculatedByNdv(boolean bloomFilterSizeCalculatedByNdv) {
        this.bloomFilterSizeCalculatedByNdv = bloomFilterSizeCalculatedByNdv;
    }
}
