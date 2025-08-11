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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.FeConstants;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    private List<RuntimeFilterTarget> targets = new ArrayList<>();
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

    private boolean singleEq = false;

    /**
     * Internal representation of a runtime filter target.
     */
    public static class RuntimeFilterTarget {
        // Scan node that applies the filter
        public PlanNode node;
        // Expr on which the filter is applied
        public Expr expr;
        // Indicates if 'expr' is bound only by partition columns
        public final boolean isBoundByKeyColumns;
        // Indicates if 'node' is in the same fragment as the join that produces the filter
        public final boolean isLocalTarget;

        public RuntimeFilterTarget(PlanNode targetNode, Expr targetExpr,
                                   boolean isBoundByKeyColumns, boolean isLocalTarget) {
            Preconditions.checkState(targetExpr.isBoundByTupleIds(targetNode.getOutputTupleIds())
                    || targetNode instanceof CTEScanNode,
                    "RuntimeFilter target " + expr + " is not bounded: slotDesc"
                            + (targetExpr instanceof SlotRef ? ((SlotRef) targetExpr).getTupleId() : "null"));
            this.node = targetNode;
            this.expr = targetExpr;
            this.isBoundByKeyColumns = isBoundByKeyColumns;
            this.isLocalTarget = isLocalTarget;
        }

        public RuntimeFilterTarget(PlanNode targetNode, Expr targetExpr) {
            this(targetNode, targetExpr, false, false);
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

    public RuntimeFilter(RuntimeFilterId filterId,
            PlanNode filterSrcNode, Expr srcExpr, int exprOrder,
            List<RuntimeFilterTarget> targets,
            TRuntimeFilterType type,
            long buildNdvOrRowCount,
            TMinMaxRuntimeFilterType tMinMaxRuntimeFilterType) {
        this.id = filterId;
        this.builderNode = filterSrcNode;
        this.srcExpr = srcExpr;
        this.exprOrder = exprOrder;
        this.targets = ImmutableList.copyOf(targets);
        this.runtimeFilterType = type;
        this.tMinMaxRuntimeFilterType = tMinMaxRuntimeFilterType;
        calculateBloomFilterSize(buildNdvOrRowCount);
        // TODO: remove after refactor v1
        origTargetExprs = targets.stream().map(target -> target.expr).collect(Collectors.toList());
        targetSlotsByTid = ImmutableList.of();
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

        boolean hasSerialTargets = false;
        for (RuntimeFilterTarget target : targets) {
            tFilter.putToPlanIdToTargetExpr(target.node.getId().asInt(), target.expr.treeToThrift());
            hasSerialTargets = hasSerialTargets
                    || (target.node.isSerialOperator() && target.node.fragment.useSerialSource(ConnectContext.get()));
        }

        boolean enableSyncFilterSize = ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable().enableSyncRuntimeFilterSize();

        // there are two cases has local exchange between join and scan
        // 1. hasRemoteTargets is true means join probe side do least once shuffle (has shuffle between join and scan)
        // 2. hasSerialTargets is true means scan is pooled (has local shuffle between join and scan)
        boolean needShuffle = hasRemoteTargets || hasSerialTargets;

        // There are two cases where all instances of rf have the same size.
        // 1. enableSyncFilterSize is true means backends will collect global size and send to every instance
        // 2. isBroadcastJoin is true means each join node instance have the same full amount of data
        boolean hasGlobalSize = enableSyncFilterSize || isBroadcastJoin;

        // build runtime filter by exact distinct count if all of 3 conditions are met:
        // 1. only single eq conjunct
        // 2. rf type may be bf
        // 3. each filter only acts on self instance(do not need any shuffle), or size of
        // all filters will be same
        boolean buildBfByRuntimeSize = singleEq && (runtimeFilterType == TRuntimeFilterType.IN_OR_BLOOM
                || runtimeFilterType == TRuntimeFilterType.BLOOM) && (!needShuffle || hasGlobalSize);
        tFilter.setBuildBfByRuntimeSize(buildBfByRuntimeSize);

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
        } else if (builderNode instanceof SetOperationNode) {
            tFilter.setNullAware(true);
        }
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

    public void setSingleEq(int eqJoinConjunctsNumbers) {
        singleEq = (eqJoinConjunctsNumbers == 1);
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

    public void calculateBloomFilterSize(long buildNdvOrRowCount) {
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        if (sessionVariable.useRuntimeFilterDefaultSize) {
            filterSizeBytes = sessionVariable.getRuntimeBloomFilterSize();
        } else {
            filterSizeBytes = expectRuntimeFilterSize(buildNdvOrRowCount);
            expectFilterSizeBytes = filterSizeBytes;
            filterSizeBytes = Math.max(filterSizeBytes, sessionVariable.getRuntimeBloomFilterMinSize());
            filterSizeBytes = Math.min(filterSizeBytes, sessionVariable.getRuntimeBloomFilterMaxSize());
        }
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

    public String getExplainString(PlanNodeId nodeId) {
        StringBuilder filterStr = new StringBuilder();
        filterStr.append(getFilterId());

        filterStr.append("[");
        filterStr.append(getTypeDesc());
        filterStr.append("]");
        if (getBuilderNode().getId().equals(nodeId)) {
            // source side
            filterStr.append(" <- ");
            filterStr.append(getSrcExpr().toSql());
            filterStr.append("(").append(getEstimateNdv()).append("/")
                    .append(getExpectFilterSizeBytes()).append("/")
                    .append(getFilterSizeBytes()).append(")");
        } else {
            // target side
            if (getTargetExpr(nodeId) != null) {
                filterStr.append(" -> ");
                filterStr.append(getTargetExpr(nodeId).toSql());
            }
        }
        return filterStr.toString();
    }

    public void setBloomFilterSizeCalculatedByNdv(boolean bloomFilterSizeCalculatedByNdv) {
        this.bloomFilterSizeCalculatedByNdv = bloomFilterSizeCalculatedByNdv;
    }
}
