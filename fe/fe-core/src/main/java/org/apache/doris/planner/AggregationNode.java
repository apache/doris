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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/AggregationNode.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.planner.normalize.Normalizer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TAggregationNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TNormalizedAggregateNode;
import org.apache.doris.thrift.TNormalizedPlanNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TSortInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Aggregation computation.
 */
public class AggregationNode extends PlanNode {
    private final AggregateInfo aggInfo;

    // Set to true if this aggregation node needs to run the Finalize step. This
    // node is the root node of a distributed aggregation.
    private boolean needsFinalize;
    private boolean isColocate = false;

    // If true, use streaming preaggregation algorithm. Not valid if this is a merge agg.
    private boolean useStreamingPreagg;

    private SortInfo sortByGroupKey;

    private boolean queryCacheCandidate;

    /**
     * Create an agg node that is not an intermediate node.
     * isIntermediate is true if it is a slave node in a 2-part agg plan.
     */
    public AggregationNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo) {
        super(id, aggInfo.getOutputTupleId().asList(), "AGGREGATE");
        this.aggInfo = aggInfo;
        this.children.add(input);
        this.needsFinalize = true;
        updateplanNodeName();
    }

    // Unsets this node as requiring finalize. Only valid to call this if it is
    // currently marked as needing finalize.
    public void unsetNeedsFinalize() {
        Preconditions.checkState(needsFinalize);
        needsFinalize = false;
        updateplanNodeName();
    }

    // Used by new optimizer
    public void setUseStreamingPreagg(boolean useStreamingPreagg) {
        this.useStreamingPreagg = useStreamingPreagg;
    }

    private void updateplanNodeName() {
        StringBuilder sb = new StringBuilder();
        sb.append("VAGGREGATE");
        sb.append(" (");
        if (aggInfo.isMerge()) {
            sb.append("merge");
        } else {
            sb.append("update");
        }
        if (needsFinalize) {
            sb.append(" finalize");
        } else {
            sb.append(" serialize");
        }
        sb.append(")");
        setPlanNodeName(sb.toString());
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        aggInfo.updateMaterializedSlots();
        msg.node_type = TPlanNodeType.AGGREGATION_NODE;
        List<TExpr> aggregateFunctions = Lists.newArrayList();
        List<TSortInfo> aggSortInfos = Lists.newArrayList();
        // only serialize agg exprs that are being materialized
        for (FunctionCallExpr e : aggInfo.getMaterializedAggregateExprs()) {
            aggregateFunctions.add(ExprToThriftVisitor.treeToThrift(e));
            List<TExpr> orderingExpr = Lists.newArrayList();
            List<Boolean> isAscs = Lists.newArrayList();
            List<Boolean> nullFirsts = Lists.newArrayList();

            e.getOrderByElements().forEach(o -> {
                orderingExpr.add(ExprToThriftVisitor.treeToThrift(o.getExpr()));
                isAscs.add(o.getIsAsc());
                nullFirsts.add(o.getNullsFirstParam());
            });
            aggSortInfos.add(new TSortInfo(orderingExpr, isAscs, nullFirsts));
        }

        msg.agg_node = new TAggregationNode(
                aggregateFunctions,
                aggInfo.getOutputTupleId().asInt(),
                aggInfo.getOutputTupleId().asInt(), needsFinalize);
        msg.agg_node.setAggSortInfos(aggSortInfos);
        msg.agg_node.setUseStreamingPreaggregation(useStreamingPreagg);
        msg.agg_node.setIsFirstPhase(aggInfo.isFirstPhase());
        msg.agg_node.setIsColocate(isColocate);
        if (sortByGroupKey != null) {
            msg.agg_node.setAggSortInfoByGroupKey(sortByGroupKey.toThrift());
        }
        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        if (groupingExprs != null) {
            msg.agg_node.setGroupingExprs(ExprToThriftVisitor.treesToThrift(groupingExprs));
        }
    }

    @Override
    public void normalize(TNormalizedPlanNode normalizedPlan, Normalizer normalizer) {
        TNormalizedAggregateNode normalizedAggregateNode = new TNormalizedAggregateNode();

        // if (aggInfo.getGroupingExprs().size() > 3) {
        //     throw new IllegalStateException("Too many grouping expressions, not use query cache");
        // }

        normalizedAggregateNode.setOutputTupleId(
                normalizer.normalizeTupleId(aggInfo.getOutputTupleId().asInt()));
        normalizedAggregateNode.setGroupingExprs(normalizeExprs(aggInfo.getGroupingExprs(), normalizer));
        normalizedAggregateNode.setAggregateFunctions(normalizeExprs(aggInfo.getAggregateExprs(), normalizer));
        normalizedAggregateNode.setIsFinalize(needsFinalize);
        normalizedAggregateNode.setUseStreamingPreaggregation(useStreamingPreagg);

        normalizeAggOutputProjects(normalizedAggregateNode, normalizer);

        normalizedPlan.setNodeType(TPlanNodeType.AGGREGATION_NODE);
        normalizedPlan.setAggregationNode(normalizedAggregateNode);
        if (sortByGroupKey != null) {
            normalizedAggregateNode.setSortInfo(sortByGroupKey.toThrift());
        }
    }

    @Override
    protected void normalizeProjects(TNormalizedPlanNode normalizedPlanNode, Normalizer normalizer) {
        List<SlotDescriptor> outputSlots =
                getOutputTupleIds()
                        .stream()
                        .flatMap(tupleId -> normalizer.getDescriptorTable().getTupleDesc(tupleId).getSlots().stream())
                        .collect(Collectors.toList());

        List<Expr> projectList = this.projectList;
        if (projectList == null) {
            projectList = this.aggInfo.getOutputTupleDesc()
                    .getSlots()
                    .stream()
                    .map(SlotRef::new)
                    .collect(Collectors.toList());
        }

        List<TExpr> projectThrift = normalizeProjects(outputSlots, projectList, normalizer);
        normalizedPlanNode.setProjects(projectThrift);
    }

    private void normalizeAggOutputProjects(TNormalizedAggregateNode aggregateNode, Normalizer normalizer) {
        List<Expr> projectToIntermediateTuple = ImmutableList.<Expr>builder()
                .addAll(aggInfo.getGroupingExprs())
                .addAll(aggInfo.getAggregateExprs())
                .build();

        List<SlotDescriptor> intermediateSlots = aggInfo.getOutputTupleDesc().getSlots();
        List<TExpr> projects = normalizeProjects(intermediateSlots, projectToIntermediateTuple, normalizer);
        aggregateNode.setProjectToAggOutputTuple(projects);
    }

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        aggInfo.updateMaterializedSlots();
        StringBuilder output = new StringBuilder();
        if (useStreamingPreagg) {
            output.append(detailPrefix).append("STREAMING").append("\n");
        }

        if (detailLevel == TExplainLevel.BRIEF) {
            output.append(detailPrefix).append(String.format(
                    "cardinality=%,d",  cardinality)).append("\n");
            return output.toString();
        }

        if (aggInfo.getAggregateExprs() != null && aggInfo.getMaterializedAggregateExprs().size() > 0) {
            List<String> labels = aggInfo.getMaterializedAggregateExprLabels();
            if (labels.isEmpty()) {
                output.append(detailPrefix).append("output: ")
                        .append(getExplainString(aggInfo.getMaterializedAggregateExprs())).append("\n");
            } else {
                output.append(detailPrefix).append("output: ")
                        .append(StringUtils.join(labels, ", ")).append("\n");
            }
        }
        // TODO: group by can be very long. Break it into multiple lines
        output.append(detailPrefix).append("group by: ")
                .append(getExplainString(aggInfo.getGroupingExprs()))
                .append("\n");
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("having: ").append(getExplainString(conjuncts)).append("\n");
        }
        output.append(detailPrefix).append("sortByGroupKey:").append(sortByGroupKey != null).append("\n");
        output.append(detailPrefix).append(String.format(
                "cardinality=%,d", cardinality)).append("\n");
        return output.toString();
    }

    // If `GroupingExprs` is empty and agg need to finalize, the result must be output by single instance
    @Override
    public boolean isSerialNode() {
        return aggInfo.getGroupingExprs().isEmpty() && needsFinalize;
    }

    public void setColocate(boolean colocate) {
        isColocate = colocate;
    }

    public boolean isColocate() {
        return isColocate;
    }

    public void setSortByGroupKey(SortInfo sortByGroupKey) {
        this.sortByGroupKey = sortByGroupKey;
    }

    public boolean isQueryCacheCandidate() {
        return queryCacheCandidate;
    }

    public void setQueryCacheCandidate(boolean queryCacheCandidate) {
        this.queryCacheCandidate = queryCacheCandidate;
    }

    @Override
    public Pair<PlanNode, LocalExchangeType> enforceAndDeriveLocalExchange(
            PlanTranslatorContext translatorContext, PlanNode parent, LocalExchangeTypeRequire parentRequire) {

        ConnectContext connectContext = translatorContext.getConnectContext();
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        // PR #62438: when false, non-finalize agg falls back to BE base class.
        boolean enableLeBeforeAgg = sessionVariable.enableLocalExchangeBeforeAgg;
        boolean hasKeys = !aggInfo.getGroupingExprs().isEmpty();

        // Each branch mirrors the corresponding BE operator's required_data_distribution()
        // check order 1:1. The helper baseClassRequire() expands BE's base class behavior.
        LocalExchangeTypeRequire requireChild;
        if (canUseDistinctStreamingAgg(sessionVariable)) {
            // DistinctStreamingAggOperatorX.  Two flavors share this operator class:
            //   - streaming preagg (useStreamingPreagg=true): performance-only,
            //     flag controls
            //   - non-streaming dedup (useStreamingPreagg=false): correctness-required,
            //     always HASH regardless of flag
            // Diverges from BE: BE's `!_needs_finalize && !enable_local_exchange_before_agg`
            // early return catches non-streaming dedup too, causing the same family of
            // wrong-result bug as AggSink (DORIS-25413).
            if (needsFinalize && !hasKeys) {
                requireChild = LocalExchangeTypeRequire.noRequire();
            } else if (!needsFinalize && useStreamingPreagg && !enableLeBeforeAgg) {
                requireChild = baseClassRequire(connectContext);
            } else if (needsFinalize || (hasKeys && !useStreamingPreagg)) {
                requireChild = AddLocalExchange.isColocated(this)
                        ? LocalExchangeTypeRequire.requireHash()
                        : parentRequire.autoRequireHash();
            } else if (sessionVariable.enableDistinctStreamingAggForcePassthrough) {
                requireChild = LocalExchangeTypeRequire.requirePassthrough();
            } else {
                requireChild = baseClassRequire(connectContext);
            }
        } else if (useStreamingPreagg) {
            // StreamingAggOperatorX
            if (children.get(0) instanceof HashJoinNode
                    && sessionVariable.enableStreamingAggHashJoinForcePassthrough) {
                requireChild = LocalExchangeTypeRequire.requirePassthrough();
            } else if (!needsFinalize && !enableLeBeforeAgg) {
                requireChild = baseClassRequire(connectContext);
            } else if (!hasKeys) {
                requireChild = needsFinalize
                        ? LocalExchangeTypeRequire.noRequire()
                        : baseClassRequire(connectContext);
            } else {
                requireChild = LocalExchangeTypeRequire.requireHash();
            }
        } else {
            // AggSinkOperatorX — covers finalize phase AND non-finalize phases (LOCAL
            // preagg / FIRST_MERGE dedup). Streaming preagg goes through the StreamingAgg
            // branch above, not here.
            //
            // Phase semantics for !needsFinalize:
            //   - FIRST / SECOND (LOCAL phase, !isMerge): performance-only, flag controls
            //   - FIRST_MERGE (correctness-required): always HASH regardless of flag
            //
            // Diverges from BE here: BE's `!_needs_finalize && !enable_local_exchange_before_agg`
            // early return also catches FIRST_MERGE, dropping the HASH requirement and
            // causing wrong-result (e.g. PASSTHROUGH over serial child breaks the
            // group-by-key invariant — DORIS-25413).
            if (!hasKeys) {
                requireChild = needsFinalize
                        ? LocalExchangeTypeRequire.noRequire()
                        : baseClassRequire(connectContext);
            } else if (!needsFinalize && !aggInfo.isMerge() && !enableLeBeforeAgg) {
                // LOCAL phase (FIRST preagg / SECOND distinct local) + user opted out
                // of pre-agg LE → base class decides: serial child → PASSTHROUGH
                // (parallelism), non-serial child → NOOP (no LE).
                requireChild = baseClassRequire(connectContext);
            } else if (!needsFinalize || AddLocalExchange.isColocated(this)) {
                // FIRST_MERGE (correctness) or finalize+colocate → HASH.
                requireChild = parentRequire.autoRequireHash();
            } else if (hasPartitionExprs(parentRequire)) {
                // FE-only heuristic: finalize non-colocate with parent hash requirement
                // → inherit parent's specific hash type.
                requireChild = parentRequire.autoRequireHash();
            } else {
                // FE-only heuristic: finalize non-colocate without parent hash → skip
                // LE (child Exchange already provides hash distribution).
                requireChild = LocalExchangeTypeRequire.noRequire();
            }
        }

        Pair<PlanNode, LocalExchangeType> enforceResult
                = enforceRequire(translatorContext, children.get(0), 0, requireChild);
        children = Lists.newArrayList(enforceResult.first);
        return Pair.of(this, enforceResult.second);
    }

    /** BE base class required_data_distribution: serial child → PASSTHROUGH, else → NOOP. */
    private LocalExchangeTypeRequire baseClassRequire(ConnectContext connectContext) {
        return children.get(0).isSerialOperatorOnBe(connectContext)
                ? LocalExchangeTypeRequire.requirePassthrough()
                : LocalExchangeTypeRequire.noRequire();
    }

    @Override
    protected List<Expr> getSemanticPartitionExprs() {
        return aggInfo.getGroupingExprs();
    }

    @Override
    protected List<Expr> getLocalExchangeDistributeExprs(int childIndex, boolean followedByShuffled) {
        // Mirror BE AggSinkOperatorX::update_operator / StreamingAggOperatorX::update_operator:
        //   _partition_exprs = (distribute_expr_lists set && (followed_by_shuffled || has_distinct))
        //                      ? distribute_expr_lists[0] : grouping_exprs
        // The HASH LocalExchange must partition by _partition_exprs so a streaming partial preagg
        // locally collapses same-key rows.  Using child distribution (default) for a non-shuffled
        // chain scatters same-group rows across N instances, leaving partial_preagg essentially a
        // no-op and breaking row-arrival order at downstream merge-finalize (e.g. group_concat).
        List<Expr> childDist = getChildDistributeExprList(childIndex);
        // Multi-distinct aggregates are detected by function name. Nereids rewrites
        // count/sum(distinct ...) into dedicated MultiDistinct* functions constructed with
        // distinct=false and a "multi_distinct_" name, so by this legacy FunctionCallExpr layer
        // isDistinct() is already false and the function name is the only remaining signal —
        // there is no structural flag to test here.
        boolean hasDistinct = aggInfo.getAggregateExprs().stream()
                .map(FunctionCallExpr::getFnName)
                .filter(name -> name != null)
                .map(name -> name.getFunction())
                .filter(name -> name != null)
                .anyMatch(name -> name.startsWith("multi_distinct_"));
        if (childDist != null && !childDist.isEmpty() && (followedByShuffled || hasDistinct)) {
            return childDist;
        }
        return Lists.newArrayList(aggInfo.getGroupingExprs());
    }

    @Override
    public boolean requiresShuffleForCorrectness() {
        // Mirrors BE's AggSinkOperatorX::is_shuffled_operator() exactly:
        //   finalize agg with group keys needs hash-distributed input for correctness.
        // GLOBAL dedup (!needsFinalize) is intentionally NOT included here — if a
        // GLOBAL dedup exists, a finalize agg always sits above it (e.g. DISTINCT_GLOBAL
        // above DISTINCT_LOCAL/GLOBAL_DEDUP), and the finalize agg propagates the flag
        // down via inheritedShuffled. A solo finalize agg satisfies hash distribution
        // through its own child requirement.
        return needsFinalize && !aggInfo.getGroupingExprs().isEmpty();
    }

    private boolean canUseDistinctStreamingAgg(SessionVariable sessionVariable) {
        return aggInfo.getAggregateExprs().isEmpty() && sortByGroupKey == null
                && sessionVariable.enableDistinctStreamingAggregation;
    }

    @Override
    protected boolean shouldResetSerialFlagForChild(int childIndex) {
        // Non-streaming AGG is a pipeline breaker: child is in AGG_Sink pipeline,
        // parent is in AGG_Source pipeline. Reset inherited serial flag from parent
        // (different pipeline), but enforceRequire still adds this node's own
        // isSerialNode() so the child sees AGG_Sink's serial status correctly.
        return !useStreamingPreagg;
    }
}
