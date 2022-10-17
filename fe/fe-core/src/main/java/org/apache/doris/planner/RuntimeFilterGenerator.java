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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.util.BitUtil;
import org.apache.doris.planner.external.ExternalFileScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TRuntimeFilterMode;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class used for generating and assigning runtime filters to a query plan using
 * runtime filter propagation. Runtime filter propagation is an optimization technique
 * used to filter scanned tuples or scan ranges based on information collected at
 * runtime. A runtime filter is constructed during the build phase of a join node, and is
 * applied at, potentially, multiple scan nodes on the probe side of that join node.
 * Runtime filters are generated from equal-join predicates but they do not replace the
 * original predicates.
 *
 * MinMax filters are of a fixed size (except for those used for string type) and
 * therefore only sizes for bloom filters need to be calculated. These calculations are
 * based on the NDV estimates of the associated table columns, the min buffer size that
 * can be allocated by the bufferpool, and the query options. Moreover, it is also bound
 * by the MIN/MAX_BLOOM_FILTER_SIZE limits which are enforced on the query options before
 * this phase of planning.
 *
 * Example: select * from T1, T2 where T1.a = T2.b and T2.c = '1';
 * Assuming that T1 is a fact table and T2 is a significantly smaller dimension table, a
 * runtime filter is constructed at the join node between tables T1 and T2 while building
 * the hash table on the values of T2.b (rhs of the join condition) from the tuples of T2
 * that satisfy predicate T2.c = '1'. The runtime filter is subsequently sent to the
 * scan node of table T1 and is applied on the values of T1.a (lhs of the join condition)
 * to prune tuples of T2 that cannot be part of the join result.
 */
public final class RuntimeFilterGenerator {
    private static final Logger LOG = LogManager.getLogger(RuntimeFilterGenerator.class);

    // Map of base table tuple ids to a list of runtime filters that
    // can be applied at the corresponding scan nodes.
    private final Map<TupleId, List<RuntimeFilter>> runtimeFiltersByTid = new HashMap<>();

    // Generator for filter ids
    private final IdGenerator<RuntimeFilterId> filterIdGenerator = RuntimeFilterId.createGenerator();

    private HashSet<TupleId> tupleHasConjuncts = null;

    /**
     * Internal class that encapsulates the max, min and default sizes used for creating
     * bloom filter objects.
     */
    public static class FilterSizeLimits {
        // Maximum filter size, in bytes, rounded up to a power of two.
        public final long maxVal;

        // Minimum filter size, in bytes, rounded up to a power of two.
        public final long minVal;

        // Pre-computed default filter size, in bytes, rounded up to a power of two.
        public final long defaultVal;

        public FilterSizeLimits(SessionVariable sessionVariable) {
            // Round up all limits to a power of two
            long maxLimit = sessionVariable.getRuntimeBloomFilterMaxSize();
            maxVal = BitUtil.roundUpToPowerOf2(maxLimit);

            long minLimit = sessionVariable.getRuntimeBloomFilterMinSize();
            // Make sure minVal <= defaultVal <= maxVal
            minVal = BitUtil.roundUpToPowerOf2(Math.min(minLimit, maxVal));

            long defaultValue = sessionVariable.getRuntimeBloomFilterSize();
            defaultValue = Math.max(defaultValue, minVal);
            defaultVal = BitUtil.roundUpToPowerOf2(Math.min(defaultValue, maxVal));
        }
    }

    // Contains size limits for bloom filters.
    private final FilterSizeLimits bloomFilterSizeLimits;

    private final Analyzer analyzer;
    private final SessionVariable sessionVariable;

    private RuntimeFilterGenerator(Analyzer analyzer) {
        this.analyzer = analyzer;
        this.sessionVariable = ConnectContext.get().getSessionVariable();
        Preconditions.checkNotNull(this.sessionVariable);
        bloomFilterSizeLimits = new FilterSizeLimits(sessionVariable);
    }

    private void collectAllTupleIdsHavingConjunct(PlanNode node, HashSet<TupleId> tupleIds) {
        // for simplicity, skip join node( which contains more than 1 tuple id )
        // we only look for the node meets either of the 2 conditions:
        // 1. The node itself has conjunct
        // 2. Its descendant have conjuncts.
        int tupleNumBeforeCheckingChildren = tupleIds.size();
        for (PlanNode child : node.getChildren()) {
            collectAllTupleIdsHavingConjunct(child, tupleIds);
        }
        if (node.getTupleIds().size() == 1
                && (!node.conjuncts.isEmpty() || tupleIds.size() > tupleNumBeforeCheckingChildren)) {
            // The node or its descendant has conjuncts
            tupleIds.add(node.getTupleIds().get(0));
        }
    }

    public void findAllTuplesHavingConjuncts(PlanNode node) {
        if (tupleHasConjuncts == null) {
            tupleHasConjuncts = new HashSet<>();
        } else {
            tupleHasConjuncts.clear();
        }
        collectAllTupleIdsHavingConjunct(node, tupleHasConjuncts);
    }

    /**
     * Generates and assigns runtime filters to a query plan tree.
     */
    public static void generateRuntimeFilters(Analyzer analyzer, PlanNode plan) {
        Preconditions.checkNotNull(analyzer);
        int maxNumBloomFilters = ConnectContext.get().getSessionVariable().getRuntimeFiltersMaxNum();
        int runtimeFilterType = ConnectContext.get().getSessionVariable().getRuntimeFilterType();
        Preconditions.checkState(maxNumBloomFilters >= 0);
        RuntimeFilterGenerator filterGenerator = new RuntimeFilterGenerator(analyzer);
        Preconditions.checkState(runtimeFilterType >= 0, "runtimeFilterType not expected");
        Preconditions.checkState(runtimeFilterType <= Arrays.stream(TRuntimeFilterType.values())
                .mapToInt(TRuntimeFilterType::getValue).sum(), "runtimeFilterType not expected");
        if (ConnectContext.get().getSessionVariable().enableRemoveNoConjunctsRuntimeFilterPolicy) {
            filterGenerator.findAllTuplesHavingConjuncts(plan);
        }
        filterGenerator.generateFilters(plan);
        List<RuntimeFilter> filters = filterGenerator.getRuntimeFilters();
        if (filters.size() > maxNumBloomFilters) {
            // If more than 'maxNumBloomFilters' were generated, sort them by increasing
            // selectivity and keep the 'maxNumBloomFilters' most selective bloom filters.
            filters.sort((a, b) -> {
                double aSelectivity =
                        a.getSelectivity() == -1 ? Double.MAX_VALUE : a.getSelectivity();
                double bSelectivity =
                        b.getSelectivity() == -1 ? Double.MAX_VALUE : b.getSelectivity();
                return Double.compare(aSelectivity, bSelectivity);
            });
        }
        // We only enforce a limit on the number of bloom filters as they are much more
        // heavy-weight than the other filter types.
        int numBloomFilters = 0;
        for (RuntimeFilter filter : filters) {
            filter.extractTargetsPosition();
            if (filter.getType() == TRuntimeFilterType.BLOOM) {
                if (numBloomFilters >= maxNumBloomFilters) {
                    continue;
                }
                ++numBloomFilters;
            }
            filter.registerToPlan(analyzer);
        }
    }

    /**
     * Returns a list of all the registered runtime filters, ordered by filter ID.
     */
    public List<RuntimeFilter> getRuntimeFilters() {
        Set<RuntimeFilter> resultSet = new HashSet<>();
        for (List<RuntimeFilter> filters : runtimeFiltersByTid.values()) {
            resultSet.addAll(filters);
        }
        List<RuntimeFilter> resultList = Lists.newArrayList(resultSet);
        resultList.sort((a, b) -> a.getFilterId().compareTo(b.getFilterId()));
        return resultList;
    }

    /**
     * Generates the runtime filters for a query by recursively traversing the distributed
     * plan tree rooted at 'root'. In the top-down traversal of the plan tree, candidate
     * runtime filters are generated from equi-join predicates assigned to hash-join nodes.
     * In the bottom-up traversal of the plan tree, the filters are assigned to destination
     * (scan) nodes. Filters that cannot be assigned to a scan node are discarded.
     */
    private void generateFilters(PlanNode root) {
        if (root instanceof HashJoinNode) {
            HashJoinNode joinNode = (HashJoinNode) root;
            List<Expr> joinConjuncts = new ArrayList<>();
            // It's not correct to push runtime filters to the left side of a left outer,
            // full outer or anti join if the filter corresponds to an equi-join predicate
            // from the ON clause.
            if (!joinNode.getJoinOp().isLeftOuterJoin()
                    && !joinNode.getJoinOp().isFullOuterJoin()
                    && !joinNode.getJoinOp().equals(JoinOperator.LEFT_ANTI_JOIN)) {
                joinConjuncts.addAll(joinNode.getEqJoinConjuncts());
            }

            // TODO(zxy) supports PlanNode.conjuncts generate runtime filter.
            // PlanNode.conjuncts (call joinNode.getConjuncts() here) Different from HashJoinNode.eqJoinConjuncts
            // and HashJoinNode.otherJoinConjuncts.
            // In previous tests, it was found that using PlanNode.conjuncts to generate runtimeFilter may cause
            // incorrect results. For example, When the `in` subquery is converted to join, the join conjunct will be
            // saved in PlanNode.conjuncts. At this time, using the automatically generated join conjunct to generate
            // a runtimeFilter, some rows may be missing in the result.
            // SQL: select * from T as a where k1 = (select count(1) from T as b where a.k1 = b.k1);
            // Table T has only one INT column. At this time, `a.k1 = b.k1` is in eqJoinConjuncts,
            // `k1` = ifnull(xxx) is in conjuncts, the runtimeFilter generated according to conjuncts will cause
            // the result to be empty, but the actual result should have data returned.

            List<RuntimeFilter> filters = new ArrayList<>();
            // Actually all types of Runtime Filter objects generated by the same joinConjunct have the same
            // properties except ID. Maybe consider avoiding repeated generation
            for (TRuntimeFilterType type : TRuntimeFilterType.values()) {
                if ((sessionVariable.getRuntimeFilterType() & type.getValue()) == 0) {
                    continue;
                }
                for (int i = 0; i < joinConjuncts.size(); i++) {
                    Expr conjunct = joinConjuncts.get(i);
                    RuntimeFilter filter = RuntimeFilter.create(filterIdGenerator,
                            analyzer, conjunct, i, joinNode, type, bloomFilterSizeLimits, tupleHasConjuncts);
                    if (filter == null) {
                        continue;
                    }
                    registerRuntimeFilter(filter);
                    filters.add(filter);
                }
            }
            generateFilters(root.getChild(0));
            // Finalize every runtime filter of that join. This is to ensure that we don't
            // assign a filter to a scan node from the right subtree of joinNode or ancestor
            // join nodes in case we don't find a destination node in the left subtree.
            for (RuntimeFilter runtimeFilter : filters) {
                finalizeRuntimeFilter(runtimeFilter);
            }
            generateFilters(root.getChild(1));
        } else if (root instanceof ScanNode) {
            assignRuntimeFilters((ScanNode) root);
        } else {
            for (PlanNode childNode : root.getChildren()) {
                generateFilters(childNode);
            }
        }
    }

    /**
     * Registers a runtime filter with the tuple id of every scan node that is a candidate
     * destination node for that filter.
     */
    private void registerRuntimeFilter(RuntimeFilter filter) {
        Map<TupleId, List<SlotId>> targetSlotsByTid = filter.getTargetSlots();
        Preconditions.checkState(targetSlotsByTid != null && !targetSlotsByTid.isEmpty());
        for (TupleId tupleId : targetSlotsByTid.keySet()) {
            registerRuntimeFilter(filter, tupleId);
        }
    }

    /**
     * Registers a runtime filter with a specific target tuple id.
     */
    private void registerRuntimeFilter(RuntimeFilter filter, TupleId targetTid) {
        Preconditions.checkState(filter.getTargetSlots().containsKey(targetTid));
        List<RuntimeFilter> filters = runtimeFiltersByTid.computeIfAbsent(targetTid, k -> new ArrayList<>());
        Preconditions.checkState(!filter.isFinalized());
        filters.add(filter);
    }

    /**
     * Finalizes a runtime filter by disassociating it from all the candidate target scan
     * nodes that haven't been used as destinations for that filter. Also sets the
     * finalized flag of that filter so that it can't be assigned to any other scan nodes.
     */
    private void finalizeRuntimeFilter(RuntimeFilter runtimeFilter) {
        Set<TupleId> targetTupleIds = new HashSet<>();
        for (RuntimeFilter.RuntimeFilterTarget target : runtimeFilter.getTargets()) {
            targetTupleIds.addAll(target.node.getTupleIds());
        }
        for (TupleId tupleId : runtimeFilter.getTargetSlots().keySet()) {
            if (!targetTupleIds.contains(tupleId)) {
                runtimeFiltersByTid.get(tupleId).remove(runtimeFilter);
            }
        }
        runtimeFilter.markFinalized();
    }

    /**
     * Assigns runtime filters to a specific scan node 'scanNode'.
     * The assigned filters are the ones for which 'scanNode' can be used as a destination
     * node. The following constraints are enforced when assigning filters to 'scanNode':
     * 1. If the RUNTIME_FILTER_MODE query option is set to LOCAL, a filter is only assigned
     *    to 'scanNode' if the filter is produced within the same fragment that contains the
     *    scan node.
     * 2. Only olap scan nodes are supported:
     */
    private void assignRuntimeFilters(ScanNode scanNode) {
        if (!(scanNode instanceof OlapScanNode) && !(scanNode instanceof ExternalFileScanNode)) {
            return;
        }
        TupleId tid = scanNode.getTupleIds().get(0);
        if (!runtimeFiltersByTid.containsKey(tid)) {
            return;
        }
        String runtimeFilterMode = sessionVariable.getRuntimeFilterMode();
        Preconditions.checkState(Arrays.stream(TRuntimeFilterMode.values()).map(Enum::name).anyMatch(
                p -> p.equals(runtimeFilterMode.toUpperCase())), "runtimeFilterMode not expected");
        for (RuntimeFilter filter : runtimeFiltersByTid.get(tid)) {
            if (filter.isFinalized()) {
                continue;
            }
            Expr targetExpr = computeTargetExpr(filter, tid);
            if (targetExpr == null) {
                continue;
            }
            boolean isBoundByKeyColumns = isBoundByKeyColumns(analyzer, targetExpr, scanNode);
            boolean isLocalTarget = isLocalTarget(filter, scanNode);
            if (runtimeFilterMode.equals(TRuntimeFilterMode.LOCAL.name()) && !isLocalTarget) {
                continue;
            }
            if (runtimeFilterMode.equals(TRuntimeFilterMode.REMOTE.name()) && isLocalTarget) {
                continue;
            }

            RuntimeFilter.RuntimeFilterTarget target = new RuntimeFilter.RuntimeFilterTarget(
                    scanNode, targetExpr, isBoundByKeyColumns, isLocalTarget);
            filter.addTarget(target);
        }
    }

    /**
     * Check if 'targetNode' is local to the source node of 'filter'.
     */
    private static boolean isLocalTarget(RuntimeFilter filter, ScanNode targetNode) {
        return targetNode.getFragment().getId().equals(filter.getBuilderNode().getFragment().getId());
    }

    /**
     * Check if all the slots of'targetExpr' is key.
     */
    private static boolean isBoundByKeyColumns(Analyzer analyzer, Expr targetExpr, ScanNode targetNode) {
        Preconditions.checkState(targetExpr.isBoundByTupleIds(targetNode.getTupleIds()));
        List<SlotId> sids = new ArrayList<>();
        targetExpr.getIds(null, sids);
        for (SlotId sid : sids) {
            // Take slotDesc from the desc of targetExpr the same
            SlotDescriptor slotDesc = analyzer.getSlotDesc(sid);
            if (slotDesc.getColumn() == null || !slotDesc.getColumn().isKey()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Computes the target expr for a specified runtime filter 'filter' to be applied at
     * the scan node with target tuple descriptor 'targetTid'.
     */
    private Expr computeTargetExpr(RuntimeFilter filter, TupleId targetTid) {
        Expr targetExpr = filter.getOrigTargetExpr();
        // if there is a subquery on the left side of join, in order to push to scan in the subquery,
        // targetExpr will return false as long as there is a slotref parent node that is not targetTid.
        // But when this slotref can be transferred to the targetTid slot, such as Aa + Bb = Cc,
        // targetTid is B, if Aa can be transferred to Ba, that is, Aa and Ba are equivalent columns,
        // then replace Aa with Ba, and then calculate for targetTid targetExpr
        if (!targetExpr.isBound(targetTid)) {
            Preconditions.checkState(filter.getTargetSlots().containsKey(targetTid));
            // Modify the filter target expr using the equivalent slots from the scan node
            // on which the filter will be applied.
            ExprSubstitutionMap smap = new ExprSubstitutionMap();
            List<SlotRef> exprSlots = new ArrayList<>();
            // Get the ids of all slotRef children of targetExpr, which is equal to the deduplication of
            // all slots of targetSlotsByTid.
            targetExpr.collect(SlotRef.class, exprSlots);
            // targetExpr specifies the id of the slotRef node in the `tupleID`
            List<SlotId> sids = filter.getTargetSlots().get(targetTid);
            for (SlotRef slotRef : exprSlots) {
                for (SlotId sid : sids) {
                    if (analyzer.hasValueTransfer(slotRef.getSlotId(), sid)) {
                        SlotRef newSlotRef = new SlotRef(analyzer.getSlotDesc(sid));
                        newSlotRef.analyzeNoThrow(analyzer);
                        smap.put(slotRef, newSlotRef);
                        break;
                    }
                }
            }
            Preconditions.checkState(exprSlots.size() == smap.size());
            try {
                targetExpr = targetExpr.substitute(smap, analyzer, false);
            } catch (Exception e) {
                return null;
            }
        }
        Type srcType = filter.getSrcExpr().getType();
        // Types of targetExpr and srcExpr must be exactly the same since runtime filters are
        // based on hashing.
        if (!targetExpr.getType().equals(srcType)) {
            try {
                targetExpr = targetExpr.castTo(srcType);
            } catch (Exception e) {
                return null;
            }
        }
        return targetExpr;
    }
}
