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

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanNodeAndHash;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatistics;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatisticsEntry;
import org.apache.doris.statistics.model.Statistics;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * StatsCalculator by using hbo plan stats. to do estimation.
 */
public class HboStatsCalculator extends StatsCalculator {
    /** Pinned lookup order for filter roots: exact form first, then the constant agnostic form. */
    private static final GroupStructInfo.LiteralMode[] FILTER_LOOKUP_MODES = {
            GroupStructInfo.LiteralMode.WITH_LITERAL,
            GroupStructInfo.LiteralMode.NO_LITERAL,
    };

    /**
     * Lookup modes of a join / aggregation root, most specific first: an entry which was pinned with
     * literals ({@code lit(1:INT)}) only describes that constant, while the constant agnostic entry
     * ({@code lit(*)}) describes every constant of the same predicate shape. The literal carrying key is
     * therefore looked up first and the agnostic one remains the fallback, so an entry which was pinned
     * before this key existed keeps being applied.
     */
    private static final GroupStructInfo.LiteralMode[] JOIN_AGG_LOOKUP_MODES = {
            GroupStructInfo.LiteralMode.WITH_LITERAL,
            GroupStructInfo.LiteralMode.NO_LITERAL,
    };

    private final HboPlanStatisticsProvider hboPlanStatisticsProvider;

    public HboStatsCalculator(GroupExpression groupExpression, boolean forbidUnknownColStats,
            Map<CTEId, Statistics> cteIdToStats, CascadesContext context) {
        super(groupExpression, forbidUnknownColStats, cteIdToStats, context);
        this.hboPlanStatisticsProvider = Objects.requireNonNull(Env.getCurrentEnv().getHboPlanStatisticsManager()
                        .getHboPlanStatisticsProvider(), "HboPlanStatisticsProvider is null");
    }

    /**
     * NOTE: Can't override computeScan since the publishing side's plan hash of scan node
     * use the scan's hbo string but embedding the filter info into the input table structure.
     * @param filter filter
     * @return Statistics
     */
    @Override
    public Statistics computeFilter(Filter filter, Statistics inputStats) {
        Statistics legacyStats = super.computeFilter(filter, inputStats);
        AbstractPlan filterNode = (AbstractPlan) filter;
        // 1) pinned, authoritative: exact (literal carrying) then constant agnostic filter
        //    fingerprint; FILTER_SMALL entries additionally require the optimizer estimate to be
        //    in the pathological "extremely small" regime
        Statistics pinnedStats = applyPinnedStats(filterNode, legacyStats, inputStats, FILTER_LOOKUP_MODES);
        if (pinnedStats != null) {
            return pinnedStats;
        }
        // 2) learned: the filter fingerprints first (so an injected learned entry keyed by a
        //    printed filter fingerprint is honored), then the scan group fingerprint used by the
        //    publish path (whose entries carry the predicates for matching)
        for (GroupStructInfo.LiteralMode mode : FILTER_LOOKUP_MODES) {
            Statistics learnedStats = applyLearnedStats(filterNode, mode, legacyStats);
            if (learnedStats != null) {
                return learnedStats;
            }
        }
        if (HboUtils.isLogicalFilterOnLogicalScan(filter) || HboUtils.isPhysicalFilterOnPhysicalScan(filter)) {
            AbstractPlan scanPlan = HboUtils.getScanUnderFilterNode(filter);
            Statistics learnedStats = applyLearnedStats(
                    scanPlan, GroupStructInfo.LiteralMode.WITH_LITERAL, legacyStats);
            if (learnedStats != null) {
                return learnedStats;
            }
        }
        return legacyStats;
    }

    @Override
    public Statistics computeJoin(Join join, Statistics leftStats, Statistics rightStats) {
        Statistics legacyStats = super.computeJoin(join, groupExpression.childStatistics(0),
                groupExpression.childStatistics(1));
        AbstractPlan joinNode = (AbstractPlan) join;
        // 1) exact pinned row count for this join group (authoritative)
        Statistics pinnedStats = applyPinnedStats(joinNode, legacyStats, null, JOIN_AGG_LOOKUP_MODES);
        if (pinnedStats != null) {
            return pinnedStats;
        }
        // 2) injected per-condition expansion: inflate the estimated output so that this join is
        //    placed as late as possible in the join order (not an accuracy correction)
        Statistics expansionStats = applyPinnedJoinExpansion(join, legacyStats);
        if (expansionStats != null) {
            return expansionStats;
        }
        // 3) learned: the same modes as the pinned lookup, most specific first, so a learned entry
        //    which was injected for the printed literal carrying key is honored as well
        for (GroupStructInfo.LiteralMode mode : JOIN_AGG_LOOKUP_MODES) {
            Statistics learnedStats = applyLearnedStats(joinNode, mode, legacyStats);
            if (learnedStats != null) {
                return learnedStats;
            }
        }
        return legacyStats;
    }

    /**
     * Apply an injected join expansion entry ({@code HBO SET STATISTICS ... TYPE=JOIN_EXPANSION}):
     * the fan-out factor of the join equality conditions scales the current input estimates, so
     * that a join known to explode looks expensive and is scheduled as late as possible.
     *
     * <p>The factor is relative to the <b>larger</b> of the two inputs
     * ({@code output rows / max(left rows, right rows)}). Anchoring it to one side instead - e.g. to
     * the left child, as this method once did - makes the estimate depend on which side the planner
     * happens to put on the left: the key is the equality conditions only, and
     * {@link HboJoinConditions} sorts the operands of every equality, so one entry is matched by
     * both child orders. The same entry would then describe {@code factor * max(l, r)} in one
     * orientation and {@code factor * min(l, r)} in the other, and for a skewed pair the injected
     * join could even look cheaper than the optimizer's own estimate.
     *
     * <p>Semi / anti / asof style joins can never expand, so an injected entry is deliberately not
     * applied there (the reason is reported by the explain annotation). A cross join has no
     * equality condition and therefore no key at all.
     */
    private Statistics applyPinnedJoinExpansion(Join join, Statistics delegateStats) {
        Optional<String> condFingerprint = HboJoinConditions.fingerprintOf(join);
        if (!condFingerprint.isPresent()) {
            return null;
        }
        Optional<HboPlanStatisticsManager.PinnedHboStatistics> expansionOpt = Env.getCurrentEnv()
                .getHboPlanStatisticsManager().getPinnedExpansion(condFingerprint.get());
        if (!expansionOpt.isPresent()) {
            return null;
        }
        if (!HboJoinConditions.isExpansionApplicable(join.getJoinType())) {
            recordExpansionSkip(condFingerprint.get(), join.getJoinType().toString().toLowerCase(Locale.ROOT)
                    + "-join-never-expands");
            return null;
        }
        double leftRows = groupExpression.childStatistics(0).getRowCount();
        double rightRows = groupExpression.childStatistics(1).getRowCount();
        double expansion = expansionOpt.get().getExpansion();
        // the factor is the fan out of the equality conditions: it scales their estimate, and the non
        // equality conditions keep their own selectivity on top of it (see JoinEstimation)
        double estimated = JoinEstimation.estimate(groupExpression.childStatistics(0),
                groupExpression.childStatistics(1), join, expansion).getRowCount();
        // the estimate the factor was applied to, reported by the annotation: the equality estimate with
        // the residual selectivity of this query
        double baseRows = expansion == 0 ? estimated : estimated / expansion;
        // an equi join can never produce more rows than the cartesian product of its inputs, except a
        // full outer join: its unmatched rows are kept as well, so one row on each side which do not
        // match already produce two rows although the product is one
        estimated = Math.min(estimated, expansionUpperBound(join.getJoinType(), leftRows, rightRows));
        // outer joins can not produce fewer rows than their preserved side
        switch (join.getJoinType()) {
            case LEFT_OUTER_JOIN:
                estimated = Math.max(estimated, leftRows);
                break;
            case RIGHT_OUTER_JOIN:
                estimated = Math.max(estimated, rightRows);
                break;
            case FULL_OUTER_JOIN:
                estimated = Math.max(estimated, Math.max(leftRows, rightRows));
                break;
            default:
                break;
        }
        long rows = Math.max(1L, (long) estimated);
        recordExpansionApplied(condFingerprint.get(), expansion, leftRows, rightRows, baseRows, rows);
        return delegateStats.withRowCountAndHboFlag(rows);
    }

    private void recordExpansionApplied(String condFingerprint, double expansion, double leftRows,
            double rightRows, double baseRows, long estimated) {
        String queryId = currentQueryId();
        if (queryId == null) {
            return;
        }
        Env.getCurrentEnv().getHboPlanStatisticsManager().getHboPlanInfoProvider()
                .putExpansionApplied(queryId, condFingerprint, "exp=" + trimDouble(expansion) + "x (left="
                        + (long) leftRows + ",right=" + (long) rightRows + ",base=" + (long) baseRows
                        + ",est=" + estimated + ")");
    }

    private void recordExpansionSkip(String condFingerprint, String reason) {
        String queryId = currentQueryId();
        if (queryId == null) {
            return;
        }
        Env.getCurrentEnv().getHboPlanStatisticsManager().getHboPlanInfoProvider()
                .putPinnedGuardSkip(queryId, condFingerprint, reason);
    }

    private String currentQueryId() {
        if (cascadesContext == null || cascadesContext.getConnectContext() == null) {
            return null;
        }
        return DebugUtil.printId(cascadesContext.getConnectContext().queryId());
    }

    private static String trimDouble(double value) {
        if (value == Math.floor(value) && !Double.isInfinite(value)) {
            return String.valueOf((long) value);
        }
        return String.valueOf(value);
    }

    /**
     * The largest number of rows an equi join of the given type can produce from its two inputs: the
     * cartesian product, except for a full outer join, which also keeps the rows of both sides which
     * found no partner - one row on each side which do not match already produce two rows although the
     * product is one, so an injected fan-out of 2 for such a pair would otherwise be capped to 1.
     */
    static double expansionUpperBound(JoinType joinType, double leftRows, double rightRows) {
        if (joinType == JoinType.FULL_OUTER_JOIN) {
            return Math.max(leftRows * rightRows, leftRows + rightRows);
        }
        return leftRows * rightRows;
    }

    @Override
    public Statistics computeAggregate(Aggregate<? extends Plan> aggregate, Statistics inputStats) {
        Statistics legacyStats = super.computeAggregate(aggregate, inputStats);
        // NOTE: aggr has two times matching, one is the global but logical aggr,
        // another is local but physical aggr.
        // the physical one can be matched but the logical one is hard to be matched.
        // e.g, logical one likes "count(*) AS `count(*)`#4"
        //      local physical one likes "partial_count(*) AS `partial_count(*)`#5"
        //      global physical one likes "count(partial_count(*)#5) AS `count(*)`#4"
        // the pinned part already tries every mode itself; the learned part has to try them one by one,
        // because the publish path keys an entry with the constant agnostic fingerprint and an injected
        // one may carry the literal carrying fingerprint (getStatsFromHboPlanStats returns the delegate
        // when nothing applied, so it cannot be used to advance the loop - see computeJoin)
        Statistics pinnedStats = applyPinnedStats((AbstractPlan) aggregate, legacyStats, null,
                JOIN_AGG_LOOKUP_MODES);
        if (pinnedStats != null) {
            return pinnedStats;
        }
        for (GroupStructInfo.LiteralMode mode : JOIN_AGG_LOOKUP_MODES) {
            Statistics learnedStats = applyLearnedStats((AbstractPlan) aggregate, mode, legacyStats);
            if (learnedStats != null) {
                return learnedStats;
            }
        }
        return legacyStats;
    }

    /**
     * Apply a pinned entry for the given plan node: exact filter form, then constant agnostic form
     * (join / aggregation only have the latter). Returns null when no entry applies.
     *
     * <p>The fingerprint of an entry does not contain the data state of its tables any more, so an
     * entry keeps matching while a table grows; whether it may be used is decided here, by comparing
     * the baseline the entry was recorded with against the data state of this query
     * (see {@link HboStructFreshness}): an entry whose data moved by more than
     * {@code Config.hbo_row_count_change_ratio} is skipped, so the query falls back to the optimizer
     * estimation instead of applying a row count which no longer describes the data.
     *
     * @param guardInputStats filter input statistics, required to evaluate the FILTER_SMALL guard
     *                        of a filter node (null for join / aggregation)
     */
    private Statistics applyPinnedStats(AbstractPlan planNode, Statistics delegateStats,
            Statistics guardInputStats, GroupStructInfo.LiteralMode[] lookupModes) {
        // the fingerprint of the node is what an entry is keyed by, and building it walks the whole
        // memo subtree: with an empty pinned cache every lookup is a guaranteed miss, so the
        // fingerprint is not built at all (this is the common case in production, where hbo
        // optimization is enabled but nothing was injected)
        if (!Env.getCurrentEnv().getHboPlanStatisticsManager().hasAnyPinnedStatistics()) {
            return null;
        }
        // the relation key of the group is a necessary condition for a match and costs one step per
        // group, while the fingerprint walks the whole memo sub tree: when no entry was recorded for
        // a sub tree which reads exactly these tables, no fingerprint of this node can match
        String relationKey = GroupStructInfo.relationKeyOfPlanNode(planNode, null);
        if (relationKey != null && !Env.getCurrentEnv().getHboPlanStatisticsManager()
                .mayHavePinnedEntryForRelations(relationKey)) {
            return null;
        }
        for (GroupStructInfo.LiteralMode mode : lookupModes) {
            Optional<GroupStructInfo> structInfoOpt = GroupStructInfo.structInfoOfPlanNode(planNode, null);
            if (!structInfoOpt.isPresent()) {
                continue;
            }
            GroupStructInfo structInfo = structInfoOpt.get();
            String fingerprint = structInfo.getFingerprint(mode);
            Optional<HboPlanStatisticsManager.PinnedHboStatistics> pinnedOpt = Env.getCurrentEnv()
                    .getHboPlanStatisticsManager().getPinnedPlanStatistics(fingerprint);
            if (!pinnedOpt.isPresent()) {
                continue;
            }
            HboPlanStatisticsManager.PinnedHboStatistics pinned = pinnedOpt.get();
            // report which literal mode of the injected entry matched (and, for FILTER_SMALL, why it
            // was skipped) through the explain annotation; its type is read from the entry itself
            recordPinnedLiteralMode(fingerprint,
                    mode == GroupStructInfo.LiteralMode.NO_LITERAL ? "no_literal" : "with_literal");
            if (pinned.getType() == HboPlanStatisticsManager.PinnedType.FILTER_SMALL
                    && guardInputStats != null
                    && !isExtremeSmallFilterEstimate(delegateStats.getRowCount(),
                            guardInputStats.getRowCount())) {
                // the estimate is not in the pathological regime this entry was injected for:
                // skip it (and try the coarser granularity / learned matching instead)
                recordGuardSkip(fingerprint, delegateStats.getRowCount(), guardInputStats.getRowCount());
                continue;
            }
            // the entry is judged against the data state of the one description this query builds for
            // the group (see Group#getOrComputeHboStructParts): the same state the explain annotation
            // prints, so what a user copies is what the next queries judge against. An entry which
            // records no data state at all is applied as it always was.
            HboStructFreshness freshness;
            if (HboStructFreshness.hasRecordedDataState(pinned.getStructCanonical())) {
                Optional<GroupStructInfo> dataState = GroupStructInfo.structInfoOfPlanNode(planNode, null);
                // nothing to compare means unknown, which keeps the entry applicable (soft strategy)
                freshness = dataState.isPresent()
                        ? HboStructFreshness.between(pinned.getStructCanonical(), dataState.get())
                        : HboStructFreshness.between(pinned.getStructCanonical(),
                                java.util.Collections.emptyList());
            } else {
                freshness = HboStructFreshness.between(pinned.getStructCanonical(), structInfo.getScans());
            }
            if (freshness.isStale()) {
                // the data this entry was measured on moved too far (or its state cannot be
                // verified any more): do not apply the recorded row count, keep looking
                recordPinnedSkip(fingerprint, freshness.getSummary());
                continue;
            }
            recordPinnedApplyState(fingerprint, freshness.getSummary());
            pinned.recordUse(freshness);
            return delegateStats.withRowCountAndHboFlag(pinned.getRows());
        }
        return null;
    }

    private Statistics getStatsFromHboPlanStats(AbstractPlan planNode, Statistics delegateStats,
            GroupStructInfo.LiteralMode mode, Statistics guardInputStats) {
        Statistics pinnedStats = applyPinnedStats(planNode, delegateStats, guardInputStats,
                JOIN_AGG_LOOKUP_MODES);
        if (pinnedStats != null) {
            return pinnedStats;
        }
        Statistics learnedStats = applyLearnedStats(planNode, mode, delegateStats);
        return learnedStats == null ? delegateStats : learnedStats;
    }

    private Statistics applyLearnedStats(AbstractPlan planNode, GroupStructInfo.LiteralMode mode,
            Statistics delegateStats) {
        // same reasoning as in applyPinnedStats: the learned key contains the hbo fingerprint of the
        // node, which is only worth building when the learned cache holds something at all
        if (!hboPlanStatisticsProvider.hasAnyHboPlanStats()) {
            return null;
        }
        Optional<PlanNodeAndHash> planNodeAndHashOpt = HboUtils.getHboPlanNodeAndHash(planNode, mode);
        if (!planNodeAndHashOpt.isPresent() || !planNodeAndHashOpt.get().getHash().isPresent()) {
            return null;
        }
        RecentRunsPlanStatistics planStatistics = hboPlanStatisticsProvider.getHboPlanStats(planNodeAndHashOpt.get());
        RecentRunsPlanStatisticsEntry matchedEntry = HboUtils.getMatchedEntry(planStatistics,
                cascadesContext.getConnectContext());
        if (matchedEntry == null) {
            return null;
        }
        // a learned key does not contain the data state of its tables either, so the entry is judged
        // by the rows it recorded for the input tables of its sub tree: the publish path records one
        // ScanPlanStatistics per scan which feeds the node, so the guard holds for every kind of node
        // (see HboStructFreshness.ofLearnedEntry). A learned join, filter or aggregation would
        // otherwise keep the row count of a run whose inputs have since grown by orders of magnitude.
        String hash = planNodeAndHashOpt.get().getHash().get();
        Optional<GroupStructInfo> liveStructInfo = GroupStructInfo.structInfoOfPlanNode(planNode, null);
        if (liveStructInfo.isPresent()) {
            HboStructFreshness freshness = learnedEntryFreshness(matchedEntry, liveStructInfo.get());
            if (freshness.isStale()) {
                recordPinnedSkip(hash, freshness.getSummary());
                return null;
            }
        }
        // a learned entry also has to be visible as "used" in the explain annotation, which is keyed
        // by the fingerprint of the node and the literal mode which matched it
        String queryId = currentQueryId();
        if (queryId != null) {
            Env.getCurrentEnv().getHboPlanStatisticsManager().getHboPlanInfoProvider()
                    .putPinnedLiteralMode(queryId, hash,
                            mode == GroupStructInfo.LiteralMode.NO_LITERAL ? "no_literal" : "with_literal");
        }
        return delegateStats.withRowCountAndHboFlag(matchedEntry.getPlanStatistics().getOutputRows());
    }

    /**
     * The verdict of a learned entry for one node: the rows the entry recorded for the input tables of
     * its sub tree against the scans this query describes for the same node. The publish path records
     * one scan statistics per scan below the node, so the comparison is the same for a scan node and for
     * a join, filter or aggregation above it - none of them may keep the row count of a run whose
     * inputs have since grown.
     */
    static HboStructFreshness learnedEntryFreshness(RecentRunsPlanStatisticsEntry matchedEntry,
            GroupStructInfo liveStructInfo) {
        return HboStructFreshness.ofLearnedEntry(matchedEntry.getInputTableStatistics(), liveStructInfo);
    }

    /** True while the optimizer's own filter estimate is in the pathological "extremely small" regime. */
    private static boolean isExtremeSmallFilterEstimate(double estimatedRows, double inputRows) {
        if (estimatedRows <= 1) {
            return true;
        }
        return inputRows > 0 && estimatedRows <= inputRows * Config.hbo_filter_small_ratio;
    }

    /** Record (per query) the literal mode of the entry that matched a fingerprint. */
    private void recordPinnedLiteralMode(String fingerprint, String literalMode) {
        String queryId = currentQueryId();
        if (queryId == null) {
            return;
        }
        Env.getCurrentEnv().getHboPlanStatisticsManager().getHboPlanInfoProvider()
                .putPinnedLiteralMode(queryId, fingerprint, literalMode);
    }

    private void recordGuardSkip(String fingerprint, double estimatedRows, double inputRows) {
        if (cascadesContext == null || cascadesContext.getConnectContext() == null) {
            return;
        }
        HboPlanInfoProvider provider = Env.getCurrentEnv().getHboPlanStatisticsManager().getHboPlanInfoProvider();
        provider.putPinnedGuardSkip(DebugUtil.printId(cascadesContext.getConnectContext().queryId()),
                fingerprint, "filterSmallGuard(E=" + (long) estimatedRows + ",I=" + (long) inputRows + ")");
    }

    /** Record (per query) that a pinned entry was rejected because its data moved too far. */
    private void recordPinnedSkip(String fingerprint, String reason) {
        String queryId = currentQueryId();
        if (queryId == null) {
            return;
        }
        Env.getCurrentEnv().getHboPlanStatisticsManager().getHboPlanInfoProvider()
                .putPinnedGuardSkip(queryId, fingerprint, reason);
    }

    /** Record (per query) the data state verdict with which a pinned entry was applied. */
    private void recordPinnedApplyState(String fingerprint, String state) {
        String queryId = currentQueryId();
        if (queryId == null) {
            return;
        }
        Env.getCurrentEnv().getHboPlanStatisticsManager().getHboPlanInfoProvider()
                .putPinnedApplyState(queryId, fingerprint, state);
    }

}
