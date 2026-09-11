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

import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanNodeAndHash;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.statistics.hbo.PlanStatistics;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatistics;
import org.apache.doris.statistics.model.Statistics;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * StatsCalculator by using hbo plan stats. to do estimation.
 */
public class HboStatsCalculator extends StatsCalculator {
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
        Statistics pinnedStats = applyPinnedStats(filterNode, legacyStats, inputStats);
        if (pinnedStats != null) {
            return pinnedStats;
        }
        // 2) learned: the filter fingerprints first (so an injected learned entry keyed by a
        //    printed filter fingerprint is honored), then the scan group fingerprint used by the
        //    publish path (whose entries carry the predicates for matching)
        for (GroupStructInfo.LiteralMode mode : FILTER_LOOKUP_MODES) {
            Statistics learnedStats = applyLearnedStats(
                    HboUtils.getHboPlanNodeAndHash(filterNode, mode), legacyStats);
            if (learnedStats != null) {
                return learnedStats;
            }
        }
        if (HboUtils.isLogicalFilterOnLogicalScan(filter) || HboUtils.isPhysicalFilterOnPhysicalScan(filter)) {
            AbstractPlan scanPlan = HboUtils.getScanUnderFilterNode(filter);
            Statistics learnedStats = applyLearnedStats(HboUtils.getHboPlanNodeAndHash(scanPlan), legacyStats);
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
        // join / aggregation keys never carry literals: only the constant agnostic fingerprint
        return getStatsFromHboPlanStats((AbstractPlan) join, legacyStats,
                GroupStructInfo.LiteralMode.NO_LITERAL, null);
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
        return getStatsFromHboPlanStats((AbstractPlan) aggregate, legacyStats,
                GroupStructInfo.LiteralMode.NO_LITERAL, null);
    }

    /** Pinned lookup order for filter roots: exact form first, then the constant agnostic form. */
    private static final GroupStructInfo.LiteralMode[] FILTER_LOOKUP_MODES = {
            GroupStructInfo.LiteralMode.WITH_LITERAL,
            GroupStructInfo.LiteralMode.NO_LITERAL,
    };

    /**
     * Apply a pinned entry for the given plan node: exact filter form, then constant agnostic form
     * (join / aggregation only have the latter). Returns null when no entry applies.
     *
     * @param guardInputStats filter input statistics, required to evaluate the FILTER_SMALL guard
     *                        of a filter node (null for join / aggregation)
     */
    private Statistics applyPinnedStats(AbstractPlan planNode, Statistics delegateStats,
            Statistics guardInputStats) {
        for (GroupStructInfo.LiteralMode mode : FILTER_LOOKUP_MODES) {
            Optional<String> fingerprint = GroupStructInfo.fingerprintOfPlanNode(planNode, mode);
            if (!fingerprint.isPresent()) {
                continue;
            }
            Optional<HboPlanStatisticsManager.PinnedHboStatistics> pinnedOpt = Env.getCurrentEnv()
                    .getHboPlanStatisticsManager().getPinnedPlanStatistics(fingerprint.get());
            if (!pinnedOpt.isPresent()) {
                continue;
            }
            HboPlanStatisticsManager.PinnedHboStatistics pinned = pinnedOpt.get();
            if (pinned.getType() == HboPlanStatisticsManager.PinnedType.FILTER_SMALL
                    && guardInputStats != null
                    && !isExtremeSmallFilterEstimate(delegateStats.getRowCount(),
                            guardInputStats.getRowCount())) {
                // the estimate is not in the pathological regime this entry was injected for:
                // skip it (and try the coarser granularity / learned matching instead)
                recordGuardSkip(fingerprint.get(), delegateStats.getRowCount(), guardInputStats.getRowCount());
                continue;
            }
            pinned.bindFingerprintKind(mode == GroupStructInfo.LiteralMode.NO_LITERAL
                    ? HboPlanStatisticsManager.FingerprintKind.NO_LITERAL
                    : HboPlanStatisticsManager.FingerprintKind.WITH_LITERAL);
            return delegateStats.withRowCountAndHboFlag(pinned.getRows());
        }
        return null;
    }

    private Statistics getStatsFromHboPlanStats(AbstractPlan planNode, Statistics delegateStats,
            GroupStructInfo.LiteralMode mode, Statistics guardInputStats) {
        Statistics pinnedStats = applyPinnedStats(planNode, delegateStats, guardInputStats);
        if (pinnedStats != null) {
            return pinnedStats;
        }
        Statistics learnedStats = applyLearnedStats(HboUtils.getHboPlanNodeAndHash(planNode, mode), delegateStats);
        return learnedStats == null ? delegateStats : learnedStats;
    }

    private Statistics applyLearnedStats(Optional<PlanNodeAndHash> planNodeAndHashOpt, Statistics delegateStats) {
        if (!planNodeAndHashOpt.isPresent() || !planNodeAndHashOpt.get().getHash().isPresent()) {
            return null;
        }
        RecentRunsPlanStatistics planStatistics = hboPlanStatisticsProvider.getHboPlanStats(planNodeAndHashOpt.get());
        PlanStatistics matchedPlanStatistics = HboUtils.getMatchedPlanStatistics(planStatistics,
                cascadesContext.getConnectContext());
        if (matchedPlanStatistics == null) {
            return null;
        }
        return delegateStats.withRowCountAndHboFlag(matchedPlanStatistics.getOutputRows());
    }

    /** True while the optimizer's own filter estimate is in the pathological "extremely small" regime. */
    private static boolean isExtremeSmallFilterEstimate(double estimatedRows, double inputRows) {
        if (estimatedRows <= 1) {
            return true;
        }
        return inputRows > 0 && estimatedRows <= inputRows * Config.hbo_filter_small_ratio;
    }

    /** Record (per query) that a FILTER_SMALL entry was skipped, for the explain annotation. */
    private void recordGuardSkip(String fingerprint, double estimatedRows, double inputRows) {
        if (cascadesContext == null || cascadesContext.getConnectContext() == null) {
            return;
        }
        HboPlanInfoProvider provider = Env.getCurrentEnv().getHboPlanStatisticsManager().getHboPlanInfoProvider();
        provider.putPinnedGuardSkip(DebugUtil.printId(cascadesContext.getConnectContext().queryId()),
                fingerprint, "filterSmallGuard(E=" + (long) estimatedRows + ",I=" + (long) inputRows + ")");
    }

}
