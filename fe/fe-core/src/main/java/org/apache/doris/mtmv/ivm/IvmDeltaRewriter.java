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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Entry point for IVM delta rewriting.
 *
 * <p>The rewriter visits the normalized tree once and constructs its delta relation recursively. Join children
 * use pre- and post-refresh snapshots as required by their algebraic expansion.
 */
public class IvmDeltaRewriter {

    private final IvmDeltaRewriteHelper helper = IvmDeltaRewriteHelper.INSTANCE;

    /**
     * Generates the rewritten sink child for the analyzer rule path.
     * The input sink child may already contain bind-sink adapter projects.
     */
    public Plan generateIncrRefreshPlan(Plan sinkChild, IvmRewriteResult rewriteResult,
            IvmRewriteContext rewriteContext, ConnectContext connectContext) {
        IvmIncrRefreshContext refreshContext = new IvmIncrRefreshContext(
                rewriteContext.getMtmv(), connectContext, rewriteResult,
                rewriteContext.isIncludeExhaustedStreams());
        if (refreshContext.getMtmv().isIvmAppendOnlyDupTable()) {
            // An append-only detail MV holds the query's own rows, so its delta is the rows its base
            // tables appended, and the plan that produces it carries no sign, no sequence and no row-id.
            // Nothing below applies to it -- and nothing below may read it: the sequence prefix (a
            // meta-service version allocation in cloud mode), the MV's sequence column type, and the
            // row-id of the normalized root all belong to the layout only the unique-key model has. The
            // sink child is taken as it stands for the same reason: the adapter projects below detach and
            // reattach the hidden columns of that layout.
            Preconditions.checkState(!rewriteResult.isAggMv(),
                    "an append-only detail MV holds no aggregate");
            Optional<Plan> deltaPlan = IvmDupDeltaRewriter.generateIncrRefreshPlan(sinkChild, refreshContext,
                    rewriteContext.getIncrementalScopePartitionIds());
            return deltaPlan.orElseGet(() -> emptyDeltaPlan(sinkChild, refreshContext));
        }
        Pair<Plan, List<LogicalProject<?>>> prefixChain = helper.detachAdaptProjectChain(sinkChild);
        Plan rootPlan = prefixChain.first;
        long sequencePrefix = refreshContext.getMtmv().getNextSequencePrefix();
        IvmDeltaRewriteState rewriteState = IvmDeltaRewriteState.forUniqueKeys(rootPlan, refreshContext, sequencePrefix,
                rewriteContext.getIncrementalScopePartitionIds());
        Optional<IvmDeltaRewriteResult> deltaResult = rewriteDelta(rootPlan, refreshContext, rewriteState);
        if (!deltaResult.isPresent()) {
            return emptyDeltaPlan(sinkChild, refreshContext);
        }
        // A non-empty delta reaches this point, and only an aggregate delta joins the MV's
        // old rows with the new delta. If the previous refresh txn committed but its data is
        // not yet visible (an MV partition's committed version is ahead of its visible
        // version), that join misses the old rows and the delta is permanently lost. Fail
        // here; the fallback chain recomputes from the base tables without reading old MV
        // state. EXPLAIN REFRESH is exempt: it only produces a plan and neither executes it
        // nor reads MV data.
        if (!rewriteContext.isExplain()
                && rewriteResult.isAggMv()
                && hasUnpublishedCommittedMvData(rewriteContext.getMtmv())) {
            throw new IvmException(IvmFailureReason.MV_COMMIT_NOT_VISIBLE,
                    "previous refresh txn committed but its MV data is not visible yet; "
                            + "aggregate delta would join stale old MV rows");
        }
        Plan deltaPlan = deltaResult.get().plan;
        IvmDeltaRewriteResult result = deltaResult.get();
        IvmDeltaRewriteResult mergedResult = new IvmDeltaRewriteResult(deltaPlan,
                helper.findSlotByName(deltaPlan.getOutput(), Column.IVM_DML_FACTOR_COL),
                helper.findSlotByName(deltaPlan.getOutput(), Column.SEQUENCE_COL), result.maxDeltaIndex);
        Slot rootRowId = IvmUtil.findRowIdSlot(rootPlan.getOutput(), "normalized plan root");
        if (!refreshContext.getRewriteResult().isDeterministic(rootRowId)) {
            mergedResult = helper.wrapDmlFactorWithRootNonDetGuard(mergedResult);
        }
        return helper.finalizeQuery(prefixChain, mergedResult, refreshContext);
    }

    /**
     * The plan of a refresh that has nothing to write: it reads no rows, so the MV keeps the rows it has.
     */
    private static Plan emptyDeltaPlan(Plan sinkChild, IvmIncrRefreshContext refreshContext) {
        return new LogicalEmptyRelation(
                refreshContext.getConnectContext().getStatementContext().getNextRelationId(),
                sinkChild.getOutput());
    }

    Optional<IvmDeltaRewriteResult> rewriteDelta(Plan plan, IvmIncrRefreshContext ctx,
            IvmDeltaRewriteState rewriteState) {
        IvmDeltaRewriteVisitor visitor = new IvmDeltaRewriteVisitor(
                new IvmLinearDeltaHandler(), new IvmJoinDeltaHandler(), new IvmAggDeltaHandler(), rewriteState);
        return visitor.rewritePlan(plan, ctx);
    }

    /**
     * A refresh txn assigns the next partition version when it commits, while the
     * partition's visible version only advances when the txn publishes. Any MV
     * partition whose committed version is still ahead of its visible version
     * therefore holds unpublished refresh data.
     */
    private boolean hasUnpublishedCommittedMvData(MTMV mtmv) {
        if (Config.isCloudMode()) {
            return false;
        }
        for (Partition partition : mtmv.getPartitions()) {
            if (partition.getCommittedVersion() > partition.getVisibleVersion()) {
                return true;
            }
        }
        return false;
    }

    static Pair<Plan, Map<Slot, Slot>> preSnapshot(Plan plan, IvmDeltaRewriteState rewriteState) {
        return rewriteSnapshot(plan, rewriteState, true);
    }

    static Pair<Plan, Map<Slot, Slot>> postSnapshot(Plan plan, IvmDeltaRewriteState rewriteState) {
        return rewriteSnapshot(plan, rewriteState, false);
    }

    private static Pair<Plan, Map<Slot, Slot>> rewriteSnapshot(Plan plan, IvmDeltaRewriteState rewriteState,
            boolean preSnapshot) {
        Plan rewritten = plan.rewriteDownShortCircuit(node -> {
            if (!(node instanceof LogicalOlapScan)) {
                return node;
            }
            LogicalOlapScan scan = (LogicalOlapScan) node;
            if (rewriteState.isExcluded(scan)) {
                // Excluded tables produce no delta, but their snapshot side still joins
                // the delta side; apply the window so the property also saves their scan.
                return rewriteState.restrictWindow(scan);
            }
            scan = rewriteState.restrictWindow(scan);
            LogicalPlan snapshotScan = preSnapshot
                    ? scan.withPreSnapshot(Optional.of(rewriteState.getStream(scan)))
                    : scan.withPostSnapshot();
            return IvmDeltaRewriteHelper.INSTANCE.remapOlapScanToPlan(scan, snapshotScan);
        });
        return IvmDeltaRewriteHelper.INSTANCE.freshPlan(rewritten);
    }

}
