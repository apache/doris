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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.common.Pair;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
        IvmRefreshContext refreshContext = new IvmRefreshContext(
                rewriteContext.getMtmv(), connectContext, rewriteResult,
                rewriteContext.isIncludeExhaustedStreams());
        Pair<Plan, List<LogicalProject<?>>> prefixChain = helper.detachAdaptProjectChain(sinkChild);
        Plan rootPlan = prefixChain.first;
        long refreshVersion = refreshContext.getMtmv().getNextRefreshVersion();
        IvmDeltaRewriteState rewriteState = createDeltaRewriteState(rootPlan, refreshContext, refreshVersion);
        Optional<IvmDeltaRewriteResult> deltaResult = rewriteDelta(rootPlan, refreshContext, rewriteState);
        if (!deltaResult.isPresent()) {
            return new LogicalEmptyRelation(
                    refreshContext.getConnectContext().getStatementContext().getNextRelationId(),
                    sinkChild.getOutput());
        }
        Plan deltaPlan = deltaResult.get().plan;
        IvmDeltaRewriteResult result = deltaResult.get();
        IvmDeltaRewriteResult mergedResult = new IvmDeltaRewriteResult(deltaPlan,
                helper.findSlotByName(deltaPlan.getOutput(), Column.IVM_DML_FACTOR_COL),
                helper.findSlotByName(deltaPlan.getOutput(), Column.SEQUENCE_COL), result.maxSeqSuffix);
        Slot rootRowId = IvmUtil.findRowIdSlot(rootPlan.getOutput(), "normalized plan root");
        if (!refreshContext.getRewriteResult().isDeterministic(rootRowId)) {
            mergedResult = helper.wrapDmlFactorWithRootNonDetGuard(mergedResult);
        }
        return helper.finalizeQuery(prefixChain, mergedResult, refreshContext);
    }

    Optional<IvmDeltaRewriteResult> rewriteDelta(Plan plan, IvmRefreshContext ctx,
            IvmDeltaRewriteState rewriteState) {
        IvmDeltaRewriteVisitor visitor = new IvmDeltaRewriteVisitor(
                new IvmLinearDeltaHandler(), new IvmJoinDeltaHandler(), new IvmAggDeltaHandler(), rewriteState);
        return visitor.rewritePlan(plan, ctx);
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
                return scan;
            }
            LogicalPlan snapshotScan = preSnapshot
                    ? scan.withPreSnapshot(Optional.of(rewriteState.getStream(scan)))
                    : scan.withPostSnapshot();
            return IvmDeltaRewriteHelper.INSTANCE.remapOlapScanToPlan(scan, snapshotScan);
        });
        return IvmDeltaRewriteHelper.INSTANCE.freshPlan(rewritten);
    }

    private IvmDeltaRewriteState createDeltaRewriteState(Plan plan, IvmRefreshContext ctx, long refreshVersion) {
        Map<OlapTable, OlapTableStream> streams = new HashMap<>();
        Set<TableNameInfo> excludedTriggerTables = ctx.getMtmv().getExcludedTriggerTables();
        plan.foreach(node -> {
            if (!(node instanceof LogicalOlapScan)) {
                return;
            }
            LogicalOlapScan scan = (LogicalOlapScan) node;
            if (isExcludedTriggerTable(scan, excludedTriggerTables)) {
                return;
            }
            OlapTableStream stream = IvmUtil.getIvmStream(ctx.getMtmv(), (OlapTable) scan.getTable());
            streams.put((OlapTable) scan.getTable(), stream);
        });
        return new IvmDeltaRewriteState(streams, ctx.isIncludeExhaustedStreams(), refreshVersion);
    }

    boolean isExcludedTriggerTable(LogicalOlapScan scan, Set<TableNameInfo> excludedTriggerTables) {
        if (excludedTriggerTables == null || excludedTriggerTables.isEmpty()) {
            return false;
        }
        TableNameInfo tableNameInfo = TableNameInfoUtils.fromTableOrNull(scan.getTable());
        if (tableNameInfo == null) {
            return false;
        }
        return MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, tableNameInfo);
    }

}
