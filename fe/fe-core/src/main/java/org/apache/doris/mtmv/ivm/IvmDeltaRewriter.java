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

import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Entry point for IVM delta rewriting. Routes the normalized plan to the appropriate strategy:
 * <ul>
 *   <li>Aggregate MVs → {@link IvmAggDeltaStrategy}</li>
 *   <li>Outer-join MVs → {@link IvmOuterJoinDeltaStrategy}</li>
 *   <li>Linear (non-aggregate) MVs → {@link IvmLinearDeltaStrategy}</li>
 * </ul>
 *
 * <h3>Multi-bundle generation</h3>
 * <p>The rewriter generates one bundle per OlapScan that has pending delta data
 * ({@code consumedTso != latestTso}). OlapScans belonging to excluded trigger tables
 * are skipped entirely (assumed unchanged). For the i-th delta scan Si:
 * <ul>
 *   <li>Si → {@link #replaceWithDelta} (marks as delta source)</li>
 *   <li>Sj where j &lt; i → {@code Sj.withTso(latestTso)} (v2, post-delta snapshot)</li>
 *   <li>Sj where j &gt; i → {@code Sj.withTso(consumedTso)} (v1, pre-delta snapshot)</li>
 * </ul>
 *
 * <p>Both the collection pass and the replacement pass use
 * {@link Plan#rewriteDownShortCircuit} to guarantee identical traversal order,
 * so the incrementing scanIndex correctly correlates each visit to the collected scan list.
 */
public class IvmDeltaRewriter {

    /**
     * Rewrites the normalized plan into a list of delta commands.
     * Dispatches to the appropriate strategy based on the normalize result.
     */
    public List<Command> rewrite(Plan normalizedPlan, IvmRefreshContext ctx) {
        Set<TableNameInfo> excluded = ctx.getMtmv().getExcludedTriggerTables();
        Predicate<LogicalOlapScan> isExcluded = scan -> isExcludedTriggerTable(scan, excluded);
        List<Plan> deltaPlans = generateDeltaPlans(normalizedPlan, ctx, isExcluded);

        List<Command> allCommands = new ArrayList<>();
        for (Plan deltaPlan : deltaPlans) {
            // Each strategy instance is single-use
            allCommands.addAll(createStrategy(ctx).rewrite(deltaPlan));
        }
        return allCommands;
    }

    /**
     * Generates delta plans from the normalized plan by replacing each pending-delta
     * OlapScan with its delta source and binding TSO snapshots on other scans.
     * Returns one plan per OlapScan that has pending delta data.
     *
     * <p>For the i-th delta scan Si in the collected scan list:
     * <ul>
     *   <li>Si is replaced with its delta source (isDelta=true)</li>
     *   <li>Sj where j &lt; i gets bound to latestTso (v2, post-delta snapshot)</li>
     *   <li>Sj where j &gt; i gets bound to consumedTso (v1, pre-delta snapshot)</li>
     * </ul>
     *
     * @return list of plans with TSO bindings, or empty if all scans are up-to-date
     */
    List<Plan> generateDeltaPlans(Plan normalizedPlan,
            IvmRefreshContext ctx,
            Predicate<LogicalOlapScan> isExcluded) {
        // Phase 1: Collect all non-excluded OlapScans and their stream refs.
        List<LogicalOlapScan> allScans = new ArrayList<>();
        List<IvmStreamRef> scanRefs = new ArrayList<>();
        rewriteOlapScans(normalizedPlan, isExcluded, scan -> {
            allScans.add(scan);
            IvmStreamRef ref = ctx.getBaseTableStream(scan);
            if (ref == null) {
                throw new AnalysisException(
                        "IVM: no stream ref found for base table: " + scan.getTable().getName());
            }
            Preconditions.checkState(ref.getLatestTso() >= ref.getConsumedTso(),
                    "IVM: latestTso (%s) must be >= consumedTso (%s) for table %s",
                    ref.getLatestTso(), ref.getConsumedTso(), scan.getTable().getName());
            scanRefs.add(ref);
            return scan;
        });

        if (allScans.isEmpty()) {
            return Collections.emptyList();
        }

        // Phase 2: Generate one plan per scan with pending delta
        List<Plan> deltaPlans = new ArrayList<>();
        for (int i = 0; i < allScans.size(); i++) {
            if (scanRefs.get(i).isUpToDate()) {
                continue;
            }

            final int deltaIndex = i;
            AtomicInteger scanIdx = new AtomicInteger(0);
            Plan modifiedPlan = rewriteOlapScans(normalizedPlan, isExcluded, scan -> {
                int currentIndex = scanIdx.getAndIncrement();
                IvmStreamRef ref = scanRefs.get(currentIndex);
                if (currentIndex == deltaIndex) {
                    return replaceWithDelta(scan, ref);
                } else if (currentIndex < deltaIndex) {
                    return scan.withTso(ref.getLatestTso());
                } else {
                    return scan.withTso(ref.getConsumedTso());
                }
            });

            // Invariant: each modified plan must have exactly one isDelta=true scan
            long deltaCount = modifiedPlan.<LogicalOlapScan>collectToList(
                    n -> n instanceof LogicalOlapScan && ((LogicalOlapScan) n).isDelta()).size();
            Preconditions.checkState(deltaCount == 1,
                    "IVM: expected exactly 1 delta scan per bundle, got " + deltaCount);

            deltaPlans.add(detachMemo(modifiedPlan));
        }

        return deltaPlans;
    }

    private Plan detachMemo(Plan plan) {
        // The normalized plan comes from the MV-query CascadesContext. Delta commands are
        // analyzed in fresh contexts, so stale GroupExpression pointers must not be reused.
        return plan.rewriteUp(node -> node.getGroupExpression().isPresent()
                ? node.withGroupExpression(Optional.empty()) : node);
    }

    /**
     * Visits every {@link LogicalOlapScan} in the plan tree using
     * {@link Plan#rewriteDownShortCircuit}, skipping scans matched by
     * {@code isExcluded}, and applying {@code visitor} to each non-excluded scan.
     */
    private Plan rewriteOlapScans(Plan plan, Predicate<LogicalOlapScan> isExcluded,
            Function<LogicalOlapScan, Plan> visitor) {
        return plan.rewriteDownShortCircuit(node -> {
            if (node instanceof LogicalOlapScan) {
                LogicalOlapScan scan = (LogicalOlapScan) node;
                if (isExcluded.test(scan)) {
                    return node;
                }
                return visitor.apply(scan);
            }
            return node;
        });
    }

    /**
     * Replaces a scan with its delta source.
     *
     * <p>Current mock: returns {@code scan.withIsDelta(true)}. This must return a terminal
     * replacement (no nested scans) because {@code rewriteDownShortCircuit} skips descendants
     * of replaced nodes.
     *
     * TODO: The real implementation will use {@code ref} to construct the actual delta scan
     * source (binlog range [consumedTso, latestTso]) once the binlog scan operator is available.
     */
    private LogicalOlapScan replaceWithDelta(LogicalOlapScan scan, IvmStreamRef ref) {
        return (LogicalOlapScan) scan.withIsDelta(true);
    }

    private IvmDeltaStrategy createStrategy(IvmRefreshContext ctx) {
        IvmNormalizeResult normalizeResult = ctx.getNormalizeResult();
        if (normalizeResult.isAggMv()) {
            return new IvmAggDeltaStrategy(ctx);
        } else if (normalizeResult.isOuterJoinMv()) {
            return new IvmOuterJoinDeltaStrategy(ctx);
        } else {
            return new IvmLinearDeltaStrategy(ctx);
        }
    }

    private boolean isExcludedTriggerTable(LogicalOlapScan scan, Set<TableNameInfo> excludedTriggerTables) {
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
