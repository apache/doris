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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.ExternalMetadataPreloadResult;
import org.apache.doris.nereids.ExternalTablePreloadInfo;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Preload external metadata after relation collection and before internal table locks are acquired.
 */
public class PreloadExternalMetadata implements AnalysisRuleFactory {
    private static final Logger LOG = LogManager.getLogger(PreloadExternalMetadata.class);

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                any().thenApply(ctx -> {
                    StatementContext statementContext = ctx.statementContext;
                    // Run preload at most once even if the collect pipeline re-enters the same statement context.
                    if (!statementContext.getExternalMetadataPreloadResult().isPresent()) {
                        statementContext.setExternalMetadataPreloadResult(executePreload(statementContext));
                    }
                    return ctx.root;
                }).toRule(RuleType.PRELOAD_EXTERNAL_METADATA)
        );
    }

    /**
     * Execute external metadata preload after relation collection and before internal table locks.
     */
    public ExternalMetadataPreloadResult executePreload(StatementContext statementContext) {
        long preloadStartTime = TimeUtils.getStartTimeMs();
        preloadCloudMtmvRefreshContexts(statementContext);
        Optional<String> skipReason = getSkipReason(statementContext);
        if (skipReason.isPresent()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("{} skip external metadata preload before lock: {}",
                        getPreloadQueryIdentifier(statementContext), skipReason.get());
            }
            return ExternalMetadataPreloadResult.skipped(
                    statementContext.getExternalTablePreloadCandidateCount(), skipReason.get());
        }
        int preloadedTableCount = 0;
        for (ExternalTablePreloadInfo preloadInfo : statementContext.getExternalTablePreloadInfos()) {
            if (preloadExternalTable(statementContext, preloadInfo)) {
                preloadedTableCount++;
            }
        }
        return ExternalMetadataPreloadResult.executed(
                statementContext.getExternalTablePreloadCandidateCount(),
                preloadedTableCount,
                TimeUtils.getElapsedTimeMs(preloadStartTime));
    }

    private void preloadCloudMtmvRefreshContexts(StatementContext statementContext) {
        if (Config.isNotCloudMode()) {
            return;
        }
        ConnectContext connectContext = statementContext.getConnectContext();
        Map<MTMV, Set<MTMVRelatedTableIf>> eligibleMtmvs = new LinkedHashMap<>();
        Set<MTMVRelatedTableIf> pctTables = new LinkedHashSet<>();
        for (MTMV mtmv : statementContext.getCandidateMTMVs()) {
            if (!requiresMtmvRefreshContext(statementContext, connectContext, mtmv)
                    || statementContext.getPreloadedMtmvRefreshContext(mtmv).isPresent()) {
                continue;
            }
            try {
                Set<MTMVRelatedTableIf> candidatePctTables = mtmv.getMvPartitionInfo().getPctTables();
                eligibleMtmvs.put(mtmv, candidatePctTables);
                candidatePctTables.stream().filter(MvccTable.class::isInstance).forEach(pctTables::add);
            } catch (AnalysisException e) {
                LOG.warn("Failed to resolve PCT tables for cloud MTMV {}", mtmv.getName(), e);
            }
        }
        if (eligibleMtmvs.isEmpty()) {
            return;
        }
        int snapshotLimit = connectContext.getSessionVariable()
                .getMaterializedViewRewriteCloudPreloadSnapshotNum();
        if (pctTables.size() > snapshotLimit) {
            LOG.warn("Skip cloud MTMV rewrite preload because {} distinct PCT snapshots exceed limit {}",
                    pctTables.size(), snapshotLimit);
            return;
        }
        for (Map.Entry<MTMV, Set<MTMVRelatedTableIf>> entry : eligibleMtmvs.entrySet()) {
            MTMV mtmv = entry.getKey();
            try {
                for (MTMVRelatedTableIf pctTable : entry.getValue()) {
                    statementContext.loadSnapshots(
                            pctTable, Optional.<TableSnapshot>empty(), Optional.<TableScanParams>empty());
                }
                MTMVRefreshContext refreshContext = MTMVRefreshContext.buildContext(mtmv);
                statementContext.putPreloadedMtmvRefreshContext(mtmv, refreshContext.compactCloudPreload());
            } catch (AnalysisException | DorisConnectorException e) {
                LOG.warn("Failed to preload cloud MTMV refresh context for {}", mtmv.getName(), e);
            }
        }
    }

    private boolean requiresMtmvRefreshContext(
            StatementContext statementContext, ConnectContext connectContext, MTMV mtmv) {
        long rewriteEpochMillis = statementContext.getMtmvRewriteEpochMillis();
        for (PlannerHook hook : statementContext.getPlannerHooks()) {
            if (hook instanceof InitMaterializationContextHook
                    && ((InitMaterializationContextHook) hook)
                            .requiresMtmvRefreshContext(mtmv, connectContext, rewriteEpochMillis)) {
                return true;
            }
        }
        return false;
    }

    private Optional<String> getSkipReason(StatementContext statementContext) {
        ConnectContext connectContext = statementContext.getConnectContext();
        if (connectContext == null || connectContext.getSessionVariable() == null
                || !connectContext.getSessionVariable().isEnablePreloadExternalMetadata()) {
            return Optional.of("session variable enable_preload_external_metadata is disabled");
        }
        if (statementContext.getExternalTablePreloadCandidateCount() == 0) {
            return Optional.of("no external preload candidates were collected");
        }
        if (!statementContext.hasAnyPlanReadLockTable()) {
            return Optional.of("no internal tables require plan-time read lock");
        }
        return Optional.empty();
    }

    private boolean preloadExternalTable(StatementContext statementContext, ExternalTablePreloadInfo preloadInfo) {
        ExternalTable table = preloadInfo.getTable();
        long preloadStartTime = TimeUtils.getStartTimeMs();
        boolean supportsLatestSnapshot = table.supportsLatestSnapshotPreload();
        boolean latestOnlyRelation = preloadInfo.shouldPreloadLatestSnapshot();
        boolean preloadLatestSnapshot = latestOnlyRelation && supportsLatestSnapshot;
        // Skip schema and partition warmup for snapshot-aware tables when only non-latest relations are referenced.
        boolean preloadSchema = !supportsLatestSnapshot || latestOnlyRelation;
        boolean preloadPartition = preloadSchema && table.supportInternalPartitionPruned();
        if (preloadLatestSnapshot) {
            statementContext.loadSnapshots(table, Optional.empty(), Optional.empty());
        }
        if (preloadSchema) {
            table.getBaseSchema();
        }
        if (preloadPartition) {
            table.initSelectedPartitions(statementContext.getSnapshot(table));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("{} preloaded external metadata for table {} "
                            + "[supportsLatestSnapshot={}, preloadLatestSnapshot={}, preloadSchema={}, "
                            + "preloadPartition={}, hasLatestRelation={}, hasNonLatestRelation={}, elapsedMs={}]",
                    getPreloadQueryIdentifier(statementContext), getExternalTableLogName(table), supportsLatestSnapshot,
                    preloadLatestSnapshot, preloadSchema, preloadPartition, preloadInfo.hasLatestOnlyRelation(),
                    preloadInfo.hasNonLatestRelation(), TimeUtils.getElapsedTimeMs(preloadStartTime));
        }
        return preloadLatestSnapshot || preloadSchema || preloadPartition;
    }

    private String getPreloadQueryIdentifier(StatementContext statementContext) {
        ConnectContext connectContext = statementContext.getConnectContext();
        return connectContext == null ? "stmt[unknown]" : connectContext.getQueryIdentifier();
    }

    private String getExternalTableLogName(ExternalTable table) {
        return table.getCatalog().getName() + "." + table.getDbName() + "." + table.getName();
    }
}
