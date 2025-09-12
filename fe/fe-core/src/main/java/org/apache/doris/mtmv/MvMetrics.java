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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.metric.LongCounterMetric;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.hint.UseMvHint;
import org.apache.doris.nereids.rules.exploration.mv.AsyncMaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.SyncMaterializationContext;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

public class MvMetrics {
    private static final Logger LOG = LogManager.getLogger(AsyncMvMetrics.class);
    // rewrite success
    private LongCounterMetric rewriteSuccessWithHint = new LongCounterMetric("mv", MetricUnit.ROWS, "");
    // rewrite failure
    private LongCounterMetric rewriteFailureShapeMismatch = new LongCounterMetric("mv", MetricUnit.ROWS, "");
    private LongCounterMetric rewriteFailureCboRejected = new LongCounterMetric("mv", MetricUnit.ROWS, "");
    private LongCounterMetric rewriteFailureWithHint = new LongCounterMetric("mv", MetricUnit.ROWS, "");

    /**
     * recordRewriteMetrics
     */
    public static void recordRewriteMetrics(ConnectContext ctx) {
        if (ctx == null) {
            return;
        }
        StmtExecutor executor = ctx.getExecutor();
        if (executor == null) {
            return;
        }
        Planner planner = executor.planner();
        if (planner == null) {
            return;
        }
        if (!(planner instanceof NereidsPlanner)) {
            return;
        }
        NereidsPlanner nereidsPlanner = (NereidsPlanner) planner;
        CascadesContext cascadesContext = nereidsPlanner.getCascadesContext();
        if (cascadesContext == null) {
            return;
        }
        List<MaterializationContext> materializationContexts = cascadesContext.getMaterializationContexts();
        if (materializationContexts == null) {
            return;
        }
        PhysicalPlan physicalPlan = nereidsPlanner.getPhysicalPlan();
        if (physicalPlan == null) {
            return;
        }
        StatementContext statementContext = nereidsPlanner.getStatementContext();
        long currentTimeMillis = System.currentTimeMillis();
        // get cbo chosen mv
        Set<MaterializationContext> chosenMvCtx = MaterializationContext.getChosenMvsContext(materializationContexts,
                physicalPlan);
        Set<List<String>> useHint = getUseHint();
        for (MaterializationContext materializationContext : chosenMvCtx) {
            Optional<MvMetrics> mvMetricsOptional = materializationContext.getMaterializationMetrics();
            if (!mvMetricsOptional.isPresent()) {
                continue;
            }
            MvMetrics mvMetrics = mvMetricsOptional.get();
            if (materializationContext instanceof AsyncMaterializationContext) {
                AsyncMvMetrics asyncMvMetrics = (AsyncMvMetrics) mvMetrics;
                AsyncMaterializationContext asyncMaterializationContext
                        = (AsyncMaterializationContext) materializationContext;
                // partition rewrite
                if (asyncMaterializationContext.isPartitionNeedUnion()) {
                    asyncMvMetrics.getRewritePartialSuccess().increase(1L);
                } else {
                    // full rewrite
                    asyncMvMetrics.getRewriteFullSuccess().increase(1L);
                }
                asyncMvMetrics.setLastRewriteTime(currentTimeMillis);
            } else if (materializationContext instanceof SyncMaterializationContext) {
                // do not care if partition union
                SyncMvMetrics syncMvMetrics = (SyncMvMetrics) mvMetrics;
                syncMvMetrics.getRewriteSuccess().increase(1L);
            }
            // if rewrite by hint
            if (useHint.contains(materializationContext.generateMaterializationIdentifier())) {
                mvMetrics.getRewriteSuccessWithHint().increase(1L);
            }
        }

        // if partitions is empty, stale data
        Map<BaseTableInfo, Collection<Partition>> mvCanRewritePartitionsMap
                = statementContext.getMvCanRewritePartitionsMap();
        for (Entry<BaseTableInfo, Collection<Partition>> entry : mvCanRewritePartitionsMap.entrySet()) {
            if (CollectionUtils.isEmpty(entry.getValue())) {
                rewriteFailureStaleData(entry.getKey());
            }
        }

        for (MaterializationContext materializationContext : materializationContexts) {
            // if shape success, and chosenMvCtx not contains, cbo reject
            if (materializationContext.isSuccess()) {
                if (!chosenMvCtx.contains(materializationContext)) {
                    materializationContext.getMaterializationMetrics()
                            .ifPresent(mvMetrics -> mvMetrics.getRewriteFailureCboRejected().increase(1L));
                }
            } else {
                // re shape mismatch
                materializationContext.getMaterializationMetrics()
                        .ifPresent(mvMetrics -> mvMetrics.getRewriteFailureShapeMismatch().increase(1L));
            }
        }
    }

    private static void rewriteFailureStaleData(BaseTableInfo baseTableInfo) {
        try {
            TableIf tableIf = MTMVUtil.getTable(baseTableInfo);
            if (tableIf instanceof MTMV) {
                MTMV mtmv = (MTMV) tableIf;
                mtmv.getAsyncMvMetrics().getRewriteFailureStaleData().increase(1L);
            }
        } catch (AnalysisException e) {
            LOG.warn("get MTMV failed by: {}", baseTableInfo, e);
        }
    }

    private static Set<List<String>> getUseHint() {
        Set<List<String>> res = Sets.newHashSet();
        Optional<UseMvHint> useMv = ConnectContext.get().getStatementContext().getUseMvHint("USE_MV");
        if (useMv.isPresent()) {
            useMv.get().getUseMvTableColumnMap().keySet();
        }
        return res;
    }
    // todo not rewrite by hint
    private Set<List<String>> getNoUseHint() {
        Set<List<String>> res = Sets.newHashSet();
        Optional<UseMvHint> noUseMv = ConnectContext.get().getStatementContext().getUseMvHint("NO_USE_MV");
        if (noUseMv.isPresent()) {
            return noUseMv.get().getNoUseMvTableColumnMap().keySet();
        }
        return res;
    }

    public LongCounterMetric getRewriteFailureCboRejected() {
        return rewriteFailureCboRejected;
    }

    public LongCounterMetric getRewriteFailureShapeMismatch() {
        return rewriteFailureShapeMismatch;
    }

    public LongCounterMetric getRewriteFailureWithHint() {
        return rewriteFailureWithHint;
    }


    public LongCounterMetric getRewriteSuccessWithHint() {
        return rewriteSuccessWithHint;
    }
}
