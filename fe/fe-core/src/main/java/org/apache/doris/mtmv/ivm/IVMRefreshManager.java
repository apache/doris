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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.Objects;

/**
 * Minimal orchestration entry point for incremental refresh.
 */
public class IVMRefreshManager {
    private final IVMCapabilityChecker capabilityChecker;
    private final IVMPlanAnalyzer planAnalyzer;
    private final IVMDeltaPlannerDispatcher deltaPlannerDispatcher;
    private final IVMDeltaExecutor deltaExecutor;

    public IVMRefreshManager(IVMCapabilityChecker capabilityChecker, IVMPlanAnalyzer planAnalyzer,
            IVMDeltaPlannerDispatcher deltaPlannerDispatcher, IVMDeltaExecutor deltaExecutor) {
        this.capabilityChecker = Objects.requireNonNull(capabilityChecker, "capabilityChecker can not be null");
        this.planAnalyzer = Objects.requireNonNull(planAnalyzer, "planAnalyzer can not be null");
        this.deltaPlannerDispatcher = Objects.requireNonNull(deltaPlannerDispatcher,
                "deltaPlannerDispatcher can not be null");
        this.deltaExecutor = Objects.requireNonNull(deltaExecutor, "deltaExecutor can not be null");
    }

    @VisibleForTesting
    IVMRefreshResult doRefresh(MTMV mtmv) {
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        final IVMRefreshContext context;
        try {
            context = buildRefreshContext(mtmv);
        } catch (Exception e) {
            return IVMRefreshResult.fallback(FallbackReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED, e.getMessage());
        }
        return doRefreshInternal(context);
    }

    @VisibleForTesting
    IVMRefreshContext buildRefreshContext(MTMV mtmv) throws Exception {
        ConnectContext connectContext = MTMVPlanUtil.createMTMVContext(mtmv,
                MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
        MTMVRefreshContext mtmvRefreshContext = MTMVRefreshContext.buildContext(mtmv);
        return new IVMRefreshContext(mtmv, connectContext, mtmvRefreshContext);
    }

    private IVMRefreshResult doRefreshInternal(IVMRefreshContext context) {
        Objects.requireNonNull(context, "context can not be null");

        IVMPlanAnalysis analysis = planAnalyzer.analyze(context);
        Objects.requireNonNull(analysis, "analysis can not be null");
        if (analysis.isInvalid()) {
            return IVMRefreshResult.fallback(FallbackReason.PLAN_PATTERN_UNSUPPORTED, analysis.getUnsupportedReason());
        }

        IVMCapabilityResult capabilityResult = capabilityChecker.check(context, analysis);
        Objects.requireNonNull(capabilityResult, "capabilityResult can not be null");
        if (!capabilityResult.isIncremental()) {
            return IVMRefreshResult.fallback(capabilityResult.getFallbackReason(),
                    capabilityResult.getDetailMessage());
        }

        try {
            List<DeltaPlanBundle> bundles = deltaPlannerDispatcher.plan(context, analysis);
            deltaExecutor.execute(context, bundles);
            return IVMRefreshResult.success();
        } catch (Exception e) {
            return IVMRefreshResult.fallback(FallbackReason.INCREMENTAL_EXECUTION_FAILED, e.getMessage());
        }
    }

    public IVMRefreshResult ivmRefresh(MTMV mtmv) {
        return doRefresh(mtmv);
    }
}
