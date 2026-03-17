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

import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Minimal orchestration entry point for incremental refresh.
 */
public class IVMManager {
    private final IVMCapabilityChecker capabilityChecker;
    private final IVMPlanAnalyzer planAnalyzer;
    private final IVMDeltaPlannerDispatcher deltaPlannerDispatcher;
    private final IVMDeltaExecutor deltaExecutor;

    public IVMManager(IVMCapabilityChecker capabilityChecker, IVMPlanAnalyzer planAnalyzer,
            IVMDeltaPlannerDispatcher deltaPlannerDispatcher, IVMDeltaExecutor deltaExecutor) {
        this.capabilityChecker = Objects.requireNonNull(capabilityChecker, "capabilityChecker can not be null");
        this.planAnalyzer = Objects.requireNonNull(planAnalyzer, "planAnalyzer can not be null");
        this.deltaPlannerDispatcher = Objects.requireNonNull(deltaPlannerDispatcher,
                "deltaPlannerDispatcher can not be null");
        this.deltaExecutor = Objects.requireNonNull(deltaExecutor, "deltaExecutor can not be null");
    }

    @VisibleForTesting
    Optional<FallbackReason> doRefresh(IVMRefreshContext context) {
        Objects.requireNonNull(context, "context can not be null");

        IVMPlanAnalysis analysis = planAnalyzer.analyze(context);
        Objects.requireNonNull(analysis, "analysis can not be null");
        if (analysis.getPattern() == IVMPlanPattern.UNSUPPORTED) {
            return Optional.of(FallbackReason.PLAN_PATTERN_UNSUPPORTED);
        }

        IVMCapabilityResult capabilityResult = capabilityChecker.check(context, analysis);
        Objects.requireNonNull(capabilityResult, "capabilityResult can not be null");
        if (!capabilityResult.isIncremental()) {
            return Optional.of(capabilityResult.getFallbackReason());
        }

        try {
            List<DeltaPlanBundle> bundles = deltaPlannerDispatcher.plan(context, analysis);
            deltaExecutor.execute(context, bundles);
            return Optional.empty();
        } catch (Exception e) {
            return Optional.of(FallbackReason.INCREMENTAL_EXECUTION_FAILED);
        }
    }

    public Optional<FallbackReason> ivmRefresh(IVMRefreshContext context) {
        return doRefresh(context);
    }
}
