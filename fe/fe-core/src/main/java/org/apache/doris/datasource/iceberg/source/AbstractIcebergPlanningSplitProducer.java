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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.PlanningSplitMetadata;
import org.apache.doris.datasource.PlanningSplitProducer;
import org.apache.doris.datasource.SplitSink;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

abstract class AbstractIcebergPlanningSplitProducer implements PlanningSplitProducer {
    private final IcebergSplitPlanningSupport planningSupport;
    private final IcebergPlanningMetadata planningMetadata;
    private final Executor executor;

    protected AbstractIcebergPlanningSplitProducer(
            IcebergSplitPlanningSupport planningSupport,
            Executor executor) {
        this.planningSupport = planningSupport;
        this.planningMetadata = new IcebergPlanningMetadata(planningSupport);
        this.executor = executor;
    }

    @Override
    public final boolean isBatchMode() {
        return planningSupport.isBatchMode();
    }

    @Override
    public final int numApproximateSplits() {
        return planningSupport.numApproximateSplits();
    }

    @Override
    public final PlanningSplitMetadata getPlanningMetadata() {
        return planningMetadata;
    }

    @Override
    public final void start(int numBackends, SplitSink splitSink) throws UserException {
        if (isBatchMode()) {
            CompletableFuture.runAsync(() -> runProduction(numBackends, splitSink), executor);
            return;
        }
        runProduction(numBackends, splitSink);
    }

    protected final IcebergSplitPlanningSupport getPlanningSupport() {
        return planningSupport;
    }

    private void runProduction(int numBackends, SplitSink splitSink) {
        try {
            doProduce(numBackends, splitSink);
            splitSink.finish();
            planningSupport.recordManifestCacheProfile();
        } catch (Exception e) {
            splitSink.fail(planningSupport.translatePlanningException(unwrapException(e)));
        }
    }

    protected abstract void doProduce(int numBackends, SplitSink splitSink) throws Exception;

    private Exception unwrapException(Exception e) {
        if (e instanceof RuntimeException && e.getCause() instanceof Exception) {
            return (Exception) e.getCause();
        }
        return e;
    }
}
