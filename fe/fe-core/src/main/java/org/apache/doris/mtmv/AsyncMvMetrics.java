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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskRefreshMode;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.job.extensions.mtmv.MTMVTaskContext;
import org.apache.doris.metric.LongCounterMetric;
import org.apache.doris.metric.Metric.MetricUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.List;
import java.util.Map;

public class AsyncMvMetrics extends MvMetrics{
    // rewrite
    // rewrite success
    private LongCounterMetric rewriteFullSuccess = new LongCounterMetric("mv", MetricUnit.ROWS, "");
    private LongCounterMetric rewritePartialSuccess = new LongCounterMetric("mv", MetricUnit.ROWS, "");
    private LongCounterMetric rewriteFailureStaleData = new LongCounterMetric("mv", MetricUnit.ROWS, "");

    // refresh
    // refresh manual
    private LongCounterMetric manualRefreshesOnAuto = new LongCounterMetric("mv", MetricUnit.ROWS, "");
    private LongCounterMetric manualRefreshesOnPartitions = new LongCounterMetric("mv", MetricUnit.ROWS, "");
    private LongCounterMetric manualRefreshesOnComplete = new LongCounterMetric("mv", MetricUnit.ROWS, "");
    // refresh auto
    private LongCounterMetric autoRefreshesOnSchedule = new LongCounterMetric("mv", MetricUnit.ROWS, "");
    private LongCounterMetric autoRefreshesOnCommit = new LongCounterMetric("mv", MetricUnit.ROWS, "");
    // refresh scope
    private LongCounterMetric refreshesSkipped = new LongCounterMetric("mv", MetricUnit.ROWS, "");
    private LongCounterMetric refreshesFast = new LongCounterMetric("mv", MetricUnit.ROWS, "");
    private LongCounterMetric refreshesPct = new LongCounterMetric("mv", MetricUnit.ROWS, "");
    private LongCounterMetric refreshesComplete = new LongCounterMetric("mv", MetricUnit.ROWS, "");
    // use time
    private long lastRewriteTime;
    private long lastQueryTime;

    public static void recordRefreshMetrics(MTMV mtmv, MTMVTaskRefreshMode refreshMode, MTMVTaskContext taskContext) {
        AsyncMvMetrics asyncMvMetrics = mtmv.getAsyncMvMetrics();
        // refresh manual
        if (taskContext.getTriggerMode().equals(MTMVTaskTriggerMode.MANUAL)){
            if (taskContext.isComplete()) {
                asyncMvMetrics.getManualRefreshesOnComplete().increase(1L);
            } else if (CollectionUtils.isEmpty(taskContext.getPartitions())) {
                asyncMvMetrics.getManualRefreshesOnAuto().increase(1L);
            } else {
                asyncMvMetrics.getManualRefreshesOnPartitions().increase(1L);
            }
        }
        // refresh auto
        if (taskContext.getTriggerMode().equals(MTMVTaskTriggerMode.COMMIT)) {
            asyncMvMetrics.getAutoRefreshesOnCommit().increase(1L);
        }
        if (taskContext.getTriggerMode().equals(MTMVTaskTriggerMode.SYSTEM)) {
            asyncMvMetrics.getAutoRefreshesOnSchedule().increase(1L);
        }
        // refresh scope
        if (refreshMode == MTMVTaskRefreshMode.NOT_REFRESH) {
            asyncMvMetrics.getRefreshesSkipped().increase(1L);
        } else if (refreshMode == MTMVTaskRefreshMode.COMPLETE) {
            asyncMvMetrics.getRefreshesComplete().increase(1L);
        } else {
            asyncMvMetrics.getRefreshesPct().increase(1L);
        }
    }

    public static void recordLastQueryTime(Map<List<String>, TableIf> tables) {
        if (MapUtils.isEmpty(tables)) {
            return;
        }
        long currentTimeMillis = System.currentTimeMillis();
        for (TableIf tableIf : tables.values()) {
            if (tableIf instanceof MTMV) {
                MTMV mtmv = (MTMV) tableIf;
                mtmv.getAsyncMvMetrics().setLastQueryTime(currentTimeMillis);
            }
        }
    }

    public LongCounterMetric getAutoRefreshesOnCommit() {
        return autoRefreshesOnCommit;
    }

    public LongCounterMetric getAutoRefreshesOnSchedule() {
        return autoRefreshesOnSchedule;
    }

    public long getLastQueryTime() {
        return lastQueryTime;
    }

    public long getLastRewriteTime() {
        return lastRewriteTime;
    }

    public LongCounterMetric getManualRefreshesOnAuto() {
        return manualRefreshesOnAuto;
    }

    public LongCounterMetric getManualRefreshesOnComplete() {
        return manualRefreshesOnComplete;
    }

    public LongCounterMetric getManualRefreshesOnPartitions() {
        return manualRefreshesOnPartitions;
    }

    public LongCounterMetric getRefreshesComplete() {
        return refreshesComplete;
    }

    public LongCounterMetric getRefreshesFast() {
        return refreshesFast;
    }

    public LongCounterMetric getRefreshesPct() {
        return refreshesPct;
    }

    public LongCounterMetric getRefreshesSkipped() {
        return refreshesSkipped;
    }

    public LongCounterMetric getRewriteFullSuccess() {
        return rewriteFullSuccess;
    }

    public LongCounterMetric getRewritePartialSuccess() {
        return rewritePartialSuccess;
    }

    public void setLastQueryTime(long lastQueryTime) {
        this.lastQueryTime = lastQueryTime;
    }

    public void setLastRewriteTime(long lastRewriteTime) {
        this.lastRewriteTime = lastRewriteTime;
    }

    public LongCounterMetric getRewriteFailureStaleData() {
        return rewriteFailureStaleData;
    }
}
