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

package org.apache.doris.metric;

import org.apache.doris.changelog.ChangelogMonitorSnapshot;
import org.apache.doris.metric.Metric.MetricType;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.mtmv.ivm.IvmRefreshSummary;

import java.util.List;

final class IvmRefreshSnapshotMetrics {
    private IvmRefreshSnapshotMetrics() {
    }

    static void visit(MetricVisitor visitor, ChangelogMonitorSnapshot snapshot) {
        for (ChangelogMonitorSnapshot.IvmRefresh refresh : snapshot.ivmRefreshes()) {
            IvmRefreshSummary.MetricValues values = refresh.values();
            List<MetricLabel> labels = List.of(new MetricLabel("db_name", refresh.dbName()),
                    new MetricLabel("mv_name", refresh.mvName()),
                    new MetricLabel("mv_id", Long.toString(refresh.mvId())));
            visit(visitor, labels, "ivm_consecutive_failures", MetricType.GAUGE, MetricUnit.NOUNIT,
                    "Consecutive failed refresh tasks of this IVM materialized view", values.consecutiveFailures());
            visit(visitor, labels, "ivm_refresh_failed_tasks_total", MetricType.COUNTER, MetricUnit.NOUNIT,
                    "Total failed refresh tasks of this IVM materialized view", values.failedTaskCount());
            visit(visitor, labels, "ivm_refresh_fallbacks_total", MetricType.COUNTER, MetricUnit.NOUNIT,
                    "Total automatic refresh fallbacks of this IVM materialized view", values.fallbackCount());
            if (values.hasCompletedRefresh()) {
                visit(visitor, labels, "ivm_refresh_duration_seconds", MetricType.GAUGE, MetricUnit.SECONDS,
                        "Duration of the last successful or failed refresh of this IVM materialized view",
                        values.lastRefreshDurationMs() / 1000.0);
            }
        }
    }

    private static void visit(MetricVisitor visitor, List<MetricLabel> labels, String name, MetricType type,
            MetricUnit unit, String description, Number value) {
        Metric<Number> metric = new Metric<>(name, type, unit, description) {
            @Override
            public Number getValue() {
                return value;
            }
        };
        visitor.visit(MetricVisitor.FE_PREFIX, metric.setLabels(labels));
    }
}
