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
import org.apache.doris.mtmv.ivm.IvmRefreshSummary;

import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class IvmRefreshSnapshotMetricsTest {
    @Test
    void rendersPerViewSamplesWithTypesAndEscapedLabels() {
        String mvName = "a\"quoted";
        ChangelogMonitorSnapshot snapshot = new ChangelogMonitorSnapshot(1, List.of(
                new ChangelogMonitorSnapshot.IvmRefresh("sales", mvName, 1,
                        new IvmRefreshSummary.MetricValues(1, 2, 3, true, 125)),
                new ChangelogMonitorSnapshot.IvmRefresh("sales", "fresh", 2,
                        new IvmRefreshSummary.MetricValues(0, 0, 0, false, 0)),
                new ChangelogMonitorSnapshot.IvmRefresh("sales", "zero", 3,
                        new IvmRefreshSummary.MetricValues(0, 0, 0, true, 0))));

        MetricVisitor prometheus = new PrometheusMetricVisitor();
        IvmRefreshSnapshotMetrics.visit(prometheus, snapshot);
        String result = prometheus.finish();
        Assertions.assertTrue(result.contains("# TYPE doris_fe_ivm_refresh_failed_tasks_total counter"));
        Assertions.assertTrue(result.contains("# TYPE doris_fe_ivm_refresh_fallbacks_total counter"));
        Assertions.assertTrue(result.contains("# TYPE doris_fe_ivm_consecutive_failures gauge"));
        Assertions.assertTrue(result.contains("# TYPE doris_fe_ivm_refresh_duration_seconds gauge"));
        Assertions.assertTrue(result.contains("doris_fe_ivm_refresh_duration_seconds"
                + "{db_name=\"sales\", mv_name=\"a\\\"quoted\", mv_id=\"1\"} 0.125"));
        Assertions.assertFalse(result.contains("doris_fe_ivm_refresh_duration_seconds"
                + "{db_name=\"sales\", mv_name=\"fresh\", mv_id=\"2\"}"));
        Assertions.assertTrue(result.contains("doris_fe_ivm_refresh_duration_seconds"
                + "{db_name=\"sales\", mv_name=\"zero\", mv_id=\"3\"} 0.0"));

        MetricVisitor json = new JsonMetricVisitor();
        IvmRefreshSnapshotMetrics.visit(json, snapshot);
        var samples = JsonParser.parseString(json.finish()).getAsJsonArray();
        Assertions.assertEquals(11, samples.size());
        Assertions.assertEquals(mvName,
                samples.get(0).getAsJsonObject().getAsJsonObject("tags").get("mv_name").getAsString());
    }
}
