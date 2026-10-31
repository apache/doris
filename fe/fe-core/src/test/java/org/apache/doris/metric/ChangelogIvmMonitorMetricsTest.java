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

import org.apache.doris.catalog.Env;
import org.apache.doris.changelog.ChangelogIvmMonitor;
import org.apache.doris.changelog.ChangelogIvmMonitorSnapshot;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.ha.FrontendNodeType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ChangelogIvmMonitorMetricsTest {
    private static final String FAILURES = "# HELP doris_fe_changelog_ivm_monitor_sample_failures_total number of "
            + "the rounds of the changelog and IVM monitor that failed\n"
            + "# TYPE doris_fe_changelog_ivm_monitor_sample_failures_total counter\n"
            + "doris_fe_changelog_ivm_monitor_sample_failures_total 0\n";

    private final Env env = Env.getCurrentEnv();
    private final ChangelogIvmMonitor monitor = env.getChangelogIvmMonitor();
    private FrontendNodeType originFeType;
    private boolean originEnableChangelog;
    private boolean originEnableMonitor;
    private String originCloudUniqueId;

    @BeforeAll
    public static void setUpMetricRepo() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @BeforeEach
    public void setUp() {
        originFeType = Deencapsulation.getField(env, "feType");
        originEnableChangelog = Config.enable_feature_changelog;
        originEnableMonitor = Config.enable_changelog_ivm_monitor;
        originCloudUniqueId = Config.cloud_unique_id;
        Deencapsulation.setField(env, "feType", FrontendNodeType.MASTER);
        Config.enable_feature_changelog = true;
        Config.enable_changelog_ivm_monitor = true;
    }

    @AfterEach
    public void tearDown() {
        Deencapsulation.setField(monitor, "snapshot", null);
        Deencapsulation.setField(env, "feType", originFeType);
        Config.enable_feature_changelog = originEnableChangelog;
        Config.enable_changelog_ivm_monitor = originEnableMonitor;
        Config.cloud_unique_id = originCloudUniqueId;
    }

    @Test
    public void testSeriesAreExportedByTheMasterOnly() {
        // Before the first round completes, the sampled series are missing rather than 0.
        Assertions.assertEquals(FAILURES, render());

        Deencapsulation.setField(monitor, "snapshot", new ChangelogIvmMonitorSnapshot(1787039821500L, 12L, 100L, 300L));
        String sampled = FAILURES
                + "# HELP doris_fe_changelog_ivm_monitor_last_sample_timestamp_seconds the time the latest round of "
                + "the changelog and IVM monitor completed, in seconds since the epoch\n"
                + "# TYPE doris_fe_changelog_ivm_monitor_last_sample_timestamp_seconds gauge\n"
                + "doris_fe_changelog_ivm_monitor_last_sample_timestamp_seconds 1787039821\n"
                + "# HELP doris_fe_changelog_ivm_monitor_sample_duration_seconds how long the latest round of the "
                + "changelog and IVM monitor took, in seconds\n"
                + "# TYPE doris_fe_changelog_ivm_monitor_sample_duration_seconds gauge\n"
                + "doris_fe_changelog_ivm_monitor_sample_duration_seconds 0.012\n"
                + "# HELP doris_fe_changelog_retained_bytes size of the changelog kept by the tables, of one "
                + "replica of each tablet or of all the replicas\n"
                + "# TYPE doris_fe_changelog_retained_bytes gauge\n"
                + "doris_fe_changelog_retained_bytes{scope=\"single_replica\"} 100\n"
                + "doris_fe_changelog_retained_bytes{scope=\"all_replicas\"} 300\n";
        Assertions.assertEquals(sampled, render());
        Assertions.assertTrue(MetricRepo.getMetric(new PrometheusMetricVisitor()).contains(sampled));

        // A Follower does not sample, and a Master once demoted must not export what it sampled before.
        Deencapsulation.setField(env, "feType", FrontendNodeType.FOLLOWER);
        Assertions.assertEquals("", render());
        Deencapsulation.setField(env, "feType", FrontendNodeType.MASTER);
        Config.enable_changelog_ivm_monitor = false;
        Assertions.assertEquals("", render());
        Config.enable_changelog_ivm_monitor = true;
        Config.enable_feature_changelog = false;
        Assertions.assertEquals("", render());
    }

    @Test
    public void testCloudModeExportsTheSizeOfOneReplica() {
        Config.cloud_unique_id = "test_cloud_unique_id";
        Deencapsulation.setField(monitor, "snapshot", new ChangelogIvmMonitorSnapshot(1787039821500L, 12L, 100L, 100L));
        String result = render();
        Assertions.assertTrue(result.contains("\ndoris_fe_changelog_retained_bytes{scope=\"single_replica\"} 100\n"),
                result);
        Assertions.assertFalse(result.contains("all_replicas"), result);
    }

    // The JSON and core outputs of /metrics render none of these series.
    @Test
    public void testOtherOutputsRenderNothing() {
        Deencapsulation.setField(monitor, "snapshot", new ChangelogIvmMonitorSnapshot(1787039821500L, 12L, 100L, 300L));
        MetricVisitor visitor = new SimpleCoreMetricVisitor();
        visitor.visitChangelogIvmMonitor();
        Assertions.assertEquals("", visitor.finish());
        String json = MetricRepo.getMetric(new JsonMetricVisitor());
        Assertions.assertFalse(json.contains("changelog_ivm_monitor"), json);
        Assertions.assertFalse(json.contains("changelog_retained_bytes"), json);
    }

    private static String render() {
        MetricVisitor visitor = new PrometheusMetricVisitor();
        visitor.visitChangelogIvmMonitor();
        return visitor.finish();
    }
}
