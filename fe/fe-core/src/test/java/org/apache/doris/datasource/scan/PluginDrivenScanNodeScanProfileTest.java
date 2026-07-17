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

package org.apache.doris.datasource.scan;

import org.apache.doris.common.profile.RuntimeProfile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.connector.api.scan.ConnectorScanProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * FIX-SCAN-METRICS — guards {@link PluginDrivenScanNode#writeScanProfilesInto}, the connector-agnostic
 * transcription of connector-supplied scan diagnostics into the query profile execution summary. WHY it
 * matters: the plugin migration dropped the paimon/iceberg SDK scan metrics (manifest cache hit/miss, scan
 * durations) from the profile; this generic writer restores them without the engine knowing any connector
 * specifics — it only get-or-creates a group and transcribes the connector's labels + metric strings.
 */
public class PluginDrivenScanNodeScanProfileTest {

    private static ConnectorScanProfile profile(String group, String label, String... kv) {
        Map<String, String> metrics = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            metrics.put(kv[i], kv[i + 1]);
        }
        return new ConnectorScanProfile(group, label, metrics);
    }

    @Test
    public void nullOrEmptyIsNoOp() {
        RuntimeProfile summary = new RuntimeProfile("Execution Summary");
        PluginDrivenScanNode.writeScanProfilesInto(summary, null);
        PluginDrivenScanNode.writeScanProfilesInto(summary, Collections.emptyList());
        Assertions.assertTrue(summary.getChildMap().isEmpty(), "no profiles -> no group written");
        // null summary must not throw.
        PluginDrivenScanNode.writeScanProfilesInto(null, Collections.singletonList(profile("G", "L")));
    }

    @Test
    public void writesGroupChildAndMetrics() {
        // THE load-bearing RED assertion: one profile becomes a group -> "Table Scan (...)" child -> info
        // strings. A mutation that skips writing leaves the summary childless.
        RuntimeProfile summary = new RuntimeProfile("Execution Summary");
        PluginDrivenScanNode.writeScanProfilesInto(summary, Collections.singletonList(
                profile("Paimon Scan Metrics", "Table Scan (db.t)",
                        "manifest_hit_cache", "4", "manifest_missed_cache", "1")));

        RuntimeProfile group = summary.getChildMap().get("Paimon Scan Metrics");
        Assertions.assertNotNull(group, "group must be created");
        RuntimeProfile scan = group.getChildMap().get("Table Scan (db.t)");
        Assertions.assertNotNull(scan, "per-scan child must be created");
        Assertions.assertEquals("4", scan.getInfoString("manifest_hit_cache"));
        Assertions.assertEquals("1", scan.getInfoString("manifest_missed_cache"));
    }

    @Test
    public void sharesGroupAcrossScans() {
        // Two scans of the same connector go under ONE get-or-created group as two children (a join over two
        // paimon tables must not create two "Paimon Scan Metrics" groups).
        RuntimeProfile summary = new RuntimeProfile("Execution Summary");
        PluginDrivenScanNode.writeScanProfilesInto(summary, Arrays.asList(
                profile("Iceberg Scan Metrics", "Table Scan (db.a)", "data_files", "3"),
                profile("Iceberg Scan Metrics", "Table Scan (db.b)", "data_files", "5")));

        RuntimeProfile group = summary.getChildMap().get("Iceberg Scan Metrics");
        Assertions.assertNotNull(group);
        Assertions.assertEquals(2, group.getChildMap().size(), "one group, two scan children");
        Assertions.assertEquals("3", group.getChildMap().get("Table Scan (db.a)").getInfoString("data_files"));
        Assertions.assertEquals("5", group.getChildMap().get("Table Scan (db.b)").getInfoString("data_files"));
    }

    @Test
    public void groupNameConstantsMatchConnectorLiterals() {
        // The connector-supplied group name is stringly-typed coupled to these fe-core constants (the connector
        // cannot import SummaryProfile). This is the fe-core half of the mirror check; the connector tests
        // assert their own literals equal the same strings.
        Assertions.assertEquals("Paimon Scan Metrics", SummaryProfile.PAIMON_SCAN_METRICS);
        Assertions.assertEquals("Iceberg Scan Metrics", SummaryProfile.ICEBERG_SCAN_METRICS);
    }
}
