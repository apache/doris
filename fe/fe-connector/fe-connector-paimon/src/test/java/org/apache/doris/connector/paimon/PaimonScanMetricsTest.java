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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.scan.ConnectorScanProfile;

import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.operation.metrics.ScanStats;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

/**
 * FIX-SCAN-METRICS — guards {@link PaimonScanMetrics}, which harvests the paimon SDK {@code ScanMetrics}
 * recorded by {@link PaimonMetricRegistry} during {@code scan.plan()} into a connector-neutral profile (the
 * migration dropped this diagnostic; restoring it needs no fe-core import — the metric API is paimon's).
 */
public class PaimonScanMetricsTest {

    @Test
    public void harvestRendersRecordedScanMetrics() {
        // THE load-bearing RED assertion: drive the real paimon SDK to record a scan into the registry, then
        // harvest it. reportScan(duration, scannedManifests, skippedTableFiles, resultedTableFiles) populates
        // the LAST_* gauges. A mutation that drops the harvest returns empty.
        PaimonMetricRegistry registry = new PaimonMetricRegistry();
        ScanMetrics metrics = new ScanMetrics(registry, "mydb.mytbl");
        metrics.reportScan(new ScanStats(2_000_000L, 5L, 3L, 7L));

        Optional<ConnectorScanProfile> profile =
                PaimonScanMetrics.harvest(registry, "mydb.mytbl", "Table Scan (mydb.mytbl)");
        Assertions.assertTrue(profile.isPresent(), "recorded scan metrics must harvest");
        Assertions.assertEquals("Paimon Scan Metrics", profile.get().getGroupName());
        Assertions.assertEquals("Table Scan (mydb.mytbl)", profile.get().getScanLabel());
        Map<String, String> m = profile.get().getMetrics();
        Assertions.assertEquals("5", m.get("last_scanned_manifests"));
        Assertions.assertEquals("3", m.get("last_scan_skipped_table_files"));
        Assertions.assertEquals("7", m.get("last_scan_resulted_table_files"));
    }

    @Test
    public void harvestEmptyWhenNoMetricsRecorded() {
        // A registry the SDK never recorded a scan group into -> nothing to harvest (unpartitioned/no-op scan,
        // unsupported scan type). Same as the un-overridden default; documents the fall-through.
        Assertions.assertEquals(Optional.empty(),
                PaimonScanMetrics.harvest(new PaimonMetricRegistry(), "mydb.mytbl", "Table Scan (mydb.mytbl)"));
        Assertions.assertEquals(Optional.empty(),
                PaimonScanMetrics.harvest(null, "mydb.mytbl", "x"));
    }

    @Test
    public void prettyMsMatchesLegacyFormatter() {
        // Self-ported fe-core DebugUtil.getPrettyStringMs (the connector cannot import fe-core).
        Assertions.assertEquals("0", PaimonScanMetrics.prettyMs(0));
        Assertions.assertEquals("500ms", PaimonScanMetrics.prettyMs(500));
        Assertions.assertEquals("1sec234ms", PaimonScanMetrics.prettyMs(1234));
        Assertions.assertEquals("1min", PaimonScanMetrics.prettyMs(60_000));
    }

    @Test
    public void groupNameMatchesFeCoreConstant() {
        // Mirror of the fe-core SummaryProfile.PAIMON_SCAN_METRICS constant (stringly-typed coupling; the two
        // modules cannot cross-import, so each asserts the shared literal).
        Assertions.assertEquals("Paimon Scan Metrics", PaimonScanMetrics.GROUP_NAME);
    }
}
