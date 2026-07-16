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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.scan.ConnectorScanProfile;

import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.ImmutableScanMetricsResult;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FIX-SCAN-METRICS — guards {@link IcebergScanProfileReporter}, the connector-local iceberg SDK
 * {@code MetricsReporter} that captures a scan's {@code ScanReport} into a connector-neutral profile stashed
 * by queryId for fe-core to drain (the migration dropped this diagnostic from the profile).
 */
public class IcebergScanProfileReporterTest {

    private static ScanReport report(String table) {
        ScanMetricsResult metrics = ImmutableScanMetricsResult.builder()
                .resultDataFiles(CounterResult.of(MetricsContext.Unit.COUNT, 3))
                .scannedDataManifests(CounterResult.of(MetricsContext.Unit.COUNT, 5))
                .skippedDataManifests(CounterResult.of(MetricsContext.Unit.COUNT, 2))
                .totalFileSizeInBytes(CounterResult.of(MetricsContext.Unit.BYTES, 2048))
                .build();
        return ImmutableScanReport.builder()
                .tableName(table)
                .snapshotId(123L)
                .schemaId(0)
                .filter(Expressions.alwaysTrue())
                .projectedFieldIds(Collections.emptyList())
                .projectedFieldNames(Collections.emptyList())
                .scanMetrics(metrics)
                .metadata(Collections.emptyMap())
                .build();
    }

    @Test
    public void reportStashesProfileKeyedByQueryId() {
        // THE load-bearing RED assertion: a real iceberg ScanReport is captured into the stash as one
        // ConnectorScanProfile with the transcribed counter keys. A mutation that drops report() leaves the
        // stash empty.
        ConcurrentHashMap<String, List<ConnectorScanProfile>> stash = new ConcurrentHashMap<>();
        new IcebergScanProfileReporter("qid-1", stash).report(report("db.tbl"));

        List<ConnectorScanProfile> profiles = stash.get("qid-1");
        Assertions.assertNotNull(profiles, "report must stash under the queryId");
        Assertions.assertEquals(1, profiles.size());
        ConnectorScanProfile profile = profiles.get(0);
        Assertions.assertEquals("Iceberg Scan Metrics", profile.getGroupName());
        Assertions.assertEquals("Table Scan (db.tbl)", profile.getScanLabel());
        Map<String, String> m = profile.getMetrics();
        Assertions.assertEquals("3", m.get("data_files"));
        Assertions.assertEquals("5", m.get("scanned_manifests"));
        Assertions.assertEquals("2", m.get("skipped_manifests"));
        // BYTES-unit counter is byte-formatted (self-ported DebugUtil.printByteWithUnit).
        Assertions.assertEquals("2.000 KB", m.get("total_size"));
    }

    @Test
    public void blankQueryIdDoesNotStash() {
        ConcurrentHashMap<String, List<ConnectorScanProfile>> stash = new ConcurrentHashMap<>();
        new IcebergScanProfileReporter("", stash).report(report("db.tbl"));
        new IcebergScanProfileReporter(null, stash).report(report("db.tbl"));
        Assertions.assertTrue(stash.isEmpty(), "a blank queryId must not accumulate an unreclaimable entry");
    }

    @Test
    public void formattersMatchLegacy() {
        Assertions.assertEquals("0", IcebergScanProfileReporter.prettyMs(0));
        Assertions.assertEquals("1sec234ms", IcebergScanProfileReporter.prettyMs(1234));
        Assertions.assertEquals("0.000 ", IcebergScanProfileReporter.printByteWithUnit(0));
        Assertions.assertEquals("2.000 KB", IcebergScanProfileReporter.printByteWithUnit(2048));
        Assertions.assertEquals("1.500 MB", IcebergScanProfileReporter.printByteWithUnit(1024L * 1536));
    }

    @Test
    public void groupNameMatchesFeCoreConstant() {
        Assertions.assertEquals("Iceberg Scan Metrics", IcebergScanProfileReporter.GROUP_NAME);
    }
}
