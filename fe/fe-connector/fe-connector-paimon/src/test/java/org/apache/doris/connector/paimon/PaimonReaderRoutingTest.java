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

import org.apache.doris.connector.spi.scan.ConnectorScanProfile;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class PaimonReaderRoutingTest {
    @Test
    void mixedRangesHaveDistinctCountsAndExplainTypes() {
        PaimonTableHandle table = new PaimonTableHandle("db", "t", Collections.emptyList(), Collections.emptyList());
        List<ConnectorScanRange> ranges = Arrays.asList(
                new PaimonScanRange.Builder().path("a.parquet").build(),
                new PaimonScanRange.Builder().rustSplit("split", "/table", "db", "t", "{}", null).build(),
                new PaimonScanRange.Builder().paimonSplit("split")
                        .rustRejectionReason(PaimonRustReaderReason.MULTI_FILE_RESOURCE_GUARD).build(),
                new PaimonScanRange.Builder().paimonSplit("split")
                        .rustRejectionReason(PaimonRustReaderReason.STORAGE_CREDENTIALS).build(),
                new PaimonScanRange.Builder().rowCount(10L).build());
        ConnectorScanProfile profile = PaimonReaderRouting.profile(table, ranges);
        Map<String, String> metrics = profile.getMetrics();
        Assertions.assertEquals("1", metrics.get(PaimonReaderRouting.NATIVE));
        Assertions.assertEquals("1", metrics.get(PaimonReaderRouting.RUST));
        Assertions.assertEquals("2", metrics.get(PaimonReaderRouting.JNI));
        Assertions.assertEquals("1", metrics.get(PaimonReaderRouting.COUNT));
        Assertions.assertEquals("1", metrics.get("RustNotConsidered.Native"));
        Assertions.assertEquals("1", metrics.get("RustNotConsidered.MetadataCount"));
        Assertions.assertEquals("1", metrics.get("RustRejected.MULTI_FILE_RESOURCE_GUARD"));
        Assertions.assertEquals("1", metrics.get("RustRejected.STORAGE_CREDENTIALS"));
        Map<String, String> properties = new HashMap<>();
        properties.put(ScanNodePropertyKeys.SYNTHETIC_NATIVE_READ_SPLITS, "1");
        properties.put(ScanNodePropertyKeys.SYNTHETIC_TOTAL_READ_SPLITS, "5");
        properties.put(ScanNodePropertyKeys.SYNTHETIC_EXPLAIN_VERBOSE, "true");
        StringBuilder explain = new StringBuilder();
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(
                PaimonCatalogProperties.of(Collections.emptyMap()), null);
        provider.appendExplainInfo(explain, "  ", properties, Collections.singletonList(profile));
        Assertions.assertTrue(explain.toString().contains("Native=1, Rust=1, JNI=2, MetadataCount=1"));
        Assertions.assertTrue(explain.toString().contains("SplitStat [type=RUST]"));
        Assertions.assertTrue(explain.toString().contains("SplitStat [type=JNI]"));
        Assertions.assertTrue(explain.toString().contains("SplitStat [type=METADATA_COUNT]"));
        Assertions.assertFalse(explain.toString().contains("RustRejected.DISABLED"));
        Assertions.assertEquals(2L, PaimonReaderRouting.aggregate(Arrays.asList(profile, profile))
                .get(PaimonReaderRouting.RUST));
    }
}
