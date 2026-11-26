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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Counts emitted ranges, not input DataSplits: Native can subdivide files and COUNT can collapse splits. */
final class PaimonReaderRouting {
    static final String GROUP = "Paimon Reader Routing";
    static final String REJECTION_PROPERTY = "paimon.rust_rejection_reason";
    static final String NATIVE = "Ranges.Native";
    static final String RUST = "Ranges.Rust";
    static final String JNI = "Ranges.Jni";
    static final String COUNT = "Ranges.MetadataCount";

    private PaimonReaderRouting() {
    }

    static ConnectorScanProfile profile(PaimonTableHandle handle, List<ConnectorScanRange> ranges) {
        Map<String, Long> counts = new LinkedHashMap<>();
        counts.put(NATIVE, 0L);
        counts.put(RUST, 0L);
        counts.put(JNI, 0L);
        counts.put(COUNT, 0L);
        for (ConnectorScanRange range : ranges) {
            Map<String, String> properties = range.getProperties();
            if (properties.containsKey("paimon.row_count")) {
                increment(counts, COUNT);
                increment(counts, "RustNotConsidered.MetadataCount");
            } else if ("PAIMON_RUST".equals(properties.get("paimon.reader_type"))) {
                increment(counts, RUST);
            } else if (properties.containsKey("paimon.split")) {
                increment(counts, JNI);
                increment(counts, "RustRejected." + properties.getOrDefault(REJECTION_PROPERTY,
                        PaimonRustReaderReason.UNSUPPORTED_SPLIT.name()));
            } else {
                increment(counts, NATIVE);
                increment(counts, "RustNotConsidered.Native");
            }
        }
        Map<String, String> metrics = new LinkedHashMap<>();
        counts.forEach((key, value) -> metrics.put(key, Long.toString(value)));
        return new ConnectorScanProfile(GROUP,
                "Table Scan (" + handle.getDatabaseName() + "." + handle.getTableName() + ")", metrics);
    }

    static Map<String, Long> aggregate(List<ConnectorScanProfile> profiles) {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (ConnectorScanProfile profile : profiles) {
            if (GROUP.equals(profile.getGroupName())) {
                profile.getMetrics().forEach((key, value) -> counts.merge(key, Long.parseLong(value), Long::sum));
            }
        }
        return counts;
    }

    private static void increment(Map<String, Long> counts, String key) {
        counts.merge(key, 1L, Long::sum);
    }
}
