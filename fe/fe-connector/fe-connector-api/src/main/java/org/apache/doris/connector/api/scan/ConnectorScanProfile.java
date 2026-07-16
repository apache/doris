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

package org.apache.doris.connector.api.scan;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A connector-neutral bundle of scan diagnostics for one table scan, produced by a connector from its own
 * SDK metrics and written verbatim into the query profile by the generic scan node
 * (via {@link ConnectorScanPlanProvider#collectScanProfiles}).
 *
 * <p>The engine treats all three fields opaquely — it get-or-creates a profile group named
 * {@link #getGroupName()} under the execution summary, adds a child named {@link #getScanLabel()}, and
 * writes each {@link #getMetrics()} entry as an info string. This keeps the engine connector-agnostic:
 * it never interprets a metric, only the connector knows what its SDK exposes (paimon manifest cache
 * hit/miss, iceberg scanned/skipped manifests, etc.). Values are ALREADY formatted strings because the
 * formatting (durations, byte sizes) lives with the SDK-specific harvest in the connector.</p>
 *
 * <p>Immutable: the metrics map is copied into an unmodifiable, insertion-ordered view so the profile
 * rendering order is stable.</p>
 */
public final class ConnectorScanProfile {
    private final String groupName;
    private final String scanLabel;
    private final Map<String, String> metrics;

    public ConnectorScanProfile(String groupName, String scanLabel, Map<String, String> metrics) {
        this.groupName = groupName;
        this.scanLabel = scanLabel;
        this.metrics = metrics == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(metrics));
    }

    /** The profile group name (e.g. {@code "Paimon Scan Metrics"}); must match the engine's ordering key. */
    public String getGroupName() {
        return groupName;
    }

    /** The per-scan child label (e.g. {@code "Table Scan (db.tbl)"}). */
    public String getScanLabel() {
        return scanLabel;
    }

    /** The ordered metric name &rarr; already-formatted value pairs (unmodifiable). */
    public Map<String, String> getMetrics() {
        return metrics;
    }
}
