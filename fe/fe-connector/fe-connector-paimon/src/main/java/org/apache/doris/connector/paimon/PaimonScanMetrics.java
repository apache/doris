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

import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.HistogramStatistics;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.operation.metrics.ScanMetrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Harvests the paimon SDK {@code ScanMetrics} group recorded by {@link PaimonMetricRegistry} during
 * {@code scan.plan()} into a connector-neutral {@link ConnectorScanProfile} for the query profile — a
 * self-contained port of the legacy {@code datasource.paimon.profile.PaimonScanMetricsReporter} extraction,
 * with fe-core's {@code DebugUtil.getPrettyStringMs} inlined (the connector cannot import fe-core).
 */
public final class PaimonScanMetrics {
    private static final double P95 = 0.95d;
    private static final long SECOND_MS = 1000L;
    private static final long MINUTE_MS = 60 * SECOND_MS;
    private static final long HOUR_MS = 60 * MINUTE_MS;

    /** Profile group name — MUST equal fe-core {@code SummaryProfile.PAIMON_SCAN_METRICS} (display ordering). */
    public static final String GROUP_NAME = "Paimon Scan Metrics";

    private PaimonScanMetrics() {
    }

    /**
     * Build the scan profile for {@code paimonTableName}'s recorded metrics, or empty when the SDK recorded
     * none (unpartitioned/no-op scan, unsupported scan type). {@code scanLabel} is the per-scan child name.
     */
    public static Optional<ConnectorScanProfile> harvest(PaimonMetricRegistry registry, String paimonTableName,
            String scanLabel) {
        if (registry == null || paimonTableName == null) {
            return Optional.empty();
        }
        MetricGroup group = registry.getGroup(ScanMetrics.GROUP_NAME, paimonTableName);
        if (group == null) {
            String prefix = ScanMetrics.GROUP_NAME + ":";
            for (Map.Entry<String, MetricGroup> entry : registry.getAllGroupsAsMap().entrySet()) {
                String key = entry.getKey();
                if (!key.startsWith(prefix)) {
                    continue;
                }
                if (group != null) {
                    // More than one candidate group — ambiguous, bail out (legacy parity).
                    group = null;
                    break;
                }
                group = entry.getValue();
            }
        }
        if (group == null) {
            return Optional.empty();
        }
        Map<String, Metric> metrics = group.getMetrics();
        if (metrics == null || metrics.isEmpty()) {
            return Optional.empty();
        }

        Map<String, String> rendered = new LinkedHashMap<>();
        appendDuration(rendered, metrics, ScanMetrics.LAST_SCAN_DURATION, "last_scan_duration");
        appendHistogram(rendered, metrics, ScanMetrics.SCAN_DURATION, "scan_duration");
        appendCounter(rendered, metrics, ScanMetrics.LAST_SCANNED_MANIFESTS, "last_scanned_manifests");
        appendCounter(rendered, metrics, ScanMetrics.LAST_SCAN_SKIPPED_TABLE_FILES,
                "last_scan_skipped_table_files");
        appendCounter(rendered, metrics, ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES,
                "last_scan_resulted_table_files");
        appendCounter(rendered, metrics, ScanMetrics.MANIFEST_HIT_CACHE, "manifest_hit_cache");
        appendCounter(rendered, metrics, ScanMetrics.MANIFEST_MISSED_CACHE, "manifest_missed_cache");
        if (rendered.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ConnectorScanProfile(GROUP_NAME, scanLabel, rendered));
    }

    private static void appendDuration(Map<String, String> out, Map<String, Metric> metrics, String metricKey,
            String profileKey) {
        Long value = getLongValue(metrics.get(metricKey));
        if (value == null) {
            return;
        }
        out.put(profileKey, formatDuration(value));
    }

    private static void appendCounter(Map<String, String> out, Map<String, Metric> metrics, String metricKey,
            String profileKey) {
        Long value = getLongValue(metrics.get(metricKey));
        if (value == null) {
            return;
        }
        out.put(profileKey, Long.toString(value));
    }

    private static void appendHistogram(Map<String, String> out, Map<String, Metric> metrics, String metricKey,
            String profileKey) {
        Metric metric = metrics.get(metricKey);
        if (!(metric instanceof Histogram)) {
            return;
        }
        Histogram histogram = (Histogram) metric;
        HistogramStatistics stats = histogram.getStatistics();
        if (stats == null) {
            return;
        }
        String formatted = "count=" + histogram.getCount()
                + ", mean=" + formatDuration(stats.getMean())
                + ", p95=" + formatDuration(stats.getQuantile(P95))
                + ", max=" + formatDuration(stats.getMax());
        out.put(profileKey, formatted);
    }

    private static Long getLongValue(Metric metric) {
        if (metric instanceof Counter) {
            return ((Counter) metric).getCount();
        }
        if (metric instanceof Gauge) {
            Object value = ((Gauge<?>) metric).getValue();
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
        }
        return null;
    }

    private static String formatDuration(double nanos) {
        return prettyMs(TimeUnit.NANOSECONDS.toMillis(Math.round(nanos)));
    }

    /** Inlined fe-core {@code DebugUtil.getPrettyStringMs}: {@code Nhour Nmin Nsec} / {@code Nms}. */
    static String prettyMs(long value) {
        if (value == 0) {
            return "0";
        }
        StringBuilder builder = new StringBuilder();
        long remaining = value;
        boolean hour = false;
        boolean minute = false;
        if (remaining >= HOUR_MS) {
            builder.append(remaining / HOUR_MS).append("hour");
            remaining %= HOUR_MS;
            hour = true;
        }
        if (remaining >= MINUTE_MS) {
            builder.append(remaining / MINUTE_MS).append("min");
            remaining %= MINUTE_MS;
            minute = true;
        }
        if (!hour && remaining >= SECOND_MS) {
            builder.append(remaining / SECOND_MS).append("sec");
            remaining %= SECOND_MS;
        }
        if (!hour && !minute) {
            builder.append(remaining).append("ms");
        }
        return builder.toString();
    }
}
