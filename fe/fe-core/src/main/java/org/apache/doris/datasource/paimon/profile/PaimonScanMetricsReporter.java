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

package org.apache.doris.datasource.paimon.profile;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.profile.RuntimeProfile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.ConnectContext;

import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.HistogramStatistics;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.operation.metrics.ScanMetrics;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PaimonScanMetricsReporter {
    private static final double P95 = 0.95d;

    public static void report(TableIf table, String paimonTableName, PaimonMetricRegistry registry) {
        if (registry == null || paimonTableName == null) {
            return;
        }
        String resolvedTableName = paimonTableName;
        MetricGroup group = registry.getGroup(ScanMetrics.GROUP_NAME, paimonTableName);
        if (group == null) {
            String prefix = ScanMetrics.GROUP_NAME + ":";
            for (Map.Entry<String, MetricGroup> entry : registry.getAllGroupsAsMap().entrySet()) {
                String key = entry.getKey();
                if (!key.startsWith(prefix)) {
                    continue;
                }
                if (group != null) {
                    group = null;
                    break;
                }
                group = entry.getValue();
                resolvedTableName = key.substring(prefix.length());
            }
        }
        if (group == null) {
            return;
        }
        Map<String, Metric> metrics = group.getMetrics();
        if (metrics == null || metrics.isEmpty()) {
            return;
        }

        SummaryProfile summaryProfile = SummaryProfile.getSummaryProfile(ConnectContext.get());
        if (summaryProfile == null) {
            return;
        }
        RuntimeProfile executionSummary = summaryProfile.getExecutionSummary();
        if (executionSummary == null) {
            return;
        }

        RuntimeProfile paimonGroup = executionSummary.getChildMap().get(SummaryProfile.PAIMON_SCAN_METRICS);
        if (paimonGroup == null) {
            paimonGroup = new RuntimeProfile(SummaryProfile.PAIMON_SCAN_METRICS);
            executionSummary.addChild(paimonGroup, true);
        }

        String displayName = table == null ? paimonTableName : table.getNameWithFullQualifiers();
        RuntimeProfile scanProfile = new RuntimeProfile("Table Scan (" + displayName + ")");
        appendDuration(scanProfile, metrics, ScanMetrics.LAST_SCAN_DURATION, "last_scan_duration");
        appendHistogram(scanProfile, metrics, ScanMetrics.SCAN_DURATION, "scan_duration");
        appendCounter(scanProfile, metrics, ScanMetrics.LAST_SCANNED_MANIFESTS, "last_scanned_manifests");
        appendCounter(scanProfile, metrics, ScanMetrics.LAST_SCAN_SKIPPED_TABLE_FILES,
                "last_scan_skipped_table_files");
        appendCounter(scanProfile, metrics, ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES,
                "last_scan_resulted_table_files");
        appendCounter(scanProfile, metrics, ScanMetrics.MANIFEST_HIT_CACHE, "manifest_hit_cache");
        appendCounter(scanProfile, metrics, ScanMetrics.MANIFEST_MISSED_CACHE, "manifest_missed_cache");
        paimonGroup.addChild(scanProfile, true);
        registry.removeGroup(ScanMetrics.GROUP_NAME, resolvedTableName);
    }

    private static void appendDuration(RuntimeProfile profile, Map<String, Metric> metrics, String metricKey,
                                       String profileKey) {
        Long value = getLongValue(metrics.get(metricKey));
        if (value == null) {
            return;
        }
        profile.addInfoString(profileKey, formatDuration(value));
    }

    private static void appendCounter(RuntimeProfile profile, Map<String, Metric> metrics, String metricKey,
                                      String profileKey) {
        Long value = getLongValue(metrics.get(metricKey));
        if (value == null) {
            return;
        }
        profile.addInfoString(profileKey, Long.toString(value));
    }

    private static void appendHistogram(RuntimeProfile profile, Map<String, Metric> metrics, String metricKey,
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
        profile.addInfoString(profileKey, formatted);
    }

    private static Long getLongValue(Metric metric) {
        if (metric instanceof Counter) {
            return ((Counter) metric).getCount();
        }
        if (metric instanceof Gauge) {
            Object value = ((Gauge) metric).getValue();
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
        }
        return null;
    }

    private static String formatDuration(double nanos) {
        long ms = TimeUnit.NANOSECONDS.toMillis(Math.round(nanos));
        return DebugUtil.getPrettyStringMs(ms);
    }
}
