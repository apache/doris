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

import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.TimerResult;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An iceberg SDK {@link MetricsReporter} that captures a scan's {@link ScanReport} into a connector-neutral
 * {@link ConnectorScanProfile}, stashed keyed by the statement queryId for fe-core to drain into the query
 * profile (FIX-SCAN-METRICS). Restores the legacy {@code datasource.iceberg.profile.IcebergMetricsReporter}
 * behavior in the plugin architecture: it is a self-contained port (the connector cannot import fe-core, so
 * {@code DebugUtil}'s time/byte formatters are inlined and Guava is avoided).
 *
 * <p>The SDK invokes {@link #report} on CLOSE of the {@code planFiles} iterable, which the connector performs
 * synchronously on the planScan thread — so a fresh reporter is created per scan bound to that scan's queryId,
 * and attached ONLY on the synchronous data/count path (never the streaming or system-table path, which fe-core
 * never drains).</p>
 */
public class IcebergScanProfileReporter implements MetricsReporter {
    /** Profile group name — MUST equal fe-core {@code SummaryProfile.ICEBERG_SCAN_METRICS} (display ordering). */
    static final String GROUP_NAME = "Iceberg Scan Metrics";
    private static final DecimalFormat BYTES_FORMAT = new DecimalFormat("0.000");
    private static final long KB = 1024L;
    private static final long MB = 1024 * KB;
    private static final long GB = 1024 * MB;
    private static final long TB = 1024 * GB;
    private static final long SECOND_MS = 1000L;
    private static final long MINUTE_MS = 60 * SECOND_MS;
    private static final long HOUR_MS = 60 * MINUTE_MS;

    private final String queryId;
    private final ConcurrentHashMap<String, List<ConnectorScanProfile>> stash;

    IcebergScanProfileReporter(String queryId, ConcurrentHashMap<String, List<ConnectorScanProfile>> stash) {
        this.queryId = queryId;
        this.stash = stash;
    }

    @Override
    public void report(MetricsReport report) {
        if (queryId == null || queryId.isEmpty() || !(report instanceof ScanReport)) {
            return;
        }
        ScanReport scanReport = (ScanReport) report;
        ScanMetricsResult metrics = scanReport.scanMetrics();
        if (metrics == null) {
            return;
        }
        Map<String, String> rendered = new LinkedHashMap<>();
        rendered.put("table", scanReport.tableName());
        rendered.put("snapshot", String.valueOf(scanReport.snapshotId()));
        String filter = sanitize(scanReport.filter() == null ? null : scanReport.filter().toString());
        if (!filter.isEmpty()) {
            rendered.put("filter", filter);
        }
        if (scanReport.projectedFieldNames() != null && !scanReport.projectedFieldNames().isEmpty()) {
            rendered.put("columns", String.join("|", scanReport.projectedFieldNames()));
        }
        appendTimer(rendered, "planning", metrics.totalPlanningDuration());
        appendCounter(rendered, "data_files", metrics.resultDataFiles());
        appendCounter(rendered, "delete_files", metrics.resultDeleteFiles());
        appendCounter(rendered, "skipped_data_files", metrics.skippedDataFiles());
        appendCounter(rendered, "skipped_delete_files", metrics.skippedDeleteFiles());
        appendCounter(rendered, "total_size", metrics.totalFileSizeInBytes());
        appendCounter(rendered, "total_delete_size", metrics.totalDeleteFileSizeInBytes());
        appendCounter(rendered, "scanned_manifests", metrics.scannedDataManifests());
        appendCounter(rendered, "skipped_manifests", metrics.skippedDataManifests());
        appendCounter(rendered, "scanned_delete_manifests", metrics.scannedDeleteManifests());
        appendCounter(rendered, "skipped_delete_manifests", metrics.skippedDeleteManifests());
        appendCounter(rendered, "indexed_delete_files", metrics.indexedDeleteFiles());
        appendCounter(rendered, "equality_delete_files", metrics.equalityDeleteFiles());
        appendCounter(rendered, "positional_delete_files", metrics.positionalDeleteFiles());
        appendMetadata(rendered, scanReport.metadata());

        ConnectorScanProfile profile = new ConnectorScanProfile(
                GROUP_NAME, "Table Scan (" + scanReport.tableName() + ")", rendered);
        stash.computeIfAbsent(queryId, k -> new CopyOnWriteArrayList<>()).add(profile);
    }

    private void appendMetadata(Map<String, String> out, Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return;
        }
        List<String> captured = new ArrayList<>();
        for (String key : new String[] {"scan-state", "scan-id"}) {
            if (metadata.containsKey(key)) {
                captured.add(key + "=" + metadata.get(key));
            }
        }
        if (!captured.isEmpty()) {
            out.put("metadata", "{" + String.join(", ", captured) + "}");
        }
    }

    private void appendTimer(Map<String, String> out, String name, TimerResult timerResult) {
        if (timerResult == null) {
            return;
        }
        Duration duration = timerResult.totalDuration();
        out.put(name, prettyMs(duration.toMillis()) + " (" + timerResult.count() + " ops)");
    }

    private void appendCounter(Map<String, String> out, String name, CounterResult counterResult) {
        if (counterResult == null) {
            return;
        }
        long value = counterResult.value();
        out.put(name, counterResult.unit() == MetricsContext.Unit.BYTES
                ? printByteWithUnit(value) : Long.toString(value));
    }

    private static String sanitize(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        return value.replaceAll("\\s+", " ").trim();
    }

    /** Inlined fe-core {@code DebugUtil.printByteWithUnit}. */
    static String printByteWithUnit(long value) {
        double d = value;
        String unit;
        if (value == 0) {
            unit = "";
        } else if (value > TB) {
            unit = "TB";
            d /= TB;
        } else if (value > GB) {
            unit = "GB";
            d /= GB;
        } else if (value > MB) {
            unit = "MB";
            d /= MB;
        } else if (value > KB) {
            unit = "KB";
            d /= KB;
        } else {
            unit = "B";
        }
        return BYTES_FORMAT.format(d) + " " + unit;
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
