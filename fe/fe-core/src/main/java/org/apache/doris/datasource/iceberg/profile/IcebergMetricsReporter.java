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

package org.apache.doris.datasource.iceberg.profile;

import org.apache.doris.common.profile.RuntimeProfile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.TimerResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Pattern;

/**
 * MetricsReporter implementation that forwards Iceberg scan metrics into Doris
 * profiles.
 */
public class IcebergMetricsReporter implements MetricsReporter {

    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    @Override
    public void report(MetricsReport report) {
        if (!(report instanceof ScanReport)) {
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

        String formatted = formatScanReport((ScanReport) report);
        if (formatted.isEmpty()) {
            return;
        }

        String existing = executionSummary.getInfoString(SummaryProfile.ICEBERG_SCAN_METRICS);
        if (Strings.isNullOrEmpty(existing) || "N/A".equals(existing)) {
            executionSummary.addInfoString(SummaryProfile.ICEBERG_SCAN_METRICS, formatted);
        } else {
            executionSummary.addInfoString(SummaryProfile.ICEBERG_SCAN_METRICS,
                    existing + System.lineSeparator() + formatted);
        }
    }

    private String formatScanReport(ScanReport report) {
        ScanMetricsResult metrics = report.scanMetrics();
        if (metrics == null) {
            return "";
        }

        StringJoiner joiner = new StringJoiner(", ");
        joiner.add("table=" + report.tableName());
        joiner.add("snapshot=" + report.snapshotId());
        String filter = sanitize(report.filter() == null ? null : report.filter().toString());
        if (!Strings.isNullOrEmpty(filter)) {
            joiner.add("filter=" + filter);
        }
        if (!report.projectedFieldNames().isEmpty()) {
            joiner.add("columns=" + Joiner.on('|').join(report.projectedFieldNames()));
        }

        appendTimer(joiner, "planning", metrics.totalPlanningDuration());
        appendCounter(joiner, "data_files", metrics.resultDataFiles());
        appendCounter(joiner, "delete_files", metrics.resultDeleteFiles());
        appendCounter(joiner, "skipped_data_files", metrics.skippedDataFiles());
        appendCounter(joiner, "skipped_delete_files", metrics.skippedDeleteFiles());
        appendCounter(joiner, "total_size", metrics.totalFileSizeInBytes());
        appendCounter(joiner, "total_delete_size", metrics.totalDeleteFileSizeInBytes());
        appendCounter(joiner, "scanned_manifests", metrics.scannedDataManifests());
        appendCounter(joiner, "skipped_manifests", metrics.skippedDataManifests());
        appendCounter(joiner, "scanned_delete_manifests", metrics.scannedDeleteManifests());
        appendCounter(joiner, "skipped_delete_manifests", metrics.skippedDeleteManifests());
        appendCounter(joiner, "indexed_delete_files", metrics.indexedDeleteFiles());
        appendCounter(joiner, "equality_delete_files", metrics.equalityDeleteFiles());
        appendCounter(joiner, "positional_delete_files", metrics.positionalDeleteFiles());

        appendMetadata(joiner, report.metadata());

        return joiner.toString();
    }

    private String sanitize(String value) {
        if (Strings.isNullOrEmpty(value)) {
            return "";
        }
        return WHITESPACE.matcher(value).replaceAll(" ").trim();
    }

    private void appendMetadata(StringJoiner joiner, Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return;
        }
        List<String> importantKeys = ImmutableList.of("scan-state", "scan-id");
        List<String> captured = new ArrayList<>();
        for (String key : importantKeys) {
            if (metadata.containsKey(key)) {
                captured.add(key + "=" + metadata.get(key));
            }
        }
        if (!captured.isEmpty()) {
            joiner.add("metadata={" + String.join(", ", captured) + "}");
        }
    }

    private void appendTimer(StringJoiner joiner, String name, TimerResult timerResult) {
        if (timerResult == null) {
            return;
        }
        joiner.add(name + "=" + formatTimer(timerResult));
    }

    private void appendCounter(StringJoiner joiner, String name, CounterResult counterResult) {
        if (counterResult == null) {
            return;
        }
        joiner.add(name + "=" + formatCounter(counterResult));
    }

    private String formatCounter(CounterResult counterResult) {
        long value = counterResult.value();
        if (counterResult.unit() == MetricsContext.Unit.BYTES) {
            return DebugUtil.printByteWithUnit(value);
        }
        return Long.toString(value);
    }

    private String formatTimer(TimerResult timerResult) {
        Duration duration = timerResult.totalDuration();
        long millis = duration.toMillis();
        String pretty = DebugUtil.getPrettyStringMs(millis);
        return pretty + " (" + timerResult.count() + " ops)";
    }
}
