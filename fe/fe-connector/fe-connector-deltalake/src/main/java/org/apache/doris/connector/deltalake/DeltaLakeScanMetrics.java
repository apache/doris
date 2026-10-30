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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.scan.ConnectorScanProfile;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.engine.FileSystemClient;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.engine.MetricsReporter;
import io.delta.kernel.engine.ParquetHandler;
import io.delta.kernel.metrics.ScanReport;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** One producer owns this collector. Only the immutable result is published to the explain thread. */
final class DeltaLakeScanMetrics {
    private final DeltaLakeTableHandle handle;
    private final DeltaLakeStatementScope.FileCount totalFiles;
    private final Set<Map<String, String>> partitions = new HashSet<>();
    private long selectedFiles;
    private long splits;
    private long countPartitions;
    private boolean complete;

    DeltaLakeScanMetrics(ConnectorSession session, DeltaLakeTableHandle handle, Snapshot snapshot) {
        this.handle = handle;
        totalFiles = DeltaLakeStatementScope.fileCount(session, handle);
        DeltaKernelCompat.fileCount(snapshot).ifPresent(value -> totalFiles.value = value);
    }

    Engine engine(Engine delegate) {
        List<MetricsReporter> reporters = new ArrayList<>(delegate.getMetricsReporters());
        reporters.add(report -> {
            if (report instanceof ScanReport) {
                ScanReport scan = (ScanReport) report;
                // Partition predicates can prune checkpoint rows before the active-file counter.
                // Only an exhaustive scan without that predicate establishes the table total.
                if (scan.getIsFullyConsumed() && scan.getPartitionPredicate().isEmpty()) {
                    totalFiles.value = scan.getScanMetrics().getNumActiveAddFiles();
                }
            }
        });
        return new Engine() {
            @Override
            public ExpressionHandler getExpressionHandler() {
                return delegate.getExpressionHandler();
            }

            @Override
            public JsonHandler getJsonHandler() {
                return delegate.getJsonHandler();
            }

            @Override
            public FileSystemClient getFileSystemClient() {
                return delegate.getFileSystemClient();
            }

            @Override
            public ParquetHandler getParquetHandler() {
                return delegate.getParquetHandler();
            }

            @Override
            public List<MetricsReporter> getMetricsReporters() {
                return reporters;
            }
        };
    }

    void file(Map<String, String> values) {
        selectedFiles++;
        if (!values.isEmpty()) {
            partitions.add(values);
        }
    }

    void split() {
        splits++;
    }

    void count(long files, long partitionCount) {
        selectedFiles = files;
        totalFiles.value = files;
        countPartitions = partitionCount;
    }

    void complete() {
        complete = true;
    }

    Result result() {
        return new Result(handle, totalFiles.value, selectedFiles, splits,
                countPartitions + partitions.size(), complete);
    }

    static final class Result {
        private final DeltaLakeTableHandle handle;
        private final long total;
        private final long selected;
        private final long splits;
        private final long partitions;
        private final boolean complete;

        private Result(DeltaLakeTableHandle handle, long total, long selected, long splits,
                long partitions, boolean complete) {
            this.handle = handle;
            this.total = total;
            this.selected = selected;
            this.splits = splits;
            this.partitions = partitions;
            this.complete = complete;
        }

        void explain(StringBuilder output, String prefix) {
            output.append(prefix).append("Delta files: ").append(selected).append('/')
                    .append(total < 0 ? "unknown" : Long.toString(total))
                    .append(", selected partitions: ").append(partitions)
                    .append(", splits: ").append(splits)
                    .append(complete ? "" : " (partial)").append('\n');
        }

        ConnectorScanProfile profile() {
            Map<String, String> metrics = new LinkedHashMap<>();
            metrics.put("SnapshotVersion", Long.toString(handle.getVersion()));
            metrics.put("TotalFiles", total < 0 ? "unknown" : Long.toString(total));
            metrics.put("SelectedFiles", Long.toString(selected));
            metrics.put("SelectedPartitions", Long.toString(partitions));
            metrics.put("GeneratedSplits", Long.toString(splits));
            metrics.put("EnumerationComplete", Boolean.toString(complete));
            return new ConnectorScanProfile("Delta Scan Metrics",
                    "Table Scan (" + handle.getDatabase() + "." + handle.getTable() + ")", metrics);
        }
    }
}
