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

package org.apache.iceberg;

import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.util.ContentFileUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Keeps partition pruning aligned with the schema selected by the Doris relation. */
public class DorisDataTableScan extends DataTableScan {
    private DorisDataTableScan(Table table, Schema schema, TableScanContext context) {
        super(table, schema, context);
    }

    public static TableScan wrap(TableScan scan) {
        if (!(scan instanceof DataTableScan)) {
            return scan;
        }
        DataTableScan dataScan = (DataTableScan) scan;
        return new DorisDataTableScan(dataScan.table(), dataScan.tableSchema(), dataScan.context());
    }

    /** Only native data scans can be replaced by local manifest planning without losing custom semantics. */
    public static boolean supportsLocalManifestPlanning(TableScan scan) {
        return (scan.getClass() == DataTableScan.class || scan.getClass() == DorisDataTableScan.class)
                && scan.table().getClass() == BaseTable.class;
    }

    /** Separate predicate-read statistics, window retention and the native task's requested statistics. */
    public static DataFileProjection dataFileProjection(TableScan scan, Set<Integer> equalityFields,
            boolean hasPredicate) {
        return new DataFileProjection((DataTableScan) scan, equalityFields, hasPredicate);
    }

    /** Immutable per-scan projection; equality keys remain available until delete association completes. */
    public static final class DataFileProjection {
        private final List<String> manifestColumns;
        private final boolean keepWindowStats;
        private final Set<Integer> windowStatColumns;
        private final boolean keepTaskStats;
        private final Set<Integer> taskStatColumns;
        private final boolean needsTaskCopy;

        private DataFileProjection(DataTableScan scan, Set<Integer> equalityFields, boolean hasPredicate) {
            keepTaskStats = scan.shouldReturnColumnStats();
            Set<Integer> requested = keepTaskStats ? scan.columnsToKeepStats() : Collections.emptySet();
            taskStatColumns = requested == null ? null : Collections.unmodifiableSet(new HashSet<>(requested));
            manifestColumns = (hasPredicate || !equalityFields.isEmpty()) && !keepTaskStats
                    ? ManifestReader.withStatsColumns(scan.scanColumns()) : scan.scanColumns();
            keepWindowStats = keepTaskStats || !equalityFields.isEmpty();
            if (taskStatColumns == null) {
                // A null native selection with returnColumnStats=true explicitly requests every column.
                windowStatColumns = null;
                needsTaskCopy = false;
            } else {
                Set<Integer> retained = new HashSet<>(taskStatColumns);
                retained.addAll(equalityFields);
                windowStatColumns = Collections.unmodifiableSet(retained);
                needsTaskCopy = !taskStatColumns.containsAll(equalityFields);
            }
        }

        public List<String> manifestColumns() {
            return manifestColumns;
        }

        public DataFile copyForManifest(DataFile file) {
            return ContentFileUtil.copy(file, keepWindowStats, windowStatColumns);
        }

        public DataFile forTask(DataFile file) {
            return needsTaskCopy ? ContentFileUtil.copy(file, keepTaskStats, taskStatColumns) : file;
        }
    }

    /** Reuse the native ScanEvent, planning timer and close-time table reporter around local planning. */
    public static CloseableIterable<FileScanTask> planFilesWithLocalPlanner(TableScan scan,
            Function<ScanMetrics, CloseableIterable<FileScanTask>> planner) {
        DataTableScan nativeScan = (DataTableScan) scan;
        return new DataTableScan(scan.table(), nativeScan.tableSchema(), nativeScan.context()) {
            @Override
            public CloseableIterable<FileScanTask> doPlanFiles() {
                return planner.apply(scanMetrics());
            }
        }.planFiles();
    }

    /** Rebinds snapshot specs by field ID to the full schema selected for the scan. */
    public static Map<Integer, PartitionSpec> specsForScan(TableScan scan) {
        Map<Integer, PartitionSpec> tableSpecs = scan.table().specs();
        Schema schema = scan.schema();
        if (tableSpecs.values().stream().allMatch(spec -> spec.schema() == schema)) {
            return tableSpecs;
        }
        Map<Integer, PartitionSpec> specs = new HashMap<>();
        // A schema-only update keeps the snapshot ID unchanged. Snapshot IDs therefore cannot
        // determine whether table specs still use the schema selected by a historical query.
        Snapshot snapshot = scan.snapshot();
        if (snapshot != null) {
            // Later, unused specs can have partition names that conflict with historical columns.
            // Bind only specs referenced by this snapshot, including those needed for delete files.
            for (ManifestFile manifest : snapshot.allManifests(scan.table().io())) {
                int specId = manifest.partitionSpecId();
                specs.computeIfAbsent(specId, id -> {
                    PartitionSpec spec = tableSpecs.get(id);
                    return spec.schema() == schema ? spec : spec.toUnbound().bind(schema, true);
                });
            }
        }
        return specs;
    }

    @Override
    protected Map<Integer, PartitionSpec> specs() {
        return specsForScan(this);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
        return new DorisDataTableScan(table, schema, context);
    }
}
