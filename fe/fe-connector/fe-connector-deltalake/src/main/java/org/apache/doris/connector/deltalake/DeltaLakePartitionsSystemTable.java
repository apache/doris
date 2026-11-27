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
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TDeltaLakeSysTableColumn;
import org.apache.doris.thrift.TDeltaLakeSysTableField;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

final class DeltaLakePartitionsSystemTable extends DeltaLakeSnapshotSystemTable {
    DeltaLakePartitionsSystemTable() {
        super("partitions", new StructType()
                .add("partition", new MapType(StringType.STRING, StringType.STRING, true))
                .add("file_count", LongType.LONG).add("total_size", LongType.LONG)
                .add("record_count", LongType.LONG).add("deleted_record_count", LongType.LONG));
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, DeltaLakeRuntime runtime,
            DeltaLakeTableHandle handle, ConnectorScanRequest request) {
        Snapshot snapshot = handle.snapshot(runtime);
        // An unpartitioned table has no partition entries, including when it has active files.
        if (snapshot.getPartitionColumnNames().isEmpty()) {
            return List.of();
        }
        List<TDeltaLakeSysTableColumn> columns = projectedColumns(runtime, handle, request);
        boolean needRecords = DeltaLakeSystemTableRows.needs(columns, "record_count");
        boolean needSize = DeltaLakeSystemTableRows.needs(columns, "total_size");
        boolean needDeleted = DeltaLakeSystemTableRows.needs(columns, "deleted_record_count");
        ZoneId zone = DeltaLakePartitionValues.sessionZone(session);
        Map<Map<String, String>, Totals> partitions = new LinkedHashMap<>();
        runtime.execute(() -> {
            try (CloseableIterator<Row> files = DeltaLakeScanFiles.open(snapshot, runtime.engine(handle.reference()),
                    needRecords, Optional.empty())) {
                while (files.hasNext()) {
                    request.getScanContext().checkCancelled();
                    Row row = files.next();
                    Map<String, String> values = DeltaLakePartitionValues.normalize(
                            snapshot, DeltaKernelCompat.partitionValues(row), zone);
                    Totals totals = partitions.computeIfAbsent(values, ignored -> new Totals());
                    totals.files++;
                    if (needSize) {
                        totals.bytes = Math.addExact(totals.bytes, DeltaKernelCompat.fileStatus(row).getSize());
                    }
                    if (needRecords && totals.knownRecords) {
                        OptionalLong records = DeltaKernelCompat.numRecords(row);
                        totals.knownRecords = records.isPresent();
                        totals.records = Math.addExact(totals.records, records.orElse(0));
                    }
                    if (needDeleted) {
                        totals.deleted = Math.addExact(totals.deleted, DeltaKernelCompat.deletionCardinality(row));
                    }
                }
            }
            return null;
        });
        var rows = partitions.entrySet().stream().map(entry -> DeltaLakeSystemTableRows.row(columns,
                col -> entry.getValue().value(col, entry.getKey())));
        return DeltaLakeSystemTableRows.pack(name(), columns, rows.iterator(), request.getScanContext());
    }

    private static final class Totals {
        long files;
        long bytes;
        long records;
        long deleted;
        boolean knownRecords = true;

        TDeltaLakeSysTableField value(String column, Map<String, String> partition) {
            return switch (column) {
                case "partition" -> DeltaLakeSystemTableRows.map(partition);
                case "file_count" -> DeltaLakeSystemTableRows.number(files);
                case "total_size" -> DeltaLakeSystemTableRows.number(bytes);
                case "record_count" -> knownRecords ? DeltaLakeSystemTableRows.number(records)
                        : new TDeltaLakeSysTableField();
                case "deleted_record_count" -> DeltaLakeSystemTableRows.number(deleted);
                default -> throw new IllegalArgumentException(column);
            };
        }
    }
}
