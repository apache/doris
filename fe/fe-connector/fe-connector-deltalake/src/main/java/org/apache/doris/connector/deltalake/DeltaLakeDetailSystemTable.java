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
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Optional;

final class DeltaLakeDetailSystemTable extends DeltaLakeSnapshotSystemTable {
    DeltaLakeDetailSystemTable() {
        super("detail", new StructType()
                .add("format", StringType.STRING).add("id", StringType.STRING).add("name", StringType.STRING)
                .add("description", StringType.STRING).add("location", StringType.STRING)
                .add("createdAt", TimestampType.TIMESTAMP).add("lastModified", TimestampType.TIMESTAMP)
                .add("partitionColumns", new ArrayType(StringType.STRING, false))
                .add("clusteringColumns", new ArrayType(StringType.STRING, false))
                .add("numFiles", LongType.LONG).add("sizeInBytes", LongType.LONG)
                .add("properties", new MapType(StringType.STRING, StringType.STRING, true))
                .add("minReaderVersion", IntegerType.INTEGER).add("minWriterVersion", IntegerType.INTEGER)
                .add("tableFeatures", new ArrayType(StringType.STRING, false)));
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, DeltaLakeRuntime runtime,
            DeltaLakeTableHandle handle, ConnectorScanRequest request) {
        Snapshot snapshot = handle.snapshot(runtime);
        List<TDeltaLakeSysTableColumn> columns = projectedColumns(runtime, handle, request);
        DeltaKernelCompat.FileTotals totals = DeltaLakeSystemTableRows.needs(columns, "numFiles")
                || DeltaLakeSystemTableRows.needs(columns, "sizeInBytes")
                ? totals(runtime, handle, snapshot, request) : null;
        var row = DeltaLakeSystemTableRows.row(columns, col -> switch (col) {
            case "format" -> DeltaLakeSystemTableRows.text("delta");
            case "id" -> DeltaLakeSystemTableRows.text(handle.getTableId());
            // Spark 4.0.1 returns NULL for DESCRIBE DETAIL delta.`path`, and the source catalog's
            // qualified identifier for a registered table. The Doris catalog can have a local alias.
            case "name" -> DeltaLakeSystemTableRows.text(
                    runtime.resolver.registeredTableName(handle.getDatabase(), handle.getTable()));
            case "description" -> DeltaLakeSystemTableRows.text(DeltaKernelCompat.tableDescription(snapshot));
            case "location" -> DeltaLakeSystemTableRows.text(new Path(snapshot.getPath()).toString());
            case "createdAt" -> DeltaKernelCompat.createdTime(snapshot)
                    .map(ts -> new TDeltaLakeSysTableField().setTimestampMillis(ts))
                    .orElseGet(TDeltaLakeSysTableField::new);
            case "lastModified" -> new TDeltaLakeSysTableField()
                    .setTimestampMillis(DeltaLakeHistorySystemTable.lastModified(runtime, handle, snapshot));
            case "partitionColumns" -> DeltaLakeSystemTableRows.strings(snapshot.getPartitionColumnNames());
            case "clusteringColumns" -> DeltaLakeSystemTableRows.strings(snapshot.getClusteringColumnInfos()
                    .orElse(List.of()).stream().map(c -> String.join(".", c.getLogicalColumn().getNames())).toList());
            case "numFiles" -> DeltaLakeSystemTableRows.number(totals.files());
            case "sizeInBytes" -> DeltaLakeSystemTableRows.number(totals.bytes());
            case "properties" -> DeltaLakeSystemTableRows.map(snapshot.getTableProperties());
            case "minReaderVersion" -> DeltaLakeSystemTableRows.number(DeltaKernelCompat.minReaderVersion(snapshot));
            case "minWriterVersion" -> DeltaLakeSystemTableRows.number(DeltaKernelCompat.minWriterVersion(snapshot));
            case "tableFeatures" -> DeltaLakeSystemTableRows.strings(DeltaKernelCompat.tableFeatures(snapshot));
            default -> throw new IllegalArgumentException(col);
        });
        return DeltaLakeSystemTableRows.pack(name(), columns, List.of(row).iterator(), request.getScanContext());
    }

    private static DeltaKernelCompat.FileTotals totals(DeltaLakeRuntime runtime, DeltaLakeTableHandle handle,
            Snapshot snapshot, ConnectorScanRequest request) {
        return DeltaKernelCompat.fileTotals(snapshot).orElseGet(() -> runtime.execute(() -> {
            long files = 0;
            long bytes = 0;
            try (CloseableIterator<Row> rows = DeltaLakeScanFiles.open(snapshot, runtime.engine(handle.reference()),
                    false, Optional.empty())) {
                while (rows.hasNext()) {
                    request.getScanContext().checkCancelled();
                    bytes = Math.addExact(bytes, DeltaKernelCompat.fileStatus(rows.next()).getSize());
                    files++;
                }
            }
            return new DeltaKernelCompat.FileTotals(files, bytes);
        }));
    }
}
