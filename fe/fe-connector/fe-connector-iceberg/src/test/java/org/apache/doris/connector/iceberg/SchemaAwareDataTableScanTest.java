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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaAwareDataTableScan;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SchemaAwareDataTableScanTest {
    @Test
    public void fileIoViewKeepsProjectionFilterSnapshotOptionsAndReporter() throws Exception {
        try (InMemoryCatalog catalog = new InMemoryCatalog()) {
            catalog.initialize("test", Collections.emptyMap());
            catalog.createNamespace(Namespace.of("db"));
            Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "value", Types.StringType.get()));
            Table table = catalog.createTable(TableIdentifier.of("db", "io_view"), schema);
            table.newAppend().appendFile(file(table, "first.parquet")).commit();
            long pinnedSnapshot = table.currentSnapshot().snapshotId();
            table.newAppend().appendFile(file(table, "second.parquet")).commit();
            AtomicInteger reports = new AtomicInteger();
            TableScan original = table.newScan().useSnapshot(pinnedSnapshot).select("value")
                    .filter(Expressions.equal("id", 1)).option(TableProperties.SPLIT_SIZE, "123")
                    .metricsReporter(report -> reports.incrementAndGet());
            TableScan guarded = SchemaAwareDataTableScan.withFileIO(original, io -> io,
                    ThreadPools.getWorkerPool()).orElseThrow();
            Assertions.assertNotSame(original.table(), guarded.table());
            Assertions.assertSame(table.io(), guarded.table().io());
            Assertions.assertEquals(original.schema().asStruct(), guarded.schema().asStruct());
            Assertions.assertEquals(original.filter(), guarded.filter());
            Assertions.assertEquals(pinnedSnapshot, guarded.snapshot().snapshotId());
            Assertions.assertEquals(123, guarded.targetSplitSize());
            List<String> paths = new ArrayList<>();
            try (CloseableIterable<FileScanTask> tasks = guarded.planFiles()) {
                tasks.forEach(task -> paths.add(task.file().location()));
            }
            Assertions.assertEquals(Collections.singletonList("first.parquet"), paths);
            Assertions.assertEquals(1, reports.get());
        }
    }

    @Test
    public void customScansKeepTheirSemanticsWithoutInvokingFileIoWrapper() throws Exception {
        try (InMemoryCatalog catalog = new InMemoryCatalog()) {
            catalog.initialize("test", Collections.emptyMap());
            catalog.createNamespace(Namespace.of("db"));
            Table table = catalog.createTable(TableIdentifier.of("db", "custom"),
                    new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));
            TableScan nativeScan = table.newScan();
            TableScan custom = (TableScan) Proxy.newProxyInstance(TableScan.class.getClassLoader(),
                    new Class<?>[] {TableScan.class}, (proxy, method, args) -> method.invoke(nativeScan, args));
            Assertions.assertTrue(SchemaAwareDataTableScan.withFileIO(custom, io -> {
                throw new AssertionError("unsupported scans must not acquire a request-local IO");
            }, ThreadPools.getWorkerPool()).isEmpty());
        }
    }

    private static DataFile file(Table table, String path) {
        return DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                .withRecordCount(1).withFileSizeInBytes(100).build();
    }
}
