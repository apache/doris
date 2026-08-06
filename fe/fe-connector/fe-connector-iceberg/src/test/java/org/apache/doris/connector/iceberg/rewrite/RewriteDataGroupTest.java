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

package org.apache.doris.connector.iceberg.rewrite;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests for the {@link RewriteDataGroup} carrier POJO (P6.4-T05 port), exercised with real {@link FileScanTask}
 * objects planned from an {@link InMemoryCatalog} table (no Mockito).
 */
public class RewriteDataGroupTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));

    private List<FileScanTask> planTwoFiles() throws IOException {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table t = catalog.createTable(TableIdentifier.of("db1", "g"), SCHEMA, PartitionSpec.unpartitioned());
        t.newAppend()
                .appendFile(file("a", 100))
                .appendFile(file("b", 250))
                .commit();
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> planned = t.newScan().planFiles()) {
            planned.forEach(tasks::add);
        }
        return tasks;
    }

    private static org.apache.iceberg.DataFile file(String path, long size) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("s3://b/db1/" + path)
                .withFileSizeInBytes(size)
                .withRecordCount(100)
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    @Test
    public void aggregatesSizeAndDataFilesFromTasks() throws IOException {
        List<FileScanTask> tasks = planTwoFiles();

        RewriteDataGroup group = new RewriteDataGroup(tasks);

        Assertions.assertEquals(2, group.getTaskCount());
        Assertions.assertEquals(350L, group.getTotalSize());
        Assertions.assertEquals(2, group.getDataFiles().size());
        // Data files with no merge-on-read deletes contribute zero to the delete-file count.
        Assertions.assertEquals(0, group.getDeleteFileCount());
        Assertions.assertFalse(group.isEmpty());
    }

    @Test
    public void emptyGroupAcceptsTasksViaAddTask() throws IOException {
        List<FileScanTask> tasks = planTwoFiles();

        RewriteDataGroup group = new RewriteDataGroup();
        Assertions.assertTrue(group.isEmpty());
        group.addTask(tasks.get(0));

        Assertions.assertEquals(1, group.getTaskCount());
        Assertions.assertEquals(tasks.get(0).file().fileSizeInBytes(), group.getTotalSize());
        Assertions.assertFalse(group.isEmpty());
    }
}
