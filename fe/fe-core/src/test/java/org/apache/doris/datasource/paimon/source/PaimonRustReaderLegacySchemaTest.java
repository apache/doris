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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PaimonRustReaderLegacySchemaTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testPersistedLegacySchemaFallsBackBeforeAndAfterEvolution() throws Exception {
        java.nio.file.Path directory = temporaryFolder.newFolder().toPath();
        Path location = new Path(directory.toUri());
        LocalFileIO fileIO = LocalFileIO.create();
        new FileSystemSchemaManager(fileIO, location).createTable(Schema.newBuilder()
                .column("id", DataTypes.INT()).option("bucket", "-1")
                .option("file.format", "parquet").build());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, location);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(7));
            commit.commit(write.prepareCommit());
        }
        assertReadable(table, true);
        // Older producers omitted version. Java accepts that schema, but Rust requires it
        // even when reopening a historical schema for otherwise compatible field evolution.
        java.nio.file.Path schemaFile = directory.resolve("schema/schema-0");
        // Fixture I/O must also work with the FE test compilation target of Java 8.
        String json = new String(Files.readAllBytes(schemaFile), StandardCharsets.UTF_8);
        String legacy = json.replaceFirst("\"version\"\\s*:\\s*\\d+\\s*,", "");
        Assert.assertNotEquals(json, legacy);
        Files.write(schemaFile, legacy.getBytes(StandardCharsets.UTF_8));
        table = FileStoreTableFactory.create(fileIO, location);
        Assert.assertEquals(1, table.schema().version());
        assertReadable(table, false);
        table.schemaManager().commitChanges(Collections.singletonList(
                SchemaChange.addColumn("extra", DataTypes.STRING())));
        table = FileStoreTableFactory.create(fileIO, location);
        Assert.assertTrue(table.schema().version() > 0);
        assertReadable(table, false);
    }

    private static void assertReadable(FileStoreTable table, boolean rustExpected) throws Exception {
        List<Split> splits = table.newScan().plan().splits();
        Assert.assertFalse(splits.isEmpty());
        PaimonRustReaderCapabilities capabilities = new PaimonRustReaderCapabilities(table,
                new TupleDescriptor(new TupleId(0)));
        List<Integer> actual = new ArrayList<>();
        for (Split split : splits) {
            Assert.assertEquals(rustExpected, capabilities.canRead((DataSplit) split));
            try (RecordReader<InternalRow> reader = table.newRead().createReader(split)) {
                reader.forEachRemaining(row -> actual.add(row.getInt(0)));
            }
        }
        Assert.assertEquals(Collections.singletonList(7), actual);
    }
}
