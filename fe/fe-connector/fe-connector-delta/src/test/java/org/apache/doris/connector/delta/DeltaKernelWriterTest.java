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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DeltaKernelWriterTest {

    @TempDir
    Path tempDirectory;

    @Test
    public void testCreatesVersionZeroTable() {
        Path tableDirectory = tempDirectory.resolve("created-table");
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaKernelWriter writer = new DeltaKernelWriter(engine);
        io.delta.kernel.types.StructType schema = DeltaTypeMapping.toDeltaSchema(List.of(
                new ConnectorColumn("id", ConnectorType.of("BIGINT"), "", false, null),
                new ConnectorColumn("payload", ConnectorType.of("STRING"), "", true, null)));

        DeltaKernelSnapshot snapshot = writer.createTable(
                tableDirectory.toUri().toString(), schema, Map.of("owner", "doris"));

        Assertions.assertEquals(0, snapshot.getVersion());
        Assertions.assertEquals(List.of("id", "payload"), snapshot.getSchema().fieldNames());
        Assertions.assertTrue(snapshot.getActiveFiles().isEmpty());
        Assertions.assertEquals("doris", snapshot.getTableProperties().get("owner"));
        Assertions.assertTrue(Files.exists(tableDirectory.resolve(
                "_delta_log/00000000000000000000.json")));
    }

    @Test
    public void testCreatesPartitionedVersionZeroTable() throws Exception {
        Path tableDirectory = tempDirectory.resolve("created-partitioned-table");
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaKernelWriter writer = new DeltaKernelWriter(engine);
        StructType schema = DeltaTypeMapping.toDeltaSchema(List.of(
                new ConnectorColumn("id", ConnectorType.of("BIGINT"), "", false, null),
                new ConnectorColumn("day", ConnectorType.of("STRING"), "", false, null)));

        DeltaKernelSnapshot snapshot = writer.createTable(
                tableDirectory.toUri().toString(), schema, Map.of(), List.of("day"));

        Assertions.assertEquals(List.of("day"), snapshot.getPartitionColumnNames());
        String commit = Files.readString(tableDirectory.resolve(
                "_delta_log/00000000000000000000.json"));
        Assertions.assertTrue(commit.contains("\"partitionColumns\":[\"day\"]"));
    }

    @Test
    public void testBlindAppendCommitsBackendDataFile() throws Exception {
        Path tableDirectory = copyPathTableFixture();
        Path dataFile = tableDirectory.resolve("part-doris.parquet");
        Files.write(dataFile, new byte[] {1, 2, 3, 4});
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaKernelWriter writer = new DeltaKernelWriter(engine);
        DeltaInsertHandle insert = writer.beginInsert(new DeltaTableHandle(
                "default", "events", tableDirectory.toUri().toString(), 1),
                "doris-query-123");

        writer.finishInsert(insert, List.of(new DeltaFileCommitInfo(
                dataFile.toUri().toString(), 2, Files.size(dataFile),
                Files.getLastModifiedTime(dataFile).toMillis(), Map.of())));

        DeltaKernelSnapshot snapshot = new DeltaKernelSnapshotLoader(engine)
                .loadLatest(tableDirectory.toUri().toString());
        Assertions.assertEquals(2, snapshot.getVersion());
        Assertions.assertTrue(snapshot.getActiveFiles().stream()
                .anyMatch(file -> file.getPath().endsWith("part-doris.parquet")));
        String commit = Files.readString(tableDirectory.resolve(
                "_delta_log/00000000000000000002.json"));
        Assertions.assertTrue(commit.contains("part-doris.parquet"));
        Assertions.assertTrue(commit.contains("Apache Doris native Delta connector"));
        Assertions.assertTrue(commit.contains("doris-query-123"));
    }

    @Test
    public void testEmptyAppendDoesNotCreateDeltaVersion() throws Exception {
        Path tableDirectory = copyPathTableFixture();
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaKernelWriter writer = new DeltaKernelWriter(engine);
        DeltaInsertHandle insert = writer.beginInsert(new DeltaTableHandle(
                "default", "events", tableDirectory.toUri().toString(), 1));

        writer.finishInsert(insert, List.of());

        Assertions.assertFalse(Files.exists(tableDirectory.resolve(
                "_delta_log/00000000000000000002.json")));
    }

    @Test
    public void testOverwriteReportsRemovedRowsFromDeltaFileStatistics() throws Exception {
        Path tableDirectory = tempDirectory.resolve("row-count-table");
        Path original = tableDirectory.resolve("part-original.parquet");
        Path replacement = tableDirectory.resolve("part-replacement.parquet");
        Files.createDirectories(tableDirectory);
        Files.write(original, new byte[] {1, 2, 3});
        Files.write(replacement, new byte[] {4, 5, 6});
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaKernelWriter writer = new DeltaKernelWriter(engine);
        StructType schema = DeltaTypeMapping.toDeltaSchema(List.of(
                new ConnectorColumn("id", ConnectorType.of("BIGINT"), "", false, null)));
        DeltaKernelSnapshot created = writer.createTable(
                tableDirectory.toUri().toString(), schema, Map.of());
        DeltaTableHandle createdHandle = new DeltaTableHandle(
                "default", "events", tableDirectory.toUri().toString(), created.getVersion());
        writer.finishInsert(writer.beginInsert(createdHandle), List.of(
                new DeltaFileCommitInfo(original.toUri().toString(), 3,
                        Files.size(original), Files.getLastModifiedTime(original).toMillis(),
                        Map.of())));
        DeltaKernelSnapshot before = new DeltaKernelSnapshotLoader(engine)
                .loadLatest(tableDirectory.toUri().toString());
        Assertions.assertEquals(3, before.getActiveRowCount().orElseThrow());

        DeltaInsertHandle overwrite = writer.beginOverwrite(
                new DeltaTableHandle("default", "events", tableDirectory.toUri().toString(),
                        before.getVersion()), before, null);
        writer.finishInsert(overwrite, List.of(
                new DeltaFileCommitInfo(replacement.toUri().toString(), 1,
                        Files.size(replacement),
                        Files.getLastModifiedTime(replacement).toMillis(), Map.of())));

        Assertions.assertEquals(3, overwrite.getOriginalRowCount().orElseThrow());
        DeltaKernelSnapshot after = new DeltaKernelSnapshotLoader(engine)
                .loadLatest(tableDirectory.toUri().toString());
        Assertions.assertEquals(1, after.getActiveRowCount().orElseThrow());
    }

    @Test
    public void testOverwriteAtomicallyReplacesActiveFiles() throws Exception {
        Path tableDirectory = copyPathTableFixture();
        Path replacement = tableDirectory.resolve("part-replacement.parquet");
        Files.write(replacement, new byte[] {7, 8, 9});
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaKernelWriter writer = new DeltaKernelWriter(engine);
        DeltaKernelSnapshot before = new DeltaKernelSnapshotLoader(engine)
                .loadLatest(tableDirectory.toUri().toString());
        DeltaTableHandle tableHandle = new DeltaTableHandle(
                "default", "events", tableDirectory.toUri().toString(), before.getVersion());

        DeltaInsertHandle overwrite = writer.beginOverwrite(
                tableHandle, before, "doris-overwrite-1");
        writer.finishInsert(overwrite, List.of(commitInfo(replacement, Map.of(), Set.of())));

        DeltaKernelSnapshot after = new DeltaKernelSnapshotLoader(engine)
                .loadLatest(tableDirectory.toUri().toString());
        Assertions.assertEquals(2, after.getVersion());
        Assertions.assertEquals(List.of("part-replacement.parquet"), after.getActiveFiles().stream()
                .map(file -> Paths.get(URI.create(file.getPath())).getFileName().toString())
                .collect(java.util.stream.Collectors.toList()));
        String commit = Files.readString(tableDirectory.resolve(
                "_delta_log/00000000000000000002.json"));
        Assertions.assertEquals(2, countOccurrences(commit, "\"remove\""));
        Assertions.assertEquals(1, countOccurrences(commit, "\"add\""));
    }

    @Test
    public void testOverwriteCanReplaceTableWithNoFiles() throws Exception {
        Path tableDirectory = copyPathTableFixture();
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaKernelWriter writer = new DeltaKernelWriter(engine);
        DeltaKernelSnapshot before = new DeltaKernelSnapshotLoader(engine)
                .loadLatest(tableDirectory.toUri().toString());
        DeltaTableHandle tableHandle = new DeltaTableHandle(
                "default", "events", tableDirectory.toUri().toString(), before.getVersion());

        writer.finishInsert(writer.beginOverwrite(tableHandle, before, null), List.of());

        DeltaKernelSnapshot after = new DeltaKernelSnapshotLoader(engine)
                .loadLatest(tableDirectory.toUri().toString());
        Assertions.assertEquals(2, after.getVersion());
        Assertions.assertTrue(after.getActiveFiles().isEmpty());
    }

    @Test
    public void testOverwriteRejectsConcurrentAppend() throws Exception {
        Path tableDirectory = copyPathTableFixture();
        Path concurrentFile = tableDirectory.resolve("part-concurrent.parquet");
        Path replacement = tableDirectory.resolve("part-stale-overwrite.parquet");
        Files.write(concurrentFile, new byte[] {1});
        Files.write(replacement, new byte[] {2});
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaKernelWriter writer = new DeltaKernelWriter(engine);
        DeltaKernelSnapshot before = new DeltaKernelSnapshotLoader(engine)
                .loadLatest(tableDirectory.toUri().toString());
        DeltaTableHandle tableHandle = new DeltaTableHandle(
                "default", "events", tableDirectory.toUri().toString(), before.getVersion());
        DeltaInsertHandle overwrite = writer.beginOverwrite(tableHandle, before, null);

        writer.finishInsert(writer.beginInsert(tableHandle),
                List.of(commitInfo(concurrentFile, Map.of(), Set.of())));

        Assertions.assertThrows(RuntimeException.class,
                () -> writer.finishInsert(overwrite,
                        List.of(commitInfo(replacement, Map.of(), Set.of()))));
        DeltaKernelSnapshot after = new DeltaKernelSnapshotLoader(engine)
                .loadLatest(tableDirectory.toUri().toString());
        Assertions.assertEquals(2, after.getVersion());
        Assertions.assertTrue(after.getActiveFiles().stream()
                .anyMatch(file -> file.getPath().endsWith("part-concurrent.parquet")));
        Assertions.assertFalse(after.getActiveFiles().stream()
                .anyMatch(file -> file.getPath().endsWith("part-stale-overwrite.parquet")));
    }

    @Test
    public void testPartitionedAppendPreservesOrderAndNullValues() throws Exception {
        Path tableDirectory = copyFixture(
                "delta/partitioned_table/_delta_log",
                List.of("00000000000000000000.json"));
        Path regularFile = tableDirectory.resolve("p2=two/p1=one/part-regular.parquet");
        Path nullFile = tableDirectory.resolve(
                "p2=__HIVE_DEFAULT_PARTITION__/p1=__HIVE_DEFAULT_PARTITION__/part-null.parquet");
        Files.createDirectories(regularFile.getParent());
        Files.createDirectories(nullFile.getParent());
        Files.write(regularFile, new byte[] {1, 2, 3});
        Files.write(nullFile, new byte[] {4, 5, 6});
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaKernelWriter writer = new DeltaKernelWriter(engine);
        DeltaInsertHandle insert = writer.beginInsert(new DeltaTableHandle(
                "default", "events", tableDirectory.toUri().toString(), 0));

        writer.finishInsert(insert, List.of(
                commitInfo(regularFile, Map.of("p1", "one", "p2", "two"), Set.of()),
                commitInfo(nullFile,
                        Map.of("p1", "__HIVE_DEFAULT_PARTITION__",
                                "p2", "__HIVE_DEFAULT_PARTITION__"),
                        Set.of("p2"))));

        DeltaKernelSnapshot snapshot = new DeltaKernelSnapshotLoader(engine)
                .loadLatest(tableDirectory.toUri().toString());
        Assertions.assertEquals(1, snapshot.getVersion());
        Assertions.assertTrue(snapshot.getActiveFiles().stream()
                .anyMatch(file -> file.getPath().contains("p2=two/p1=one/part-regular.parquet")));
        String commit = Files.readString(tableDirectory.resolve(
                "_delta_log/00000000000000000001.json"));
        Assertions.assertTrue(commit.contains("p2=two/p1=one/part-regular.parquet"));
        Assertions.assertTrue(commit.contains("\"p1\":\"__HIVE_DEFAULT_PARTITION__\""));
        Assertions.assertTrue(commit.contains("\"p2\":null"));
    }

    @Test
    public void testBeginInsertRejectsChangedSnapshot() throws Exception {
        Path tableDirectory = copyPathTableFixture();
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaKernelWriter writer = new DeltaKernelWriter(engine);

        Assertions.assertThrows(DorisConnectorException.class,
                () -> writer.beginInsert(new DeltaTableHandle(
                        "default", "events", tableDirectory.toUri().toString(), 0)));
    }

    private Path copyPathTableFixture() throws Exception {
        return copyFixture("delta/path_table/_delta_log", List.of(
                "00000000000000000000.json", "00000000000000000001.json"));
    }

    private DeltaFileCommitInfo commitInfo(Path file, Map<String, String> partitionValues,
            Set<String> nullPartitionColumns) throws Exception {
        return new DeltaFileCommitInfo(file.toUri().toString(), 1, Files.size(file),
                Files.getLastModifiedTime(file).toMillis(), partitionValues, nullPartitionColumns);
    }

    private static int countOccurrences(String value, String needle) {
        int count = 0;
        int offset = 0;
        while ((offset = value.indexOf(needle, offset)) >= 0) {
            count++;
            offset += needle.length();
        }
        return count;
    }

    private Path copyFixture(String resource, List<String> fileNames) throws Exception {
        URL fixture = Objects.requireNonNull(getClass().getClassLoader().getResource(resource));
        Path sourceLog = Paths.get(fixture.toURI());
        Path targetLog = tempDirectory.resolve("table-" + System.nanoTime()).resolve("_delta_log");
        Files.createDirectories(targetLog);
        for (String fileName : fileNames) {
            Files.copy(sourceLog.resolve(fileName), targetLog.resolve(fileName),
                    StandardCopyOption.REPLACE_EXISTING);
        }
        return targetLog.getParent();
    }
}
