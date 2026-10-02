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

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class DeltaConnectorTruncateTest {

    @TempDir
    Path tempDirectory;

    @Test
    public void testTruncatePreservesHistoryAndAllowsAppendAfterRepeatedTruncate() throws Exception {
        Path tablePath = copyPathTableFixture();
        DeltaConnector connector = connector(tablePath);
        DeltaConnectorMetadata metadata = connector.getMetadata(null);
        DeltaTableHandle before = tableHandle(metadata);
        List<String> originalFiles = scanFiles(connector, before);
        Assertions.assertEquals(List.of("part-00001.parquet", "part-00002.parquet"), originalFiles);

        metadata.truncateTable(null, before);

        DeltaTableHandle empty = tableHandle(metadata);
        Assertions.assertEquals(before.getSnapshotVersion() + 1, empty.getSnapshotVersion());
        Assertions.assertTrue(scanFiles(connector, empty).isEmpty());
        Assertions.assertEquals(metadata.getTableSchema(null, before).getColumns(),
                metadata.getTableSchema(null, empty).getColumns());
        ConnectorTableHandle historical = metadata.applyTableSnapshot(
                null, empty, DeltaTableSnapshot.version(before.getSnapshotVersion()));
        Assertions.assertEquals(originalFiles, scanFiles(connector, historical));
        Assertions.assertEquals(originalFiles, scanFiles(connector, before));

        metadata.truncateTable(null, empty);

        DeltaTableHandle stillEmpty = tableHandle(metadata);
        Assertions.assertEquals(empty.getSnapshotVersion(), stillEmpty.getSnapshotVersion());
        Assertions.assertTrue(scanFiles(connector, stillEmpty).isEmpty());
        appendFile(metadata, stillEmpty, tablePath.resolve("part-after-truncate.parquet"));

        DeltaTableHandle appended = tableHandle(metadata);
        Assertions.assertEquals(stillEmpty.getSnapshotVersion() + 1, appended.getSnapshotVersion());
        Assertions.assertEquals(List.of("part-after-truncate.parquet"), scanFiles(connector, appended));
        Assertions.assertEquals(originalFiles, scanFiles(connector, historical));
    }

    @Test
    public void testTruncateRejectsStaleSnapshotWithoutRemovingNewCommit() throws Exception {
        Path tablePath = copyPathTableFixture();
        DeltaConnector connector = connector(tablePath);
        DeltaConnectorMetadata metadata = connector.getMetadata(null);
        DeltaTableHandle stale = tableHandle(metadata);
        appendFile(metadata, stale, tablePath.resolve("part-new-commit.parquet"));
        DeltaTableHandle committed = tableHandle(metadata);
        List<String> committedFiles = scanFiles(connector, committed);

        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.truncateTable(null, stale));

        Assertions.assertTrue(failure.getMessage().contains("Delta table changed"));
        DeltaTableHandle afterFailure = tableHandle(metadata);
        Assertions.assertEquals(committed.getSnapshotVersion(), afterFailure.getSnapshotVersion());
        Assertions.assertEquals(committedFiles, scanFiles(connector, afterFailure));
    }

    @Test
    public void testTruncateRequiresSameBaselineWriterProtocolAsInsert() throws Exception {
        Path tablePath = copyPathTableFixture();
        Path firstCommit = tablePath.resolve("_delta_log/00000000000000000000.json");
        // Kernel accepts writer version 3 when no check constraints are configured, but
        // the Doris native writer deliberately supports only the baseline version 2 protocol.
        Files.writeString(firstCommit,
                Files.readString(firstCommit).replace("\"minWriterVersion\":2", "\"minWriterVersion\":3"));
        DeltaConnector connector = connector(tablePath);
        DeltaConnectorMetadata metadata = connector.getMetadata(null);
        DeltaTableHandle before = tableHandle(metadata);
        List<String> originalFiles = scanFiles(connector, before);

        UnsupportedOperationException insertFailure = Assertions.assertThrows(UnsupportedOperationException.class,
                () -> metadata.getWriteConfig(null, before,
                        metadata.getTableSchema(null, before).getColumns()));
        Assertions.assertTrue(insertFailure.getMessage().contains("baseline writer protocol"));
        UnsupportedOperationException truncateFailure = Assertions.assertThrows(UnsupportedOperationException.class,
                () -> metadata.truncateTable(null, before));
        Assertions.assertTrue(truncateFailure.getMessage().contains("baseline writer protocol"));

        DeltaTableHandle afterFailure = tableHandle(metadata);
        Assertions.assertEquals(before.getSnapshotVersion(), afterFailure.getSnapshotVersion());
        Assertions.assertEquals(originalFiles, scanFiles(connector, afterFailure));
    }

    private Path copyPathTableFixture() throws Exception {
        Path source = Paths.get(Objects.requireNonNull(getClass().getClassLoader()
                .getResource("delta/path_table/_delta_log")).toURI());
        Path tablePath = tempDirectory.resolve("events");
        Path target = Files.createDirectories(tablePath.resolve("_delta_log"));
        for (String file : List.of("00000000000000000000.json", "00000000000000000001.json")) {
            Files.copy(source.resolve(file), target.resolve(file));
        }
        return tablePath;
    }

    private static DeltaConnector connector(Path tablePath) {
        return new DeltaConnectorProvider().create(Map.of(
                "type", "delta",
                DeltaConnectorProperties.CATALOG_TYPE, DeltaConnectorProperties.CATALOG_TYPE_PATH,
                DeltaConnectorProperties.DATABASE, "default",
                DeltaConnectorProperties.TABLE, "events",
                DeltaConnectorProperties.TABLE_PATH, tablePath.toUri().toString(),
                DeltaConnectorProperties.WRITE_ENABLED, "true"), new ConnectorContext() {
                    @Override
                    public String getCatalogName() {
                        return "delta_truncate_test";
                    }

                    @Override
                    public long getCatalogId() {
                        return 1L;
                    }
                });
    }

    private static DeltaTableHandle tableHandle(DeltaConnectorMetadata metadata) {
        return (DeltaTableHandle) metadata.getTableHandle(null, "default", "events").orElseThrow();
    }

    private static List<String> scanFiles(DeltaConnector connector, ConnectorTableHandle handle) {
        return connector.getScanPlanProvider().planScan(null, handle, List.of(), Optional.empty())
                .stream().map(range -> Paths.get(URI.create(range.getPath().orElseThrow()))
                        .getFileName().toString())
                .sorted().collect(Collectors.toList());
    }

    private static void appendFile(DeltaConnectorMetadata metadata, ConnectorTableHandle handle,
            Path file) throws Exception {
        Files.write(file, new byte[] {1, 2, 3});
        DeltaInsertHandle insert = metadata.beginInsert(
                null, handle, metadata.getTableSchema(null, handle).getColumns());
        metadata.finishFileInsert(null, insert, List.of(new DeltaFileCommitInfo(
                file.toUri().toString(), 1L, Files.size(file),
                Files.getLastModifiedTime(file).toMillis(), Map.of())));
    }
}
