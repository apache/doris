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

package org.apache.doris.datasource.iceberg.action;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergMetadataOps;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class IcebergExpireSnapshotsActionTest {
    private InMemoryCatalog catalog;
    private Table table;
    private IcebergExpireSnapshotsAction action;
    private boolean failNextRead;
    private final IOException readFailure = new IOException("injected manifest read failure");

    @BeforeEach
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("expiration", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        table = catalog.createTable(TableIdentifier.of("db1", "expiration"), new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "2"));
        action = new IcebergExpireSnapshotsAction(Collections.emptyMap(), Optional.empty(), Optional.empty(),
                Mockito.mock(IcebergMetadataOps.class));
    }

    @AfterEach
    public void tearDown() throws IOException {
        catalog.close();
    }

    @Test
    public void readsSharedDeleteManifestsOncePerCallAcrossPartitionSpecs() throws Exception {
        appendBaseFile();
        table.newRowDelta().addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
                .withPath("s3://b/db1/position-delete.parquet").withReferencedDataFile("s3://b/db1/base.parquet")
                .withFileSizeInBytes(64).withRecordCount(1).withFormat(FileFormat.PARQUET).build()).commit();

        table.updateSpec().addField("id").commit();
        appendPartitionedFile("partitioned.parquet");
        table.newRowDelta().addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofEqualityDeletes(1)
                .withPath("s3://b/db1/equality-delete.parquet").withPartitionPath("id=1")
                .withFileSizeInBytes(64).withRecordCount(1).withFormat(FileFormat.PARQUET).build()).commit();
        appendPartitionedFile("next.parquet");
        appendPartitionedFile("last.parquet");

        Set<String> manifestPaths = new HashSet<>();
        Set<Integer> specIds = new HashSet<>();
        int manifestReferences = 0;
        for (Snapshot snapshot : table.snapshots()) {
            for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
                manifestPaths.add(manifest.path());
                specIds.add(manifest.partitionSpecId());
                manifestReferences++;
            }
        }
        Assertions.assertTrue(manifestReferences > manifestPaths.size(), "snapshots must share delete manifests");
        Assertions.assertEquals(2, specIds.size(), "both partition specs must have delete manifests");

        Map<String, Integer> streamOpens = new HashMap<>();
        Table recordedTable = recordManifestOpens(streamOpens);
        Map<String, FileContent> expected = ImmutableMap.of(
                "s3://b/db1/position-delete.parquet", FileContent.POSITION_DELETES,
                "s3://b/db1/equality-delete.parquet", FileContent.EQUALITY_DELETES);
        Assertions.assertEquals(expected, action.buildDeleteFileContentMap(recordedTable));
        Assertions.assertEquals(manifestPaths, streamOpens.keySet());
        for (String path : manifestPaths) {
            Assertions.assertEquals(1, streamOpens.get(path).intValue(), path);
        }

        Assertions.assertEquals(expected, action.buildDeleteFileContentMap(recordedTable));
        for (String path : manifestPaths) {
            Assertions.assertEquals(2, streamOpens.get(path).intValue(), "a new call must reopen " + path);
        }
    }

    @Test
    public void handlesTablesWithoutSnapshotsOrDeleteManifests() throws Exception {
        Map<String, Integer> streamOpens = new HashMap<>();
        Assertions.assertTrue(action.buildDeleteFileContentMap(recordManifestOpens(streamOpens)).isEmpty());
        appendBaseFile();
        Assertions.assertTrue(action.buildDeleteFileContentMap(recordManifestOpens(streamOpens)).isEmpty());
        Assertions.assertTrue(streamOpens.isEmpty());
    }

    @Test
    public void propagatesReadFailuresAndReopensTheManifestOnRetry() throws Exception {
        appendBaseFile();
        table.newRowDelta().addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofEqualityDeletes(1)
                .withPath("s3://b/db1/equality-delete.parquet")
                .withFileSizeInBytes(64).withRecordCount(1).withFormat(FileFormat.PARQUET).build()).commit();
        Map<String, Integer> streamOpens = new HashMap<>();
        Table recordedTable = recordManifestOpens(streamOpens);
        failNextRead = true;

        UserException failure = Assertions.assertThrows(UserException.class,
                () -> action.buildDeleteFileContentMap(recordedTable));
        Assertions.assertTrue(failure.getMessage().contains("Failed to build delete file content map"));
        Assertions.assertSame(readFailure, Throwables.getRootCause(failure));
        Assertions.assertEquals(ImmutableMap.of("s3://b/db1/equality-delete.parquet", FileContent.EQUALITY_DELETES),
                action.buildDeleteFileContentMap(recordedTable));
        Assertions.assertEquals(1, streamOpens.size());
        Assertions.assertEquals(2, streamOpens.values().iterator().next().intValue());
    }

    private void appendBaseFile() {
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath("s3://b/db1/base.parquet")
                .withFileSizeInBytes(64).withRecordCount(1).withFormat(FileFormat.PARQUET).build()).commit();
    }

    private void appendPartitionedFile(String name) {
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath("s3://b/db1/" + name)
                .withPartitionPath("id=1").withFileSizeInBytes(64)
                .withRecordCount(1).withFormat(FileFormat.PARQUET).build()).commit();
    }

    private Table recordManifestOpens(Map<String, Integer> streamOpens) {
        FileIO recordingIo = Mockito.spy(table.io());
        Mockito.doAnswer(invocation -> {
            InputFile input = table.io().newInputFile(invocation.getArgument(0, ManifestFile.class));
            InputFile recordingInput = Mockito.spy(input);
            Mockito.doAnswer(open -> {
                streamOpens.merge(input.location(), 1, Integer::sum);
                if (failNextRead) {
                    failNextRead = false;
                    throw new UncheckedIOException(readFailure);
                }
                return input.newStream();
            }).when(recordingInput).newStream();
            return recordingInput;
        }).when(recordingIo).newInputFile(Mockito.any(ManifestFile.class));
        Table recordedTable = Mockito.spy(table);
        Mockito.doReturn(recordingIo).when(recordedTable).io();
        return recordedTable;
    }
}
