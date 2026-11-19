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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.procedure.ConnectorRewriteGroup;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class IcebergRewriteSnapshotTest {
    private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "events");
    private static final String ORIGINAL = "s3://bucket/events/original.parquet";
    private static final String REPLACEMENT = "s3://bucket/events/replacement.parquet";
    private static final String UNRELATED = "s3://bucket/events/unrelated.parquet";

    @Test
    public void groupsShareTheOperationSnapshotAcrossAnUnrelatedAppend() throws Exception {
        Fixture fixture = new Fixture();
        String secondOriginal = "s3://bucket/events/second-original.parquet";
        String secondReplacement = "s3://bucket/events/second-replacement.parquet";
        Table table = fixture.currentTable();
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(secondOriginal)
                .withFormat(FileFormat.PARQUET).withFileSizeInBytes(1024L).withRecordCount(5L).build()).commit();
        long operationSnapshot = table.currentSnapshot().snapshotId();
        fixture.ops.table = table;
        IcebergTableHandle handle = new IcebergTableHandle("db", "events").withSnapshot(
                operationSnapshot, null, table.schema().schemaId());
        IcebergProcedureOps procedures = new IcebergProcedureOps(Collections.emptyMap(), fixture.ops,
                new RecordingConnectorContext());
        List<ConnectorRewriteGroup> groups = procedures.planRewrite(fixture, handle, "rewrite_data_files",
                Map.of("rewrite-all", "true", "max-file-group-size-bytes", "1024"), null, Collections.emptyList());
        Assertions.assertEquals(2, groups.size());

        fixture.begin(operationSnapshot);
        fixture.appendUnrelatedFile();
        fixture.begin(operationSnapshot);
        Set<String> sourceFiles = new HashSet<>();
        groups.forEach(group -> sourceFiles.addAll(group.getDataFilePaths()));
        fixture.transaction.registerRewriteSourceFiles(sourceFiles);
        fixture.addCommitFile(REPLACEMENT, 5L);
        fixture.addCommitFile(secondReplacement, 5L);
        fixture.transaction.commit();

        Assertions.assertEquals(operationSnapshot, fixture.transaction.getStartingSnapshotId());
        Assertions.assertEquals(new HashSet<>(Arrays.asList(REPLACEMENT, secondReplacement, UNRELATED)),
                fixture.currentPaths());
    }

    @Test
    public void rejectsDeleteBetweenReadAndBegin() throws Exception {
        Fixture fixture = new Fixture();
        fixture.deleteOriginalRow();
        fixture.begin(fixture.readSnapshot);
        fixture.addReplacement(5L);
        assertDeleteConflict(fixture.transaction);
        Assertions.assertEquals(Collections.singleton(ORIGINAL), fixture.currentPaths());
    }

    @Test
    public void rejectsDeleteAfterBegin() throws Exception {
        Fixture fixture = new Fixture();
        fixture.begin(fixture.readSnapshot);
        fixture.deleteOriginalRow();
        fixture.addReplacement(5L);
        assertDeleteConflict(fixture.transaction);
        Assertions.assertEquals(Collections.singleton(ORIGINAL), fixture.currentPaths());
    }

    @Test
    public void allowsDeleteAlreadyVisibleToRead() throws Exception {
        Fixture fixture = new Fixture();
        fixture.deleteOriginalRow();
        fixture.begin(fixture.currentSnapshot());
        fixture.addReplacement(4L);
        fixture.transaction.commit();
        Assertions.assertEquals(Collections.singleton(REPLACEMENT), fixture.currentPaths());
        Assertions.assertEquals("4", fixture.currentTable().currentSnapshot().summary().get("total-records"));
    }

    @Test
    public void allowsUnrelatedAppendBetweenReadAndBegin() throws Exception {
        Fixture fixture = new Fixture();
        fixture.appendUnrelatedFile();
        fixture.begin(fixture.readSnapshot);
        Assertions.assertEquals(fixture.readSnapshot, fixture.transaction.getStartingSnapshotId());
        fixture.addReplacement(5L);
        fixture.transaction.commit();
        Assertions.assertEquals(new HashSet<>(Arrays.asList(REPLACEMENT, UNRELATED)), fixture.currentPaths());
    }

    @Test
    public void rejectsOlderReadFromAnotherGroup() {
        Fixture fixture = new Fixture();
        fixture.deleteOriginalRow();
        fixture.begin(fixture.currentSnapshot());
        DorisConnectorException exception = Assertions.assertThrows(DorisConnectorException.class,
                () -> fixture.begin(fixture.readSnapshot));
        Assertions.assertTrue(exception.getMessage().contains("does not match starting snapshot"));
    }

    @Test
    public void rejectsNewerReadFromAnotherGroup() {
        Fixture fixture = new Fixture();
        fixture.begin(fixture.readSnapshot);
        fixture.appendUnrelatedFile();
        DorisConnectorException exception = Assertions.assertThrows(DorisConnectorException.class,
                () -> fixture.begin(fixture.currentSnapshot()));
        Assertions.assertTrue(exception.getMessage().contains("does not match starting snapshot"));
        Assertions.assertEquals(fixture.readSnapshot, fixture.transaction.getStartingSnapshotId());
    }

    @Test
    public void rejectsUnresolvedInitialRead() {
        Fixture fixture = new Fixture();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> fixture.transaction.beginWrite(fixture, "db", "events", unresolvedContext()));
        Assertions.assertNull(fixture.transaction.getTransaction());
    }

    @Test
    public void rejectsUnresolvedReadFromAnotherGroup() {
        Fixture fixture = new Fixture();
        fixture.begin(fixture.readSnapshot);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> fixture.transaction.beginWrite(fixture, "db", "events", unresolvedContext()));
    }

    @Test
    public void rejectsExpiredReadSnapshot() {
        Fixture fixture = new Fixture();
        fixture.appendUnrelatedFile();
        fixture.currentTable().expireSnapshots().expireSnapshotId(fixture.readSnapshot).commit();
        DorisConnectorException exception = Assertions.assertThrows(DorisConnectorException.class,
                () -> fixture.begin(fixture.readSnapshot));
        Assertions.assertTrue(exception.getMessage().contains("no longer available"));
    }

    @Test
    public void rejectsReadOutsideCurrentHistory() {
        Fixture fixture = new Fixture();
        fixture.appendUnrelatedFile();
        long abandonedSnapshot = fixture.currentSnapshot();
        fixture.currentTable().manageSnapshots().rollbackTo(fixture.readSnapshot).commit();
        DorisConnectorException exception = Assertions.assertThrows(DorisConnectorException.class,
                () -> fixture.begin(abandonedSnapshot));
        Assertions.assertTrue(exception.getMessage().contains("not an ancestor"));
    }

    @Test
    public void rejectsEmptyReadAfterFirstAppend() {
        Fixture fixture = new Fixture();
        DorisConnectorException exception = Assertions.assertThrows(DorisConnectorException.class,
                () -> fixture.begin(-1L));
        Assertions.assertTrue(exception.getMessage().contains("changed after the statement read an empty snapshot"));
    }

    private static IcebergWriteContext unresolvedContext() {
        return new IcebergWriteContext(WriteOperation.REWRITE, false, Collections.emptyMap(), Optional.empty());
    }

    private static void assertDeleteConflict(IcebergConnectorTransaction transaction) {
        DorisConnectorException exception = Assertions.assertThrows(DorisConnectorException.class, transaction::commit);
        Assertions.assertInstanceOf(ValidationException.class, exception.getCause());
        Assertions.assertTrue(exception.getCause().getMessage().contains("found new delete for replaced data file"),
                exception.toString());
    }

    private static final class Fixture implements ConnectorSession {
        private final InMemoryCatalog catalog = new InMemoryCatalog();
        private final RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        private final IcebergConnectorTransaction transaction =
                new IcebergConnectorTransaction(42L, ops, new RecordingConnectorContext());
        private final long readSnapshot;

        private Fixture() {
            catalog.initialize("test", Collections.emptyMap());
            catalog.createNamespace(Namespace.of("db"));
            Table table = catalog.createTable(IDENTIFIER,
                    new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                    PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "2"));
            table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(ORIGINAL)
                    .withFormat(FileFormat.PARQUET).withFileSizeInBytes(1024L).withRecordCount(5L).build()).commit();
            readSnapshot = table.currentSnapshot().snapshotId();
            ops.table = table;
            transaction.addCommitLocation("s3://bucket/events");
        }

        private Table currentTable() {
            return catalog.loadTable(IDENTIFIER);
        }

        private long currentSnapshot() {
            return currentTable().currentSnapshot().snapshotId();
        }

        private void begin(long snapshotId) {
            ops.table = currentTable();
            transaction.beginWrite(this, "db", "events", new IcebergWriteContext(WriteOperation.REWRITE,
                    false, Collections.emptyMap(), Optional.empty(), snapshotId, true));
        }

        private void deleteOriginalRow() {
            Table table = currentTable();
            table.newRowDelta().addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
                    .withPath("s3://bucket/events/delete.parquet").withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(64L).withRecordCount(1L).withReferencedDataFile(ORIGINAL).build()).commit();
        }

        private void appendUnrelatedFile() {
            Table table = currentTable();
            table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(UNRELATED)
                    .withFormat(FileFormat.PARQUET).withFileSizeInBytes(128L).withRecordCount(1L).build()).commit();
        }

        private void addReplacement(long recordCount) throws Exception {
            transaction.registerRewriteSourceFiles(Collections.singleton(ORIGINAL));
            addCommitFile(REPLACEMENT, recordCount);
        }

        private void addCommitFile(String path, long recordCount) throws Exception {
            // Only commit metadata is simulated; no physical data file is read or written by this fixture.
            TIcebergCommitData replacement = new TIcebergCommitData();
            replacement.setFilePath(path);
            replacement.setRowCount(recordCount);
            replacement.setFileSize(1024L);
            replacement.setFileContent(TFileContent.DATA);
            transaction.addCommitData(new TSerializer(new TBinaryProtocol.Factory()).serialize(replacement));
        }

        private Set<String> currentPaths() throws Exception {
            Set<String> paths = new HashSet<>();
            try (CloseableIterable<FileScanTask> tasks = currentTable().newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    paths.add(task.file().path().toString());
                }
            }
            return paths;
        }

        @Override
        public String getQueryId() {
            return "rewrite-snapshot-test";
        }

        @Override
        public String getUser() {
            return "test";
        }

        @Override
        public String getTimeZone() {
            return "UTC";
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public long getCatalogId() {
            return 42L;
        }

        @Override
        public String getCatalogName() {
            return "test";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }
    }
}
