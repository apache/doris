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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class PaimonWriteTargetTest {
    @TempDir
    Path warehouse;

    private FileStoreTable main;
    private FileStoreTable branch;
    private RecordingPaimonCatalogOps ops;
    private PaimonWritePlanProvider provider;

    @BeforeEach
    public void createTable() throws Exception {
        org.apache.paimon.fs.Path location = new org.apache.paimon.fs.Path(warehouse.toUri().toString());
        LocalFileIO fileIO = LocalFileIO.create();
        new FileSystemSchemaManager(fileIO, location).createTable(Schema.newBuilder()
                .column("id", org.apache.paimon.types.DataTypes.INT())
                .option("bucket", "-1").build());
        main = FileStoreTableFactory.create(fileIO, location);
        main.createBranch("audit");
        branch = main.switchToBranch("audit");
        ops = new RecordingPaimonCatalogOps();
        ops.table = main;
        ops.branchTable = branch;
        ops.branchExists = true;
        provider = new PaimonWritePlanProvider(PaimonCatalogProperties.of(Collections.emptyMap()),
                ops, new RecordingConnectorContext());
    }

    @Test
    public void branchUsesLatestSchemaWithoutSnapshotAndIgnoresReadHandle() throws Exception {
        branch.schemaManager().commitChanges(Collections.singletonList(SchemaChange.renameColumn("id", "audit_id")));
        main.schemaManager().commitChanges(Collections.singletonList(SchemaChange.renameColumn("id", "main_id")));
        PaimonTableHandle read = source().withScanOptions(
                Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "123"));
        read.setPaimonTable(main);

        PaimonWriteTarget target = resolve(read, Optional.of("audit"));

        Assertions.assertEquals(Collections.singletonList("audit_id"), target.getColumns().stream()
                .map(ConnectorColumn::getName).collect(Collectors.toList()));
        Assertions.assertNull(target.getTable().snapshotManager().latestSnapshotId());
        Assertions.assertEquals("audit", CoreOptions.branch(target.getTable().options()));
        Assertions.assertTrue(target.getTableHandle().getScanOptions().isEmpty());
        Assertions.assertFalse(target.getTable().options().containsKey(CoreOptions.SCAN_SNAPSHOT_ID.key()));
        Assertions.assertDoesNotThrow(target::validateCurrentSchema);
    }

    @Test
    public void writeSchemaMayBeNewerThanTheBranchHeadSnapshot() throws Exception {
        append(branch, 1);
        long snapshotSchemaId = branch.snapshotManager().latestSnapshot().schemaId();
        branch.schemaManager().commitChanges(Collections.singletonList(SchemaChange.renameColumn("id", "audit_id")));
        PaimonWriteTarget target = resolve(source(), Optional.of("audit"));
        Assertions.assertEquals("audit_id", target.getColumns().get(0).getName());
        Assertions.assertTrue(target.getTable().schema().id() > snapshotSchemaId);
    }

    @Test
    public void mainAndImplicitMainResolveTheCurrentMainSchema() throws Exception {
        main.schemaManager().commitChanges(Collections.singletonList(SchemaChange.renameColumn("id", "main_id")));
        for (Optional<String> name : java.util.Arrays.asList(Optional.<String>empty(), Optional.of("main"))) {
            PaimonWriteTarget target = resolve(source().withBranch("audit"), name);
            Assertions.assertEquals(name, target.getBranchName());
            Assertions.assertEquals("main_id", target.getColumns().get(0).getName());
            Assertions.assertEquals("main", CoreOptions.branch(target.getTable().options()));
            Assertions.assertDoesNotThrow(target::validateCurrentSchema);
        }
    }

    @Test
    public void readBranchPolicyCannotRedirectTheWriteSchema() throws Exception {
        branch.schemaManager().commitChanges(Collections.singletonList(SchemaChange.renameColumn("id", "audit_id")));
        // A catalog lookup can return a primary-branch read decorator. Its schema manager belongs
        // to main, while the Identifier of the requested destination still names audit.
        Map<String, String> options = new HashMap<>(main.options());
        options.put(CoreOptions.SCAN_PRIMARY_BRANCH.key(), "main");
        ops.branchTable = main.copy(main.schema().copy(options));
        PaimonWriteTarget target = resolve(source(), Optional.of("audit"));
        Assertions.assertEquals("audit_id", target.getColumns().get(0).getName());
        Assertions.assertEquals("audit", CoreOptions.branch(target.getTable().options()));
        Assertions.assertFalse(target.getTable().options().containsKey(CoreOptions.SCAN_PRIMARY_BRANCH.key()));
        Assertions.assertDoesNotThrow(target::validateCurrentSchema);
    }

    @Test
    public void catalogRuntimeOptionsSurviveSchemaOnlyAlter() throws Exception {
        Map<String, String> options = new HashMap<>(branch.options());
        options.put(CoreOptions.FILE_FORMAT.key(), "orc");
        ops.branchTable = branch.copy(branch.schema().copy(options));
        branch.schemaManager().commitChanges(Collections.singletonList(SchemaChange.renameColumn("id", "audit_id")));
        PaimonWriteTarget target = resolve(source(), Optional.of("audit"));
        Assertions.assertEquals("audit_id", target.getColumns().get(0).getName());
        Assertions.assertEquals("orc", target.getTable().options().get(CoreOptions.FILE_FORMAT.key()));
        Assertions.assertDoesNotThrow(target::validateCurrentSchema);
    }

    @Test
    public void schemaDriftFailsBeforeWriting() throws Exception {
        PaimonWriteTarget target = resolve(source(), Optional.of("audit"));
        branch.schemaManager().commitChanges(Collections.singletonList(SchemaChange.renameColumn("id", "changed")));
        Assertions.assertThrows(DorisConnectorException.class, target::validateCurrentSchema);
        Assertions.assertEquals("id", target.getColumns().get(0).getName());
    }

    @Test
    public void missingBranchAndSystemTableAreRejected() {
        ops.branchExists = false;
        Assertions.assertThrows(DorisConnectorException.class,
                () -> resolve(source(), Optional.of("missing")));
        Assertions.assertThrows(DorisConnectorException.class, () -> resolve(
                PaimonTableHandle.forSystemTable("db", "tbl", "snapshots", false), Optional.empty()));
        Assertions.assertFalse(provider.resolveWriteTarget(null, source(), WriteOperation.UPDATE,
                Optional.empty()).isPresent());
    }

    @Test
    public void serializedWriterAndEmptyOverwriteCommitStayOnBranch() throws Exception {
        append(main, 1);
        append(branch, 2);
        Long mainSnapshot = main.snapshotManager().latestSnapshotId();
        Long branchSnapshot = branch.snapshotManager().latestSnapshotId();
        PaimonWriteTarget target = resolve(source(), Optional.of("audit"));
        ConnectorWriteHandle write = new ConnectorWriteHandle() {
            @Override
            public ConnectorTableHandle getTableHandle() {
                return target.getTableHandle();
            }

            @Override
            public List<ConnectorColumn> getColumns() {
                return target.getColumns();
            }

            @Override
            public Map<String, String> getStaticPartitionSpec() {
                return Collections.emptyMap();
            }

            @Override
            public boolean isOverwrite() {
                return true;
            }
        };
        PaimonWriteBinding binding = PaimonWriteBinding.create(target.getTableHandle(), target.getTable(),
                Collections.emptyMap(), write, true);
        FileStoreTable writer = InstantiationUtil.deserializeObject(
                Base64.getDecoder().decode(binding.getSerializedTable()), getClass().getClassLoader());
        Assertions.assertEquals("audit", CoreOptions.branch(writer.options()));
        Assertions.assertEquals(target.getTable().schemaManager().toSchemaPath(0),
                writer.schemaManager().toSchemaPath(0));
        PaimonConnectorTransaction transaction = new PaimonConnectorTransaction(123L, new RecordingConnectorContext());
        transaction.bind(binding);
        transaction.commit();
        Assertions.assertNotEquals(branchSnapshot, branch.snapshotManager().latestSnapshotId());
        Assertions.assertEquals(mainSnapshot, main.snapshotManager().latestSnapshotId());
    }

    private static void append(FileStoreTable table, int value) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            write.write(GenericRow.of(value));
            List<CommitMessage> messages = write.prepareCommit();
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(messages);
            }
        }
    }

    private PaimonWriteTarget resolve(PaimonTableHandle handle, Optional<String> name) {
        return (PaimonWriteTarget) provider.resolveWriteTarget(null, handle, WriteOperation.INSERT, name)
                .orElseThrow(AssertionError::new);
    }

    private static PaimonTableHandle source() {
        return new PaimonTableHandle("db", "tbl", Collections.emptyList(), Collections.emptyList());
    }
}
