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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergInsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;
import org.apache.doris.thrift.TIcebergDeleteSink;
import org.apache.doris.thrift.TIcebergMergeSink;
import org.apache.doris.thrift.TIcebergTableSink;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

class IcebergCommitLocationTest {
    private static final String LOCATION = "abfss://container@account.dfs.core.windows.net/custom/data";
    private static final String NORMALIZED_LOCATION = "s3://container/custom/data";

    enum WriteKind {
        INSERT, OVERWRITE, DYNAMIC_OVERWRITE, DELETE, MERGE, REWRITE
    }

    @ParameterizedTest
    @EnumSource(WriteKind.class)
    void testAcceptRawLocationWithAndWithoutTrailingSlash(WriteKind kind) throws Exception {
        for (String location : Arrays.asList(LOCATION, LOCATION + "/")) {
            try (Fixture fixture = new Fixture(kind)) {
                fixture.txn.addCommitLocations(sink(kind, location, location));
                fixture.txn.updateIcebergCommitData(Collections.singletonList(
                        file(LOCATION + "/p=1/file.parquet", kind)));
                fixture.finish();
                fixture.txn.commit();
                Mockito.verify(fixture.nativeTxn).commitTransaction();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(WriteKind.class)
    void testRejectEntireBatchBeforeStagingAnyFiles(WriteKind kind) throws Exception {
        for (String path : Arrays.asList(NORMALIZED_LOCATION + "/file.parquet",
                LOCATION + "2/file.parquet", LOCATION, "file.parquet")) {
            try (Fixture fixture = new Fixture(kind)) {
                fixture.txn.addCommitLocations(sink(kind, LOCATION, LOCATION));
                fixture.txn.updateIcebergCommitData(Arrays.asList(
                        file(LOCATION + "/valid.parquet", kind), file(path, kind)));
                RuntimeException error = Assertions.assertThrows(RuntimeException.class, fixture::finish);
                Assertions.assertTrue(error.getMessage().contains("outside the sink locations"));
                Assertions.assertTrue(error.getMessage().contains(path));
                Mockito.verifyNoInteractions(fixture.nativeTxn);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(WriteKind.class)
    void testRejectMissingFilePath(WriteKind kind) throws Exception {
        for (String path : Arrays.asList(null, "")) {
            try (Fixture fixture = new Fixture(kind)) {
                fixture.txn.addCommitLocations(sink(kind, LOCATION, LOCATION));
                fixture.txn.updateIcebergCommitData(Collections.singletonList(file(path, kind)));
                RuntimeException error = Assertions.assertThrows(RuntimeException.class, fixture::finish);
                Assertions.assertTrue(error.getMessage().contains("missing its file path"));
                Mockito.verifyNoInteractions(fixture.nativeTxn);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(WriteKind.class)
    void testRejectFilesWhenSinkWasNotRegistered(WriteKind kind) throws Exception {
        try (Fixture fixture = new Fixture(kind)) {
            fixture.txn.updateIcebergCommitData(Collections.singletonList(file(LOCATION + "/file.parquet", kind)));
            RuntimeException error = Assertions.assertThrows(RuntimeException.class, fixture::finish);
            Assertions.assertTrue(error.getMessage().contains("outside the sink locations"));
            Mockito.verifyNoInteractions(fixture.nativeTxn);
        }
    }

    @ParameterizedTest
    @EnumSource(WriteKind.class)
    void testRejectMissingRawSinkLocation(WriteKind kind) {
        IcebergTransaction txn = new IcebergTransaction(Mockito.mock(IcebergMetadataOps.class));
        for (String location : Arrays.asList(null, "")) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> txn.addCommitLocations(sink(kind, location, location)));
        }
    }

    @Test
    void testMergeRegistersBothRawLocations() throws Exception {
        try (Fixture fixture = new Fixture(WriteKind.MERGE)) {
            fixture.txn.addCommitLocations(sink(WriteKind.MERGE, LOCATION + "/insert", LOCATION + "/delete/"));
            fixture.txn.updateIcebergCommitData(Arrays.asList(
                    file(LOCATION + "/insert/data.parquet", WriteKind.INSERT),
                    file(LOCATION + "/delete/pos.parquet", WriteKind.DELETE)));
            fixture.finish();
            fixture.txn.commit();
            Mockito.verify(fixture.nativeTxn).commitTransaction();
        }
    }

    @Test
    void testRewriteAccumulatesLocationsFromAllGroups() throws Exception {
        try (Fixture fixture = new Fixture(WriteKind.REWRITE)) {
            fixture.txn.addCommitLocations(sink(WriteKind.REWRITE, LOCATION + "/first", LOCATION));
            fixture.txn.addCommitLocations(sink(WriteKind.REWRITE, LOCATION + "/second", LOCATION));
            fixture.txn.updateIcebergCommitData(Arrays.asList(
                    file(LOCATION + "/first/data.parquet", WriteKind.REWRITE),
                    file(LOCATION + "/second/data.parquet", WriteKind.REWRITE)));
            fixture.finish();
            fixture.txn.commit();
            Mockito.verify(fixture.nativeTxn).commitTransaction();
        }
    }

    private static TDataSink sink(WriteKind kind, String dataLocation, String deleteLocation) {
        switch (kind) {
            case DELETE:
                return new TDataSink(TDataSinkType.ICEBERG_DELETE_SINK).setIcebergDeleteSink(
                        new TIcebergDeleteSink().setTableLocation(deleteLocation)
                                .setOutputPath(NORMALIZED_LOCATION));
            case MERGE:
                return new TDataSink(TDataSinkType.ICEBERG_MERGE_SINK).setIcebergMergeSink(
                        new TIcebergMergeSink().setOriginalOutputPath(dataLocation).setTableLocation(deleteLocation)
                                .setOutputPath(NORMALIZED_LOCATION));
            default:
                return new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK).setIcebergTableSink(
                        new TIcebergTableSink().setOriginalOutputPath(dataLocation)
                                .setOutputPath(NORMALIZED_LOCATION));
        }
    }

    private static TIcebergCommitData file(String path, WriteKind kind) {
        return new TIcebergCommitData().setFilePath(path).setRowCount(1).setFileSize(128)
                .setFileContent(kind == WriteKind.DELETE ? TFileContent.POSITION_DELETES : TFileContent.DATA);
    }

    private static class Fixture implements AutoCloseable {
        private final WriteKind kind;
        private final IcebergTransaction txn;
        private final org.apache.iceberg.Transaction nativeTxn = Mockito.mock(org.apache.iceberg.Transaction.class);
        private final MockedStatic<IcebergUtils> utils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS);

        Fixture(WriteKind kind) throws Exception {
            this.kind = kind;
            Table table = Mockito.mock(Table.class);
            PartitionSpec spec = PartitionSpec.unpartitioned();
            Mockito.when(table.schema()).thenReturn(new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get())));
            Mockito.when(table.spec()).thenReturn(spec);
            Mockito.when(table.specs()).thenReturn(Collections.singletonMap(spec.specId(), spec));
            Mockito.when(table.properties()).thenReturn(Collections.singletonMap(TableProperties.FORMAT_VERSION, "2"));
            Mockito.when(table.name()).thenReturn("db.table");
            Mockito.when(table.newTransaction()).thenReturn(nativeTxn);
            Mockito.when(nativeTxn.table()).thenReturn(table);
            AppendFiles append = Mockito.mock(AppendFiles.class, Mockito.RETURNS_SELF);
            Mockito.when(append.scanManifestsWith(Mockito.any())).thenReturn(append);
            Mockito.when(nativeTxn.newAppend()).thenReturn(append);
            ReplacePartitions overwrite = Mockito.mock(ReplacePartitions.class, Mockito.RETURNS_SELF);
            Mockito.when(overwrite.scanManifestsWith(Mockito.any())).thenReturn(overwrite);
            Mockito.when(nativeTxn.newReplacePartitions()).thenReturn(overwrite);
            OverwriteFiles wholeTableOverwrite = Mockito.mock(OverwriteFiles.class, Mockito.RETURNS_SELF);
            Mockito.when(wholeTableOverwrite.scanManifestsWith(Mockito.any())).thenReturn(wholeTableOverwrite);
            Mockito.when(nativeTxn.newOverwrite()).thenReturn(wholeTableOverwrite);
            Mockito.when(nativeTxn.newRowDelta()).thenReturn(Mockito.mock(RowDelta.class, Mockito.RETURNS_SELF));
            RewriteFiles rewrite = Mockito.mock(RewriteFiles.class, Mockito.RETURNS_SELF);
            Mockito.when(rewrite.scanManifestsWith(Mockito.any())).thenReturn(rewrite);
            Mockito.when(nativeTxn.newRewrite()).thenReturn(rewrite);

            IcebergMetadataOps ops = Mockito.mock(IcebergMetadataOps.class);
            IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
            Mockito.when(dorisTable.getName()).thenReturn("table");
            IcebergRuntimeContext runtime = Mockito.mock(IcebergRuntimeContext.class);
            Mockito.when(runtime.getAuthenticator()).thenReturn(new ExecutionAuthenticator() { });
            utils.when(() -> IcebergUtils.acquireWritableIcebergTable(dorisTable, ops))
                    .thenAnswer(invocation -> new IcebergExternalMetaCache.WritableTableLease(
                            table, ops, runtime, false, false, () -> { }));
            txn = new IcebergTransaction(ops);
            switch (kind) {
                case DELETE:
                    txn.beginDelete(dorisTable, table);
                    break;
                case MERGE:
                    txn.beginMerge(dorisTable, table, Optional.empty());
                    break;
                case REWRITE:
                    txn.beginRewrite(dorisTable, table);
                    break;
                default:
                    IcebergInsertCommandContext context = new IcebergInsertCommandContext();
                    context.setOverwrite(kind == WriteKind.OVERWRITE || kind == WriteKind.DYNAMIC_OVERWRITE);
                    txn.beginInsert(dorisTable, table, Optional.of(context),
                            kind == WriteKind.DYNAMIC_OVERWRITE ? "dynamic" : "static");
                    break;
            }
            Mockito.clearInvocations(nativeTxn);
        }

        void finish() {
            NameMapping name = NameMapping.createForTest("db", "table");
            switch (kind) {
                case DELETE:
                    txn.finishDelete(name);
                    break;
                case MERGE:
                    txn.finishMerge(name);
                    break;
                case REWRITE:
                    txn.finishRewrite();
                    break;
                default:
                    txn.finishInsert(name);
                    break;
            }
        }

        @Override
        public void close() {
            try {
                txn.rollback();
            } finally {
                utils.close();
            }
        }
    }
}
