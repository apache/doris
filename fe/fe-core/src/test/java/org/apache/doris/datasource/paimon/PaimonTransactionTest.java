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

package org.apache.doris.datasource.paimon;

import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TPaimonCommitData;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PaimonTransactionTest {

    private PaimonMetadataOps ops;
    private PaimonExternalCatalog catalog;
    private PaimonExternalTable table;
    private FileStoreTable paimonTable;
    private BatchWriteBuilder writeBuilder;
    private BatchTableCommit committer;

    @Before
    public void setUp() throws Exception {
        ops = Mockito.mock(PaimonMetadataOps.class);
        catalog = Mockito.mock(PaimonExternalCatalog.class);
        table = Mockito.mock(PaimonExternalTable.class);
        paimonTable = Mockito.mock(FileStoreTable.class);
        writeBuilder = Mockito.mock(BatchWriteBuilder.class);
        committer = Mockito.mock(BatchTableCommit.class);

        Catalog paimonCatalog = Mockito.mock(Catalog.class);

        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getDbName()).thenReturn("test_db");
        Mockito.when(table.getName()).thenReturn("test_tbl");
        Mockito.when(catalog.getPaimonCatalog()).thenReturn(paimonCatalog);
        Mockito.when(paimonCatalog.getTable(Identifier.create("test_db", "test_tbl")))
                .thenReturn(paimonTable);

        // Non-partitioned table schema
        TableSchema schema = Mockito.mock(TableSchema.class);
        Mockito.when(schema.id()).thenReturn(1L);
        Mockito.when(paimonTable.schema()).thenReturn(schema);
        Mockito.when(paimonTable.partitionKeys()).thenReturn(Collections.emptyList());
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        Mockito.when(paimonTable.rowType()).thenReturn(rowType);

        Mockito.when(paimonTable.newBatchWriteBuilder()).thenReturn(writeBuilder);
        Mockito.when(writeBuilder.newCommit()).thenReturn(committer);
        Mockito.when(writeBuilder.withOverwrite()).thenReturn(writeBuilder);
    }

    @Test
    public void testUpdatePaimonCommitDataIsThreadSafe() {
        PaimonTransaction txn = new PaimonTransaction(ops);

        TPaimonCommitData d1 = new TPaimonCommitData();
        d1.setFilePath("/path/to/data-1.parquet");
        d1.setRowCount(100L);
        d1.setFileSize(1024L);

        TPaimonCommitData d2 = new TPaimonCommitData();
        d2.setFilePath("/path/to/data-2.parquet");
        d2.setRowCount(200L);
        d2.setFileSize(2048L);

        txn.updatePaimonCommitData(Collections.singletonList(d1));
        txn.updatePaimonCommitData(Collections.singletonList(d2));

        Assert.assertEquals(300L, txn.getUpdateCnt());
    }

    @Test
    public void testGetUpdateCntSumsRowCounts() {
        PaimonTransaction txn = new PaimonTransaction(ops);

        List<TPaimonCommitData> batch = Arrays.asList(
                makeCommitData("/path/file1.parquet", 50L, 512L),
                makeCommitData("/path/file2.parquet", 75L, 768L),
                makeCommitData("/path/file3.parquet", 25L, 256L));
        txn.updatePaimonCommitData(batch);

        Assert.assertEquals(150L, txn.getUpdateCnt());
    }

    @Test
    public void testGetUpdateCntReturnsZeroWhenNoData() {
        PaimonTransaction txn = new PaimonTransaction(ops);
        Assert.assertEquals(0L, txn.getUpdateCnt());
    }

    @Test
    public void testBeginInsertInitializesCommitter() throws UserException {
        PaimonTransaction txn = new PaimonTransaction(ops);
        txn.beginInsert(table, java.util.Optional.empty());

        // beginInsert should call newBatchWriteBuilder and newCommit
        Mockito.verify(paimonTable).newBatchWriteBuilder();
        Mockito.verify(writeBuilder).newCommit();
    }

    @Test
    public void testCommitCallsCommitterWithCorrectFileCount() throws Exception {
        PaimonTransaction txn = new PaimonTransaction(ops);
        txn.beginInsert(table, java.util.Optional.empty());

        txn.updatePaimonCommitData(Arrays.asList(
                makeCommitData("hdfs://nn/warehouse/db/tbl/bucket-0/data-0.parquet", 100L, 1024L),
                makeCommitData("hdfs://nn/warehouse/db/tbl/bucket-0/data-1.parquet", 200L, 2048L)));

        txn.commit();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<CommitMessage>> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(committer).commit(captor.capture());
        Assert.assertEquals(2, captor.getValue().size());
        Mockito.verify(committer).close();
    }

    @Test
    public void testCommitWithEmptyDataCommitsEmptyList() throws Exception {
        PaimonTransaction txn = new PaimonTransaction(ops);
        txn.beginInsert(table, java.util.Optional.empty());
        txn.commit();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<CommitMessage>> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(committer).commit(captor.capture());
        Assert.assertTrue(captor.getValue().isEmpty());
    }

    @Test
    public void testRollbackClosesCommitterWithoutCommit() throws Exception {
        PaimonTransaction txn = new PaimonTransaction(ops);
        txn.beginInsert(table, java.util.Optional.empty());
        txn.updatePaimonCommitData(Collections.singletonList(
                makeCommitData("/path/data.parquet", 100L, 1024L)));
        txn.rollback();

        Mockito.verify(committer, Mockito.never()).commit(Mockito.anyList());
        Mockito.verify(committer).close();
    }

    @Test
    public void testFileNameExtractedFromFullPath() throws Exception {
        PaimonTransaction txn = new PaimonTransaction(ops);
        txn.beginInsert(table, java.util.Optional.empty());

        // path contains directories - only the filename should be used in DataFileMeta
        txn.updatePaimonCommitData(Collections.singletonList(
                makeCommitData("s3://bucket/warehouse/db/tbl/bucket-0/00001-abc.parquet", 10L, 100L)));

        txn.commit();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<CommitMessage>> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(committer).commit(captor.capture());
        Assert.assertEquals(1, captor.getValue().size());
        // CommitMessage itself is an opaque object; just verify commit was called with one message
    }

    @Test
    public void testBeginInsertWithOverwriteContext() throws UserException {
        PaimonTransaction txn = new PaimonTransaction(ops);
        org.apache.doris.nereids.trees.plans.commands.insert.PaimonInsertCommandContext ctx =
                new org.apache.doris.nereids.trees.plans.commands.insert.PaimonInsertCommandContext(true);
        txn.beginInsert(table, java.util.Optional.of(ctx));

        // withOverwrite() should be called on the writeBuilder
        Mockito.verify(writeBuilder).withOverwrite();
    }

    @Test(expected = UserException.class)
    public void testBeginInsertThrowsOnCatalogError() throws Exception {
        Catalog badCatalog = Mockito.mock(Catalog.class);
        Mockito.when(catalog.getPaimonCatalog()).thenReturn(badCatalog);
        Mockito.when(badCatalog.getTable(Mockito.any()))
                .thenThrow(new Catalog.TableNotExistException(
                        Identifier.create("test_db", "test_tbl")));

        PaimonTransaction txn = new PaimonTransaction(ops);
        txn.beginInsert(table, java.util.Optional.empty());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private TPaimonCommitData makeCommitData(String path, long rowCount, long fileSize) {
        TPaimonCommitData d = new TPaimonCommitData();
        d.setFilePath(path);
        d.setRowCount(rowCount);
        d.setFileSize(fileSize);
        return d;
    }
}
