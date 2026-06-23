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

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergColumnStats;
import org.apache.doris.thrift.TIcebergCommitData;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Pins the P6.3-T03 skeleton of {@link IcebergConnectorTransaction}: the single SDK
 * transaction held through the {@link IcebergCatalogOps} seam (auth-wrapped), the 14-field
 * {@link TIcebergCommitData} {@code TBinaryProtocol} round-trip accumulated in {@link #addCommitData},
 * the data/delete-split {@code getUpdateCnt}, and the commit/rollback shells.
 *
 * <p>Mirrors the no-Mockito, real-{@link InMemoryCatalog} style of {@code IcebergScanPlanProviderTest}.
 * Op selection (T04), commit-validation suite (T05), sink (T06) and capability dispatch (T07) are out
 * of scope here — T03 is the transaction skeleton only.</p>
 */
public class IcebergConnectorTransactionTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    private static InMemoryCatalog freshCatalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        return catalog;
    }

    private static RecordingIcebergCatalogOps opsReturning(Table table) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        return ops;
    }

    private static IcebergConnectorTransaction txnFor(RecordingIcebergCatalogOps ops, RecordingConnectorContext ctx) {
        return new IcebergConnectorTransaction(42L, ops, ctx);
    }

    private static DataFile dataFile(PartitionSpec spec, String path, long records) {
        return DataFiles.builder(spec)
                .withPath(path)
                .withFileSizeInBytes(1024)
                .withRecordCount(records)
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    /** A data (or delete) commit fragment serialized exactly as BE would send it. */
    private static byte[] commitBytes(TIcebergCommitData data) {
        try {
            return new TSerializer(new TBinaryProtocol.Factory()).serialize(data);
        } catch (TException e) {
            throw new AssertionError(e);
        }
    }

    private static TIcebergCommitData dataItem(long affectedRows, long rowCount, TFileContent content) {
        TIcebergCommitData d = new TIcebergCommitData();
        d.setRowCount(rowCount);
        if (affectedRows >= 0) {
            d.setAffectedRows(affectedRows);
        }
        if (content != null) {
            d.setFileContent(content);
        }
        return d;
    }

    // ─────────────────── addCommitData: 14-field TBinaryProtocol round-trip ───────────────────

    @Test
    public void addCommitDataRoundTripsAll14Fields() {
        // Guards parity-by-omission: every field BE puts on the wire must survive the connector's
        // TDeserializer accumulation, since T04/T05 build the iceberg DataFile/DeleteFile/Metrics from them.
        TIcebergColumnStats stats = new TIcebergColumnStats();
        stats.putToColumnSizes(1, 100L);
        stats.putToValueCounts(1, 10L);
        stats.putToNullValueCounts(1, 2L);
        stats.putToNanValueCounts(1, 0L);
        stats.putToLowerBounds(1, ByteBuffer.wrap(new byte[] {1}));
        stats.putToUpperBounds(1, ByteBuffer.wrap(new byte[] {9}));

        TIcebergCommitData d = new TIcebergCommitData();
        d.setFilePath("s3://b/db/t/f.parquet");          // 1
        d.setRowCount(123L);                              // 2
        d.setFileSize(4096L);                             // 3
        d.setFileContent(TFileContent.POSITION_DELETES); // 4
        d.setPartitionValues(Arrays.asList("a", "b"));   // 5
        d.setReferencedDataFiles(Arrays.asList("d1"));   // 6
        d.setColumnStats(stats);                         // 7
        d.setEqualityFieldIds(Arrays.asList(1, 2));      // 8
        d.setReferencedDataFilePath("s3://b/db/t/d1.parquet"); // 9
        d.setPartitionSpecId(7);                         // 10
        d.setPartitionDataJson("{\"p\":1}");           // 11
        d.setContentOffset(64L);                         // 12
        d.setContentSizeInBytes(256L);                   // 13
        d.setAffectedRows(99L);                          // 14

        IcebergConnectorTransaction txn = txnFor(opsReturning(null), new RecordingConnectorContext());
        txn.addCommitData(commitBytes(d));

        List<TIcebergCommitData> acc = txn.getCommitDataList();
        Assertions.assertEquals(1, acc.size());
        Assertions.assertEquals(d, acc.get(0),
                "every one of the 14 TIcebergCommitData fields (and the nested TIcebergColumnStats) "
                        + "must survive the TBinaryProtocol round-trip into the accumulator");
    }

    @Test
    public void addCommitDataAccumulatesInOrder() {
        IcebergConnectorTransaction txn = txnFor(opsReturning(null), new RecordingConnectorContext());
        txn.addCommitData(commitBytes(dataItem(1L, 1L, TFileContent.DATA)));
        txn.addCommitData(commitBytes(dataItem(2L, 2L, TFileContent.DATA)));
        Assertions.assertEquals(2, txn.getCommitDataList().size());
        Assertions.assertEquals(1L, txn.getCommitDataList().get(0).getAffectedRows());
        Assertions.assertEquals(2L, txn.getCommitDataList().get(1).getAffectedRows());
    }

    @Test
    public void addCommitDataFailsLoudOnMalformedBytes() {
        IcebergConnectorTransaction txn = txnFor(opsReturning(null), new RecordingConnectorContext());
        // Not a valid TBinaryProtocol-encoded TIcebergCommitData.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> txn.addCommitData(new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}));
    }

    // ─────────────────── getUpdateCnt: data/delete split, affectedRows priority ───────────────────

    @Test
    public void getUpdateCntSumsDataRows() {
        IcebergConnectorTransaction txn = txnFor(opsReturning(null), new RecordingConnectorContext());
        txn.addCommitData(commitBytes(dataItem(5L, 5L, TFileContent.DATA)));
        txn.addCommitData(commitBytes(dataItem(7L, 7L, TFileContent.DATA)));
        Assertions.assertEquals(12L, txn.getUpdateCnt());
    }

    @Test
    public void getUpdateCntFallsBackToRowCountWhenAffectedRowsUnset() {
        IcebergConnectorTransaction txn = txnFor(opsReturning(null), new RecordingConnectorContext());
        // affectedRows unset -> rowCount used.
        txn.addCommitData(commitBytes(dataItem(-1L, 8L, TFileContent.DATA)));
        Assertions.assertEquals(8L, txn.getUpdateCnt());
    }

    @Test
    public void getUpdateCntPrefersAffectedRowsOverRowCount() {
        IcebergConnectorTransaction txn = txnFor(opsReturning(null), new RecordingConnectorContext());
        txn.addCommitData(commitBytes(dataItem(3L, 999L, TFileContent.DATA)));
        Assertions.assertEquals(3L, txn.getUpdateCnt());
    }

    @Test
    public void getUpdateCntReturnsDeleteRowsWhenNoDataRows() {
        IcebergConnectorTransaction txn = txnFor(opsReturning(null), new RecordingConnectorContext());
        txn.addCommitData(commitBytes(dataItem(4L, 4L, TFileContent.POSITION_DELETES)));
        txn.addCommitData(commitBytes(dataItem(6L, 6L, TFileContent.DELETION_VECTOR)));
        Assertions.assertEquals(10L, txn.getUpdateCnt(),
                "with no data files, the affected count is the delete-file row sum");
    }

    @Test
    public void getUpdateCntDoesNotDoubleCountDeletesWhenDataRowsPresent() {
        // MERGE/UPDATE write both data and position-delete files; the affected count is the data rows
        // (position deletes are an internal MOR detail), mirroring legacy IcebergTransaction:577-596.
        IcebergConnectorTransaction txn = txnFor(opsReturning(null), new RecordingConnectorContext());
        txn.addCommitData(commitBytes(dataItem(5L, 5L, TFileContent.DATA)));
        txn.addCommitData(commitBytes(dataItem(5L, 5L, TFileContent.POSITION_DELETES)));
        Assertions.assertEquals(5L, txn.getUpdateCnt());
    }

    @Test
    public void getUpdateCntIsZeroForEmptyTransaction() {
        IcebergConnectorTransaction txn = txnFor(opsReturning(null), new RecordingConnectorContext());
        Assertions.assertEquals(0L, txn.getUpdateCnt());
    }

    // ─────────────────── identity / profile ───────────────────

    @Test
    public void carriesTransactionIdAndIcebergProfileLabel() {
        IcebergConnectorTransaction txn = new IcebergConnectorTransaction(
                7777L, opsReturning(null), new RecordingConnectorContext());
        Assertions.assertEquals(7777L, txn.getTransactionId());
        Assertions.assertEquals("ICEBERG", txn.profileLabel());
    }

    // ─────────────────── beginWrite: SDK txn opened through seam + auth ───────────────────

    @Test
    public void beginWriteOpensSdkTransactionThroughAuthWrappedSeam() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(
                TableIdentifier.of("db1", "t1"), SCHEMA, PartitionSpec.unpartitioned());
        RecordingIcebergCatalogOps ops = opsReturning(table);
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergConnectorTransaction txn = txnFor(ops, ctx);

        txn.beginWrite("db1", "t1");

        Assertions.assertEquals(1, ctx.authCount, "loadTable must run INSIDE executeAuthenticated");
        Assertions.assertTrue(ops.log.contains("loadTable:db1.t1"));
        Assertions.assertNotNull(txn.getTransaction(), "SDK transaction must be opened");
        Assertions.assertSame(table, txn.getTable());
    }

    @Test
    public void beginWriteFailsLoudWhenLoadTableThrows() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.throwOnLoadTable = true;
        IcebergConnectorTransaction txn = txnFor(ops, new RecordingConnectorContext());
        Assertions.assertThrows(DorisConnectorException.class, () -> txn.beginWrite("db1", "t1"));
    }

    @Test
    public void beginWriteRunsLoadTableInsideAuthenticator() {
        // failAuth proves the seam call sits INSIDE executeAuthenticated (the task must not run).
        RecordingIcebergCatalogOps ops = opsReturning(null);
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        IcebergConnectorTransaction txn = txnFor(ops, ctx);
        Assertions.assertThrows(DorisConnectorException.class, () -> txn.beginWrite("db1", "t1"));
        Assertions.assertFalse(ops.log.contains("loadTable:db1.t1"),
                "loadTable must not run when the authenticator throws first");
    }

    // ─────────────────── commit / rollback / close ───────────────────

    @Test
    public void commitFlushesStagedTransaction() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned());
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());

        txn.beginWrite("db1", "t1");
        // Simulate the T04 op: stage an append onto the held SDK transaction.
        txn.getTransaction().newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/f1.parquet", 10L))
                .commit();
        Assertions.assertNull(catalog.loadTable(id).currentSnapshot(),
                "staged append must NOT be visible before commitTransaction()");

        txn.commit();

        Assertions.assertNotNull(catalog.loadTable(id).currentSnapshot(),
                "commit() must flush the held SDK transaction (commitTransaction)");
    }

    @Test
    public void commitWithoutBeginFailsLoud() {
        IcebergConnectorTransaction txn = txnFor(opsReturning(null), new RecordingConnectorContext());
        Assertions.assertThrows(DorisConnectorException.class, txn::commit);
    }

    @Test
    public void rollbackAndCloseAreNoOps() {
        IcebergConnectorTransaction txn = txnFor(opsReturning(null), new RecordingConnectorContext());
        Assertions.assertDoesNotThrow(() -> {
            txn.rollback();
            txn.close();
        });
    }
}
