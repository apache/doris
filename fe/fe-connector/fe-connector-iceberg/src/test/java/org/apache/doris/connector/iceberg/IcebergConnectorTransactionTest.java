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

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorBetween;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergColumnStats;
import org.apache.doris.thrift.TIcebergCommitData;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pins {@link IcebergConnectorTransaction}: the T03 skeleton (single SDK transaction held through the
 * {@link IcebergCatalogOps} seam, the 14-field {@link TIcebergCommitData} round-trip, the data/delete-split
 * {@code getUpdateCnt}) AND the T04 op selection (begin* guards + {@code commit()} dispatch onto
 * AppendFiles / ReplacePartitions / OverwriteFiles / RowDelta, ported from legacy {@code IcebergTransaction}).
 *
 * <p>Mirrors the no-Mockito, real-{@link InMemoryCatalog} style of {@code IcebergScanPlanProviderTest}.
 * The commit-validation suite (T05), sink (T06) and capability dispatch (T07) are out of scope here —
 * the RowDelta built here intentionally carries no conflict-detection validation (T05 adds it).</p>
 */
public class IcebergConnectorTransactionTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    private static final Schema PART_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "region", Types.StringType.get()));

    private static InMemoryCatalog freshCatalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        return catalog;
    }

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static RecordingIcebergCatalogOps opsReturning(Table table) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        return ops;
    }

    private static IcebergConnectorTransaction txnFor(RecordingIcebergCatalogOps ops, RecordingConnectorContext ctx) {
        return new IcebergConnectorTransaction(42L, ops, ctx);
    }

    private static final ConnectorSession SESSION = new FakeWriteSession("UTC");

    private static IcebergWriteContext insertCtx() {
        return new IcebergWriteContext(WriteOperation.INSERT, false, Collections.emptyMap(), Optional.empty());
    }

    private static IcebergWriteContext insertToBranch(String branch) {
        return new IcebergWriteContext(
                WriteOperation.INSERT, false, Collections.emptyMap(), Optional.of(branch));
    }

    private static IcebergWriteContext overwriteCtx() {
        return new IcebergWriteContext(WriteOperation.OVERWRITE, true, Collections.emptyMap(), Optional.empty());
    }

    private static IcebergWriteContext overwriteStaticCtx(Map<String, String> staticValues) {
        return new IcebergWriteContext(WriteOperation.OVERWRITE, true, staticValues, Optional.empty());
    }

    private static IcebergWriteContext deleteCtx() {
        return new IcebergWriteContext(WriteOperation.DELETE, false, Collections.emptyMap(), Optional.empty());
    }

    private static IcebergWriteContext mergeCtx() {
        return new IcebergWriteContext(WriteOperation.MERGE, false, Collections.emptyMap(), Optional.empty());
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

    private static TIcebergCommitData dataFileItem(String path, long rowCount, long fileSize) {
        TIcebergCommitData d = new TIcebergCommitData();
        d.setFilePath(path);
        d.setRowCount(rowCount);
        d.setFileSize(fileSize);
        d.setFileContent(TFileContent.DATA);
        return d;
    }

    private static TIcebergCommitData dataFileItem(String path, long rowCount, long fileSize, List<String> partVals) {
        TIcebergCommitData d = dataFileItem(path, rowCount, fileSize);
        d.setPartitionValues(partVals);
        return d;
    }

    private static TIcebergCommitData positionDeleteItem(String path, long rowCount, String referencedDataFile) {
        TIcebergCommitData d = new TIcebergCommitData();
        d.setFilePath(path);
        d.setRowCount(rowCount);
        d.setFileSize(512);
        d.setFileContent(TFileContent.POSITION_DELETES);
        d.setReferencedDataFilePath(referencedDataFile);
        return d;
    }

    private static Snapshot reloadCurrentSnapshot(InMemoryCatalog catalog, TableIdentifier id) {
        return catalog.loadTable(id).currentSnapshot();
    }

    // ─────────────────── addCommitData: 14-field TBinaryProtocol round-trip (T03, regression) ───────────────────

    @Test
    public void addCommitDataRoundTripsAll14Fields() {
        TIcebergColumnStats stats = new TIcebergColumnStats();
        stats.putToColumnSizes(1, 100L);
        stats.putToValueCounts(1, 10L);
        stats.putToNullValueCounts(1, 2L);
        stats.putToNanValueCounts(1, 0L);
        stats.putToLowerBounds(1, ByteBuffer.wrap(new byte[] {1}));
        stats.putToUpperBounds(1, ByteBuffer.wrap(new byte[] {9}));

        TIcebergCommitData d = new TIcebergCommitData();
        d.setFilePath("s3://b/db/t/f.parquet");
        d.setRowCount(123L);
        d.setFileSize(4096L);
        d.setFileContent(TFileContent.POSITION_DELETES);
        d.setPartitionValues(Arrays.asList("a", "b"));
        d.setReferencedDataFiles(Arrays.asList("d1"));
        d.setColumnStats(stats);
        d.setEqualityFieldIds(Arrays.asList(1, 2));
        d.setReferencedDataFilePath("s3://b/db/t/d1.parquet");
        d.setPartitionSpecId(7);
        d.setPartitionDataJson("{\"p\":1}");
        d.setContentOffset(64L);
        d.setContentSizeInBytes(256L);
        d.setAffectedRows(99L);

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
        Assertions.assertThrows(DorisConnectorException.class,
                () -> txn.addCommitData(new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}));
    }

    // ─────────────────── getUpdateCnt: data/delete split, affectedRows priority (T03, regression) ───────────────────

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
        Assertions.assertEquals(10L, txn.getUpdateCnt());
    }

    @Test
    public void getUpdateCntDoesNotDoubleCountDeletesWhenDataRowsPresent() {
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

    // ─────────────────── identity / profile (T03, regression) ───────────────────

    @Test
    public void carriesTransactionIdAndIcebergProfileLabel() {
        IcebergConnectorTransaction txn = new IcebergConnectorTransaction(
                7777L, opsReturning(null), new RecordingConnectorContext());
        Assertions.assertEquals(7777L, txn.getTransactionId());
        Assertions.assertEquals("ICEBERG", txn.profileLabel());
    }

    // ─────────────────── beginWrite: SDK txn opened through seam + auth (T03, now op-aware) ───────────────────

    @Test
    public void beginWriteOpensSdkTransactionThroughAuthWrappedSeam() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(
                TableIdentifier.of("db1", "t1"), SCHEMA, PartitionSpec.unpartitioned());
        RecordingIcebergCatalogOps ops = opsReturning(table);
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergConnectorTransaction txn = txnFor(ops, ctx);

        txn.beginWrite(SESSION, "db1", "t1", insertCtx());

        Assertions.assertEquals(1, ctx.authCount, "loadTable + newTransaction must run INSIDE executeAuthenticated");
        Assertions.assertTrue(ops.log.contains("loadTable:db1.t1"));
        Assertions.assertNotNull(txn.getTransaction(), "SDK transaction must be opened");
        Assertions.assertSame(table, txn.getTable());
    }

    @Test
    public void beginWriteFailsLoudWhenLoadTableThrows() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.throwOnLoadTable = true;
        IcebergConnectorTransaction txn = txnFor(ops, new RecordingConnectorContext());
        Assertions.assertThrows(DorisConnectorException.class,
                () -> txn.beginWrite(SESSION, "db1", "t1", insertCtx()));
    }

    @Test
    public void beginWriteRunsLoadTableInsideAuthenticator() {
        RecordingIcebergCatalogOps ops = opsReturning(null);
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        IcebergConnectorTransaction txn = txnFor(ops, ctx);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> txn.beginWrite(SESSION, "db1", "t1", insertCtx()));
        Assertions.assertFalse(ops.log.contains("loadTable:db1.t1"),
                "loadTable must not run when the authenticator throws first");
    }

    // ─────────────────── begin* guards (T04) ───────────────────

    @Test
    public void beginDeleteRejectsFormatVersion1Table() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"), SCHEMA,
                PartitionSpec.unpartitioned(), props("format-version", "1"));
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());
        // DELETE needs position deletes -> format-version >= 2 (legacy IcebergTransaction.beginDelete:291).
        Assertions.assertThrows(DorisConnectorException.class,
                () -> txn.beginWrite(SESSION, "db1", "t1", deleteCtx()));
    }

    @Test
    public void beginMergeRejectsFormatVersion1Table() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"), SCHEMA,
                PartitionSpec.unpartitioned(), props("format-version", "1"));
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());
        Assertions.assertThrows(DorisConnectorException.class,
                () -> txn.beginWrite(SESSION, "db1", "t1", mergeCtx()));
    }

    @Test
    public void beginInsertRejectsBranchThatIsATag() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned());
        // Seed a snapshot so a tag can be created, then tag it.
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/seed.parquet", 1L)).commit();
        Table reloaded = catalog.loadTable(id);
        reloaded.manageSnapshots().createTag("mytag", reloaded.currentSnapshot().snapshotId()).commit();

        IcebergConnectorTransaction txn = txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        // A tag cannot be a target for producing snapshots (legacy beginInsert:156).
        Assertions.assertThrows(DorisConnectorException.class,
                () -> txn.beginWrite(SESSION, "db1", "t1", insertToBranch("mytag")));
    }

    @Test
    public void beginInsertRejectsUnknownBranch() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"), SCHEMA, PartitionSpec.unpartitioned());
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());
        Assertions.assertThrows(DorisConnectorException.class,
                () -> txn.beginWrite(SESSION, "db1", "t1", insertToBranch("nope")));
    }

    @Test
    public void beginDeleteCapturesBaseSnapshotId() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), props("format-version", "2"));
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/seed.parquet", 3L)).commit();
        Table reloaded = catalog.loadTable(id);
        long expected = reloaded.currentSnapshot().snapshotId();

        IcebergConnectorTransaction txn = txnFor(opsReturning(reloaded), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", deleteCtx());

        // T05 consumes baseSnapshotId for validateFromSnapshot; T04 only captures it at begin time.
        Assertions.assertEquals(Long.valueOf(expected), txn.getBaseSnapshotId());
    }

    @Test
    public void beginInsertDoesNotCaptureBaseSnapshotId() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/seed.parquet", 1L)).commit();

        IcebergConnectorTransaction txn = txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", insertCtx());
        Assertions.assertNull(txn.getBaseSnapshotId(), "INSERT must not pin a base snapshot (append, not RowDelta)");
    }

    // ─────────────────── commit: op selection (T04) ───────────────────

    @Test
    public void insertAppendsDataFiles() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(),
                props("write.format.default", "parquet"));
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());

        txn.beginWrite(SESSION, "db1", "t1", insertCtx());
        txn.addCommitData(commitBytes(dataFileItem("s3://b/db1/t1/f1.parquet", 10L, 2048L)));
        Assertions.assertNull(reloadCurrentSnapshot(catalog, id), "nothing visible before commit()");

        txn.commit();

        Snapshot snap = reloadCurrentSnapshot(catalog, id);
        Assertions.assertNotNull(snap);
        Assertions.assertEquals("append", snap.operation());
        Assertions.assertEquals("1", snap.summary().get("added-data-files"));
    }

    @Test
    public void insertToBranchCommitsOnBranch() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/seed.parquet", 1L)).commit();
        Table reloaded = catalog.loadTable(id);
        reloaded.manageSnapshots().createBranch("b1", reloaded.currentSnapshot().snapshotId()).commit();

        IcebergConnectorTransaction txn = txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", insertToBranch("b1"));
        txn.addCommitData(commitBytes(dataFileItem("s3://b/db1/t1/f1.parquet", 5L, 1024L)));
        txn.commit();

        Table after = catalog.loadTable(id);
        // The branch advanced past the seed snapshot; main stayed on the seed.
        long branchSnap = after.snapshot(after.refs().get("b1").snapshotId()).snapshotId();
        Assertions.assertNotEquals(after.currentSnapshot().snapshotId(), branchSnap,
                "the append must land on branch b1, not on main");
    }

    @Test
    public void overwriteDynamicReplacesPartitions() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        PartitionSpec spec = PartitionSpec.builderFor(PART_SCHEMA).identity("region").build();
        Table table = catalog.createTable(id, PART_SCHEMA, spec, props("write.format.default", "parquet"));
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());

        txn.beginWrite(SESSION, "db1", "t1", overwriteCtx());
        txn.addCommitData(commitBytes(
                dataFileItem("s3://b/db1/t1/region=us/f1.parquet", 4L, 1024L, Collections.singletonList("us"))));
        txn.commit();

        Snapshot snap = reloadCurrentSnapshot(catalog, id);
        Assertions.assertNotNull(snap);
        // ReplacePartitions produces an overwrite snapshot with the new data file.
        Assertions.assertEquals("overwrite", snap.operation());
        Assertions.assertEquals("1", snap.summary().get("added-data-files"));
    }

    @Test
    public void overwriteEmptyUnpartitionedClearsTable() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(),
                props("write.format.default", "parquet"));
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/old.parquet", 9L)).commit();

        IcebergConnectorTransaction txn = txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        // INSERT OVERWRITE ... SELECT * FROM empty: no commit data, unpartitioned -> table is emptied.
        txn.beginWrite(SESSION, "db1", "t1", overwriteCtx());
        txn.commit();

        Snapshot snap = reloadCurrentSnapshot(catalog, id);
        // An OverwriteFiles that only removes files (no adds) is labelled "delete" by iceberg, not "overwrite".
        Assertions.assertEquals("delete", snap.operation());
        Assertions.assertEquals("1", snap.summary().get("deleted-data-files"),
                "the existing data file must be removed (table cleared)");
    }

    @Test
    public void overwriteStaticPartitionUsesRowFilter() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        PartitionSpec spec = PartitionSpec.builderFor(PART_SCHEMA).identity("region").build();
        Table table = catalog.createTable(id, PART_SCHEMA, spec, props("write.format.default", "parquet"));
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());

        // INSERT OVERWRITE ... PARTITION(region='us') -> OverwriteFiles.overwriteByRowFilter(region == 'us').
        txn.beginWrite(SESSION, "db1", "t1", overwriteStaticCtx(Collections.singletonMap("region", "us")));
        txn.addCommitData(commitBytes(
                dataFileItem("s3://b/db1/t1/region=us/f1.parquet", 4L, 1024L, Collections.singletonList("us"))));
        txn.commit();

        Snapshot snap = reloadCurrentSnapshot(catalog, id);
        Assertions.assertEquals("overwrite", snap.operation());
        Assertions.assertEquals("1", snap.summary().get("added-data-files"));
    }

    @Test
    public void deleteWritesRowDeltaDeleteFiles() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), props("format-version", "2"));
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());

        txn.beginWrite(SESSION, "db1", "t1", deleteCtx());
        txn.addCommitData(commitBytes(
                positionDeleteItem("s3://b/db1/t1/del.parquet", 3L, "s3://b/db1/t1/data.parquet")));
        txn.commit();

        Snapshot snap = reloadCurrentSnapshot(catalog, id);
        Assertions.assertNotNull(snap);
        Assertions.assertEquals("1", snap.summary().get("added-delete-files"),
                "DELETE must add a position-delete file via RowDelta");
    }

    @Test
    public void mergeWritesRowDeltaDataAndDeleteFiles() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), props("format-version", "2"));
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());

        txn.beginWrite(SESSION, "db1", "t1", mergeCtx());
        txn.addCommitData(commitBytes(dataFileItem("s3://b/db1/t1/new.parquet", 2L, 1024L)));
        txn.addCommitData(commitBytes(
                positionDeleteItem("s3://b/db1/t1/del.parquet", 2L, "s3://b/db1/t1/old.parquet")));
        txn.commit();

        Snapshot snap = reloadCurrentSnapshot(catalog, id);
        Assertions.assertNotNull(snap);
        Assertions.assertEquals("1", snap.summary().get("added-data-files"));
        Assertions.assertEquals("1", snap.summary().get("added-delete-files"));
    }

    @Test
    public void emptyInsertCommitsWithoutThrowing() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned());
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());

        txn.beginWrite(SESSION, "db1", "t1", insertCtx());
        // No commit data -> empty append; legacy still commits the (empty) transaction.
        Assertions.assertDoesNotThrow(txn::commit);
    }

    @Test
    public void emptyDeleteCommitsWithoutAddingDeleteFiles() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), props("format-version", "2"));
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());

        txn.beginWrite(SESSION, "db1", "t1", deleteCtx());
        // No delete commit data -> no RowDelta is built (legacy early-return), but commit() still flushes.
        Assertions.assertDoesNotThrow(txn::commit);
        Assertions.assertNull(reloadCurrentSnapshot(catalog, id), "an empty delete must not create a snapshot");
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

    // ─────────────────── commit-time conflict-detection validation suite (T05) ───────────────────

    @Test
    public void deleteDetectsConcurrentDataFileConflict() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), props("format-version", "2"));
        // seed snapshot S1 with one data file
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/seed.parquet", 5L)).commit();

        IcebergConnectorTransaction txn = txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", deleteCtx()); // baseSnapshotId pinned to S1

        // A concurrent writer appends a NEW data file -> snapshot S2, AFTER our transaction pinned S1.
        catalog.loadTable(id).newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/concurrent.parquet", 7L)).commit();

        txn.addCommitData(commitBytes(
                positionDeleteItem("s3://b/db1/t1/del.parquet", 2L, "s3://b/db1/t1/seed.parquet")));

        // validateFromSnapshot(S1) + serializable validateNoConflictingDataFiles detect the concurrent append.
        // Under T04 (no validation suite) this DELETE would silently win; the suite makes it fail loud.
        Assertions.assertThrows(DorisConnectorException.class, txn::commit,
                "a concurrent data-file append since the base snapshot must be detected as a conflict");
    }

    @Test
    public void deletePassesValidationSuiteWhenNoConcurrentChange() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), props("format-version", "2"));
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/seed.parquet", 5L)).commit();

        IcebergConnectorTransaction txn = txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", deleteCtx());
        txn.addCommitData(commitBytes(
                positionDeleteItem("s3://b/db1/t1/del.parquet", 2L, "s3://b/db1/t1/seed.parquet")));
        txn.commit();

        Snapshot snap = reloadCurrentSnapshot(catalog, id);
        Assertions.assertEquals("1", snap.summary().get("added-delete-files"),
                "with no concurrent change the full validation suite passes and the delete commits");
    }

    @Test
    public void isSerializableIsolationLevelDefaultsAndReadsProperty() {
        InMemoryCatalog catalog = freshCatalog();
        IcebergConnectorTransaction txn = txnFor(opsReturning(null), new RecordingConnectorContext());

        Table dflt = catalog.createTable(TableIdentifier.of("db1", "d"), SCHEMA, PartitionSpec.unpartitioned());
        Assertions.assertTrue(txn.isSerializableIsolationLevel(dflt), "missing property defaults to serializable");

        Table ser = catalog.createTable(TableIdentifier.of("db1", "s"), SCHEMA, PartitionSpec.unpartitioned(),
                props("delete_isolation_level", "serializable"));
        Assertions.assertTrue(txn.isSerializableIsolationLevel(ser));

        Table snap = catalog.createTable(TableIdentifier.of("db1", "n"), SCHEMA, PartitionSpec.unpartitioned(),
                props("delete_isolation_level", "snapshot"));
        Assertions.assertFalse(txn.isSerializableIsolationLevel(snap), "snapshot isolation is not serializable");
    }

    @Test
    public void collectReferencedDataFilesKeepsOnlyDeleteFragments() {
        IcebergConnectorTransaction txn = txnFor(opsReturning(null), new RecordingConnectorContext());

        TIcebergCommitData dataFrag = dataFileItem("s3://b/db1/t1/data.parquet", 3L, 1024L); // DATA -> ignored
        TIcebergCommitData posDelete = new TIcebergCommitData();
        posDelete.setFilePath("s3://b/db1/t1/pos.parquet");
        posDelete.setFileContent(TFileContent.POSITION_DELETES);
        posDelete.setReferencedDataFilePath("s3://b/db1/t1/ref-by-path.parquet");
        posDelete.setReferencedDataFiles(Arrays.asList("s3://b/db1/t1/ref-list.parquet", ""));

        List<String> refs = txn.collectReferencedDataFiles(Arrays.asList(dataFrag, posDelete));

        Assertions.assertEquals(
                Arrays.asList("s3://b/db1/t1/ref-list.parquet", "s3://b/db1/t1/ref-by-path.parquet"), refs,
                "only POSITION_DELETES/DELETION_VECTOR fragments contribute referenced files; "
                        + "both referenced_data_files (non-empty) and referenced_data_file_path are kept");
    }

    @Test
    public void shouldRewritePreviousDeleteFilesGatesOnFormatVersion3() {
        InMemoryCatalog catalog = freshCatalog();

        Table v2 = catalog.createTable(TableIdentifier.of("db1", "v2"), SCHEMA,
                PartitionSpec.unpartitioned(), props("format-version", "2"));
        IcebergConnectorTransaction t2 = txnFor(opsReturning(v2), new RecordingConnectorContext());
        t2.beginWrite(SESSION, "db1", "v2", deleteCtx());
        Assertions.assertFalse(t2.shouldRewritePreviousDeleteFiles(), "format-version 2 has no DV rewrite");

        Table v3 = catalog.createTable(TableIdentifier.of("db1", "v3"), SCHEMA,
                PartitionSpec.unpartitioned(), props("format-version", "3"));
        IcebergConnectorTransaction t3 = txnFor(opsReturning(v3), new RecordingConnectorContext());
        t3.beginWrite(SESSION, "db1", "v3", deleteCtx());
        Assertions.assertTrue(t3.shouldRewritePreviousDeleteFiles(), "format-version 3 enables DV rewrite");
    }

    @Test
    public void collectRewrittenDeleteFilesDedupsFileScopedDeletes() {
        IcebergConnectorTransaction txn = txnFor(opsReturning(null), new RecordingConnectorContext());
        PartitionSpec spec = PartitionSpec.unpartitioned();

        // Two distinct old file-scoped (referenced) delete files for one referenced data file, plus a dup.
        DeleteFile old1 = fileScopedDelete(spec, "s3://b/db1/t1/old-del-1.parquet", "s3://b/db1/t1/data-1.parquet");
        DeleteFile old1Dup = fileScopedDelete(spec, "s3://b/db1/t1/old-del-1.parquet", "s3://b/db1/t1/data-1.parquet");
        DeleteFile old2 = fileScopedDelete(spec, "s3://b/db1/t1/old-del-2.parquet", "s3://b/db1/t1/data-1.parquet");
        Map<String, List<DeleteFile>> map = new HashMap<>();
        map.put("s3://b/db1/t1/data-1.parquet", Arrays.asList(old1, old1Dup, old2));
        txn.setRewrittenDeleteFilesByReferencedDataFile(map);

        TIcebergCommitData newDelete = positionDeleteItem(
                "s3://b/db1/t1/new-del.parquet", 1L, "s3://b/db1/t1/data-1.parquet");

        List<DeleteFile> rewritten = txn.collectRewrittenDeleteFiles(Collections.singletonList(newDelete));

        Assertions.assertEquals(2, rewritten.size(), "the duplicate old delete file must be deduped away");

        // An empty rewrite map yields no rewritten files.
        IcebergConnectorTransaction empty = txnFor(opsReturning(null), new RecordingConnectorContext());
        Assertions.assertTrue(
                empty.collectRewrittenDeleteFiles(Collections.singletonList(newDelete)).isEmpty());
    }

    @Test
    public void applyWriteConstraintConvertedLazilyAtCommit() {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.builderFor(PART_SCHEMA).identity("region").build();
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"), PART_SCHEMA, spec);

        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());
        // O5-2: a target-only neutral predicate region = 'us'.
        ConnectorComparison eq = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("region", ConnectorType.of("UNKNOWN")),
                new ConnectorLiteral(ConnectorType.of("VARCHAR"), "us"));
        txn.applyWriteConstraint(new ConnectorPredicate(eq));

        Optional<Expression> expr = txn.buildWriteConstraintExpression(table);
        Assertions.assertTrue(expr.isPresent(), "the stashed write constraint is converted at commit time");
        Assertions.assertEquals(Expressions.equal("region", "us").toString(), expr.get().toString(),
                "applyWriteConstraint converts the neutral predicate via IcebergPredicateConverter");
    }

    @Test
    public void buildWriteConstraintExpressionEmptyWhenNoConstraint() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"), SCHEMA, PartitionSpec.unpartitioned());
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());

        Assertions.assertFalse(txn.buildWriteConstraintExpression(table).isPresent(),
                "no applyWriteConstraint -> no conflict-detection query filter");

        txn.applyWriteConstraint(new ConnectorPredicate(null));
        Assertions.assertFalse(txn.buildWriteConstraintExpression(table).isPresent(),
                "a null inner expression -> empty");
    }

    @Test
    public void buildWriteConstraintUsesConflictMatrixNotScanMatrix() {
        // T07b: the O5-2 path must convert in conflict mode, whose matrix differs from scan pushdown.
        // IS NULL / BETWEEN are *dropped* by scan pushdown but *pushed* for conflict detection; their
        // presence here proves buildWriteConstraintExpression selects conflict mode.
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(
                TableIdentifier.of("db1", "t1"), PART_SCHEMA, PartitionSpec.unpartitioned());
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());

        txn.applyWriteConstraint(new ConnectorPredicate(
                new ConnectorIsNull(new ConnectorColumnRef("region", ConnectorType.of("UNKNOWN")), false)));
        Assertions.assertEquals(Expressions.isNull("region").toString(),
                txn.buildWriteConstraintExpression(table).map(Expression::toString).orElse(null));

        txn.applyWriteConstraint(new ConnectorPredicate(new ConnectorBetween(
                new ConnectorColumnRef("id", ConnectorType.of("UNKNOWN")),
                new ConnectorLiteral(ConnectorType.of("INT"), 1L),
                new ConnectorLiteral(ConnectorType.of("INT"), 9L))));
        Assertions.assertEquals(
                Expressions.and(Expressions.greaterThanOrEqual("id", 1), Expressions.lessThanOrEqual("id", 9))
                        .toString(),
                txn.buildWriteConstraintExpression(table).map(Expression::toString).orElse(null));
    }

    @Test
    public void buildWriteConstraintAndsMultipleConjuncts() {
        // A top-level ConnectorAnd (as the extractor produces for >1 target conjunct) is flattened and
        // re-ANDed; each conjunct is converted in conflict mode (the IS NULL arm would be dropped by scan).
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(
                TableIdentifier.of("db1", "t1"), PART_SCHEMA, PartitionSpec.unpartitioned());
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());

        ConnectorComparison eq = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("region", ConnectorType.of("UNKNOWN")),
                new ConnectorLiteral(ConnectorType.of("VARCHAR"), "us"));
        ConnectorIsNull isNull = new ConnectorIsNull(
                new ConnectorColumnRef("id", ConnectorType.of("UNKNOWN")), false);
        txn.applyWriteConstraint(new ConnectorPredicate(new ConnectorAnd(java.util.Arrays.asList(eq, isNull))));

        Expression expected = Expressions.and(Expressions.equal("region", "us"), Expressions.isNull("id"));
        Assertions.assertEquals(expected.toString(),
                txn.buildWriteConstraintExpression(table).map(Expression::toString).orElse(null));
    }

    /** Builds an old, file-scoped position-delete {@link DeleteFile} (referenced -> ContentFileUtil.isFileScoped). */
    private static DeleteFile fileScopedDelete(PartitionSpec spec, String path, String referencedDataFile) {
        return FileMetadata.deleteFileBuilder(spec)
                .ofPositionDeletes()
                .withPath(path)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(64L)
                .withRecordCount(1L)
                .withReferencedDataFile(referencedDataFile)
                .build();
    }

    /** Minimal {@link ConnectorSession} exposing a time zone (for partition-timestamp parsing); no Mockito. */
    private static final class FakeWriteSession implements ConnectorSession {
        private final String timeZone;

        FakeWriteSession(String timeZone) {
            this.timeZone = timeZone;
        }

        @Override
        public String getQueryId() {
            return "q";
        }

        @Override
        public String getUser() {
            return "u";
        }

        @Override
        public String getTimeZone() {
            return timeZone;
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public long getCatalogId() {
            return 0;
        }

        @Override
        public String getCatalogName() {
            return "test";
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }
    }
}
