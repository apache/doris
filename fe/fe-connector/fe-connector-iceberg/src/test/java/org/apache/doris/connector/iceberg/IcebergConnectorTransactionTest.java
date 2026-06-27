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
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

    private static IcebergWriteContext rewriteCtx() {
        return new IcebergWriteContext(WriteOperation.REWRITE, false, Collections.emptyMap(), Optional.empty());
    }

    private static IcebergWriteContext deleteCtxPinned(long readSnapshotId) {
        return new IcebergWriteContext(
                WriteOperation.DELETE, false, Collections.emptyMap(), Optional.empty(), readSnapshotId);
    }

    private static IcebergWriteContext mergeCtxPinned(long readSnapshotId) {
        return new IcebergWriteContext(
                WriteOperation.MERGE, false, Collections.emptyMap(), Optional.empty(), readSnapshotId);
    }

    /**
     * The data files of the table's current snapshot — the connector-side equivalent of the rewrite planner's
     * {@code RewriteDataGroup.getDataFiles()} ({@code FileScanTask.file()}), i.e. the original files a rewrite
     * group hands to {@code updateRewriteFiles} as files-to-delete.
     */
    private static List<DataFile> currentDataFiles(Table table) {
        List<DataFile> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask t : tasks) {
                files.add(t.file());
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return files;
    }

    private static DataFile dataFile(PartitionSpec spec, String path, long records) {
        return DataFiles.builder(spec)
                .withPath(path)
                .withFileSizeInBytes(1024)
                .withRecordCount(records)
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    /** A data file placed into a concrete partition (e.g. {@code "region=us"}) so partition-scoped
     *  conflict detection can discriminate it. */
    private static DataFile partitionedDataFile(PartitionSpec spec, String path, long records, String partitionPath) {
        return DataFiles.builder(spec)
                .withPath(path)
                .withFileSizeInBytes(1024)
                .withRecordCount(records)
                .withFormat(FileFormat.PARQUET)
                .withPartitionPath(partitionPath)
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
    public void beginDeleteHonorsPinnedReadSnapshotOverCurrent() {
        // [SHOULD-2] / Fix B: the write must anchor baseSnapshotId at the statement's READ snapshot
        // (the MVCC pin the scan used, S_read), not at a fresh re-read of the current snapshot (S_write).
        // WHY: option-D's commit-time removeDeletes re-derives from baseSnapshotId, while BE unions the
        // scan-time (S_read) old deletes into the new DV. If baseSnapshotId drifted to a newer current
        // snapshot, a concurrent delete file landing in (S_read, S_write] would be removed-but-not-unioned
        // and its rows would silently resurrect (the iceberg OCC anchored at S_write cannot catch it).
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), props("format-version", "2"));
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/seed.parquet", 1L)).commit();
        long readSnapshot = catalog.loadTable(id).currentSnapshot().snapshotId();
        // A concurrent writer advances the table past the read snapshot before begin-write reloads it.
        Table reloaded = catalog.loadTable(id);
        reloaded.newAppend().appendFile(dataFile(reloaded.spec(), "s3://b/db1/t1/concurrent.parquet", 2L)).commit();
        long current = catalog.loadTable(id).currentSnapshot().snapshotId();
        Assertions.assertNotEquals(readSnapshot, current, "test must advance the table past the read snapshot");

        IcebergConnectorTransaction txn = txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", deleteCtxPinned(readSnapshot));

        Assertions.assertEquals(Long.valueOf(readSnapshot), txn.getBaseSnapshotId(),
                "DELETE must anchor baseSnapshotId at the pinned read snapshot, not the current snapshot");
    }

    @Test
    public void beginMergeHonorsPinnedReadSnapshotOverCurrent() {
        // Same as the DELETE arm for MERGE/UPDATE (RowDelta path): the OR-capability must be honored on
        // both arms, so the pin is exercised independently for MERGE.
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), props("format-version", "2"));
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/seed.parquet", 1L)).commit();
        long readSnapshot = catalog.loadTable(id).currentSnapshot().snapshotId();
        Table reloaded = catalog.loadTable(id);
        reloaded.newAppend().appendFile(dataFile(reloaded.spec(), "s3://b/db1/t1/concurrent.parquet", 2L)).commit();
        long current = catalog.loadTable(id).currentSnapshot().snapshotId();
        Assertions.assertNotEquals(readSnapshot, current, "test must advance the table past the read snapshot");

        IcebergConnectorTransaction txn = txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", mergeCtxPinned(readSnapshot));

        Assertions.assertEquals(Long.valueOf(readSnapshot), txn.getBaseSnapshotId(),
                "MERGE must anchor baseSnapshotId at the pinned read snapshot, not the current snapshot");
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
    public void deletePartitionedIdentityNarrowsConflictDetectionToTouchedPartition() {
        // T05/parity: for an identity-partitioned DELETE the commit-time conflict-detection filter is narrowed to
        // the touched partition (buildConflictDetectionFilter -> buildIdentityPartitionExpression -> col = value),
        // so a concurrent data-file append to a DIFFERENT partition is NOT a conflict, while one in the SAME
        // partition is. Every existing DELETE/MERGE conflict test is UNPARTITIONED, so this whole partition-filter
        // path (areAllIdentityPartitions / extractPartitionValues / spec-id match / combine) was never exercised.
        PartitionSpec spec = PartitionSpec.builderFor(PART_SCHEMA).identity("region").build();

        // (a) concurrent append in a DIFFERENT partition (eu) -> narrowed out -> the delete still commits.
        Assertions.assertDoesNotThrow(() -> runPartitionedDelete(spec, "us", "eu"),
                "an identity-partition filter (region='us') must exclude a concurrent append to region='eu'");

        // (b) concurrent append in the SAME partition (us) -> within the filter -> conflict detected. This also
        // proves the validation actually runs, so (a) passing is narrowing — not validation being skipped.
        Assertions.assertThrows(DorisConnectorException.class, () -> runPartitionedDelete(spec, "us", "us"),
                "a concurrent append to the SAME partition the delete touches must be detected as a conflict");
    }

    @Test
    public void deleteNonIdentityPartitionSpecDisablesConflictNarrowing() {
        // parity: when not every partition transform is identity (areAllIdentityPartitions == false), no partition
        // narrowing is applied, so conflict detection falls back to the whole table — a concurrent append in ANY
        // bucket is a conflict (contrast the identity case above, where a different partition is excluded).
        PartitionSpec spec = PartitionSpec.builderFor(PART_SCHEMA).bucket("region", 4).build();
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, PART_SCHEMA, spec, props("format-version", "2"));
        String seedPath = "s3://b/db1/t1/region_bucket=0/seed.parquet";
        table.newAppend().appendFile(partitionedDataFile(spec, seedPath, 5L, "region_bucket=0")).commit();

        IcebergConnectorTransaction txn = txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", deleteCtx());
        catalog.loadTable(id).newAppend().appendFile(partitionedDataFile(spec,
                "s3://b/db1/t1/region_bucket=1/concurrent.parquet", 7L, "region_bucket=1")).commit();

        TIcebergCommitData del = positionDeleteItem("s3://b/db1/t1/region_bucket=0/del.parquet", 2L, seedPath);
        del.setPartitionValues(Collections.singletonList("0"));
        del.setPartitionSpecId(spec.specId());
        txn.addCommitData(commitBytes(del));

        Assertions.assertThrows(DorisConnectorException.class, txn::commit,
                "a non-identity (bucket) partition spec disables narrowing -> the concurrent append still conflicts");
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
    public void deleteSnapshotIsolationSkipsConcurrentDataFileValidation() {
        // parity: at delete_isolation_level=snapshot (non-serializable) validateNoConflictingDataFiles is NOT
        // applied, so a concurrent data-file append since the base snapshot does NOT fail the delete — the inverse
        // of deleteDetectsConcurrentDataFileConflict (which runs at the default serializable level).
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(),
                props("format-version", "2", "delete_isolation_level", "snapshot"));
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/seed.parquet", 5L)).commit();

        IcebergConnectorTransaction txn = txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", deleteCtx());
        catalog.loadTable(id).newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/concurrent.parquet", 7L)).commit();
        txn.addCommitData(commitBytes(
                positionDeleteItem("s3://b/db1/t1/del.parquet", 2L, "s3://b/db1/t1/seed.parquet")));

        Assertions.assertDoesNotThrow(txn::commit,
                "snapshot isolation skips validateNoConflictingDataFiles -> the concurrent append is not a conflict");
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
    public void collectRewrittenDeleteFilesOnlyForTouchedDataFiles() {
        // The re-derive is keyed by the data files this commit touched (referencedDataFilePath): the existing DV
        // of a data file the commit did NOT touch must not be returned (nor removeDeletes-ed).
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        PartitionSpec spec = PartitionSpec.unpartitioned();
        String data1 = "s3://b/db1/t1/data-1.parquet";
        String data2 = "s3://b/db1/t1/data-2.parquet";
        Table table = catalog.createTable(id, SCHEMA, spec, props("format-version", "3"));
        table.newAppend()
                .appendFile(dataFile(spec, data1, 10L))
                .appendFile(dataFile(spec, data2, 10L))
                .commit();
        table.newRowDelta()
                .addDeletes(deletionVector(spec, "s3://b/db1/t1/dv-1.puffin", data1, 0L, 64L))
                .addDeletes(deletionVector(spec, "s3://b/db1/t1/dv-2.puffin", data2, 0L, 64L))
                .commit();

        IcebergConnectorTransaction txn =
                txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", deleteCtx());

        // Only data-1 is touched by this commit.
        List<DeleteFile> rewritten = txn.collectRewrittenDeleteFiles(
                Collections.singletonList(positionDeleteItem("s3://b/db1/t1/new-del.puffin", 1L, data1)));

        Assertions.assertEquals(1, rewritten.size(), "only the touched data file's existing delete is collected");
        Assertions.assertEquals("s3://b/db1/t1/dv-1.puffin", rewritten.get(0).path().toString());
    }

    @Test
    public void collectRewrittenDeleteFilesKeepsDistinctDeletionVectorsSharingOnePuffin() {
        // DV-T04 parity preserved under the re-derive: buildDeleteFileDedupKey keys PUFFIN deletion vectors by
        // path#contentOffset#contentSizeInBytes, NOT the bare path. Two DVs packed into the SAME puffin file (one
        // per data file, distinct offset/size) must BOTH survive — keying by bare path would silently merge them
        // and drop one data file's DV from removeDeletes.
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        PartitionSpec spec = PartitionSpec.unpartitioned();
        String data1 = "s3://b/db1/t1/data-1.parquet";
        String data2 = "s3://b/db1/t1/data-2.parquet";
        String puffin = "s3://b/db1/t1/deletes.puffin";
        Table table = catalog.createTable(id, SCHEMA, spec, props("format-version", "3"));
        table.newAppend()
                .appendFile(dataFile(spec, data1, 10L))
                .appendFile(dataFile(spec, data2, 10L))
                .commit();
        table.newRowDelta()
                .addDeletes(deletionVector(spec, puffin, data1, 0L, 64L))
                .addDeletes(deletionVector(spec, puffin, data2, 64L, 80L))
                .commit();

        IcebergConnectorTransaction txn =
                txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", deleteCtx());

        List<DeleteFile> rewritten = txn.collectRewrittenDeleteFiles(Arrays.asList(
                positionDeleteItem("s3://b/db1/t1/new-1.puffin", 1L, data1),
                positionDeleteItem("s3://b/db1/t1/new-2.puffin", 1L, data2)));

        Assertions.assertEquals(2, rewritten.size(),
                "two DVs in one puffin file with distinct (offset,size) are both kept (key includes offset/size)");
    }

    @Test
    public void collectRewrittenDeleteFilesRemovesBothLegacyAndDeletionVectorForUpgradedTable() {
        // Intentional divergence from Trino, locked in: on a v2->v3 upgraded table where one data file carries
        // BOTH a legacy file-scoped position delete AND a deletion vector, BOTH must be returned (and removed).
        // Doris's BE unions the old positions from both kinds into the new DV, so both old files are superseded;
        // Trino instead suppresses the legacy file once a DV exists. A regression toward the Trino behavior
        // (dropping the legacy file) would return 1 here.
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        PartitionSpec spec = PartitionSpec.unpartitioned();
        String data1 = "s3://b/db1/t1/data-1.parquet";
        Table table = catalog.createTable(id, SCHEMA, spec, props("format-version", "2"));
        table.newAppend().appendFile(dataFile(spec, data1, 10L)).commit();
        // v2: a legacy parquet file-scoped position delete for data-1.
        table.newRowDelta().addDeletes(fileScopedDelete(spec, "s3://b/db1/t1/legacy.parquet", data1)).commit();
        // upgrade to v3, then add a deletion vector for the SAME data file (both survive in the delete manifests).
        catalog.loadTable(id).updateProperties().set("format-version", "3").commit();
        catalog.loadTable(id).newRowDelta()
                .addDeletes(deletionVector(spec, "s3://b/db1/t1/dv.puffin", data1, 0L, 64L))
                .commit();

        IcebergConnectorTransaction txn =
                txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", deleteCtx());

        List<DeleteFile> rewritten = txn.collectRewrittenDeleteFiles(
                Collections.singletonList(positionDeleteItem("s3://b/db1/t1/new-del.puffin", 1L, data1)));

        Assertions.assertEquals(2, rewritten.size(),
                "both the legacy file-scoped delete and the deletion vector for the touched data file are removed "
                        + "(Doris's BE unions both into the new DV; unlike Trino which suppresses the legacy file)");
    }

    @Test
    public void collectRewrittenDeleteFilesEmptyWhenCommitCarriesNoReferencedDataFile() {
        // The keystone is TIcebergCommitData.referencedDataFilePath; a delete fragment without it contributes no
        // touched data file, so nothing is re-derived (mirrors a data-only fragment).
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        PartitionSpec spec = PartitionSpec.unpartitioned();
        String data1 = "s3://b/db1/t1/data-1.parquet";
        Table table = catalog.createTable(id, SCHEMA, spec, props("format-version", "3"));
        table.newAppend().appendFile(dataFile(spec, data1, 10L)).commit();
        table.newRowDelta()
                .addDeletes(deletionVector(spec, "s3://b/db1/t1/dv-1.puffin", data1, 0L, 64L))
                .commit();

        IcebergConnectorTransaction txn =
                txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", deleteCtx());

        TIcebergCommitData noRef = new TIcebergCommitData();
        noRef.setFilePath("s3://b/db1/t1/new-del.puffin");
        noRef.setRowCount(1L);
        noRef.setFileContent(TFileContent.POSITION_DELETES);

        Assertions.assertTrue(txn.collectRewrittenDeleteFiles(Collections.singletonList(noRef)).isEmpty(),
                "a delete fragment with no referencedDataFilePath touches no data file -> nothing to rewrite");
    }

    @Test
    public void collectRewrittenDeleteFilesReDerivesFromBaseSnapshotManifest() {
        // Trino-style commit-time re-derive (option D): the old file-scoped delete files to removeDeletes are
        // read from the base snapshot's delete manifests (metadata-only), keyed by the data-file paths the
        // commit touched (TIcebergCommitData.referencedDataFilePath) — NOT from any scan-time map.
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        PartitionSpec spec = PartitionSpec.unpartitioned();
        String dataPath = "s3://b/db1/t1/data-1.parquet";
        Table table = catalog.createTable(id, SCHEMA, spec, props("format-version", "3"));
        table.newAppend().appendFile(dataFile(spec, dataPath, 10L)).commit();
        // Seed an existing file-scoped deletion vector for the data file into the snapshot beginWrite will pin.
        table.newRowDelta()
                .addDeletes(deletionVector(spec, "s3://b/db1/t1/old.puffin", dataPath, 0L, 64L))
                .commit();

        IcebergConnectorTransaction txn =
                txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", deleteCtx());

        List<DeleteFile> rewritten = txn.collectRewrittenDeleteFiles(
                Collections.singletonList(positionDeleteItem("s3://b/db1/t1/new-del.puffin", 1L, dataPath)));

        Assertions.assertEquals(1, rewritten.size(),
                "the existing file-scoped delete for the touched data file is re-derived from the base snapshot");
        Assertions.assertEquals("s3://b/db1/t1/old.puffin", rewritten.get(0).path().toString());
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

    // ─────────────────── rewrite_data_files: WriteOperation.REWRITE variant (P6.4-T06, dormant) ───────────────────

    @Test
    public void rewriteCapturesStartingSnapshotIdAtBegin() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/seed.parquet", 3L)).commit();
        Table reloaded = catalog.loadTable(id);
        long expected = reloaded.currentSnapshot().snapshotId();

        IcebergConnectorTransaction txn = txnFor(opsReturning(reloaded), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());

        // legacy IcebergTransaction.beginRewrite:187-188 captures the current snapshot for the commit-time
        // validateFromSnapshot OCC anchor; REWRITE does NOT pin baseSnapshotId (that drives the RowDelta path).
        Assertions.assertEquals(expected, txn.getStartingSnapshotId());
        Assertions.assertNull(txn.getBaseSnapshotId(), "REWRITE uses startingSnapshotId, not baseSnapshotId");
    }

    @Test
    public void rewriteOnEmptyTableCapturesSentinelStartingSnapshot() {
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"), SCHEMA, PartitionSpec.unpartitioned());
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());
        // No current snapshot -> -1L sentinel passed verbatim to validateFromSnapshot (legacy beginRewrite:188).
        Assertions.assertEquals(-1L, txn.getStartingSnapshotId());
    }

    @Test
    public void beginWriteIsBeginOnceForSharedRewriteTransaction() {
        // A distributed rewrite runs N per-group writes that SHARE one transaction; each group's plan-time
        // sink planWrite calls beginWrite again (concurrently). The begin-once guard must load the table +
        // open the SDK transaction + pin the OCC snapshot EXACTLY ONCE; the rest are no-ops.
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/seed.parquet", 3L)).commit();
        Table reloaded = catalog.loadTable(id);
        long expected = reloaded.currentSnapshot().snapshotId();
        RecordingIcebergCatalogOps ops = opsReturning(reloaded);
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergConnectorTransaction txn = txnFor(ops, ctx);

        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());
        Object firstSdkTxn = txn.getTransaction();
        int authAfterFirst = ctx.authCount;
        long loadsAfterFirst = ops.log.stream().filter("loadTable:db1.t1"::equals).count();

        // Subsequent concurrent group writes must reuse the shared state, not rebuild it.
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());

        Assertions.assertSame(firstSdkTxn, txn.getTransaction(), "shared SDK transaction must not be rebuilt");
        Assertions.assertEquals(authAfterFirst, ctx.authCount, "begin-once: no extra auth-wrapped begins");
        Assertions.assertEquals(loadsAfterFirst, ops.log.stream().filter("loadTable:db1.t1"::equals).count(),
                "begin-once: the table must be loaded exactly once");
        Assertions.assertEquals(expected, txn.getStartingSnapshotId(), "OCC anchor must stay pinned to S1");
    }

    @Test
    public void registerRewriteSourceFilesBeforeBeginFailsLoud() {
        // registerRewriteSourceFiles re-derives the source files from the table at the pinned snapshot, both
        // loaded by beginWrite. The driver must register only AFTER a group's write began the transaction;
        // calling it before begin must fail loud, not NPE on table.newScan().
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"), SCHEMA, PartitionSpec.unpartitioned());
        IcebergConnectorTransaction txn = txnFor(opsReturning(table), new RecordingConnectorContext());

        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> txn.registerRewriteSourceFiles(new HashSet<>(Arrays.asList("s3://b/db1/t1/x.parquet"))));
        Assertions.assertTrue(ex.getMessage().contains("before the rewrite transaction began"),
                "must fail loud with the begin-ordering message, got: " + ex.getMessage());
    }

    @Test
    public void rewriteCommitsReplaceDeletingOldAddingNew() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(),
                props("write.format.default", "parquet"));
        // Seed two small data files — the bin-packed group to compact.
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/old1.parquet", 5L))
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/old2.parquet", 7L))
                .commit();
        Table reloaded = catalog.loadTable(id);
        List<DataFile> oldFiles = currentDataFiles(reloaded);
        Assertions.assertEquals(2, oldFiles.size());

        IcebergConnectorTransaction txn = txnFor(opsReturning(reloaded), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());
        txn.updateRewriteFiles(oldFiles);                       // files-to-delete (planner FileScanTask.file())
        // BE reports one new compacted data file (same commitDataList channel INSERT uses).
        txn.addCommitData(commitBytes(dataFileItem("s3://b/db1/t1/compacted.parquet", 12L, 2048L)));
        long beforeCommit = reloadCurrentSnapshot(catalog, id).snapshotId();

        txn.commit();

        Assertions.assertNotEquals(beforeCommit, reloadCurrentSnapshot(catalog, id).snapshotId(),
                "commit() must produce a new snapshot");

        Snapshot snap = reloadCurrentSnapshot(catalog, id);
        Assertions.assertNotNull(snap);
        Assertions.assertEquals("replace", snap.operation(),
                "RewriteFiles (newRewrite) produces a replace snapshot, not append/overwrite");
        Assertions.assertEquals("2", snap.summary().get("deleted-data-files"));
        Assertions.assertEquals("1", snap.summary().get("added-data-files"));
    }

    @Test
    public void rewriteDeleteOnlyStillCommitsReplace() {
        // The both-empty skip is the AND (filesToDelete.isEmpty() && filesToAdd.isEmpty(), port :391 /
        // legacy updateManifestAfterRewrite:248). A delete-only rewrite (files to delete, no new files added)
        // has filesToDelete non-empty AND filesToAdd empty, so the skip MUST NOT fire — an &&->|| mutation
        // would short-circuit on the empty filesToAdd and silently drop the deletes, leaving the snapshot
        // unchanged. The assertNotEquals below goes RED under that mutation.
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(),
                props("write.format.default", "parquet"));
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/old1.parquet", 5L))
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/old2.parquet", 7L))
                .commit();
        Table reloaded = catalog.loadTable(id);
        List<DataFile> oldFiles = currentDataFiles(reloaded);
        Assertions.assertEquals(2, oldFiles.size());

        IcebergConnectorTransaction txn = txnFor(opsReturning(reloaded), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());
        txn.updateRewriteFiles(oldFiles);                       // files-to-delete; NO commit data -> filesToAdd empty
        long beforeSnapshotId = reloadCurrentSnapshot(catalog, id).snapshotId();

        txn.commit();

        long afterSnapshotId = reloadCurrentSnapshot(catalog, id).snapshotId();
        Assertions.assertNotEquals(beforeSnapshotId, afterSnapshotId,
                "a delete-only rewrite must still produce a new snapshot (&& skip, not ||)");

        Snapshot snap = reloadCurrentSnapshot(catalog, id);
        Assertions.assertNotNull(snap);
        // RewriteFiles always reports "replace", even when only deleting.
        Assertions.assertEquals("replace", snap.operation());
        Assertions.assertEquals("2", snap.summary().get("deleted-data-files"));
        Assertions.assertNull(snap.summary().get("added-data-files"),
                "no new files were added -> the summary carries no added-data-files key");
    }

    @Test
    public void rewriteFailsLoudWhenRewrittenFileRemovedConcurrently() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(),
                props("write.format.default", "parquet"));
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/old.parquet", 5L)).commit();
        Table reloaded = catalog.loadTable(id);
        List<DataFile> oldFiles = currentDataFiles(reloaded);

        IcebergConnectorTransaction txn = txnFor(opsReturning(reloaded), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());     // pins startingSnapshotId = S1
        txn.updateRewriteFiles(oldFiles);

        // A concurrent writer removes the very file we are about to rewrite, advancing the table past S1.
        DeleteFiles concurrent = catalog.loadTable(id).newDelete();
        oldFiles.forEach(f -> concurrent.deleteFile(f.path()));
        concurrent.commit();

        txn.addCommitData(commitBytes(dataFileItem("s3://b/db1/t1/compacted.parquet", 5L, 2048L)));

        // commit() runs newRewrite().validateFromSnapshot(S1).deleteFile(old).commit(); the concurrent removal of a
        // rewritten file is a conflict -> the SDK throws -> the connector wraps it as DorisConnectorException.
        Assertions.assertThrows(DorisConnectorException.class, txn::commit,
                "rewriting a data file removed since the pinned snapshot must fail loud");
    }

    @Test
    public void rewriteDetectsConcurrentDeleteOnRewrittenFile() {
        // A concurrent writer adds a position-delete file targeting the very data file we are rewriting. The
        // rewrite must fail loud rather than silently drop that concurrent delete (a data-loss bug). This pins
        // the conflict-detection BEHAVIOR of the REWRITE commit. NOTE (verified by mutation check): the throw is
        // raised by iceberg's RewriteFiles machinery from the transaction's begin-time base snapshot, so it does
        // NOT isolate the explicit validateFromSnapshot(startingSnapshotId) line — removing that line keeps this
        // test green. The explicit call is a byte-faithful legacy port; its distinct cross-refresh value is not
        // distinguishable in a single-process offline test (P6.6 docker/concurrent gate). See task record.
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(),
                props("format-version", "2", "write.format.default", "parquet"));
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/old.parquet", 5L)).commit();
        Table reloaded = catalog.loadTable(id);
        List<DataFile> oldFiles = currentDataFiles(reloaded);

        IcebergConnectorTransaction txn = txnFor(opsReturning(reloaded), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());     // pins startingSnapshotId = S1
        txn.updateRewriteFiles(oldFiles);

        // Concurrent RowDelta adds a position-delete for the rewritten data file, advancing the table past S1.
        // The data file itself still exists, so this is detectable ONLY via the from-S1 conflict validation.
        catalog.loadTable(id).newRowDelta()
                .addDeletes(fileScopedDelete(table.spec(), "s3://b/db1/t1/del.parquet", "s3://b/db1/t1/old.parquet"))
                .commit();

        txn.addCommitData(commitBytes(dataFileItem("s3://b/db1/t1/compacted.parquet", 5L, 2048L)));

        Assertions.assertThrows(DorisConnectorException.class, txn::commit,
                "validateFromSnapshot must detect the new delete added to a rewritten data file since the pin");
    }

    @Test
    public void rewriteWithNoFilesSkipsRewriteOpButStillCommits() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/seed.parquet", 1L)).commit();
        Table reloaded = catalog.loadTable(id);
        long before = reloaded.currentSnapshot().snapshotId();

        IcebergConnectorTransaction txn = txnFor(opsReturning(reloaded), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());
        // No files to delete + no new files -> updateManifestAfterRewrite early-returns (legacy :248-251);
        // the (empty) SDK transaction still commits without throwing.
        Assertions.assertDoesNotThrow(txn::commit);
        Assertions.assertEquals(before, reloadCurrentSnapshot(catalog, id).snapshotId(),
                "an empty rewrite must not create a new snapshot");
    }

    @Test
    public void rewriteCountAndSizeAccessorsReflectDeletedAndAddedFiles() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(),
                props("write.format.default", "parquet"));
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/old1.parquet", 5L))
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/old2.parquet", 7L))
                .commit();
        Table reloaded = catalog.loadTable(id);
        List<DataFile> oldFiles = currentDataFiles(reloaded);

        IcebergConnectorTransaction txn = txnFor(opsReturning(reloaded), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());
        txn.updateRewriteFiles(oldFiles);
        // filesToDelete is populated at updateRewriteFiles time; each dataFile() helper is 1024 bytes.
        Assertions.assertEquals(2, txn.getFilesToDeleteCount());
        Assertions.assertEquals(2048L, txn.getFilesToDeleteSize());

        txn.addCommitData(commitBytes(dataFileItem("s3://b/db1/t1/compacted.parquet", 12L, 4096L)));
        // filesToAdd is populated DURING commit (convertCommitDataToFilesToAdd), NOT at addCommitData: the
        // fragment is buffered on commitDataList and only materialized into filesToAdd inside commitRewriteTxn.
        // An early-materialization mutation (folding convertCommitDataToFilesToAdd into addCommitData) would
        // make these pre-commit reads non-zero, turning these assertions RED.
        Assertions.assertEquals(0, txn.getFilesToAddCount(),
                "filesToAdd materialized DURING commit, not at addCommitData");
        Assertions.assertEquals(0L, txn.getFilesToAddSize());
        // filesToAdd is populated DURING commit (convertCommitDataToFilesToAdd) — the legacy executor reads
        // getFilesToAddCount only AFTER finishRewrite, so these reads must be post-commit.
        txn.commit();
        Assertions.assertEquals(1, txn.getFilesToAddCount());
        Assertions.assertEquals(4096L, txn.getFilesToAddSize());
    }

    @Test
    public void updateRewriteFilesAccumulatesAcrossCalls() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/a.parquet", 1L))
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/b.parquet", 1L))
                .commit();
        List<DataFile> files = currentDataFiles(catalog.loadTable(id));

        IcebergConnectorTransaction txn = txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());
        // legacy updateRewriteFiles is called once per bin-packed group; the connector accumulates across calls.
        txn.updateRewriteFiles(Collections.singletonList(files.get(0)));
        txn.updateRewriteFiles(Collections.singletonList(files.get(1)));
        Assertions.assertEquals(2, txn.getFilesToDeleteCount());
    }

    @Test
    public void registerRewriteSourceFilesResolvesRawPathsToFilesToDelete() {
        // The neutral SPI hands the connector only RAW String paths (fe-core cannot pass DataFile); the
        // connector re-derives the matching DataFiles from the table at the pinned snapshot. Register a SUBSET
        // (2 of 3 committed files) to prove it matches BY PATH, not "delete everything".
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(),
                props("write.format.default", "parquet"));
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/old1.parquet", 5L))
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/old2.parquet", 7L))
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/keep.parquet", 9L))
                .commit();
        Table reloaded = catalog.loadTable(id);

        IcebergConnectorTransaction txn = txnFor(opsReturning(reloaded), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());
        txn.registerRewriteSourceFiles(new HashSet<>(Arrays.asList(
                "s3://b/db1/t1/old1.parquet", "s3://b/db1/t1/old2.parquet")));

        // Resolved exactly the two registered files (1024 bytes each), leaving keep.parquet untouched.
        Assertions.assertEquals(2, txn.getFilesToDeleteCount());
        Assertions.assertEquals(2048L, txn.getFilesToDeleteSize());
    }

    @Test
    public void registerRewriteSourceFilesFailsLoudOnUnmatchedPath() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(),
                props("write.format.default", "parquet"));
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/old1.parquet", 5L)).commit();
        Table reloaded = catalog.loadTable(id);

        IcebergConnectorTransaction txn = txnFor(opsReturning(reloaded), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());
        // A path absent at the pinned snapshot (stale plan / table moved since planning) must fail loud rather
        // than silently delete fewer files than the engine intended.
        Assertions.assertThrows(DorisConnectorException.class, () ->
                txn.registerRewriteSourceFiles(new HashSet<>(Arrays.asList("s3://b/db1/t1/ghost.parquet"))));
    }

    @Test
    public void registerRewriteSourceFilesEmptyIsNoOp() {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(),
                props("write.format.default", "parquet"));
        table.newAppend().appendFile(dataFile(table.spec(), "s3://b/db1/t1/old1.parquet", 5L)).commit();
        Table reloaded = catalog.loadTable(id);

        IcebergConnectorTransaction txn = txnFor(opsReturning(reloaded), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());
        txn.registerRewriteSourceFiles(Collections.emptySet());
        Assertions.assertEquals(0, txn.getFilesToDeleteCount(), "an empty registration is a no-op");
    }

    @Test
    public void registerRewriteSourceFilesThenCommitReplacesAndReportsAddedCount() {
        // End-to-end via the neutral SPI: register source paths -> re-derive -> commit RewriteFiles. Mirrors
        // rewriteCommitsReplaceDeletingOldAddingNew but through registerRewriteSourceFiles, and asserts the
        // neutral getRewriteAddedDataFilesCount() reports the BE-added file post-commit (the one rewrite stat
        // the engine driver cannot compute from its planning groups).
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(),
                props("write.format.default", "parquet"));
        table.newAppend()
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/old1.parquet", 5L))
                .appendFile(dataFile(table.spec(), "s3://b/db1/t1/old2.parquet", 7L))
                .commit();
        Table reloaded = catalog.loadTable(id);

        IcebergConnectorTransaction txn = txnFor(opsReturning(reloaded), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", rewriteCtx());
        txn.registerRewriteSourceFiles(new HashSet<>(Arrays.asList(
                "s3://b/db1/t1/old1.parquet", "s3://b/db1/t1/old2.parquet")));
        txn.addCommitData(commitBytes(dataFileItem("s3://b/db1/t1/compacted.parquet", 12L, 4096L)));

        txn.commit();

        Snapshot snap = reloadCurrentSnapshot(catalog, id);
        Assertions.assertEquals("replace", snap.operation());
        Assertions.assertEquals("2", snap.summary().get("deleted-data-files"));
        Assertions.assertEquals("1", snap.summary().get("added-data-files"));
        Assertions.assertEquals(1, txn.getRewriteAddedDataFilesCount(),
                "the neutral SPI reports the post-commit added-data-files count for the driver's result row");
    }

    /**
     * Seeds an identity-partitioned table, opens a DELETE that touches partition {@code deleteRegion}, races a
     * concurrent data-file append into {@code concurrentRegion}, then commits — exercising the identity-partition
     * conflict-detection narrowing end-to-end through {@code commit()}.
     */
    private static void runPartitionedDelete(PartitionSpec spec, String deleteRegion, String concurrentRegion) {
        InMemoryCatalog catalog = freshCatalog();
        TableIdentifier id = TableIdentifier.of("db1", "t1");
        Table table = catalog.createTable(id, PART_SCHEMA, spec, props("format-version", "2"));
        String seedPath = "s3://b/db1/t1/region=" + deleteRegion + "/seed.parquet";
        table.newAppend().appendFile(partitionedDataFile(spec, seedPath, 5L, "region=" + deleteRegion)).commit();

        IcebergConnectorTransaction txn = txnFor(opsReturning(catalog.loadTable(id)), new RecordingConnectorContext());
        txn.beginWrite(SESSION, "db1", "t1", deleteCtx());

        catalog.loadTable(id).newAppend().appendFile(partitionedDataFile(spec,
                "s3://b/db1/t1/region=" + concurrentRegion + "/concurrent.parquet", 7L,
                "region=" + concurrentRegion)).commit();

        TIcebergCommitData del =
                positionDeleteItem("s3://b/db1/t1/region=" + deleteRegion + "/del.parquet", 2L, seedPath);
        del.setPartitionValues(Collections.singletonList(deleteRegion));
        del.setPartitionSpecId(spec.specId());
        txn.addCommitData(commitBytes(del));
        txn.commit();
    }

    /** Builds an old, file-scoped deletion-vector (PUFFIN) {@link DeleteFile} keyed by path#offset#size. */
    private static DeleteFile deletionVector(PartitionSpec spec, String path, String referencedDataFile,
            long contentOffset, long contentSize) {
        return FileMetadata.deleteFileBuilder(spec)
                .ofPositionDeletes()
                .withPath(path)
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(128L)
                .withRecordCount(1L)
                .withReferencedDataFile(referencedDataFile)
                .withContentOffset(contentOffset)
                .withContentSizeInBytes(contentSize)
                .build();
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
