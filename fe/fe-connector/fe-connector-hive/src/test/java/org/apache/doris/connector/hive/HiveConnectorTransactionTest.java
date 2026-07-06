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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsPartitionStatistics;
import org.apache.doris.connector.hms.HmsPartitionWithStatistics;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.UploadPartResult;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.filesystem.spi.ObjFileSystem;
import org.apache.doris.filesystem.spi.ObjStorage;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TS3MPUPendingUpload;
import org.apache.doris.thrift.TUpdateMode;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link HiveConnectorTransaction}, exercised offline against a recording {@link HmsClient}
 * fake (no metastore, no Mockito — the connector test convention). These cover the parts of the ported
 * {@code HMSTransaction} whose behaviour a compile cannot verify and where a silent regression corrupts
 * data or drops writes:
 *
 * <ul>
 *   <li><b>Classification / NEW&rarr;APPEND downgrade</b> — the {@code finishInsertTable} state machine and
 *       the {@code partitionExists} probe that saves a partitioned {@code INSERT} from failing when Doris'
 *       cache missed a partition that already exists in HMS (a wrong branch here either double-adds a
 *       partition or loses the rows).</li>
 *   <li><b>transactional write reject (DEC-2)</b> — writing to ANY transactional table (full-ACID AND
 *       insert-only) must be refused; the transactional flag is derived plugin-side from the raw HMS
 *       parameters (an insert-only ACID table would otherwise get plain files — corrupt/invisible data).</li>
 *   <li><b>{@code addCommitData}/{@code getUpdateCnt}</b> — the affected-row count reported back to the
 *       engine is the sum of the BE fragment row counts (D3); an off-by-one here misreports load results.</li>
 *   <li><b>SPI identity</b> — {@code profileLabel()} must stay {@code "HMS"} (D2: maps to the existing
 *       fe-core {@code TransactionType.HMS}; any other value silently degrades to UNKNOWN).</li>
 * </ul>
 *
 * <p>The commit-time object-store work is driven through a fake {@code ObjFileSystem}/{@code ObjStorage}
 * injected via the {@code resolveObjectStoreFileSystem} seam: multipart-upload <b>complete</b> on commit
 * and <b>abort</b> on rollback (D6/D9), the <b>idempotency</b> of a repeated rollback, and the single
 * batched {@code addPartitions} call (GAP-7 — the 20-at-a-time batching lives in the client, so the
 * committer adds the whole list once). The staging-directory rename walk and the full multi-partition
 * commit remain for the P7.4/P7.5 cutover integration gate (the live write path).</p>
 */
public class HiveConnectorTransactionTest {

    private static final long CATALOG_ID = 0L;
    private static final String DB = "db";
    private static final String TBL = "t";

    private HiveConnectorTransaction newTxn(HmsClient client) {
        return new HiveConnectorTransaction(42L, client, new FakeConnectorContext(
                "test_catalog", CATALOG_ID, Collections.emptyMap()));
    }

    private NameMapping nameMapping() {
        return new NameMapping(CATALOG_ID, DB, TBL, DB, TBL);
    }

    private HiveWriteContext ctx(boolean overwrite) {
        // FILE_S3 keeps the classification path from needing a real staging filesystem (createAndAddPartition
        // then uses the write path directly rather than a target path).
        return new HiveWriteContext(overwrite ? WriteOperation.OVERWRITE : WriteOperation.INSERT, overwrite,
                Collections.emptyMap(), "query-1", TFileType.FILE_S3, "s3://bucket/db/t/_staging");
    }

    private static ConnectorColumn col(String name, String type) {
        return new ConnectorColumn(name, ConnectorType.of(type), "", true, null);
    }

    private static HmsTableInfo table(boolean partitioned, Map<String, String> params) {
        return HmsTableInfo.builder()
                .dbName(DB).tableName(TBL).tableType("MANAGED_TABLE")
                .location("s3://bucket/db/t")
                .inputFormat("org.apache.hadoop.mapred.TextInputFormat")
                .outputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                .serializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
                .columns(Collections.singletonList(col("c1", "int")))
                .partitionKeys(partitioned
                        ? Collections.singletonList(col("dt", "string"))
                        : Collections.emptyList())
                .parameters(params == null ? Collections.emptyMap() : params)
                .build();
    }

    private static THivePartitionUpdate pu(String name, TUpdateMode mode, String writePath,
            List<String> fileNames, long fileSize, long rowCount) {
        THivePartitionUpdate u = new THivePartitionUpdate();
        u.setName(name);
        u.setUpdateMode(mode);
        THiveLocationParams loc = new THiveLocationParams();
        loc.setWritePath(writePath);
        loc.setTargetPath(writePath);
        u.setLocation(loc);
        u.setFileNames(fileNames);
        u.setFileSize(fileSize);
        u.setRowCount(rowCount);
        return u;
    }

    private static byte[] serialize(THivePartitionUpdate u) throws TException {
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(u);
    }

    private static THivePartitionUpdate puWithMpu(String name, TUpdateMode mode, String writePath,
            String bucket, String key, String uploadId, Map<Integer, String> etags) {
        THivePartitionUpdate u = pu(name, mode, writePath, Collections.singletonList("f1"), 100, 4);
        TS3MPUPendingUpload mpu = new TS3MPUPendingUpload();
        mpu.setBucket(bucket);
        mpu.setKey(key);
        mpu.setUploadId(uploadId);
        mpu.setEtags(etags);
        u.setS3MpuPendingUploads(new ArrayList<>(Collections.singletonList(mpu)));
        return u;
    }

    // Builds a transaction whose object-store FileSystem is the injected fake, bypassing the production
    // ServiceLoader discovery (a cutover-time concern) — the seam the design carved out for exactly this.
    private HiveConnectorTransaction newTxnWithFs(HmsClient client, FileSystem fakeFs) {
        return new HiveConnectorTransaction(42L, client,
                new FakeConnectorContext("test_catalog", CATALOG_ID, Collections.emptyMap())) {
            @Override
            protected FileSystem resolveObjectStoreFileSystem(StorageProperties objSp) {
                return fakeFs;
            }
        };
    }

    // ─────────────────────────────── SPI identity ───────────────────────────────

    @Test
    public void testProfileLabelAndTransactionId() {
        // D2: the profile label must resolve to the existing fe-core TransactionType.HMS; a drift to "HIVE"
        // (or anything else) would silently map to UNKNOWN and mislabel the transaction in the engine.
        HiveConnectorTransaction txn = newTxn(new RecordingHmsClient());
        Assertions.assertEquals("HMS", txn.profileLabel());
        Assertions.assertEquals(42L, txn.getTransactionId());
    }

    // ─────────────────────────────── addCommitData / getUpdateCnt ───────────────────────────────

    @Test
    public void testGetUpdateCntSumsFragmentRowCounts() throws TException {
        // D3: the affected-row count is the SUM of the BE fragment row counts. Feed three serialized
        // fragments and assert the running total — proves both the TBinaryProtocol round-trip in
        // addCommitData and the accumulation (a wrong reducer here misreports load results to the client).
        HiveConnectorTransaction txn = newTxn(new RecordingHmsClient());
        txn.addCommitData(serialize(pu("dt=a", TUpdateMode.NEW, "s3://b/a", Arrays.asList("f1"), 10, 3)));
        txn.addCommitData(serialize(pu("dt=b", TUpdateMode.NEW, "s3://b/b", Arrays.asList("f2"), 20, 5)));
        txn.addCommitData(serialize(pu("dt=c", TUpdateMode.NEW, "s3://b/c", Arrays.asList("f3"), 30, 7)));
        Assertions.assertEquals(15L, txn.getUpdateCnt());
    }

    @Test
    public void testAddCommitDataRejectsGarbageBytes() {
        // A corrupt commit fragment must surface as a DorisConnectorException (not a raw TException leaking
        // through the SPI), so the engine can fail the load cleanly.
        HiveConnectorTransaction txn = newTxn(new RecordingHmsClient());
        Assertions.assertThrows(DorisConnectorException.class,
                () -> txn.addCommitData(new byte[] {1, 2, 3, 4, 5}));
    }

    // ─────────────────────────────── transactional write reject (DEC-2) ───────────────────────────────

    @Test
    public void testBeginWriteRejectsFullAcidTable() {
        // DEC-2: writing to a full-ACID table (transactional=true, not insert-only) is not supported and must
        // be refused at begin — otherwise the write would silently corrupt the ACID base/delta layout.
        RecordingHmsClient client = new RecordingHmsClient();
        Map<String, String> params = new HashMap<>();
        params.put("transactional", "true");
        client.table = table(false, params);
        HiveConnectorTransaction txn = newTxn(client);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> txn.beginWrite(null, DB, TBL, ctx(false)));
        Assertions.assertTrue(ex.getMessage().contains("begin write"),
                "expected the begin-write wrapper message, got: " + ex.getMessage());
    }

    @Test
    public void testBeginWriteRejectsInsertOnlyTransactionalTable() {
        // DEC-2: the write guard now rejects ANY transactional table — full-ACID AND insert-only. An
        // insert-only ACID table would otherwise slip through the narrower full-ACID-only guard and receive
        // plain files (corrupt/invisible data), matching legacy InsertIntoTableCommand's isTransactionalTable
        // reject.
        RecordingHmsClient client = new RecordingHmsClient();
        Map<String, String> params = new HashMap<>();
        params.put("transactional", "true");
        params.put("transactional_properties", "insert_only");
        client.table = table(false, params);
        HiveConnectorTransaction txn = newTxn(client);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> txn.beginWrite(null, DB, TBL, ctx(false)));
        Assertions.assertTrue(ex.getMessage().contains("transactional"),
                "expected the transactional-table reject, got: " + ex.getMessage());
    }

    @Test
    public void testBeginWriteAllowsPlainTable() {
        // A plain non-transactional table is the common INSERT case — begin must succeed.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = table(false, Collections.emptyMap());
        HiveConnectorTransaction txn = newTxn(client);
        txn.beginWrite(null, DB, TBL, ctx(false)); // must not throw
    }

    // ─────────────────────────────── classification / NEW→APPEND downgrade ───────────────────────────────

    @Test
    public void testPartitionedNewWhenAbsentCreatesPartition() throws TException {
        // A NEW partition that HMS does NOT already have must be created: the existence probe is consulted,
        // and because it is absent we take the create branch — which needs no getPartitions() lookup.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = table(true, Collections.emptyMap());
        client.partitionExistsResult = false;
        HiveConnectorTransaction txn = newTxn(client);
        txn.beginWrite(null, DB, TBL, ctx(false));
        txn.addCommitData(serialize(pu("dt=2024-01-01", TUpdateMode.NEW, "s3://bucket/db/t/dt=2024-01-01",
                Arrays.asList("f1"), 100, 4)));

        txn.finishInsertTable(nameMapping());

        Assertions.assertTrue(client.calls.contains("partitionExists:[2024-01-01]"),
                "the NEW-mode branch must probe partition existence; calls=" + client.calls);
        Assertions.assertFalse(client.calls.stream().anyMatch(c -> c.startsWith("getPartitions")),
                "a genuinely-new partition takes the create path, which does not fetch existing partitions; "
                        + "calls=" + client.calls);
    }

    @Test
    public void testPartitionedNewWhenPresentDowngradesToAppend() throws TException {
        // NEW→APPEND downgrade: when Doris' cache missed a partition that HMS actually has, the probe returns
        // true and we must treat the write as an APPEND into the existing partition (getPartitions() is then
        // consulted to build the INSERT_EXISTING action) — instead of trying to ADD a duplicate partition.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = table(true, Collections.emptyMap());
        client.partitionExistsResult = true;
        client.partitions = Collections.singletonList(new HmsPartitionInfo(
                Collections.singletonList("2024-01-01"), "s3://bucket/db/t/dt=2024-01-01",
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", Collections.emptyMap()));
        HiveConnectorTransaction txn = newTxn(client);
        txn.beginWrite(null, DB, TBL, ctx(false));
        txn.addCommitData(serialize(pu("dt=2024-01-01", TUpdateMode.NEW, "s3://bucket/db/t/dt=2024-01-01",
                Arrays.asList("f1"), 100, 4)));

        txn.finishInsertTable(nameMapping());

        Assertions.assertTrue(client.calls.contains("partitionExists:[2024-01-01]"),
                "the downgrade must first probe existence; calls=" + client.calls);
        Assertions.assertTrue(client.calls.stream().anyMatch(c -> c.startsWith("getPartitions")),
                "an existing partition downgrades to APPEND, which fetches the real partition metadata; "
                        + "calls=" + client.calls);
    }

    @Test
    public void testPartitionedAppendFetchesExistingPartition() throws TException {
        // A declared APPEND into a partitioned table goes straight to the INSERT_EXISTING path, which fetches
        // the existing partition (no existence probe needed) — a wrong branch here would try to add it.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = table(true, Collections.emptyMap());
        client.partitions = Collections.singletonList(new HmsPartitionInfo(
                Collections.singletonList("2024-01-01"), "s3://bucket/db/t/dt=2024-01-01",
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", Collections.emptyMap()));
        HiveConnectorTransaction txn = newTxn(client);
        txn.beginWrite(null, DB, TBL, ctx(false));
        txn.addCommitData(serialize(pu("dt=2024-01-01", TUpdateMode.APPEND, "s3://bucket/db/t/dt=2024-01-01",
                Arrays.asList("f1"), 100, 4)));

        txn.finishInsertTable(nameMapping());

        Assertions.assertTrue(client.calls.stream().anyMatch(c -> c.startsWith("getPartitions")),
                "APPEND into a partitioned table must fetch the existing partition; calls=" + client.calls);
        Assertions.assertFalse(client.calls.contains("partitionExists:[2024-01-01]"),
                "a declared APPEND does not need the NEW-mode existence probe; calls=" + client.calls);
    }

    @Test
    public void testDuplicatePartitionValuesFromHmsFailsLoud() throws TException {
        // Fidelity guard, ported from HiveUtil.convertToNamePartitionMap's Collectors.toMap: if HMS returns
        // two partitions with identical values for a batch, the txn must abort LOUDLY rather than silently
        // overwrite one. A silent HashMap overwrite would shrink the name→partition map below the requested
        // count, and the size()-bounded loop would then drop a trailing partition's INSERT_EXISTING action —
        // a silently lost write instead of a failed load. (HEAD's toMap throws IllegalStateException here.)
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = table(true, Collections.emptyMap());
        HmsPartitionInfo dup = new HmsPartitionInfo(
                Collections.singletonList("2024-01-01"), "s3://bucket/db/t/dt=2024-01-01",
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", Collections.emptyMap());
        client.partitions = Arrays.asList(dup, dup); // HMS returns the same partition values twice
        HiveConnectorTransaction txn = newTxn(client);
        txn.beginWrite(null, DB, TBL, ctx(false));
        txn.addCommitData(serialize(pu("dt=2024-01-01", TUpdateMode.APPEND, "s3://bucket/db/t/dt=2024-01-01",
                Arrays.asList("f1"), 100, 4)));

        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> txn.finishInsertTable(nameMapping()));
        Assertions.assertTrue(ex.getMessage().contains("Duplicate key"),
                "expected a fail-loud abort on duplicate partition values, got: " + ex.getMessage());
    }

    // ─────────────────────────────── commit-time object-store MPU (D6) ───────────────────────────────

    @Test
    public void testCommitCompletesMultipartUploads() throws TException {
        // On commit the FE finalizes the multipart uploads that BE staged on the object store (D6). For an
        // unpartitioned APPEND whose write path already equals the table location (no rename needed), the
        // committer must complete the MPU with the s3://bucket/key URI, the upload id, and the part ETags
        // sorted by part number — a missing/mis-ordered completion would leave the written data invisible.
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = table(false, Collections.emptyMap());
        RecordingObjStorage objStorage = new RecordingObjStorage();
        HiveConnectorTransaction txn = newTxnWithFs(client, new RecordingObjFileSystem(objStorage));
        txn.beginWrite(null, DB, TBL, ctx(false));
        Map<Integer, String> etags = new HashMap<>();
        etags.put(2, "etag-2");
        etags.put(1, "etag-1");
        // writePath == table location "s3://bucket/db/t" -> needRename=false -> objCommit (MPU complete) path.
        txn.addCommitData(serialize(puWithMpu("", TUpdateMode.APPEND, "s3://bucket/db/t",
                "bucket", "db/t/data", "upload-1", etags)));

        txn.commit();
        txn.close();

        Assertions.assertEquals(
                Collections.singletonList("complete:s3://bucket/db/t/data:upload-1:[1=etag-1, 2=etag-2]"),
                objStorage.calls,
                "commit must complete the MPU once with the ETags sorted by part number; calls=" + objStorage.calls);
        Assertions.assertTrue(client.calls.contains("updateTableStatistics:" + DB + "." + TBL),
                "an unpartitioned INSERT_EXISTING must also update the table statistics; calls=" + client.calls);
    }

    @Test
    public void testRollbackAbortsPendingMultipartUploads() throws TException {
        // rollback() is NOT a no-op for hive (D9): data files are staged before commit, so a rollback must
        // abort the in-flight object-store multipart uploads — otherwise they linger server-side (leaked /
        // billed). Here the committer was never built (rollback before commit), so the pending uploads are
        // collected straight from the accumulated commit fragments.
        RecordingObjStorage objStorage = new RecordingObjStorage();
        HiveConnectorTransaction txn = newTxnWithFs(new RecordingHmsClient(),
                new RecordingObjFileSystem(objStorage));
        txn.addCommitData(serialize(puWithMpu("", TUpdateMode.APPEND, "s3://bucket/db/t",
                "bucket", "db/t/data", "upload-9", Collections.singletonMap(1, "etag-1"))));

        txn.rollback();
        txn.close();

        Assertions.assertEquals(Collections.singletonList("abort:s3://bucket/db/t/data:upload-9"),
                objStorage.calls,
                "rollback must abort the pending MPU exactly once; calls=" + objStorage.calls);
    }

    @Test
    public void testSecondRollbackIsIdempotent() throws TException {
        // Rollback must be safe to call more than once (the engine may retry cleanup): the second call must
        // neither throw nor re-abort an already-aborted upload (a double abort would error against the store).
        RecordingObjStorage objStorage = new RecordingObjStorage();
        HiveConnectorTransaction txn = newTxnWithFs(new RecordingHmsClient(),
                new RecordingObjFileSystem(objStorage));
        txn.addCommitData(serialize(puWithMpu("", TUpdateMode.APPEND, "s3://bucket/db/t",
                "bucket", "db/t/data", "upload-9", Collections.singletonMap(1, "etag-1"))));

        txn.rollback();
        Assertions.assertDoesNotThrow(txn::rollback, "a second rollback must not throw");
        txn.close();

        Assertions.assertEquals(Collections.singletonList("abort:s3://bucket/db/t/data:upload-9"),
                objStorage.calls,
                "the pending MPU must be aborted exactly once across repeated rollbacks; calls=" + objStorage.calls);
    }

    @Test
    public void testCommitAddsNewPartitionOnce() throws TException {
        // GAP-7: the 20-at-a-time batching moved INTO ThriftHmsClient.addPartitions, so the committer must
        // call addPartitions ONCE with the whole list (not re-batch it). GAP-4: the new partition's storage
        // descriptor (values/location/columns) is rebuilt from the table at commit time. A genuinely-new
        // partition takes the ADD path; on FILE_S3 the write path == target path, so no rename/MPU runs and
        // the object-store FileSystem is never resolved (hence newTxn, not newTxnWithFs).
        RecordingHmsClient client = new RecordingHmsClient();
        client.table = table(true, Collections.emptyMap());
        client.partitionExistsResult = false;
        HiveConnectorTransaction txn = newTxn(client);
        txn.beginWrite(null, DB, TBL, ctx(false));
        txn.addCommitData(serialize(pu("dt=2024-01-01", TUpdateMode.NEW, "s3://bucket/db/t/dt=2024-01-01",
                Collections.singletonList("f1"), 100, 4)));

        txn.commit();
        txn.close();

        Assertions.assertEquals(1, client.addedPartitions.size(),
                "exactly one partition must be added; calls=" + client.calls);
        Assertions.assertEquals(1L, client.calls.stream().filter(c -> c.startsWith("addPartitions:")).count(),
                "addPartitions must be invoked exactly once (batching lives in the client); calls=" + client.calls);
        HmsPartitionWithStatistics added = client.addedPartitions.get(0);
        Assertions.assertEquals(Collections.singletonList("2024-01-01"), added.getPartitionValues());
        Assertions.assertEquals("s3://bucket/db/t/dt=2024-01-01", added.getLocation());
        Assertions.assertEquals(Collections.singletonList("c1"),
                added.getColumns().stream().map(FieldSchema::getName).collect(Collectors.toList()),
                "the new partition's SD columns must be rebuilt from the table schema");
    }

    /**
     * Recording {@link HmsClient} fake: implements the abstract read surface, records the calls the
     * transaction makes, and returns canned metadata. The Phase-3+ write primitives ({@code addPartitions} /
     * statistics / {@code dropPartition}) are recorded too — the committer reaches them once the FS tests
     * drive a full commit.
     */
    private static final class RecordingHmsClient implements HmsClient {
        private final List<String> calls = new ArrayList<>();
        private final List<HmsPartitionWithStatistics> addedPartitions = new ArrayList<>();
        private HmsTableInfo table;
        private boolean partitionExistsResult;
        private List<HmsPartitionInfo> partitions = Collections.emptyList();

        @Override
        public List<String> listDatabases() {
            return Collections.emptyList();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            throw new UnsupportedOperationException("getDatabase");
        }

        @Override
        public List<String> listTables(String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return table != null;
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            calls.add("getTable:" + dbName + "." + tableName);
            if (table == null) {
                throw new UnsupportedOperationException("no canned table");
            }
            return table;
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            return Collections.emptyMap();
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            return Collections.emptyList();
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            calls.add("getPartitions:" + partNames);
            return partitions;
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new UnsupportedOperationException("getPartition");
        }

        @Override
        public boolean partitionExists(String dbName, String tableName, List<String> partitionValues) {
            calls.add("partitionExists:" + partitionValues);
            return partitionExistsResult;
        }

        @Override
        public void addPartitions(String dbName, String tableName, List<HmsPartitionWithStatistics> partitions) {
            calls.add("addPartitions:" + dbName + "." + tableName + ":" + partitions.size());
            addedPartitions.addAll(partitions);
        }

        @Override
        public void updateTableStatistics(String dbName, String tableName,
                Function<HmsPartitionStatistics, HmsPartitionStatistics> update) {
            calls.add("updateTableStatistics:" + dbName + "." + tableName);
        }

        @Override
        public void updatePartitionStatistics(String dbName, String tableName, String partitionName,
                Function<HmsPartitionStatistics, HmsPartitionStatistics> update) {
            calls.add("updatePartitionStatistics:" + dbName + "." + tableName + ":" + partitionName);
        }

        @Override
        public boolean dropPartition(String dbName, String tableName, List<String> partitionValues,
                boolean deleteData) {
            calls.add("dropPartition:" + partitionValues + ":" + deleteData);
            return true;
        }

        @Override
        public void close() {
        }
    }

    /**
     * Recording {@link ObjStorage} fake capturing both multipart-upload completions and aborts (the two
     * object-store operations the commit/rollback paths reach). Every other operation fails loud so an
     * unexpected code path surfaces immediately rather than silently no-op'ing.
     */
    private static final class RecordingObjStorage implements ObjStorage<Object> {
        private final List<String> calls = new ArrayList<>();

        @Override
        public void completeMultipartUpload(String remotePath, String uploadId, List<UploadPartResult> parts) {
            String partStr = parts.stream().map(p -> p.partNumber() + "=" + p.etag())
                    .collect(Collectors.joining(", ", "[", "]"));
            calls.add("complete:" + remotePath + ":" + uploadId + ":" + partStr);
        }

        @Override
        public void abortMultipartUpload(String remotePath, String uploadId) {
            calls.add("abort:" + remotePath + ":" + uploadId);
        }

        @Override
        public Object getClient() {
            throw new UnsupportedOperationException("getClient");
        }

        @Override
        public RemoteObjects listObjects(String remotePath, String continuationToken) {
            throw new UnsupportedOperationException("listObjects");
        }

        @Override
        public RemoteObject headObject(String remotePath) {
            throw new UnsupportedOperationException("headObject");
        }

        @Override
        public void putObject(String remotePath, RequestBody requestBody) {
            throw new UnsupportedOperationException("putObject");
        }

        @Override
        public void deleteObject(String remotePath) {
            throw new UnsupportedOperationException("deleteObject");
        }

        @Override
        public void copyObject(String srcPath, String dstPath) {
            throw new UnsupportedOperationException("copyObject");
        }

        @Override
        public String initiateMultipartUpload(String remotePath) {
            throw new UnsupportedOperationException("initiateMultipartUpload");
        }

        @Override
        public UploadPartResult uploadPart(String remotePath, String uploadId, int partNum, RequestBody body) {
            throw new UnsupportedOperationException("uploadPart");
        }

        @Override
        public void close() {
        }
    }

    /**
     * Fake {@link ObjFileSystem} over a {@link RecordingObjStorage}. {@code ObjFileSystem} already provides
     * {@code exists()}/{@code close()} and the {@code completeMultipartUpload(Map)} overload that converts the
     * ETag map to a sorted part list before delegating to the storage; the remaining core FileSystem methods
     * are not exercised by the MPU tests and fail loud if unexpectedly reached.
     */
    private static final class RecordingObjFileSystem extends ObjFileSystem {
        RecordingObjFileSystem(ObjStorage<?> objStorage) {
            super(objStorage);
        }

        @Override
        public void mkdirs(Location location) {
            throw new UnsupportedOperationException("mkdirs");
        }

        @Override
        public void delete(Location location, boolean recursive) {
            throw new UnsupportedOperationException("delete");
        }

        @Override
        public void rename(Location src, Location dst) {
            throw new UnsupportedOperationException("rename");
        }

        @Override
        public FileIterator list(Location location) {
            throw new UnsupportedOperationException("list");
        }

        @Override
        public DorisInputFile newInputFile(Location location) {
            throw new UnsupportedOperationException("newInputFile");
        }

        @Override
        public DorisOutputFile newOutputFile(Location location) {
            throw new UnsupportedOperationException("newOutputFile");
        }
    }
}
