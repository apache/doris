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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Primary-key split planning against a real fluss cluster started in this JVM.
 *
 * <p>{@link FlussSplitPlanTest} proves the planning logic over recorded snapshot answers; this proves
 * the premise those recordings encode — that a cluster really reports a never-snapshotted bucket as
 * "no snapshot at all" rather than snapshot 0, and that the log offset it reports alongside a snapshot
 * is the one a reader has to resume from. A recording that has drifted from the real answer leaves
 * every unit test above green while a primary-key query returns superseded rows.
 *
 * <p>Snapshots are <em>triggered</em>, not waited for: the cluster's periodic interval is ten minutes,
 * so nothing snapshots on its own and every test states exactly which buckets have one. That is what
 * makes "this bucket has a snapshot and a log tail after it" and "this bucket has none" both
 * deterministic in the same class.
 *
 * <p>Named {@code ...Test}, not {@code ...ITCase}: surefire's default includes do not match
 * {@code *ITCase}, so that name would leave the class silently unexecuted with a green build.
 */
public class FlussPkScanPlanClusterTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER = FlussClusterExtension.builder()
            .setNumOfTabletServers(1)
            .build();

    /** One bucket, so every fixture row lands where the test can trigger a snapshot for it. */
    private static final int BUCKETS = 1;

    /** The cluster extension drops every non-built-in database after each test. */
    private static int databaseCounter;

    private static Connection connection;
    private static Admin admin;
    private static Connector connector;

    private String db;

    @BeforeAll
    public static void connectToCluster() {
        connection = ConnectionFactory.createConnection(FLUSS_CLUSTER.getClientConfig());
        admin = connection.getAdmin();

        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(FlussConnectorProperties.BOOTSTRAP_SERVERS,
                FLUSS_CLUSTER.getBootstrapServers());
        connector = new FlussConnectorProvider().create(catalogProperties, new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "fluss_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        });
    }

    @AfterAll
    public static void disconnect() throws Exception {
        if (connector != null) {
            connector.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @BeforeEach
    public void createTables() throws Exception {
        db = "doris_pk_scan_plan_test_" + (++databaseCounter);
        admin.createDatabase(db, DatabaseDescriptor.EMPTY, true).get();

        admin.createTable(TablePath.of(db, "pk_table"),
                TableDescriptor.builder()
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.BIGINT().copy(false))
                                .column("name", DataTypes.STRING())
                                .primaryKey("id")
                                .build())
                        .distributedBy(BUCKETS, "id")
                        .build(),
                true).get();
    }

    /**
     * The two halves of a primary-key range have to line up with each other: the snapshot the cluster
     * reports, and the change log resumed at exactly where that snapshot ended. Resuming earlier
     * replays changes the snapshot already holds; resuming later drops the ones it does not.
     */
    @Test
    public void snapshottedBucketResumesTheChangeLogWhereTheSnapshotEnded() throws Exception {
        TablePath tablePath = TablePath.of(db, "pk_table");
        upsert(tablePath, 1, 2, 3);
        FLUSS_CLUSTER.triggerAndWaitSnapshot(new TableBucket(tableId(tablePath), 0));
        // A tail the snapshot does not cover. Nothing snapshots it: the periodic interval is ten
        // minutes and this test does not trigger again.
        upsert(tablePath, 4, 5);

        List<ConnectorScanRange> ranges = plan("pk_table");

        KvSnapshots snapshots = admin.getLatestKvSnapshots(tablePath).get();
        long latest = admin.listOffsets(tablePath, Collections.singletonList(0),
                new OffsetSpec.LatestSpec()).all().get().get(0);
        Assertions.assertEquals(1, ranges.size());
        Map<String, String> props = ranges.get(0).getProperties();
        Assertions.assertEquals("PK_FULL", props.get("fluss.range_type"));
        Assertions.assertEquals("0", props.get("fluss.bucket_id"));
        Assertions.assertEquals(String.valueOf(snapshots.getSnapshotId(0).orElse(-1L)),
                props.get("fluss.kv_snapshot_id"));
        Assertions.assertEquals(String.valueOf(snapshots.getLogOffset(0).orElse(-2L)),
                props.get("fluss.log_start_offset"));
        Assertions.assertEquals(String.valueOf(latest), props.get("fluss.log_stop_offset"));

        // And the fixture really did produce the shape this test is about, rather than passing because
        // there was no snapshot and no tail to disagree about.
        long logStart = Long.parseLong(props.get("fluss.log_start_offset"));
        Assertions.assertTrue(Long.parseLong(props.get("fluss.kv_snapshot_id")) >= 0, props.toString());
        Assertions.assertTrue(logStart > 0, props.toString());
        Assertions.assertTrue(logStart < latest, props.toString());
    }

    /**
     * With no snapshot the state has to be rebuilt from the whole change log, and the cluster says so
     * by reporting no snapshot id AND no log offset for the bucket. Reading either as a number — 0 for
     * the snapshot, 0 for the offset — would silently read a snapshot that does not exist.
     */
    @Test
    public void bucketFlussHasNeverSnapshottedReplaysItsWholeChangeLog() throws Exception {
        TablePath tablePath = TablePath.of(db, "pk_table");
        upsert(tablePath, 1, 2, 3);

        List<ConnectorScanRange> ranges = plan("pk_table");

        KvSnapshots snapshots = admin.getLatestKvSnapshots(tablePath).get();
        Assertions.assertFalse(snapshots.getSnapshotId(0).isPresent(), "the fixture snapshotted");
        Assertions.assertFalse(snapshots.getLogOffset(0).isPresent(), "the fixture snapshotted");
        Assertions.assertEquals(1, ranges.size());
        Map<String, String> props = ranges.get(0).getProperties();
        Assertions.assertEquals("PK_FULL", props.get("fluss.range_type"));
        Assertions.assertEquals("-1", props.get("fluss.kv_snapshot_id"));
        Assertions.assertEquals("-2", props.get("fluss.log_start_offset"));
        long latest = admin.listOffsets(tablePath, Collections.singletonList(0),
                new OffsetSpec.LatestSpec()).all().get().get(0);
        Assertions.assertEquals(String.valueOf(latest), props.get("fluss.log_stop_offset"));
    }

    @Test
    public void primaryKeyTableNothingWasWrittenToPlansNoRanges() {
        Assertions.assertTrue(plan("pk_table").isEmpty());
    }

    /**
     * A partitioned primary-key table is snapshotted per partition, so its snapshots have to be asked
     * for per partition too. Asking at table level returns the wrong partition's offsets, which reads
     * the change log from the wrong place.
     */
    @Test
    public void partitionedPrimaryKeyRangesCarryTheirOwnPartitionSnapshots() throws Exception {
        TablePath tablePath = TablePath.of(db, "pk_part");
        admin.createTable(tablePath,
                TableDescriptor.builder()
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.BIGINT().copy(false))
                                .column("dt", DataTypes.STRING().copy(false))
                                .primaryKey("id", "dt")
                                .build())
                        .partitionedBy("dt")
                        .distributedBy(BUCKETS, "id")
                        .build(),
                true).get();
        admin.createPartition(tablePath,
                new PartitionSpec(Collections.singletonMap("dt", "2026_08_03")), true).get();
        admin.createPartition(tablePath,
                new PartitionSpec(Collections.singletonMap("dt", "2026_08_04")), true).get();
        long partitionId = partitionId(tablePath, "2026_08_03");
        upsertPartitioned(tablePath, "2026_08_03", 1, 2);
        FLUSS_CLUSTER.triggerAndWaitSnapshot(new TableBucket(tableId(tablePath), partitionId, 0));
        upsertPartitioned(tablePath, "2026_08_03", 3);

        List<ConnectorScanRange> ranges = plan("pk_part");

        // Only the written partition has anything to read; the empty one yields no range.
        Assertions.assertEquals(1, ranges.size());
        Map<String, String> props = ranges.get(0).getProperties();
        Assertions.assertEquals("PK_FULL", props.get("fluss.range_type"));
        Assertions.assertEquals("dt=2026_08_03", props.get("fluss.partition_name"));
        Assertions.assertEquals(String.valueOf(partitionId), props.get("fluss.partition_id"));
        KvSnapshots snapshots = admin.getLatestKvSnapshots(tablePath, "2026_08_03").get();
        Assertions.assertEquals(String.valueOf(snapshots.getSnapshotId(0).orElse(-1L)),
                props.get("fluss.kv_snapshot_id"));
        Assertions.assertEquals(String.valueOf(snapshots.getLogOffset(0).orElse(-2L)),
                props.get("fluss.log_start_offset"));
        Assertions.assertTrue(Long.parseLong(props.get("fluss.kv_snapshot_id")) >= 0, props.toString());
    }

    private List<ConnectorScanRange> plan(String tableName) {
        FlussTestSession session = new FlussTestSession(1L, "cluster-pk-plan");
        ConnectorTableHandle handle = connector.getMetadata(session)
                .getTableHandle(session, db, tableName).orElseThrow(AssertionError::new);
        return connector.getScanPlanProvider().planScan(session,
                ConnectorScanRequest.builder(handle, Collections.emptyList()).build());
    }

    private static long tableId(TablePath tablePath) throws Exception {
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    private static long partitionId(TablePath tablePath, String partitionName) throws Exception {
        return admin.listPartitionInfos(tablePath).get().stream()
                .filter(p -> p.getPartitionName().equals(partitionName))
                .findFirst().orElseThrow(AssertionError::new)
                .getPartitionId();
    }

    private static void upsert(TablePath tablePath, long... ids) throws Exception {
        try (Table table = connection.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (long id : ids) {
                writer.upsert(GenericRow.of(id, BinaryString.fromString("name-" + id)));
            }
            writer.flush();
        }
    }

    private static void upsertPartitioned(TablePath tablePath, String partition, long... ids)
            throws Exception {
        try (Table table = connection.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (long id : ids) {
                writer.upsert(GenericRow.of(id, BinaryString.fromString(partition)));
            }
            writer.flush();
        }
    }
}
