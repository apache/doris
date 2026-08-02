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
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Split planning against a real fluss cluster started in this JVM.
 *
 * <p>{@link FlussSplitPlanTest} proves the planning logic over recorded answers; this proves the
 * premise those recordings encode — that a cluster really reports a bucket nobody wrote to as offset
 * 0, that the stopping offset planning takes is the one a reader would see, and that a partition's
 * buckets are addressed the way this connector addresses them. A recorded answer that has drifted from
 * the real one makes every unit test above pass while nothing works.
 *
 * <p>Named {@code ...Test}, not {@code ...ITCase}: surefire's default includes do not match
 * {@code *ITCase}, so that name would leave the class silently unexecuted with a green build.
 */
public class FlussLogScanPlanClusterTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER = FlussClusterExtension.builder()
            .setNumOfTabletServers(1)
            .build();

    private static final int BUCKETS = 3;

    /** The cluster extension drops every non-built-in database after each test, so fixtures are per-test. */
    private static int databaseCounter;

    private static Connection connection;
    private static Admin admin;
    private static Connector connector;

    private String db;

    @BeforeAll
    public static void connectToCluster() throws Exception {
        Configuration clientConf = FLUSS_CLUSTER.getClientConfig();
        connection = ConnectionFactory.createConnection(clientConf);
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

    @BeforeEach
    public void createTables() throws Exception {
        db = "doris_scan_plan_test_" + (++databaseCounter);
        admin.createDatabase(db, DatabaseDescriptor.EMPTY, true).get();

        admin.createTable(TablePath.of(db, "log_table"),
                TableDescriptor.builder()
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.BIGINT())
                                .column("name", DataTypes.STRING())
                                .build())
                        .distributedBy(BUCKETS, "id")
                        .build(),
                true).get();

        admin.createTable(TablePath.of(db, "log_part"),
                TableDescriptor.builder()
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.BIGINT())
                                .column("dt", DataTypes.STRING())
                                .build())
                        .partitionedBy("dt")
                        .distributedBy(BUCKETS, "id")
                        .build(),
                true).get();
        admin.createPartition(TablePath.of(db, "log_part"),
                new PartitionSpec(Collections.singletonMap("dt", "2026_08_02")), true).get();
        admin.createPartition(TablePath.of(db, "log_part"),
                new PartitionSpec(Collections.singletonMap("dt", "2026_08_03")), true).get();
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

    /**
     * The stopping offset a range carries has to be the offset the log had actually reached, asked of
     * the same cluster the reader will read. Anything else silently truncates the scan or runs past the
     * end.
     */
    @Test
    public void rangesStopAtTheOffsetsTheClusterReports() throws Exception {
        TablePath tablePath = TablePath.of(db, "log_table");
        appendRows(tablePath, 20);

        Map<Integer, Long> latest = admin.listOffsets(tablePath, allBuckets(), new OffsetSpec.LatestSpec())
                .all().get();
        List<ConnectorScanRange> ranges = plan("log_table");

        Map<Integer, Long> planned = new HashMap<>();
        for (ConnectorScanRange range : ranges) {
            Map<String, String> props = range.getProperties();
            Assertions.assertEquals("LOG", props.get("fluss.range_type"));
            // The earliest sentinel goes out verbatim; only fluss resolves it.
            Assertions.assertEquals("-2", props.get("fluss.log_start_offset"));
            planned.put(Integer.parseInt(props.get("fluss.bucket_id")),
                    Long.parseLong(props.get("fluss.log_stop_offset")));
        }

        // Every non-empty bucket, and only those, with exactly the cluster's offset.
        Map<Integer, Long> expected = new HashMap<>();
        latest.forEach((bucket, offset) -> {
            if (offset > 0) {
                expected.put(bucket, offset);
            }
        });
        Assertions.assertEquals(expected, planned);
        Assertions.assertFalse(expected.isEmpty(), "the fixture wrote no rows anywhere");
        long total = 0;
        for (long offset : expected.values()) {
            total += offset;
        }
        Assertions.assertEquals(20, total, "planned ranges must cover every appended row");
    }

    /** The premise behind skipping empty buckets: a cluster reports a never-written bucket as 0. */
    @Test
    public void tableNothingWasWrittenToPlansNoRanges() {
        Assertions.assertTrue(plan("log_table").isEmpty());
    }

    /**
     * A partitioned table is planned partition by partition, addressed by fluss's own partition name
     * and reported to Doris under the Hive-style one. Getting either wrong is invisible until a query
     * returns the wrong partition's rows.
     */
    @Test
    public void partitionedRangesCarryTheClusterAssignedPartitionIdentity() throws Exception {
        TablePath tablePath = TablePath.of(db, "log_part");
        appendPartitionedRows(tablePath, "2026_08_02", 6);

        List<ConnectorScanRange> ranges = plan("log_part");

        Assertions.assertFalse(ranges.isEmpty());
        long partitionId = admin.listPartitionInfos(tablePath).get().stream()
                .filter(p -> p.getPartitionName().equals("2026_08_02"))
                .findFirst().orElseThrow(AssertionError::new)
                .getPartitionId();
        long rows = 0;
        for (ConnectorScanRange range : ranges) {
            Map<String, String> props = range.getProperties();
            // Only the written partition produced ranges; the empty one has nothing to read.
            Assertions.assertEquals("dt=2026_08_02", props.get("fluss.partition_name"));
            Assertions.assertEquals(String.valueOf(partitionId), props.get("fluss.partition_id"));
            Assertions.assertEquals(Collections.singletonMap("dt", "2026_08_02"),
                    range.getPartitionValues());
            rows += Long.parseLong(props.get("fluss.log_stop_offset"));
        }
        Assertions.assertEquals(6, rows);
    }

    private List<ConnectorScanRange> plan(String tableName) {
        FlussTestSession session = new FlussTestSession(1L, "cluster-plan");
        ConnectorTableHandle handle = connector.getMetadata(session)
                .getTableHandle(session, db, tableName).orElseThrow(AssertionError::new);
        ConnectorScanPlanProvider provider = connector.getScanPlanProvider();
        return provider.planScan(session,
                ConnectorScanRequest.builder(handle, Collections.emptyList()).build());
    }

    private static void appendRows(TablePath tablePath, int rows) throws Exception {
        try (Table table = connection.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < rows; i++) {
                writer.append(GenericRow.of((long) i, BinaryString.fromString("name-" + i)));
            }
            writer.flush();
        }
    }

    private static void appendPartitionedRows(TablePath tablePath, String partition, int rows)
            throws Exception {
        try (Table table = connection.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < rows; i++) {
                writer.append(GenericRow.of((long) i, BinaryString.fromString(partition)));
            }
            writer.flush();
        }
    }

    private static List<Integer> allBuckets() {
        List<Integer> buckets = new ArrayList<>(BUCKETS);
        for (int bucket = 0; bucket < BUCKETS; bucket++) {
            buckets.add(bucket);
        }
        return buckets;
    }
}
