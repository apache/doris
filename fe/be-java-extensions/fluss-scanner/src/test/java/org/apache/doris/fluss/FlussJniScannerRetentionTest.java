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

package org.apache.doris.fluss;

import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.ManualClock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Fluss server states that an ordinary append-and-read test cannot produce on demand. */
public class FlussJniScannerRetentionTest {

    private static final ManualClock CLOCK = new ManualClock(System.currentTimeMillis());

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER = FlussClusterExtension.builder()
            .setNumOfTabletServers(1)
            .setClock(CLOCK)
            .setClusterConf(new Configuration()
                    .set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ZERO)
                    .set(ConfigOptions.LOG_RETENTION_ROLL_ACTIVE_SEGMENT_ENABLED, true))
            .build();

    private static FlussConnection connection;
    private static Admin admin;
    private static int databaseCounter;

    private String database;

    @BeforeAll
    public static void connect() {
        connection = (FlussConnection) ConnectionFactory.createConnection(FLUSS_CLUSTER.getClientConfig());
        admin = connection.getAdmin();
    }

    @AfterAll
    public static void disconnect() throws Exception {
        connection.close();
    }

    @BeforeEach
    public void createDatabase() throws Exception {
        database = "doris_fluss_retention_" + (++databaseCounter);
        admin.createDatabase(database, DatabaseDescriptor.EMPTY, true).get();
    }

    @Test
    public void expiredWrittenBucketHasEarliestEqualToPositiveLatest() throws Exception {
        TablePath path = createLogTable("expired", Map.of("table.log.ttl", "1s"));
        append(path, 1);
        TableBucket bucket = bucket(path);
        LogTablet log = replica(bucket).getLogTablet();
        Assertions.assertEquals(1L, log.getHighWatermark());

        CLOCK.advanceTime(Duration.ofSeconds(2));
        log.deleteExpiredSegments();
        log.deleteExpiredSegments();

        Assertions.assertEquals(1L, log.localLogStartOffset());
        Assertions.assertEquals(1L, offset(path, new OffsetSpec.EarliestSpec()));
        Assertions.assertEquals(1L, offset(path, new OffsetSpec.LatestSpec()));
    }

    @Test
    public void lakeCoveredGapIsReportedForFlussOnlyAndPinnedTailReads() throws Exception {
        TablePath path = createLogTable("lake_gap", Map.of("table.log.ttl", "1s"));
        append(path, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        TableBucket bucket = bucket(path);
        LogTablet log = replica(bucket).getLogTablet();
        log.roll(Optional.empty());
        CLOCK.advanceTime(Duration.ofSeconds(2));
        append(path, 10, 11);
        log.deleteExpiredSegments();
        // Fluss's fetch path tests the lake offset interval, not the table's Paimon files. Stamp
        // that interval directly so this embedded server exercises the same empty-success reply
        // as a tiered table, without bringing up a tiering service for the test.
        log.updateLakeLogStartOffset(0L);
        log.updateLakeLogEndOffset(10L);

        Assertions.assertEquals(10L, log.localLogStartOffset());
        Assertions.assertEquals(0L, offset(path, new OffsetSpec.EarliestSpec()));
        Assertions.assertEquals(12L, offset(path, new OffsetSpec.LatestSpec()));
        BoundedLogRecords.ProbeResult lakeOnly = new FlussLogRangeProbe(connection, bucket).probe(0L);
        Assertions.assertFalse(lakeOnly.mayBeReadable);
        Assertions.assertEquals(12L, lakeOnly.highWatermark);

        assertUnreadable(path, bucket, LogScanner.EARLIEST_OFFSET, 12L);
        assertUnreadable(path, bucket, 0L, 12L);
    }

    private void assertUnreadable(TablePath path, TableBucket bucket, long start, long stop)
            throws Exception {
        AtomicLong now = new AtomicLong();
        try (Table table = connection.getTable(path)) {
            LogScanner scanner = table.newScan().project(new int[] {0}).createLogScanner();
            try (BoundedLogRecords records = new BoundedLogRecords(scanner, bucket, start, stop,
                    true, path.toString(), new FlussLogRangeProbe(connection, bucket), now::get)) {
                now.set(Duration.ofSeconds(6).toNanos());
                IOException failure = Assertions.assertThrows(IOException.class,
                        () -> records.poll(Duration.ofMillis(100)));
                Assertions.assertTrue(failure.getMessage().contains("local or remote"), failure.getMessage());
            }
        }
    }

    private TablePath createLogTable(String name, Map<String, String> properties) throws Exception {
        TablePath path = TablePath.of(database, name);
        admin.createTable(path, TableDescriptor.builder()
                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                .distributedBy(1)
                .properties(properties)
                .build(), true).get();
        return path;
    }

    private static void append(TablePath path, int... ids) throws Exception {
        try (Table table = connection.getTable(path)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int id : ids) {
                writer.append(GenericRow.of(id));
            }
            writer.flush();
        }
    }

    private static TableBucket bucket(TablePath path) {
        try (Table table = connection.getTable(path)) {
            return new TableBucket(table.getTableInfo().getTableId(), 0);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Replica replica(TableBucket bucket) {
        return FLUSS_CLUSTER.getTabletServerById(0).getReplicaManager().getReplicaOrException(bucket);
    }

    private static long offset(TablePath path, OffsetSpec spec) throws Exception {
        return admin.listOffsets(path, java.util.List.of(0), spec)
                .all().get(10, TimeUnit.SECONDS).get(0);
    }
}
