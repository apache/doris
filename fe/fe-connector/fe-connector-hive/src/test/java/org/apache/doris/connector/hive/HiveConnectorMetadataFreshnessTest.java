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

import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTableFreshness;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Tests {@link HiveConnectorMetadata#getTableFreshness} / {@link HiveConnectorMetadata#getPartitionFreshnessMillis}
 * (HMS cutover — MVCC/MTMV substep, dormant).
 *
 * <p><b>Why these matter:</b> a flipped hive table is a {@code PluginDrivenMvccExternalTable}, and MTMV
 * change-detection over it must vary with the source. Hive's whole-table / per-partition change signal is a
 * last-modified TIMESTAMP ({@code transient_lastDdlTime}), NOT a snapshot id — so these methods must reproduce
 * legacy {@code HiveDlaTable}:</p>
 * <ul>
 *   <li>table freshness = the table's last-DDL time (unpartitioned) or the max partition modify time
 *       (partitioned), carrying the owning partition name so dropping it is detected as a change;</li>
 *   <li>the seconds-&gt;millis (&times;1000) conversion and absent-&gt;0 policy live connector-side (fe-core must
 *       not parse the raw HMS property);</li>
 *   <li>the unpartitioned table pays NO {@code get_partitions_by_names} round-trip (the time is already on the
 *       handle), and per-partition freshness is fetched on demand here (the MTMV path), never in the names-only
 *       {@code listPartitions} hot path.</li>
 * </ul>
 */
public class HiveConnectorMetadataFreshnessTest {

    private static final List<String> PART_KEYS = Arrays.asList("year", "month");
    private static final String TRANSIENT_LAST_DDL_TIME = "transient_lastDdlTime";

    private HiveConnectorMetadata metadata(FakeHmsClient client) {
        return new HiveConnectorMetadata(client, Collections.emptyMap(), new FakeConnectorContext());
    }

    private HiveTableHandle partitionedHandle() {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(PART_KEYS)
                .build();
    }

    private HiveTableHandle unpartitionedHandle(Map<String, String> tableParams) {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(Collections.emptyList())
                .tableParameters(tableParams)
                .build();
    }

    // ==================== table freshness: unpartitioned ====================

    @Test
    public void testUnpartitionedUsesTableLastDdlTimeAndNoRoundTrip() {
        FakeHmsClient client = new FakeHmsClient();
        // transient_lastDdlTime is in SECONDS; the connector must return millis (x1000), parity
        // HMSExternalTable.getLastDdlTime.
        ConnectorTableFreshness freshness = metadata(client).getTableFreshness(null,
                unpartitionedHandle(Collections.singletonMap(TRANSIENT_LAST_DDL_TIME, "100"))).orElse(null);

        Assertions.assertNotNull(freshness);
        Assertions.assertEquals("t", freshness.getName(),
                "an unpartitioned table's freshness is named by the table (parity MTMVMaxTimestampSnapshot)");
        Assertions.assertEquals(100_000L, freshness.getTimestampMillis(),
                "transient_lastDdlTime seconds must be converted to millis");
        // The time is already on the handle; touching the metastore here would be a needless round-trip.
        Assertions.assertFalse(client.listPartitionNamesCalled,
                "an unpartitioned table must not list partitions for freshness");
        Assertions.assertFalse(client.getPartitionsCalled,
                "an unpartitioned table must not fetch partitions for freshness");
    }

    @Test
    public void testUnpartitionedAbsentParamReturnsZero() {
        // Absent transient_lastDdlTime -> 0 (parity HMSExternalTable.getLastDdlTime, e.g. hive views).
        ConnectorTableFreshness freshness = metadata(new FakeHmsClient()).getTableFreshness(null,
                unpartitionedHandle(Collections.emptyMap())).orElse(null);
        Assertions.assertNotNull(freshness);
        Assertions.assertEquals("t", freshness.getName());
        Assertions.assertEquals(0L, freshness.getTimestampMillis(),
                "an absent transient_lastDdlTime must yield 0, not a crash");
    }

    // ==================== table freshness: partitioned ====================

    @Test
    public void testPartitionedReturnsMaxModifyTimeAndOwningPartitionName() {
        FakeHmsClient client = new FakeHmsClient()
                .partition("year=2023/month=12", 100L)
                .partition("year=2024/month=01", 300L)   // the max
                .partition("year=2024/month=02", 200L);

        ConnectorTableFreshness freshness =
                metadata(client).getTableFreshness(null, partitionedHandle()).orElse(null);

        Assertions.assertNotNull(freshness);
        // Parity HiveDlaTable.getTableSnapshot: max(partition lastModifiedTime) + the owning partition NAME,
        // rendered from values (raw key=value join, parity HivePartition.getPartitionName).
        Assertions.assertEquals("year=2024/month=01", freshness.getName(),
                "the freshness must be named by the partition owning the max modify time");
        Assertions.assertEquals(300_000L, freshness.getTimestampMillis(),
                "the freshness millis must be the max transient_lastDdlTime x 1000");
    }

    @Test
    public void testPartitionedEmptyPartitionSetReturnsTableNameZero() {
        // No partitions -> MTMVMaxTimestampSnapshot(tableName, 0) parity HiveDlaTable.getTableSnapshot.
        ConnectorTableFreshness freshness =
                metadata(new FakeHmsClient()).getTableFreshness(null, partitionedHandle()).orElse(null);
        Assertions.assertNotNull(freshness);
        Assertions.assertEquals("t", freshness.getName());
        Assertions.assertEquals(0L, freshness.getTimestampMillis());
    }

    @Test
    public void testPartitionedTieKeepsFirstMax() {
        // Two partitions share the max: strictly-greater comparison keeps the FIRST (parity HiveDlaTable's
        // `> maxVersionTime`), so the earlier-listed partition name wins.
        FakeHmsClient client = new FakeHmsClient()
                .partition("year=2024/month=01", 300L)
                .partition("year=2024/month=02", 300L);
        ConnectorTableFreshness freshness =
                metadata(client).getTableFreshness(null, partitionedHandle()).orElse(null);
        Assertions.assertNotNull(freshness);
        Assertions.assertEquals("year=2024/month=01", freshness.getName(),
                "a tie on the max modify time must keep the first partition (strictly-greater)");
        Assertions.assertEquals(300_000L, freshness.getTimestampMillis());
    }

    // ==================== per-partition freshness ====================

    @Test
    public void testPartitionFreshnessMillis() {
        FakeHmsClient client = new FakeHmsClient()
                .partition("year=2024/month=01", 300L);
        OptionalLong millis = metadata(client).getPartitionFreshnessMillis(null, partitionedHandle(),
                "year=2024/month=01");
        Assertions.assertTrue(millis.isPresent());
        Assertions.assertEquals(300_000L, millis.getAsLong(),
                "per-partition freshness is transient_lastDdlTime x 1000 (parity HivePartition.getLastModifiedTime)");
    }

    @Test
    public void testPartitionFreshnessMillisAbsentParamZero() {
        FakeHmsClient client = new FakeHmsClient().partitionNoParam("year=2024/month=01");
        OptionalLong millis = metadata(client).getPartitionFreshnessMillis(null, partitionedHandle(),
                "year=2024/month=01");
        Assertions.assertTrue(millis.isPresent());
        Assertions.assertEquals(0L, millis.getAsLong(),
                "an absent transient_lastDdlTime on a partition must yield 0");
    }

    @Test
    public void testPartitionFreshnessMillisVanishedPartitionReturnsEmpty() {
        // A partition that vanished between the materialize (existence check) and this fetch (a rare
        // refresh-time race): return EMPTY so fe-core raises the legacy "can not find partition" (parity
        // HiveDlaTable.checkPartitionExists), rather than emitting a bogus MTMVTimestampSnapshot(0).
        OptionalLong millis = metadata(new FakeHmsClient()).getPartitionFreshnessMillis(null,
                partitionedHandle(), "year=2024/month=01");
        Assertions.assertFalse(millis.isPresent(),
                "a vanished partition must yield empty (fe-core throws 'can not find partition')");
    }

    // ==================== query-begin pin: flags last-modified freshness ====================

    @Test
    public void testBeginQuerySnapshotIsEmptyPinFlaggedLastModified() {
        // Hive's query-begin pin is a non-MVCC EMPTY pin (snapshot id -1, no scan options) but FLAGGED
        // lastModifiedFreshness, so the generic model serves this table's MTMV snapshots from the last-modified
        // freshness SPI instead of pinning a constant snapshot id. The flag rides on the pin so a snapshot-id
        // connector (which leaves it false) never fires the freshness probe.
        ConnectorMvccSnapshot pin = metadata(new FakeHmsClient())
                .beginQuerySnapshot(null, partitionedHandle()).orElse(null);
        Assertions.assertNotNull(pin);
        Assertions.assertEquals(-1L, pin.getSnapshotId(),
                "hive's pin is the empty (-1) pin: scan reads current (applySnapshot is a no-op)");
        Assertions.assertTrue(pin.isLastModifiedFreshness(),
                "hive's pin must flag last-modified freshness so MTMV freshness comes from the on-demand SPI");
    }

    /**
     * Minimal {@link HmsClient} double: records the requested partitions by name and returns their
     * parameters (with {@code transient_lastDdlTime}) via {@code getPartitions}; {@code listPartitionNames}
     * returns the registered names. Records which of the two metastore calls were made.
     */
    private static final class FakeHmsClient implements HmsClient {
        // Insertion-ordered so getPartitions returns partitions in registration order (drives the tie test).
        private final Map<String, Long> partitionDdlSeconds = new LinkedHashMap<>();
        private final List<String> paramlessPartitions = new ArrayList<>();
        private boolean listPartitionNamesCalled;
        private boolean getPartitionsCalled;

        FakeHmsClient partition(String name, long ddlSeconds) {
            partitionDdlSeconds.put(name, ddlSeconds);
            return this;
        }

        FakeHmsClient partitionNoParam(String name) {
            paramlessPartitions.add(name);
            return this;
        }

        private HmsPartitionInfo toInfo(String name) {
            List<String> values = HiveWriteUtils.toPartitionValues(name);
            Map<String, String> params;
            if (paramlessPartitions.contains(name)) {
                params = Collections.emptyMap();
            } else {
                params = Collections.singletonMap(TRANSIENT_LAST_DDL_TIME,
                        Long.toString(partitionDdlSeconds.get(name)));
            }
            return new HmsPartitionInfo(values, "loc", "if", "of", "serde", params);
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            listPartitionNamesCalled = true;
            List<String> names = new ArrayList<>(partitionDdlSeconds.keySet());
            names.addAll(paramlessPartitions);
            return names;
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            getPartitionsCalled = true;
            List<HmsPartitionInfo> result = new ArrayList<>();
            for (String name : partNames) {
                if (partitionDdlSeconds.containsKey(name) || paramlessPartitions.contains(name)) {
                    result.add(toInfo(name));
                }
            }
            return result;
        }

        @Override
        public List<String> listDatabases() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listTables(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
