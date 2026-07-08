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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pins the HMS-cutover §4.4 S4 pivot: {@link HiveConnectorMetadata#getTableHandle} diverts an ICEBERG table to the
 * embedded iceberg sibling connector — returning the sibling's OWN (foreign iceberg) handle verbatim — while HIVE
 * and HUDI tables keep building a {@link HiveTableHandle} on the hive path and never touch the sibling.
 *
 * <p>WHY (Rule 9): S4 is the step that makes a foreign iceberg handle START flowing out of getTableHandle, which is
 * what activates every {@code instanceof HiveTableHandle} guard-and-forward override in {@link HiveConnectorMetadata}
 * (S3). The contract that must hold BEFORE the flip wires it:</p>
 * <ul>
 *   <li>An iceberg-on-HMS table must return the iceberg connector's handle unchanged (so iceberg's own
 *       unconditional {@code (IcebergTableHandle)} cast on the scan/metadata path succeeds) — NOT a HiveTableHandle
 *       stamped ICEBERG, which would CCE the moment iceberg cast it.</li>
 *   <li>HUDI is deliberately left on the hive-handle path (its delegation is a later substep); diverting it now
 *       would silently reroute hudi reads to iceberg.</li>
 *   <li>A pure-hive table must resolve entirely on the hive path, never building the sibling (a hive-only
 *       deployment has no iceberg plugin).</li>
 * </ul>
 *
 * <p>Dormant until hms enters {@code SPI_READY_TYPES}: no production path calls getTableHandle for this connector
 * yet, so these assertions are a guard, not a live-path test.</p>
 */
public class HiveConnectorMetadataTableHandleDivertTest {

    private static final String PARQUET = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    private static final String HUDI = "org.apache.hudi.hadoop.HoodieParquetInputFormat";

    /** The foreign (non-hive) handle the iceberg sibling's getTableHandle produces post-flip. */
    private static final class ForeignHandle implements ConnectorTableHandle {
    }

    private final ForeignHandle siblingHandle = new ForeignHandle();
    private final RecordingSibling siblingConnector = new RecordingSibling(siblingHandle);

    @Test
    public void icebergTableDivertsAndReturnsSiblingHandleUnmodified() {
        HiveConnectorMetadata md = withSibling(icebergTable());

        Optional<ConnectorTableHandle> handle = md.getTableHandle(null, "db", "t");

        Assertions.assertTrue(handle.isPresent(), "an existing iceberg-on-HMS table must resolve a handle");
        Assertions.assertSame(siblingHandle, handle.get(),
                "getTableHandle must return the iceberg sibling's OWN handle unmodified (a rewrap poisons the "
                        + "downstream iceberg cast)");
        Assertions.assertFalse(handle.get() instanceof HiveTableHandle,
                "an iceberg table must NOT be served a HiveTableHandle stamped ICEBERG");
        Assertions.assertEquals(1, siblingConnector.metadata.getTableHandleCalls,
                "the divert must consult the sibling exactly once");
    }

    @Test
    public void icebergDivertPropagatesSiblingEmpty() {
        // The sibling is authoritative for iceberg existence: if its getTableHandle returns empty (e.g. the
        // iceberg catalog cannot load it), the gateway forwards that empty rather than fabricating a hive handle.
        siblingConnector.metadata.returnHandle = null;
        HiveConnectorMetadata md = withSibling(icebergTable());

        Assertions.assertFalse(md.getTableHandle(null, "db", "t").isPresent(),
                "an empty from the sibling must pass through unchanged");
        // Prove the empty was FORWARDED from the sibling (its getTableHandle was consulted), not short-circuited
        // to empty by the gateway — otherwise a broken `if (ICEBERG) return Optional.empty()` would pass this test.
        Assertions.assertEquals(1, siblingConnector.metadata.getTableHandleCalls,
                "the sibling is authoritative for iceberg existence: its getTableHandle must be the source of empty");
    }

    @Test
    public void hiveTableBuildsHiveHandleWithoutConsultingSibling() {
        HiveConnectorMetadata md = withSibling(hiveTable(PARQUET));

        Optional<ConnectorTableHandle> handle = md.getTableHandle(null, "db", "t");

        Assertions.assertTrue(handle.get() instanceof HiveTableHandle, "a hive table resolves a HiveTableHandle");
        Assertions.assertEquals(HiveTableType.HIVE, ((HiveTableHandle) handle.get()).getTableType());
        Assertions.assertEquals(0, siblingConnector.getMetadataCalls,
                "a hive table must never build or consult the iceberg sibling");
    }

    @Test
    public void hudiTableStaysOnHiveHandlePathNotDiverted() {
        // HUDI is NOT diverted in S4: it keeps building a HUDI-stamped HiveTableHandle (its own delegation is a
        // later substep). Diverting it here would silently reroute hudi reads to the iceberg sibling.
        HiveConnectorMetadata md = withSibling(hiveTable(HUDI));

        Optional<ConnectorTableHandle> handle = md.getTableHandle(null, "db", "t");

        Assertions.assertTrue(handle.get() instanceof HiveTableHandle, "a hudi table stays on the hive path");
        Assertions.assertEquals(HiveTableType.HUDI, ((HiveTableHandle) handle.get()).getTableType());
        Assertions.assertEquals(0, siblingConnector.getMetadataCalls,
                "a hudi table must NOT be diverted to the iceberg sibling");
    }

    @Test
    public void missingTableReturnsEmptyWithoutConsultingSibling() {
        HiveConnectorMetadata md = new HiveConnectorMetadata(
                new FakeHmsClient(icebergTable(), false), Collections.emptyMap(), new FakeConnectorContext(),
                () -> siblingConnector, handle -> siblingConnector);

        Assertions.assertFalse(md.getTableHandle(null, "db", "t").isPresent(),
                "a non-existent table short-circuits to empty before any format detection or divert");
        Assertions.assertEquals(0, siblingConnector.getMetadataCalls, "a missing table must not build the sibling");
    }

    @Test
    public void icebergTableFailsLoudWhenNoSiblingConfigured() {
        // The 3-arg constructor (hive-only construction) installs a fail-loud sibling supplier: an iceberg table
        // must raise a clear error, not NPE, when the iceberg plugin is unavailable.
        HiveConnectorMetadata md = new HiveConnectorMetadata(
                new FakeHmsClient(icebergTable(), true), Collections.emptyMap(), new FakeConnectorContext());

        Assertions.assertThrows(DorisConnectorException.class, () -> md.getTableHandle(null, "db", "t"),
                "an iceberg table with no sibling configured must fail loud");
    }

    // ===== helpers =====

    private HiveConnectorMetadata withSibling(HmsTableInfo tableInfo) {
        // getTableHandle diverts iceberg BY TYPE (icebergSiblingSupplier); the by-handle owner resolver is unused
        // in this suite, so it just returns the recording sibling.
        return new HiveConnectorMetadata(new FakeHmsClient(tableInfo, true), Collections.emptyMap(),
                new FakeConnectorContext(), () -> siblingConnector, handle -> siblingConnector);
    }

    private static HmsTableInfo hiveTable(String inputFormat) {
        return HmsTableInfo.builder()
                .dbName("db").tableName("t").tableType("MANAGED_TABLE")
                .inputFormat(inputFormat)
                .build();
    }

    private static HmsTableInfo icebergTable() {
        return HmsTableInfo.builder()
                .dbName("db").tableName("t").tableType("EXTERNAL_TABLE")
                .parameters(Collections.singletonMap("table_type", "ICEBERG"))
                .build();
    }

    /** A sibling {@link Connector} whose getMetadata hands back a recording metadata and counts the builds. */
    private static final class RecordingSibling implements Connector {
        private final RecordingSiblingMetadata metadata;
        private int getMetadataCalls;

        RecordingSibling(ConnectorTableHandle handle) {
            this.metadata = new RecordingSiblingMetadata(handle);
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            getMetadataCalls++;
            return metadata;
        }
    }

    /** Records getTableHandle calls and returns a configurable foreign handle (null -> empty). */
    private static final class RecordingSiblingMetadata implements ConnectorMetadata {
        private ConnectorTableHandle returnHandle;
        private int getTableHandleCalls;

        RecordingSiblingMetadata(ConnectorTableHandle handle) {
            this.returnHandle = handle;
        }

        @Override
        public Optional<ConnectorTableHandle> getTableHandle(ConnectorSession session, String dbName,
                String tableName) {
            getTableHandleCalls++;
            return Optional.ofNullable(returnHandle);
        }
    }

    /** Minimal {@link HmsClient} double serving one prebuilt table; the rest fail loud. */
    private static final class FakeHmsClient implements HmsClient {
        private final HmsTableInfo table;
        private final boolean exists;

        FakeHmsClient(HmsTableInfo table, boolean exists) {
            this.table = table;
            this.exists = exists;
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return exists;
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            return table;
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
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
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
