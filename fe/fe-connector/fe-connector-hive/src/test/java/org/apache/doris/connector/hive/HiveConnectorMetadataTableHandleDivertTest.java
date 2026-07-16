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
import java.util.function.Function;

/**
 * Pins the HMS-cutover foreign-handle diverts in {@link HiveConnectorMetadata#getTableHandle}: an ICEBERG table is
 * diverted to the embedded iceberg sibling and a HUDI table (HD-B2, the arming pivot) to the embedded hudi sibling —
 * each returning that sibling's OWN (foreign) handle verbatim — while a plain HIVE table keeps building a
 * {@link HiveTableHandle} on the hive path and never touches either sibling.
 *
 * <p>WHY (Rule 9): these diverts are what makes a foreign iceberg/hudi handle START flowing out of getTableHandle,
 * which activates every {@code instanceof HiveTableHandle} guard-and-forward override in {@link HiveConnectorMetadata}
 * (routed 3-way by the owning sibling). The contract that must hold BEFORE the flip wires it:</p>
 * <ul>
 *   <li>An iceberg-on-HMS table must return the iceberg connector's handle unchanged, and a hudi-on-HMS table the
 *       hudi connector's handle unchanged — so each sibling's own unconditional concrete-handle cast on its
 *       scan/metadata path succeeds — NOT a HiveTableHandle stamped ICEBERG/HUDI, which would CCE the moment the
 *       sibling cast it.</li>
 *   <li>Routing must be BY TYPE and to the RIGHT sibling: a HUDI table must reach the hudi sibling, never the iceberg
 *       one (diverting it to iceberg would CCE / silently mis-read hudi data).</li>
 *   <li>A plain-hive table must resolve entirely on the hive path, never building either sibling (a hive-only
 *       deployment has no iceberg or hudi plugin).</li>
 *   <li>The HUDI arm must not swallow the genuine-UNKNOWN fail-loud (an unsupported non-view format still throws).</li>
 * </ul>
 *
 * <p>Dormant until hms enters {@code SPI_READY_TYPES}: no production path calls getTableHandle for this connector
 * yet, so these assertions are a guard, not a live-path test.</p>
 */
public class HiveConnectorMetadataTableHandleDivertTest {

    private static final String PARQUET = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    private static final String HUDI = "org.apache.hudi.hadoop.HoodieParquetInputFormat";
    private static final String UNKNOWN_FORMAT = "com.example.NotAHiveOrHudiOrIcebergInputFormat";

    // getTableHandle routes BY TYPE (the two by-TYPE suppliers), NEVER via the by-handle owner resolver — so wire
    // the owner resolver to fail loud if anything reaches for it.
    private static final Function<ConnectorTableHandle, Connector> OWNER_RESOLVER_UNUSED = handle -> {
        throw new AssertionError("getTableHandle must divert BY TYPE, never via the by-handle owner resolver");
    };

    /** The foreign (non-hive) handle a sibling's getTableHandle produces post-flip. */
    private static final class ForeignHandle implements ConnectorTableHandle {
    }

    private final ForeignHandle icebergHandle = new ForeignHandle();
    private final ForeignHandle hudiHandle = new ForeignHandle();
    private final RecordingSibling icebergSibling = new RecordingSibling(icebergHandle);
    private final RecordingSibling hudiSibling = new RecordingSibling(hudiHandle);

    @Test
    public void icebergTableDivertsToIcebergSiblingNotHudi() {
        HiveConnectorMetadata md = withSiblings(icebergTable());

        Optional<ConnectorTableHandle> handle = md.getTableHandle(null, "db", "t");

        Assertions.assertTrue(handle.isPresent(), "an existing iceberg-on-HMS table must resolve a handle");
        Assertions.assertSame(icebergHandle, handle.get(),
                "getTableHandle must return the iceberg sibling's OWN handle unmodified (a rewrap poisons the "
                        + "downstream iceberg cast)");
        Assertions.assertFalse(handle.get() instanceof HiveTableHandle,
                "an iceberg table must NOT be served a HiveTableHandle stamped ICEBERG");
        Assertions.assertEquals(1, icebergSibling.metadata.getTableHandleCalls,
                "the divert must consult the iceberg sibling exactly once");
        Assertions.assertEquals(0, hudiSibling.getMetadataCalls,
                "an iceberg table must NEVER build or consult the hudi sibling");
    }

    @Test
    public void icebergDivertPropagatesSiblingEmpty() {
        // The sibling is authoritative for iceberg existence: if its getTableHandle returns empty (e.g. the
        // iceberg catalog cannot load it), the gateway forwards that empty rather than fabricating a hive handle.
        icebergSibling.metadata.returnHandle = null;
        HiveConnectorMetadata md = withSiblings(icebergTable());

        Assertions.assertFalse(md.getTableHandle(null, "db", "t").isPresent(),
                "an empty from the sibling must pass through unchanged");
        // Prove the empty was FORWARDED from the sibling (its getTableHandle was consulted), not short-circuited
        // to empty by the gateway — otherwise a broken `if (ICEBERG) return Optional.empty()` would pass this test.
        Assertions.assertEquals(1, icebergSibling.metadata.getTableHandleCalls,
                "the sibling is authoritative for iceberg existence: its getTableHandle must be the source of empty");
    }

    @Test
    public void hudiTableDivertsToHudiSiblingNotIceberg() {
        // HD-B2 arming pivot: a hudi-on-HMS table is diverted to the hudi sibling and returns the hudi sibling's
        // OWN foreign handle verbatim — NOT a HiveTableHandle stamped HUDI, and NOT routed to the iceberg sibling.
        HiveConnectorMetadata md = withSiblings(hiveTable(HUDI));

        Optional<ConnectorTableHandle> handle = md.getTableHandle(null, "db", "t");

        Assertions.assertTrue(handle.isPresent(), "an existing hudi-on-HMS table must resolve a handle");
        Assertions.assertSame(hudiHandle, handle.get(),
                "getTableHandle must return the hudi sibling's OWN handle unmodified (a rewrap poisons the "
                        + "downstream hudi cast)");
        Assertions.assertFalse(handle.get() instanceof HiveTableHandle,
                "a hudi table must NOT be served a HiveTableHandle stamped HUDI");
        Assertions.assertEquals(1, hudiSibling.metadata.getTableHandleCalls,
                "the divert must consult the hudi sibling exactly once");
        Assertions.assertEquals(0, icebergSibling.getMetadataCalls,
                "a hudi table must NEVER be diverted to the iceberg sibling");
    }

    @Test
    public void hudiDivertPropagatesSiblingEmpty() {
        // The hudi sibling is authoritative for hudi existence: an empty from it passes through, and it must be
        // the SOURCE of that empty (its getTableHandle was consulted), not a gateway short-circuit.
        hudiSibling.metadata.returnHandle = null;
        HiveConnectorMetadata md = withSiblings(hiveTable(HUDI));

        Assertions.assertFalse(md.getTableHandle(null, "db", "t").isPresent(),
                "an empty from the hudi sibling must pass through unchanged");
        Assertions.assertEquals(1, hudiSibling.metadata.getTableHandleCalls,
                "the hudi sibling is authoritative for hudi existence: its getTableHandle must be the source of empty");
    }

    @Test
    public void hiveTableBuildsHiveHandleWithoutConsultingSibling() {
        HiveConnectorMetadata md = withSiblings(hiveTable(PARQUET));

        Optional<ConnectorTableHandle> handle = md.getTableHandle(null, "db", "t");

        Assertions.assertTrue(handle.get() instanceof HiveTableHandle, "a hive table resolves a HiveTableHandle");
        Assertions.assertEquals(HiveTableType.HIVE, ((HiveTableHandle) handle.get()).getTableType());
        Assertions.assertEquals(0, icebergSibling.getMetadataCalls,
                "a hive table must never build or consult the iceberg sibling");
        Assertions.assertEquals(0, hudiSibling.getMetadataCalls,
                "a hive table must never build or consult the hudi sibling");
    }

    @Test
    public void unknownNonViewTableStillFailsLoud() {
        // The HUDI arm sits BEFORE the UNKNOWN fail-loud and fires only on tableType == HUDI, so a genuine-UNKNOWN
        // non-view table is still rejected (not swallowed into a hudi divert).
        HiveConnectorMetadata md = withSiblings(hiveTable(UNKNOWN_FORMAT));

        Assertions.assertThrows(DorisConnectorException.class, () -> md.getTableHandle(null, "db", "t"),
                "an unsupported non-view input format must fail loud, not be diverted to a sibling");
        Assertions.assertEquals(0, hudiSibling.getMetadataCalls,
                "the UNKNOWN fail-loud must not consult the hudi sibling");
        Assertions.assertEquals(0, icebergSibling.getMetadataCalls,
                "the UNKNOWN fail-loud must not consult the iceberg sibling");
    }

    @Test
    public void missingTableReturnsEmptyWithoutConsultingSibling() {
        HiveConnectorMetadata md = new HiveConnectorMetadata(
                new FakeHmsClient(icebergTable(), false), Collections.emptyMap(), new FakeConnectorContext(),
                () -> icebergSibling, () -> hudiSibling, OWNER_RESOLVER_UNUSED);

        Assertions.assertFalse(md.getTableHandle(null, "db", "t").isPresent(),
                "a non-existent table short-circuits to empty before any format detection or divert");
        Assertions.assertEquals(0, icebergSibling.getMetadataCalls, "a missing table must not build the iceberg sibling");
        Assertions.assertEquals(0, hudiSibling.getMetadataCalls, "a missing table must not build the hudi sibling");
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

    @Test
    public void hudiTableFailsLoudWhenNoSiblingConfigured() {
        // Symmetric with iceberg: the 3-arg constructor installs a fail-loud hudi sibling supplier, so a hudi
        // table must raise a clear error, not NPE, when the hudi plugin is unavailable.
        HiveConnectorMetadata md = new HiveConnectorMetadata(
                new FakeHmsClient(hiveTable(HUDI), true), Collections.emptyMap(), new FakeConnectorContext());

        Assertions.assertThrows(DorisConnectorException.class, () -> md.getTableHandle(null, "db", "t"),
                "a hudi table with no sibling configured must fail loud");
    }

    // ===== helpers =====

    private HiveConnectorMetadata withSiblings(HmsTableInfo tableInfo) {
        // getTableHandle diverts iceberg/hudi BY TYPE (the two by-TYPE suppliers); the by-handle owner resolver is
        // never used on this path, so it fails loud if anything reaches for it.
        return new HiveConnectorMetadata(new FakeHmsClient(tableInfo, true), Collections.emptyMap(),
                new FakeConnectorContext(), () -> icebergSibling, () -> hudiSibling, OWNER_RESOLVER_UNUSED);
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
