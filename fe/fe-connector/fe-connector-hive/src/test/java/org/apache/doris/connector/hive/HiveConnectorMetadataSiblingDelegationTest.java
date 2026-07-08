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
import org.apache.doris.connector.api.ConnectorColumnStatistics;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorTableStatistics;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartitionView;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTableFreshness;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.api.pushdown.FilterApplicationResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Pins the HMS-cutover §4.4 S3 gateway metadata delegation: {@link HiveConnectorMetadata}'s per-handle methods
 * route by the concrete handle type — a hive handle runs the existing hive logic, a foreign (iceberg) handle is
 * forwarded to the embedded iceberg sibling connector — and NEVER cast the foreign handle.
 *
 * <p>Dormant until hms enters {@code SPI_READY_TYPES}: no production path builds a foreign handle for this
 * metadata yet, so these assertions are a Rule-9 guard that the divert contract (forward every per-handle read +
 * DROP/TRUNCATE, return the sibling's handle UNMODIFIED, fill the iceberg-only silent gaps) is correct BEFORE the
 * flip wires it. The hive-handle byte-parity for these methods is covered by the existing per-method suites.</p>
 */
public class HiveConnectorMetadataSiblingDelegationTest {

    /** A foreign (non-hive) handle — the marker type the iceberg sibling's getTableHandle produces post-flip. */
    private static final class ForeignHandle implements ConnectorTableHandle {
    }

    private final ForeignHandle foreignHandle = new ForeignHandle();
    private final RecordingSiblingMetadata siblingMetadata = new RecordingSiblingMetadata();
    private final RecordingSiblingConnector siblingConnector = new RecordingSiblingConnector(siblingMetadata);

    /** Metadata wired with a working sibling (hmsClient is null: the hive path is never exercised here). */
    private HiveConnectorMetadata withSibling() {
        return new HiveConnectorMetadata(null, Collections.emptyMap(), new FakeConnectorContext(),
                () -> siblingConnector);
    }

    private HiveTableHandle hiveHandle() {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE).build();
    }

    @Test
    public void everyPerHandleMethodForwardsAForeignHandleToTheSibling() {
        HiveConnectorMetadata md = withSibling();

        // ---- set (a): methods hive overrides — a foreign handle must NOT run hive logic, it must divert ----
        md.getTableSchema(null, foreignHandle);
        md.getColumnHandles(null, foreignHandle);
        md.getTableStatistics(null, foreignHandle);
        md.getColumnStatistics(null, foreignHandle, "c");
        long size = md.estimateDataSizeByListingFiles(null, foreignHandle);
        Optional<FilterApplicationResult<ConnectorTableHandle>> filter = md.applyFilter(null, foreignHandle, null);
        List<String> partNames = md.listPartitionNames(null, foreignHandle);
        md.listPartitions(null, foreignHandle, Optional.empty());
        ConnectorMvccSnapshot pin = md.beginQuerySnapshot(null, foreignHandle).orElse(null);
        md.getTableFreshness(null, foreignHandle);
        md.getPartitionFreshnessMillis(null, foreignHandle, "p");
        md.dropTable(null, foreignHandle);
        md.truncateTable(null, foreignHandle, Collections.emptyList());

        // ---- set (b): methods hive does NOT override — the silent gaps that must be filled by forwarding ----
        md.getTableSchema(null, foreignHandle, null);
        md.getMvccPartitionView(null, foreignHandle);
        md.resolveTimeTravel(null, foreignHandle, null);
        ConnectorTableHandle afterSnapshot = md.applySnapshot(null, foreignHandle, null);
        ConnectorTableHandle afterScope = md.applyRewriteFileScope(null, foreignHandle, Collections.emptySet());
        ConnectorTableHandle afterTopn = md.applyTopnLazyMaterialization(null, foreignHandle);
        List<String> sysTables = md.listSupportedSysTables(null, foreignHandle);
        Optional<ConnectorTableHandle> sysHandle = md.getSysTableHandle(null, foreignHandle, "snapshots");

        // Every per-handle method reached the sibling (proves the divert covers the whole surface).
        Assertions.assertEquals(RecordingSiblingMetadata.EXPECTED_METHODS, siblingMetadata.calls,
                "every per-handle read + DROP/TRUNCATE + iceberg-only gap method must forward a foreign handle");

        // A few return values prove the ANSWER is the sibling's, not hive's default.
        Assertions.assertEquals(RecordingSiblingMetadata.SENTINEL_SIZE, size,
                "estimateDataSize must return the sibling's value, not hive's -1");
        Assertions.assertEquals(RecordingSiblingMetadata.SENTINEL_SNAPSHOT_ID, pin.getSnapshotId(),
                "beginQuerySnapshot must return the sibling's snapshot-id pin, not hive's -1 last-modified pin");
        Assertions.assertEquals(Collections.singletonList("sibling-part"), partNames,
                "listPartitionNames must return the sibling's names");
        Assertions.assertEquals(Collections.singletonList("snapshots"), sysTables,
                "iceberg-on-HMS system tables must resolve through the sibling (hive exposes none)");

        // Handle-out methods must return the sibling's handle/result UNMODIFIED (a rewrap poisons a scan cast).
        Assertions.assertSame(siblingMetadata.filterResult, filter, "applyFilter must return the sibling result");
        Assertions.assertSame(RecordingSiblingMetadata.SIBLING_HANDLE, afterSnapshot,
                "applySnapshot must thread and return the sibling's handle unmodified");
        Assertions.assertSame(RecordingSiblingMetadata.SIBLING_HANDLE, afterScope,
                "applyRewriteFileScope must return the sibling's handle unmodified");
        Assertions.assertSame(RecordingSiblingMetadata.SIBLING_HANDLE, afterTopn,
                "applyTopnLazyMaterialization must return the sibling's handle unmodified");
        Assertions.assertSame(RecordingSiblingMetadata.SIBLING_HANDLE, sysHandle.orElse(null),
                "getSysTableHandle must return the sibling's sys-table handle unmodified");
    }

    @Test
    public void hiveHandleRunsHiveBranchAndNeverConsultsSibling() {
        HiveConnectorMetadata md = withSibling();
        HiveTableHandle hive = hiveHandle();

        // The set-(b) + beginQuerySnapshot branches reproduce the SPI default / hive pin WITHOUT the sibling and
        // without touching the (null) hmsClient — proving the guard falls through to the hive path for a hive handle.
        Assertions.assertFalse(md.getMvccPartitionView(null, hive).isPresent(), "hive has no range partition view");
        Assertions.assertFalse(md.resolveTimeTravel(null, hive, null).isPresent(), "hive has no time travel");
        Assertions.assertSame(hive, md.applySnapshot(null, hive, null), "hive applySnapshot returns the handle");
        Assertions.assertSame(hive, md.applyRewriteFileScope(null, hive, Collections.emptySet()),
                "hive applyRewriteFileScope returns the handle");
        Assertions.assertSame(hive, md.applyTopnLazyMaterialization(null, hive),
                "hive applyTopnLazyMaterialization returns the handle");
        Assertions.assertTrue(md.listSupportedSysTables(null, hive).isEmpty(), "hive exposes no system tables");
        Assertions.assertFalse(md.getSysTableHandle(null, hive, "snapshots").isPresent(),
                "hive exposes no system tables");
        ConnectorMvccSnapshot pin = md.beginQuerySnapshot(null, hive).orElse(null);
        Assertions.assertNotNull(pin);
        Assertions.assertEquals(-1L, pin.getSnapshotId(), "hive's pin is the empty (-1) last-modified pin");
        Assertions.assertTrue(pin.isLastModifiedFreshness(), "hive's pin flags last-modified freshness");

        Assertions.assertEquals(0, siblingConnector.getMetadataCount,
                "a hive handle must never build/consult the iceberg sibling");
        Assertions.assertTrue(siblingMetadata.calls.isEmpty(), "the sibling must not be forwarded a hive handle");
    }

    @Test
    public void foreignHandleFailsLoudWhenNoSiblingConfigured() {
        // The 3-arg constructor (hive-only construction) installs a fail-loud supplier: a foreign handle must
        // raise a clear error, not NPE deep in a forward.
        HiveConnectorMetadata md = new HiveConnectorMetadata(null, Collections.emptyMap(),
                new FakeConnectorContext());
        Assertions.assertThrows(DorisConnectorException.class, () -> md.getTableSchema(null, foreignHandle),
                "a foreign handle with no sibling configured must fail loud");
    }

    /** A sibling {@link Connector} whose getMetadata hands back the recording metadata and counts the calls. */
    private static final class RecordingSiblingConnector implements Connector {
        private final ConnectorMetadata metadata;
        private int getMetadataCount;

        RecordingSiblingConnector(ConnectorMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            getMetadataCount++;
            return metadata;
        }
    }

    /** Records each forwarded method name and returns distinguishable sentinels. */
    private static final class RecordingSiblingMetadata implements ConnectorMetadata {
        static final ConnectorTableHandle SIBLING_HANDLE = new ForeignHandle();
        static final long SENTINEL_SIZE = 4242L;
        static final long SENTINEL_SNAPSHOT_ID = 99L;

        // The exact set + order of forwarded methods the foreign-handle test drives (a Rule-9 completeness lock:
        // dropping a guard, or adding one that should not forward, changes this list and fails the test).
        static final List<String> EXPECTED_METHODS = Collections.unmodifiableList(Arrays.asList(
                "getTableSchema", "getColumnHandles", "getTableStatistics", "getColumnStatistics",
                "estimateDataSizeByListingFiles", "applyFilter", "listPartitionNames", "listPartitions",
                "beginQuerySnapshot", "getTableFreshness", "getPartitionFreshnessMillis", "dropTable",
                "truncateTable", "getTableSchemaAtSnapshot", "getMvccPartitionView", "resolveTimeTravel",
                "applySnapshot", "applyRewriteFileScope", "applyTopnLazyMaterialization", "listSupportedSysTables",
                "getSysTableHandle"));

        final List<String> calls = new ArrayList<>();
        final Optional<FilterApplicationResult<ConnectorTableHandle>> filterResult =
                Optional.of(new FilterApplicationResult<>(SIBLING_HANDLE, null, false));

        @Override
        public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle) {
            calls.add("getTableSchema");
            return new ConnectorTableSchema("sibling", Collections.emptyList(), "iceberg", Collections.emptyMap());
        }

        @Override
        public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle,
                ConnectorMvccSnapshot snapshot) {
            calls.add("getTableSchemaAtSnapshot");
            return new ConnectorTableSchema("sibling", Collections.emptyList(), "iceberg", Collections.emptyMap());
        }

        @Override
        public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorSession session,
                ConnectorTableHandle handle) {
            calls.add("getColumnHandles");
            return Collections.emptyMap();
        }

        @Override
        public Optional<ConnectorTableStatistics> getTableStatistics(ConnectorSession session,
                ConnectorTableHandle handle) {
            calls.add("getTableStatistics");
            return Optional.of(new ConnectorTableStatistics(1L, 2L));
        }

        @Override
        public Optional<ConnectorColumnStatistics> getColumnStatistics(ConnectorSession session,
                ConnectorTableHandle handle, String columnName) {
            calls.add("getColumnStatistics");
            return Optional.empty();
        }

        @Override
        public long estimateDataSizeByListingFiles(ConnectorSession session, ConnectorTableHandle handle) {
            calls.add("estimateDataSizeByListingFiles");
            return SENTINEL_SIZE;
        }

        @Override
        public Optional<FilterApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
                ConnectorTableHandle handle, ConnectorFilterConstraint constraint) {
            calls.add("applyFilter");
            return filterResult;
        }

        @Override
        public List<String> listPartitionNames(ConnectorSession session, ConnectorTableHandle handle) {
            calls.add("listPartitionNames");
            return Collections.singletonList("sibling-part");
        }

        @Override
        public List<ConnectorPartitionInfo> listPartitions(ConnectorSession session, ConnectorTableHandle handle,
                Optional<ConnectorExpression> filter) {
            calls.add("listPartitions");
            return Collections.emptyList();
        }

        @Override
        public Optional<ConnectorMvccSnapshot> beginQuerySnapshot(ConnectorSession session,
                ConnectorTableHandle handle) {
            calls.add("beginQuerySnapshot");
            return Optional.of(ConnectorMvccSnapshot.builder().snapshotId(SENTINEL_SNAPSHOT_ID).build());
        }

        @Override
        public Optional<ConnectorTableFreshness> getTableFreshness(ConnectorSession session,
                ConnectorTableHandle handle) {
            calls.add("getTableFreshness");
            return Optional.empty();
        }

        @Override
        public OptionalLong getPartitionFreshnessMillis(ConnectorSession session, ConnectorTableHandle handle,
                String partitionName) {
            calls.add("getPartitionFreshnessMillis");
            return OptionalLong.of(55L);
        }

        @Override
        public void dropTable(ConnectorSession session, ConnectorTableHandle handle) {
            calls.add("dropTable");
        }

        @Override
        public void truncateTable(ConnectorSession session, ConnectorTableHandle handle, List<String> partitions) {
            calls.add("truncateTable");
        }

        @Override
        public Optional<ConnectorMvccPartitionView> getMvccPartitionView(ConnectorSession session,
                ConnectorTableHandle handle) {
            calls.add("getMvccPartitionView");
            return Optional.empty();
        }

        @Override
        public Optional<ConnectorMvccSnapshot> resolveTimeTravel(ConnectorSession session,
                ConnectorTableHandle handle, ConnectorTimeTravelSpec spec) {
            calls.add("resolveTimeTravel");
            return Optional.empty();
        }

        @Override
        public ConnectorTableHandle applySnapshot(ConnectorSession session, ConnectorTableHandle handle,
                ConnectorMvccSnapshot snapshot) {
            calls.add("applySnapshot");
            return SIBLING_HANDLE;
        }

        @Override
        public ConnectorTableHandle applyRewriteFileScope(ConnectorSession session, ConnectorTableHandle handle,
                Set<String> rawDataFilePaths) {
            calls.add("applyRewriteFileScope");
            return SIBLING_HANDLE;
        }

        @Override
        public ConnectorTableHandle applyTopnLazyMaterialization(ConnectorSession session,
                ConnectorTableHandle handle) {
            calls.add("applyTopnLazyMaterialization");
            return SIBLING_HANDLE;
        }

        @Override
        public List<String> listSupportedSysTables(ConnectorSession session, ConnectorTableHandle baseTableHandle) {
            calls.add("listSupportedSysTables");
            return Collections.singletonList("snapshots");
        }

        @Override
        public Optional<ConnectorTableHandle> getSysTableHandle(ConnectorSession session,
                ConnectorTableHandle baseTableHandle, String sysName) {
            calls.add("getSysTableHandle");
            return Optional.of(SIBLING_HANDLE);
        }
    }
}
