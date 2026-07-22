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
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorColumnStatistics;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorStatementScope;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorTableStatistics;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.BranchChange;
import org.apache.doris.connector.api.ddl.ConnectorColumnPosition;
import org.apache.doris.connector.api.ddl.DropRefChange;
import org.apache.doris.connector.api.ddl.PartitionFieldChange;
import org.apache.doris.connector.api.ddl.TagChange;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.NoOpConnectorTransaction;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartitionView;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTableFreshness;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.api.pushdown.FilterApplicationResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;

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

    // A session whose per-statement scope is NONE (offline): the sibling-metadata funnel runs its factory on every
    // forward, so the sibling is consulted per call exactly as before the funnel — these forwarding assertions are
    // byte-equivalent to the pre-funnel behavior. Per-statement REUSE (one sibling metadata across many forwards)
    // is pinned by the "per-statement sibling-metadata funnel" tests below, which run under a live TestStatementScope.
    private final ConnectorSession session = new ScopeSession(1L, "q1", ConnectorStatementScope.NONE);

    /**
     * The by-TYPE force-build supplier constructor arg. This suite exercises only per-handle (by-handle) sites —
     * which must ALL route via the peek resolver — and never calls getTableHandle (the only by-type site), so the
     * supplier must never be invoked here. It fails loud if it is, so a per-handle site that regressed from
     * {@code siblingMetadata(session, handle)} (peek resolver) to {@code icebergSiblingMetadata(session)} (by-type
     * force-build supplier) blows up instead of silently returning the same sibling.
     */
    private static final Supplier<Connector> SUPPLIER_MUST_NOT_BE_USED = () -> {
        throw new AssertionError(
                "a per-handle site must route via the peek resolver, not the by-type force-build supplier");
    };

    /**
     * Metadata wired so every foreign-handle per-handle site MUST route via the by-handle peek resolver (which
     * returns the recording sibling), while the by-type force-build supplier is a fail-loud stub (see
     * {@link #SUPPLIER_MUST_NOT_BE_USED}). hmsClient is null: the hive path is never exercised here. This suite
     * pins that the per-handle sites FORWARD the whole surface; the 3-way ownsHandle dispatch that PICKS the owner
     * is pinned by {@code HiveConnectorThreeWayRoutingTest}.
     */
    private HiveConnectorMetadata withSibling() {
        return new HiveConnectorMetadata(null, Collections.emptyMap(), new FakeConnectorContext(),
                SUPPLIER_MUST_NOT_BE_USED, SUPPLIER_MUST_NOT_BE_USED,
                handle -> new SiblingOwner(siblingConnector, SiblingOwner.ICEBERG_LABEL));
    }

    private HiveTableHandle hiveHandle() {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE).build();
    }

    @Test
    public void everyPerHandleMethodForwardsAForeignHandleToTheSibling() {
        HiveConnectorMetadata md = withSibling();

        // ---- set (a): methods hive overrides — a foreign handle must NOT run hive logic, it must divert ----
        md.getTableSchema(session, foreignHandle);
        md.getColumnHandles(session, foreignHandle);
        md.getTableStatistics(session, foreignHandle);
        md.getColumnStatistics(session, foreignHandle, "c");
        long size = md.estimateDataSizeByListingFiles(session, foreignHandle);
        Optional<FilterApplicationResult<ConnectorTableHandle>> filter = md.applyFilter(session, foreignHandle, null);
        List<String> partNames = md.listPartitionNames(session, foreignHandle);
        md.listPartitions(session, foreignHandle, Optional.empty());
        ConnectorMvccSnapshot pin = md.beginQuerySnapshot(session, foreignHandle).orElse(null);
        md.getTableFreshness(session, foreignHandle);
        md.getPartitionFreshnessMillis(session, foreignHandle, "p");
        md.dropTable(session, foreignHandle);
        md.truncateTable(session, foreignHandle, Collections.emptyList());

        // ---- set (b): methods hive does NOT override — the silent gaps that must be filled by forwarding ----
        md.getTableSchema(session, foreignHandle, null);
        md.getMvccPartitionView(session, foreignHandle);
        md.resolveTimeTravel(session, foreignHandle, null);
        ConnectorTableHandle afterSnapshot = md.applySnapshot(session, foreignHandle, null);
        List<ConnectorExpression> predicates = md.getSyntheticScanPredicates(session, foreignHandle, null);
        ConnectorTableHandle afterScope = md.applyRewriteFileScope(session, foreignHandle, Collections.emptySet());
        ConnectorTableHandle afterTopn = md.applyTopnLazyMaterialization(session, foreignHandle);
        List<String> sysTables = md.listSupportedSysTables(session, foreignHandle);
        Optional<ConnectorTableHandle> sysHandle = md.getSysTableHandle(session, foreignHandle, "snapshots");
        // "snapshots" (not "partitions"): hive's own logic returns false for it, so a true answer proves the
        // reply came from the sibling, not hive.
        boolean sysIsTvf = md.isPartitionValuesSysTable(session, foreignHandle, "snapshots");

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
                "iceberg-on-HMS system tables must resolve through the sibling (hive exposes only partitions)");
        Assertions.assertTrue(sysIsTvf,
                "isPartitionValuesSysTable must return the sibling's answer for a foreign handle, not hive's — "
                        + "dropping this delegation would misroute an iceberg-on-HMS t$partitions into the hive TVF");

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
        Assertions.assertSame(RecordingSiblingMetadata.SIBLING_PREDICATES, predicates,
                "getSyntheticScanPredicates must return the sibling's residual predicates unmodified — a "
                        + "hudi-on-HMS @incr read gets its row filter from the hudi sibling, not hive's empty default");
    }

    @Test
    public void hiveHandleRunsHiveBranchAndNeverConsultsSibling() {
        HiveConnectorMetadata md = withSibling();
        HiveTableHandle hive = hiveHandle();

        // The set-(b) + beginQuerySnapshot branches reproduce the SPI default / hive pin WITHOUT the sibling and
        // without touching the (null) hmsClient — proving the guard falls through to the hive path for a hive handle.
        Assertions.assertFalse(md.getMvccPartitionView(session, hive).isPresent(), "hive has no range partition view");
        Assertions.assertFalse(md.resolveTimeTravel(session, hive, null).isPresent(), "hive has no time travel");
        Assertions.assertSame(hive, md.applySnapshot(session, hive, null), "hive applySnapshot returns the handle");
        Assertions.assertTrue(md.getSyntheticScanPredicates(session, hive, null).isEmpty(),
                "plain hive has no synthetic scan predicate");
        Assertions.assertSame(hive, md.applyRewriteFileScope(session, hive, Collections.emptySet()),
                "hive applyRewriteFileScope returns the handle");
        Assertions.assertSame(hive, md.applyTopnLazyMaterialization(session, hive),
                "hive applyTopnLazyMaterialization returns the handle");
        Assertions.assertEquals(Collections.singletonList("partitions"), md.listSupportedSysTables(session, hive),
                "hive exposes the partitions sys table (t$partitions), served by the partition_values TVF");
        Assertions.assertTrue(md.isPartitionValuesSysTable(session, hive, "partitions"),
                "hive's partitions sys table is TVF-backed");
        Assertions.assertFalse(md.isPartitionValuesSysTable(session, hive, "snapshots"),
                "hive exposes no sys table other than partitions");
        Assertions.assertFalse(md.getSysTableHandle(session, hive, "snapshots").isPresent(),
                "hive's TVF-backed sys table has no native handle");
        ConnectorMvccSnapshot pin = md.beginQuerySnapshot(session, hive).orElse(null);
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
        Assertions.assertThrows(DorisConnectorException.class, () -> md.getTableSchema(session, foreignHandle),
                "a foreign handle with no sibling configured must fail loud");
    }

    @Test
    public void everyAlterDdlAndValidateMethodForwardsAForeignHandleToTheSibling() {
        HiveConnectorMetadata md = withSibling();

        // The 14 ALTER-DDL mutators + 2 write validators: a foreign (iceberg-on-HMS) handle must divert, never
        // run the hive branch and never be cast. Change objects are null — the guard fires on the handle type
        // before any param is touched.
        md.renameTable(session, foreignHandle, "new");
        md.addColumn(session, foreignHandle, null, null);
        md.addColumns(session, foreignHandle, Collections.emptyList());
        md.dropColumn(session, foreignHandle, "c");
        md.renameColumn(session, foreignHandle, "a", "b");
        md.modifyColumn(session, foreignHandle, null, null);
        md.reorderColumns(session, foreignHandle, Collections.emptyList());
        md.createOrReplaceBranch(session, foreignHandle, null);
        md.createOrReplaceTag(session, foreignHandle, null);
        md.dropBranch(session, foreignHandle, null);
        md.dropTag(session, foreignHandle, null);
        md.addPartitionField(session, foreignHandle, null);
        md.dropPartitionField(session, foreignHandle, null);
        md.replacePartitionField(session, foreignHandle, null);
        md.validateRowLevelDmlMode(session, foreignHandle, null);
        md.validateStaticPartitionColumns(session, foreignHandle, Collections.emptyList());
        // Empty list on purpose: a foreign handle must forward REGARDLESS of emptiness (the empty-early-return is
        // hive-only) — this would fail if the empty check were placed before the foreign-handle divert.
        md.validateWritePartitionNames(session, foreignHandle, Collections.emptyList());

        Assertions.assertEquals(RecordingSiblingMetadata.EXPECTED_WRITE_METHODS, siblingMetadata.calls,
                "every ALTER-DDL mutator + write validator must forward a foreign handle to the sibling");
    }

    @Test
    public void hiveHandleRejectsNonEmptyPartitionNamesWithLegacyMessage() {
        HiveConnectorMetadata md = withSibling();
        HiveTableHandle hive = hiveHandle();

        // Net-new port of the legacy fe-core reject (retired BindSink.bindHiveTableSink): the dynamic
        // partition-NAME list form INSERT ... PARTITION(p1, p2) is unsupported on a hive table. UNLIKE the two
        // permissive validators, a hive handle here THROWS the EXACT legacy message on a non-empty list. The e2e
        // test_hive_write_type.groovy asserts on this literal substring, so it must stay byte-identical.
        assertThrowsMessage(() -> md.validateWritePartitionNames(session, hive, Arrays.asList("p1", "p2")),
                "Not support insert with partition spec in hive catalog.");

        // An empty list (a plain INSERT ... SELECT or a static PARTITION(col='val') INSERT) is legal plain-hive
        // and MUST return silently — a throw here would newly reject legal writes.
        md.validateWritePartitionNames(session, hive, Collections.emptyList());

        Assertions.assertEquals(0, siblingConnector.getMetadataCount,
                "a hive handle must never build/consult the iceberg sibling to validate partition names");
        Assertions.assertTrue(siblingMetadata.calls.isEmpty(), "the sibling must not be forwarded a hive handle");
    }

    @Test
    public void hiveHandleAlterDdlThrowsAndValidateIsNoopAndNeverConsultsSibling() {
        HiveConnectorMetadata md = withSibling();
        HiveTableHandle hive = hiveHandle();

        // Group-1: ALTER-DDL for a hive handle throws the EXACT inherited SPI-default message (byte-parity with
        // pre-override behavior) without building or consulting the sibling.
        assertThrowsMessage(() -> md.renameTable(session, hive, "n"), "RENAME TABLE not supported");
        assertThrowsMessage(() -> md.addColumn(session, hive, null, null), "ADD COLUMN not supported");
        assertThrowsMessage(() -> md.addColumns(session, hive, Collections.emptyList()), "ADD COLUMNS not supported");
        assertThrowsMessage(() -> md.dropColumn(session, hive, "c"), "DROP COLUMN not supported");
        assertThrowsMessage(() -> md.renameColumn(session, hive, "a", "b"), "RENAME COLUMN not supported");
        assertThrowsMessage(() -> md.modifyColumn(session, hive, null, null), "MODIFY COLUMN not supported");
        assertThrowsMessage(() -> md.reorderColumns(session, hive, Collections.emptyList()),
                "REORDER COLUMNS not supported");
        assertThrowsMessage(() -> md.createOrReplaceBranch(session, hive, null), "CREATE/REPLACE BRANCH not supported");
        assertThrowsMessage(() -> md.createOrReplaceTag(session, hive, null), "CREATE/REPLACE TAG not supported");
        assertThrowsMessage(() -> md.dropBranch(session, hive, null), "DROP BRANCH not supported");
        assertThrowsMessage(() -> md.dropTag(session, hive, null), "DROP TAG not supported");
        assertThrowsMessage(() -> md.addPartitionField(session, hive, null), "ADD PARTITION FIELD not supported");
        assertThrowsMessage(() -> md.dropPartitionField(session, hive, null), "DROP PARTITION FIELD not supported");
        assertThrowsMessage(() -> md.replacePartitionField(session, hive, null), "REPLACE PARTITION FIELD not supported");

        // Group-2: validate* for a hive handle MUST return silently — a throw here would newly reject legal
        // plain-hive row-level DML / static-partition INSERTs.
        md.validateRowLevelDmlMode(session, hive, null);
        md.validateStaticPartitionColumns(session, hive, Collections.emptyList());

        Assertions.assertEquals(0, siblingConnector.getMetadataCount,
                "a hive handle must never build/consult the iceberg sibling for ALTER-DDL / validate");
        Assertions.assertTrue(siblingMetadata.calls.isEmpty(), "the sibling must not be forwarded a hive handle");
    }

    @Test
    public void beginTransactionForwardsAForeignHandleToTheSibling() {
        HiveConnectorMetadata md = withSibling();

        // A foreign (iceberg-on-HMS) write must open the SIBLING's transaction, so iceberg's write plan can
        // downcast the session-bound transaction to IcebergConnectorTransaction — a HiveConnectorTransaction
        // (what the unconditional open would bind) would ClassCastException there.
        ConnectorTransaction txn = md.beginTransaction(session, foreignHandle);

        Assertions.assertSame(RecordingSiblingMetadata.SIBLING_TXN, txn,
                "a foreign handle must open the sibling's transaction, not a hive one");
        Assertions.assertEquals(Collections.singletonList("beginTransaction"), siblingMetadata.calls,
                "beginTransaction must forward the foreign handle to the sibling");
        Assertions.assertEquals(1, siblingConnector.getMetadataCount, "the sibling must be consulted once");
    }

    @Test
    public void beginTransactionForHiveHandleOpensHiveTxnAndNeverConsultsSibling() {
        // A hive handle must fall through to the connector-level beginTransaction. Stub the no-arg factory so the
        // test does not build a real HiveConnectorTransaction (which spins a file-system thread pool); the point
        // is the per-handle guard routes a hive handle to the connector's OWN transaction, a foreign one to the
        // sibling. The selection must be symmetric — hive and iceberg write plans downcast to different types.
        ConnectorTransaction hiveTxn = new NoOpConnectorTransaction(70099L, "HIVE");
        HiveConnectorMetadata md = new HiveConnectorMetadata(null, Collections.emptyMap(), new FakeConnectorContext(),
                SUPPLIER_MUST_NOT_BE_USED, SUPPLIER_MUST_NOT_BE_USED,
                handle -> new SiblingOwner(siblingConnector, SiblingOwner.ICEBERG_LABEL)) {
            @Override
            public ConnectorTransaction beginTransaction(ConnectorSession session) {
                return hiveTxn;
            }
        };

        Assertions.assertSame(hiveTxn, md.beginTransaction(session, hiveHandle()),
                "a hive handle must open the connector-level (hive) transaction, not the sibling's");
        Assertions.assertEquals(0, siblingConnector.getMetadataCount,
                "a hive handle must never build/consult the iceberg sibling to open a transaction");
        Assertions.assertTrue(siblingMetadata.calls.isEmpty(), "the sibling must not be forwarded a hive handle");
    }

    @Test
    public void foreignHandleSchemaReflectsSiblingScanCapabilitiesAsPerTableMarker() {
        // Option C: fe-core's PluginDrivenExternalTable.hasScanCapability reads only the CATALOG (hive) connector,
        // never the embedded sibling — so the hive gateway must reflect the sibling's connector-wide scan
        // capabilities onto the delegated schema as a per-table marker, or an iceberg-on-HMS table silently loses
        // auto-analyze / Top-N lazy / nested-column prune (all of which the iceberg sibling declares connector-wide).
        // MUTATION: dropping the reflection -> the returned schema carries no marker -> the embedded table drops the
        // capabilities post-flip -> red here.
        Set<ConnectorCapability> siblingCaps = EnumSet.of(
                ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE,
                ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE,
                ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE);
        HiveConnectorMetadata md = new HiveConnectorMetadata(null, Collections.emptyMap(), new FakeConnectorContext(),
                SUPPLIER_MUST_NOT_BE_USED, SUPPLIER_MUST_NOT_BE_USED,
                handle -> new SiblingOwner(new CapabilityDeclaringSiblingConnector(siblingCaps),
                        SiblingOwner.ICEBERG_LABEL));

        ConnectorTableSchema schema = md.getTableSchema(session, foreignHandle);
        String csv = schema.getProperties().get(ConnectorTableSchema.PER_TABLE_CAPABILITIES_KEY);
        Assertions.assertNotNull(csv, "the delegated schema must carry the reflected per-table capability marker");
        List<String> names = Arrays.asList(csv.split(","));
        Assertions.assertTrue(names.contains(ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE.name()),
                "auto-analyze must survive the delegation as a per-table marker");
        Assertions.assertTrue(names.contains(ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE.name()),
                "Top-N lazy must survive the delegation as a per-table marker");
        Assertions.assertTrue(names.contains(ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE.name()),
                "nested-column prune must survive the delegation as a per-table marker");
    }

    @Test
    public void foreignHandleSchemaUnchangedWhenSiblingDeclaresNoCapabilities() {
        // A sibling declaring an EMPTY capability set hits the ownerCaps.isEmpty() early-return in
        // reflectSiblingScanCapabilities -> the sibling schema is returned untouched -> no marker is stamped. This
        // guards the empty-owner branch specifically; the real hudi-on-HMS withholding (a NON-empty sibling that
        // lacks auto-analyze) is pinned by foreignHandleSchemaWithholdsAutoAnalyzeFromRealHudiSibling below.
        // MUTATION: dropping the isEmpty() early-return and stamping an (empty) marker unconditionally -> red here.
        HiveConnectorMetadata md = withSibling(); // RecordingSiblingConnector declares no capabilities
        ConnectorTableSchema schema = md.getTableSchema(session, foreignHandle);
        Assertions.assertNull(schema.getProperties().get(ConnectorTableSchema.PER_TABLE_CAPABILITIES_KEY),
                "no marker when the sibling declares no capabilities");
    }

    // ============== per-statement sibling-metadata funnel (HMS heterogeneous gateway) ==============
    // Within one statement, the gateway obtains ONE sibling ConnectorMetadata per (catalogId, owner) and reuses it
    // across every forward (read / scan / DDL / MVCC / the write-transaction open), keyed
    // "metadata:<catalogId>:<ownerLabel>" on the session's per-statement scope — mirroring fe-core's own funnel for
    // a plain connector. RecordingSiblingConnector.getMetadataCount is the load count.

    @Test
    public void liveScopeSharesOneSiblingMetadataAcrossEveryForwardIncludingTheWriteTxn() {
        // Many read forwards (including the getTableSchema stray) + the per-handle beginTransaction open, all in one
        // statement -> the sibling is built ONCE and every forward (reads AND the write transaction) reuses it.
        // MUTATION: not memoizing -> a build per forward -> count > 1 -> red.
        HiveConnectorMetadata md = withSibling();
        ConnectorSession live = new ScopeSession(1L, "q1", new TestStatementScope());

        md.getColumnHandles(live, foreignHandle);
        md.listPartitionNames(live, foreignHandle);
        md.getTableSchema(live, foreignHandle);
        md.beginTransaction(live, foreignHandle);

        Assertions.assertEquals(1, siblingConnector.getMetadataCount,
                "one sibling metadata per (catalog, owner) per statement — reads and the write txn share it");
    }

    @Test
    public void noneScopeBuildsAFreshSiblingMetadataEachForward() {
        // No live statement scope (offline / no ConnectContext): the funnel factory runs on every forward, exactly
        // as before the funnel existed. This is the byte-equivalence guard for the NONE path.
        HiveConnectorMetadata md = withSibling();

        md.getColumnHandles(session, foreignHandle);
        md.listPartitionNames(session, foreignHandle);
        md.getColumnHandles(session, foreignHandle);

        Assertions.assertEquals(3, siblingConnector.getMetadataCount,
                "NONE scope -> the sibling metadata factory runs on every forward (pre-funnel behavior)");
    }

    @Test
    public void byTypeDivertAndByHandleForwardShareOneSiblingMetadata() {
        // The getTableHandle divert asks the sibling BY TYPE (icebergSiblingMetadata) before any handle exists; the
        // later per-handle forwards resolve BY HANDLE. Both must mint the SAME funnel key for the same owner (the
        // by-TYPE literal label == the by-HANDLE resolver-arm label), or the statement would hold two sibling
        // metadata instances. Wire the iceberg by-TYPE supplier to the SAME recording connector the resolver
        // returns, then drive both paths under one live scope. MUTATION: mismatched labels -> two builds -> red.
        HiveConnectorMetadata md = new HiveConnectorMetadata(null, Collections.emptyMap(), new FakeConnectorContext(),
                () -> siblingConnector, SUPPLIER_MUST_NOT_BE_USED,
                handle -> new SiblingOwner(siblingConnector, SiblingOwner.ICEBERG_LABEL));
        ConnectorSession live = new ScopeSession(1L, "q1", new TestStatementScope());

        md.icebergSiblingMetadata(live);            // by-TYPE (getTableHandle divert path)
        md.getColumnHandles(live, foreignHandle);   // by-HANDLE (per-handle forward)

        Assertions.assertEquals(1, siblingConnector.getMetadataCount,
                "the by-TYPE divert and the by-HANDLE forward key the same owner -> one shared sibling metadata");
    }

    @Test
    public void differentCatalogIdIsolatesTheSiblingMetadata() {
        // A statement joining two heterogeneous HMS catalogs shares one scope map; the catalog id in the key keeps
        // each catalog's sibling metadata isolated. MUTATION: dropping catalogId from the key -> the two collide.
        HiveConnectorMetadata md = withSibling();
        TestStatementScope scope = new TestStatementScope();

        md.getColumnHandles(new ScopeSession(1L, "q1", scope), foreignHandle);
        md.getColumnHandles(new ScopeSession(2L, "q1", scope), foreignHandle);

        Assertions.assertEquals(2, siblingConnector.getMetadataCount,
                "a different catalog id keys a different sibling metadata (no cross-catalog collapse)");
    }

    @Test
    public void icebergAndHudiSiblingsAreIsolatedWithinAStatement() {
        // The whole point of the owner label: iceberg-on-HMS and hudi-on-HMS tables of ONE gateway share a catalog
        // id, so ONLY the label ("iceberg" vs "hudi") keeps their metadata entries apart. Each owner is built once
        // and never collapses onto the other. MUTATION: a shared (label-less) key -> one owner's metadata serves
        // the other -> a count is 0 while the other is 2 -> red.
        RecordingSiblingConnector hudiConnector = new RecordingSiblingConnector(new RecordingSiblingMetadata());
        ForeignHandle hudiHandle = new ForeignHandle();
        HiveConnectorMetadata md = new HiveConnectorMetadata(null, Collections.emptyMap(), new FakeConnectorContext(),
                SUPPLIER_MUST_NOT_BE_USED, SUPPLIER_MUST_NOT_BE_USED,
                handle -> handle == hudiHandle
                        ? new SiblingOwner(hudiConnector, SiblingOwner.HUDI_LABEL)
                        : new SiblingOwner(siblingConnector, SiblingOwner.ICEBERG_LABEL));
        ConnectorSession live = new ScopeSession(1L, "q1", new TestStatementScope());

        md.getColumnHandles(live, foreignHandle);   // iceberg owner
        md.getColumnHandles(live, hudiHandle);       // hudi owner
        md.getColumnHandles(live, foreignHandle);   // iceberg owner again -> reuse

        Assertions.assertEquals(1, siblingConnector.getMetadataCount,
                "the iceberg owner is built once under its label");
        Assertions.assertEquals(1, hudiConnector.getMetadataCount,
                "the hudi owner is isolated under its own label and built once (never collapsed onto iceberg)");
    }

    private static void assertThrowsMessage(Executable exec, String expectedMessage) {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, exec);
        Assertions.assertEquals(expectedMessage, e.getMessage(),
                "the hive branch must reproduce the exact inherited SPI-default message");
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

    /** A sibling {@link Connector} declaring a fixed capability set; its metadata returns a marker-less schema. */
    private static final class CapabilityDeclaringSiblingConnector implements Connector {
        private final Set<ConnectorCapability> caps;

        CapabilityDeclaringSiblingConnector(Set<ConnectorCapability> caps) {
            this.caps = caps;
        }

        @Override
        public Set<ConnectorCapability> getCapabilities() {
            return caps;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return new RecordingSiblingMetadata();
        }
    }

    /** Records each forwarded method name and returns distinguishable sentinels. */
    private static final class RecordingSiblingMetadata implements ConnectorMetadata {
        static final ConnectorTableHandle SIBLING_HANDLE = new ForeignHandle();
        static final ConnectorTransaction SIBLING_TXN = new NoOpConnectorTransaction(4243L, "ICEBERG");
        static final long SENTINEL_SIZE = 4242L;
        static final long SENTINEL_SNAPSHOT_ID = 99L;
        static final List<ConnectorExpression> SIBLING_PREDICATES = Collections.singletonList(
                new ConnectorColumnRef("sibling-pred", ConnectorType.of("STRING")));

        // The exact set + order of forwarded methods the foreign-handle test drives (a Rule-9 completeness lock:
        // dropping a guard, or adding one that should not forward, changes this list and fails the test).
        static final List<String> EXPECTED_METHODS = Collections.unmodifiableList(Arrays.asList(
                "getTableSchema", "getColumnHandles", "getTableStatistics", "getColumnStatistics",
                "estimateDataSizeByListingFiles",
                "applyFilter", "listPartitionNames", "listPartitions",
                "beginQuerySnapshot", "getTableFreshness", "getPartitionFreshnessMillis", "dropTable",
                "truncateTable", "getTableSchemaAtSnapshot", "getMvccPartitionView", "resolveTimeTravel",
                "applySnapshot", "getSyntheticScanPredicates", "applyRewriteFileScope",
                "applyTopnLazyMaterialization", "listSupportedSysTables", "getSysTableHandle",
                "isPartitionValuesSysTable"));

        // The exact set + order of ALTER-DDL / validate methods the foreign-handle write test drives (Rule-9
        // completeness lock for §4.4 W1: dropping a guard, or adding one that should not forward, fails the test).
        static final List<String> EXPECTED_WRITE_METHODS = Collections.unmodifiableList(Arrays.asList(
                "renameTable", "addColumn", "addColumns", "dropColumn", "renameColumn", "modifyColumn",
                "reorderColumns", "createOrReplaceBranch", "createOrReplaceTag", "dropBranch", "dropTag",
                "addPartitionField", "dropPartitionField", "replacePartitionField",
                "validateRowLevelDmlMode", "validateStaticPartitionColumns", "validateWritePartitionNames"));

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
        public ConnectorTransaction beginTransaction(ConnectorSession session, ConnectorTableHandle handle) {
            calls.add("beginTransaction");
            return SIBLING_TXN;
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
        public List<ConnectorExpression> getSyntheticScanPredicates(ConnectorSession session,
                ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
            calls.add("getSyntheticScanPredicates");
            return SIBLING_PREDICATES;
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

        @Override
        public boolean isPartitionValuesSysTable(ConnectorSession session,
                ConnectorTableHandle baseTableHandle, String sysName) {
            calls.add("isPartitionValuesSysTable");
            // A distinctive true (hive's own logic would say false for "snapshots") proves the divert.
            return true;
        }

        // ---- §4.4 W1: ALTER-DDL mutators + write validators (the write-delegation surface) ----

        @Override
        public void renameTable(ConnectorSession session, ConnectorTableHandle handle, String newName) {
            calls.add("renameTable");
        }

        @Override
        public void addColumn(ConnectorSession session, ConnectorTableHandle handle, ConnectorColumn column,
                ConnectorColumnPosition position) {
            calls.add("addColumn");
        }

        @Override
        public void addColumns(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorColumn> columns) {
            calls.add("addColumns");
        }

        @Override
        public void dropColumn(ConnectorSession session, ConnectorTableHandle handle, String columnName) {
            calls.add("dropColumn");
        }

        @Override
        public void renameColumn(ConnectorSession session, ConnectorTableHandle handle, String oldName,
                String newName) {
            calls.add("renameColumn");
        }

        @Override
        public void modifyColumn(ConnectorSession session, ConnectorTableHandle handle, ConnectorColumn column,
                ConnectorColumnPosition position) {
            calls.add("modifyColumn");
        }

        @Override
        public void reorderColumns(ConnectorSession session, ConnectorTableHandle handle, List<String> newOrder) {
            calls.add("reorderColumns");
        }

        @Override
        public void createOrReplaceBranch(ConnectorSession session, ConnectorTableHandle handle,
                BranchChange branch) {
            calls.add("createOrReplaceBranch");
        }

        @Override
        public void createOrReplaceTag(ConnectorSession session, ConnectorTableHandle handle, TagChange tag) {
            calls.add("createOrReplaceTag");
        }

        @Override
        public void dropBranch(ConnectorSession session, ConnectorTableHandle handle, DropRefChange branch) {
            calls.add("dropBranch");
        }

        @Override
        public void dropTag(ConnectorSession session, ConnectorTableHandle handle, DropRefChange tag) {
            calls.add("dropTag");
        }

        @Override
        public void addPartitionField(ConnectorSession session, ConnectorTableHandle handle,
                PartitionFieldChange change) {
            calls.add("addPartitionField");
        }

        @Override
        public void dropPartitionField(ConnectorSession session, ConnectorTableHandle handle,
                PartitionFieldChange change) {
            calls.add("dropPartitionField");
        }

        @Override
        public void replacePartitionField(ConnectorSession session, ConnectorTableHandle handle,
                PartitionFieldChange change) {
            calls.add("replacePartitionField");
        }

        @Override
        public void validateRowLevelDmlMode(ConnectorSession session, ConnectorTableHandle handle,
                WriteOperation op) {
            calls.add("validateRowLevelDmlMode");
        }

        @Override
        public void validateStaticPartitionColumns(ConnectorSession session, ConnectorTableHandle handle,
                List<String> staticPartitionColumnNames) {
            calls.add("validateStaticPartitionColumns");
        }

        @Override
        public void validateWritePartitionNames(ConnectorSession session, ConnectorTableHandle handle,
                List<String> partitionNames) {
            calls.add("validateWritePartitionNames");
        }
    }
}
