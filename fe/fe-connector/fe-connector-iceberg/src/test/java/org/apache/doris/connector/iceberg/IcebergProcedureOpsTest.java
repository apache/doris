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
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.api.procedure.ConnectorRewriteGroup;
import org.apache.doris.connector.api.procedure.ProcedureExecutionMode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Pins the {@link IcebergProcedureOps} dispatch (P6.4-T03 skeleton + T04 bodies).
 *
 * <p><b>WHY this matters:</b> {@code getSupportedProcedures()} exports the factory's name list and
 * {@code execute()} routes through the factory to a {@link org.apache.doris.connector.iceberg.action.BaseIcebergAction}.
 * T04 makes a known procedure executable end-to-end: the body's SDK mutation + {@code commit()} run inside ONE
 * {@code executeAuthenticated} scope (the auth fix the legacy fe-core actions lacked), and the
 * {@link ConnectorSession} is threaded to the body (the {@code rollback_to_timestamp} time zone). The whole
 * path is dormant pre-cutover (iceberg is not {@code PluginDrivenExternalTable} until P6.6).</p>
 *
 * <p><b>Cache invalidation is the engine's responsibility (H-6 fix):</b> the dispatch must NOT invalidate any
 * cache — after the procedure returns, the engine ({@code ConnectorExecuteAction}) refreshes the mutated table
 * through the standard refresh-table path, the only path that drops both the engine meta cache (LOCAL-name
 * keyed) and the connector's own per-table cache (REMOTE-name keyed). So {@code ctx.invalidatedTables} stays
 * empty after every dispatch (success or failure); a non-empty list here would mean the removed connector-side
 * notification was re-introduced.</p>
 */
public class IcebergProcedureOpsTest {

    private static final ConnectorSession SESSION = new TzSession("Asia/Shanghai");

    private static IcebergProcedureOps newOps() {
        // getSupportedProcedures + the unknown-name rejection never touch the catalog/context, so they may
        // be null here; the catalog-backed path is exercised below with an InMemoryCatalog. The (IcebergCatalogOps)
        // cast selects the ops constructor over the session-aware resolver overload (a bare null matches both).
        return new IcebergProcedureOps(Collections.emptyMap(), (IcebergCatalogOps) null, null);
    }

    @Test
    public void getSupportedProceduresExportsFactoryNamesInLegacyOrder() {
        Assertions.assertEquals(
                ImmutableList.of(
                        "rollback_to_snapshot",
                        "rollback_to_timestamp",
                        "set_current_snapshot",
                        "cherrypick_snapshot",
                        "fast_forward",
                        "expire_snapshots",
                        "rewrite_data_files",
                        "publish_changes",
                        "rewrite_manifests"),
                newOps().getSupportedProcedures());
    }

    @Test
    public void rewriteDataFilesIsTheOnlyDistributedProcedure() {
        IcebergProcedureOps ops = newOps();
        // rewrite_data_files runs N per-group INSERT-SELECT writes under one shared transaction, so the engine
        // must orchestrate it (DISTRIBUTED) rather than dispatch it through execute(). Every other procedure is
        // a synchronous SDK call (SINGLE_CALL). This is what lets the engine route rewrite without a name literal.
        Assertions.assertEquals(ProcedureExecutionMode.DISTRIBUTED,
                ops.getExecutionMode("rewrite_data_files"),
                "rewrite_data_files must be DISTRIBUTED so the engine drives the per-group rewrite loop");
        Assertions.assertEquals(ProcedureExecutionMode.DISTRIBUTED,
                ops.getExecutionMode("REWRITE_DATA_FILES"),
                "execution-mode lookup must be case-insensitive (mirrors the factory's toLowerCase dispatch)");
        for (String name : ops.getSupportedProcedures()) {
            if ("rewrite_data_files".equals(name)) {
                continue;
            }
            Assertions.assertEquals(ProcedureExecutionMode.SINGLE_CALL, ops.getExecutionMode(name),
                    name + " is a synchronous SDK procedure and must be SINGLE_CALL");
        }
    }

    @Test
    public void executeRejectsUnknownProcedureWithLegacyMessage() {
        IcebergTableHandle handle = new IcebergTableHandle("db", "tbl");
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> newOps().execute(null, handle, "no_such_proc",
                        Collections.emptyMap(), null, Collections.emptyList()));
        Assertions.assertEquals(
                "Unsupported Iceberg procedure: no_such_proc. Supported procedures: rollback_to_snapshot, "
                        + "rollback_to_timestamp, set_current_snapshot, cherrypick_snapshot, fast_forward, "
                        + "expire_snapshots, rewrite_data_files, publish_changes, rewrite_manifests",
                e.getMessage());
    }

    // rewrite_data_files is advertised in the name list but its body is ported in T05/T06; until then it
    // reaches the factory's "Unsupported" rejection (the whole path is dormant, so this is invisible).
    @Test
    public void rewriteDataFilesIsNotYetExecutable() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> newOps().execute(null, new IcebergTableHandle("db", "tbl"), "rewrite_data_files",
                        Collections.emptyMap(), null, Collections.emptyList()));
        Assertions.assertTrue(e.getMessage().startsWith("Unsupported Iceberg procedure: rewrite_data_files"),
                e.getMessage());
    }

    // ─────────────────── WS-REWRITE R3: planRewrite (DISTRIBUTED planning half) ───────────────────

    @Test
    public void planRewriteGroupsBinPackedFilesWithRawPaths() {
        InMemoryCatalog catalog = tableWithThreeSmallFiles();
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = catalog.loadTable(TableIdentifier.of("db1", "t"));
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergProcedureOps procOps = new IcebergProcedureOps(Collections.emptyMap(), ops, ctx);

        // rewrite-all packs all three small files into one group (one bin, far under max-file-group-size), so a
        // group is produced without needing the default min-input-files=5.
        List<ConnectorRewriteGroup> groups = procOps.planRewrite(SESSION, new IcebergTableHandle("db1", "t"),
                "rewrite_data_files", ImmutableMap.of("rewrite-all", "true"), null, Collections.emptyList());

        Assertions.assertEquals(1, groups.size());
        ConnectorRewriteGroup g = groups.get(0);
        // WHY: the engine driver scopes each group's INSERT-SELECT scan by these RAW data-file paths, so the
        // group must carry them verbatim (the same raw path the scan provider matches — R2 [INV-M1]). MUTATION:
        // toConnectorRewriteGroup mapping the normalized path / wrong field -> paths mismatch -> red.
        Assertions.assertEquals(
                ImmutableSet.of("s3://b/db1/f1.parquet", "s3://b/db1/f2.parquet", "s3://b/db1/f3.parquet"),
                g.getDataFilePaths());
        // WHY: the per-group counts/size are summed into the rewrite result row, so they must be carried from the
        // planner's group verbatim. MUTATION: any stat read off the wrong RewriteDataGroup accessor -> red.
        Assertions.assertEquals(3, g.getDataFileCount());
        Assertions.assertEquals(3 * 1024L, g.getTotalSizeBytes());
        Assertions.assertEquals(0, g.getDeleteFileCount());
        // WHY: manifest reads run under the catalog auth context (Kerberos), like the procedure bodies; planning
        // does NOT mutate the table, so it must not invalidate the cache. MUTATION: planning outside auth scope
        // -> authCount 0 -> red; invalidating after planning -> invalidatedTables non-empty -> red.
        Assertions.assertEquals(1, ctx.authCount, "planning runs in one auth scope");
        Assertions.assertTrue(ctx.invalidatedTables.isEmpty(), "planning must not invalidate the table");
    }

    @Test
    public void planRewriteEmptyTableReturnsNoGroups() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        catalog.createTable(TableIdentifier.of("db1", "t"), schema, PartitionSpec.unpartitioned());
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = catalog.loadTable(TableIdentifier.of("db1", "t"));
        IcebergProcedureOps procOps =
                new IcebergProcedureOps(Collections.emptyMap(), ops, new RecordingConnectorContext());

        List<ConnectorRewriteGroup> groups = procOps.planRewrite(SESSION, new IcebergTableHandle("db1", "t"),
                "rewrite_data_files", Collections.emptyMap(), null, Collections.emptyList());

        // WHY: an empty table (no current snapshot) has nothing to rewrite -> zero groups, so the engine driver
        // emits the all-zero result row (the legacy short-circuit). MUTATION: planning a non-existent snapshot
        // -> exception or non-empty -> red.
        Assertions.assertTrue(groups.isEmpty());
    }

    @Test
    public void planRewriteValidatesArgsBeforeTouchingCatalog() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        IcebergProcedureOps procOps = new IcebergProcedureOps(Collections.emptyMap(), ops, null);

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> procOps.planRewrite(SESSION, new IcebergTableHandle("db1", "t"), "rewrite_data_files",
                        ImmutableMap.of("min-file-size-bytes", "100", "max-file-size-bytes", "50"),
                        null, Collections.emptyList()));
        // WHY: argument validation (byte-identical message) runs BEFORE the catalog is touched, mirroring the
        // single-call path. MUTATION: planning before validate() -> loadTable called / wrong message -> red.
        Assertions.assertEquals(
                "min-file-size-bytes must be less than or equal to max-file-size-bytes", e.getMessage());
        Assertions.assertTrue(ops.log.isEmpty(), "validation must fail before the catalog is loaded");
    }

    @Test
    public void planRewriteRejectsNonRewriteProcedure() {
        IcebergProcedureOps procOps = newOps();
        // WHY: only rewrite_data_files is DISTRIBUTED; a miswired caller passing another name must fail loud, not
        // build a rewrite action for the wrong procedure. MUTATION: dropping the name guard -> a rollback name
        // would build a rewrite action -> no throw here -> red.
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> procOps.planRewrite(SESSION, new IcebergTableHandle("db1", "t"), "rollback_to_snapshot",
                        Collections.emptyMap(), null, Collections.emptyList()));
        Assertions.assertTrue(e.getMessage().contains("Unsupported distributed iceberg procedure"),
                e.getMessage());
    }

    // ─────────────────── catalog-backed dispatch (T04) ───────────────────

    @Test
    public void runsBodyInOneAuthScopeAndDoesNotInvalidateAtDispatch() {
        InMemoryCatalog catalog = catalogWithTwoSnapshots();
        TableIdentifier id = TableIdentifier.of("db1", "t");
        long snap1 = catalog.loadTable(id).history().get(0).snapshotId();
        long snap2 = catalog.loadTable(id).currentSnapshot().snapshotId();

        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = catalog.loadTable(id);
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergProcedureOps procOps = new IcebergProcedureOps(Collections.emptyMap(), ops, ctx);

        ConnectorProcedureResult result = procOps.execute(SESSION, new IcebergTableHandle("db1", "t"),
                "rollback_to_snapshot", ImmutableMap.of("snapshot_id", String.valueOf(snap1)),
                null, Collections.emptyList());

        Assertions.assertEquals(1, ctx.authCount, "the body (load + SDK mutation + commit) runs in ONE auth scope");
        Assertions.assertTrue(ops.log.contains("loadTable:db1.t"));
        // H-6: the connector dispatch must NOT invalidate — the engine refreshes the table after this returns.
        // A non-empty list would mean the removed connector-side getMetaInvalidator notification was re-added.
        Assertions.assertTrue(ctx.invalidatedTables.isEmpty(),
                "the connector dispatch must not invalidate any cache (the engine owns invalidation)");
        Assertions.assertEquals(ImmutableList.of(String.valueOf(snap2), String.valueOf(snap1)),
                result.getRows().get(0));
        Assertions.assertEquals(snap1, catalog.loadTable(id).currentSnapshot().snapshotId());
    }

    @Test
    public void threadsSessionToTimestampBody() {
        InMemoryCatalog catalog = catalogWithTwoSnapshots();
        TableIdentifier id = TableIdentifier.of("db1", "t");
        long snap1 = catalog.loadTable(id).history().get(0).snapshotId();
        long snap2 = catalog.loadTable(id).currentSnapshot().snapshotId();
        long t2 = catalog.loadTable(id).currentSnapshot().timestampMillis();

        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = catalog.loadTable(id);
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergProcedureOps procOps = new IcebergProcedureOps(Collections.emptyMap(), ops, ctx);

        // rollback_to_timestamp consults SESSION's time zone (it must be threaded through dispatch). Rolling
        // back to snap2's instant lands on snap1 (the latest snapshot strictly older than t2). Reaching this
        // result without NPE proves the session reached the body.
        ConnectorProcedureResult result = procOps.execute(SESSION, new IcebergTableHandle("db1", "t"),
                "rollback_to_timestamp", ImmutableMap.of("timestamp", String.valueOf(t2)),
                null, Collections.emptyList());

        Assertions.assertEquals(ImmutableList.of(String.valueOf(snap2), String.valueOf(snap1)),
                result.getRows().get(0));
        Assertions.assertTrue(ctx.invalidatedTables.isEmpty(),
                "the connector dispatch must not invalidate any cache (the engine owns invalidation)");
    }

    @Test
    public void failedAuthSurfacesAndDoesNotInvalidate() {
        InMemoryCatalog catalog = catalogWithTwoSnapshots();
        TableIdentifier id = TableIdentifier.of("db1", "t");
        long snap1 = catalog.loadTable(id).history().get(0).snapshotId();

        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = catalog.loadTable(id);
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        IcebergProcedureOps procOps = new IcebergProcedureOps(Collections.emptyMap(), ops, ctx);

        Assertions.assertThrows(DorisConnectorException.class, () -> procOps.execute(SESSION,
                new IcebergTableHandle("db1", "t"), "rollback_to_snapshot",
                ImmutableMap.of("snapshot_id", String.valueOf(snap1)), null, Collections.emptyList()));
        Assertions.assertTrue(ctx.invalidatedTables.isEmpty(),
                "a failed auth (body never ran) must not invalidate the table cache");
    }

    @Test
    public void argumentValidationRunsBeforeTouchingTheCatalog() {
        // A bad argument is rejected before loadTable/auth — no catalog work happens.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergProcedureOps procOps = new IcebergProcedureOps(Collections.emptyMap(), ops, ctx);

        Assertions.assertThrows(DorisConnectorException.class, () -> procOps.execute(SESSION,
                new IcebergTableHandle("db1", "t"), "rollback_to_snapshot",
                Collections.emptyMap(), null, Collections.emptyList()));
        Assertions.assertEquals(0, ctx.authCount, "validation precedes the auth-wrapped load");
        Assertions.assertTrue(ops.log.isEmpty(), "no catalog call before validation passes");
    }

    @Test
    public void wrapsLoadTableFailure() {
        // loadTable runs INSIDE executeAuthenticated and throws a plain RuntimeException; runInAuthScope's
        // generic catch wraps it with the "Failed to load iceberg table" prefix (the DorisConnectorException
        // re-throw branch is for body failures, not load failures). The wrap happens before the dispatch-level
        // invalidation, so a load failure must not invalidate the cache.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.throwOnLoadTable = true;
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergProcedureOps procOps = new IcebergProcedureOps(Collections.emptyMap(), ops, ctx);

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> procOps.execute(SESSION, new IcebergTableHandle("db1", "t"), "rollback_to_snapshot",
                        ImmutableMap.of("snapshot_id", "1"), null, Collections.emptyList()));
        Assertions.assertEquals(
                "Failed to load iceberg table db1.t: simulated loadTable failure for db1.t", e.getMessage());
        Assertions.assertTrue(ctx.invalidatedTables.isEmpty(), "a load failure must not invalidate");
    }

    @Test
    public void rollbackToCurrentSnapshotShortCircuitsWithoutCommit() {
        // Rolling back to the snapshot that is ALREADY current short-circuits in the body (no commit). The
        // connector dispatch invalidates nothing regardless (engine owns invalidation), so the no-op short
        // circuit and a real commit are indistinguishable from the connector's cache perspective.
        InMemoryCatalog catalog = catalogWithTwoSnapshots();
        TableIdentifier id = TableIdentifier.of("db1", "t");
        long snap2 = catalog.loadTable(id).currentSnapshot().snapshotId();
        long historyBefore = catalog.loadTable(id).history().size();

        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = catalog.loadTable(id);
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergProcedureOps procOps = new IcebergProcedureOps(Collections.emptyMap(), ops, ctx);

        ConnectorProcedureResult result = procOps.execute(SESSION, new IcebergTableHandle("db1", "t"),
                "rollback_to_snapshot", ImmutableMap.of("snapshot_id", String.valueOf(snap2)),
                null, Collections.emptyList());

        Assertions.assertEquals(ImmutableList.of(String.valueOf(snap2), String.valueOf(snap2)),
                result.getRows().get(0));
        Assertions.assertEquals(historyBefore, catalog.loadTable(id).history().size(), "short-circuit: no commit");
        Assertions.assertTrue(ctx.invalidatedTables.isEmpty(),
                "the connector dispatch must not invalidate any cache (the engine owns invalidation)");
    }

    @Test
    public void bodyFailureAfterSuccessfulLoadDoesNotInvalidate() {
        // The load succeeds (under auth), then the body throws because the snapshot id does not exist. This is
        // distinct from failedAuth (where the body never runs): authCount is 1, and the dispatch-level
        // invalidation still must not fire because the body's DorisConnectorException is re-thrown before it.
        InMemoryCatalog catalog = catalogWithTwoSnapshots();
        TableIdentifier id = TableIdentifier.of("db1", "t");
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = catalog.loadTable(id);
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergProcedureOps procOps = new IcebergProcedureOps(Collections.emptyMap(), ops, ctx);

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> procOps.execute(SESSION, new IcebergTableHandle("db1", "t"), "rollback_to_snapshot",
                        ImmutableMap.of("snapshot_id", "999999999"), null, Collections.emptyList()));
        Assertions.assertEquals("Snapshot 999999999 not found in table " + ops.table.name(), e.getMessage());
        Assertions.assertEquals(1, ctx.authCount, "the load ran under auth (body executed, then threw)");
        Assertions.assertTrue(ctx.invalidatedTables.isEmpty(),
                "a body failure after a successful load must not invalidate");
    }

    private static InMemoryCatalog tableWithThreeSmallFiles() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        catalog.createTable(TableIdentifier.of("db1", "t"), schema, PartitionSpec.unpartitioned());
        append(catalog, "f1", 1L);
        append(catalog, "f2", 1L);
        append(catalog, "f3", 1L);
        return catalog;
    }

    private static InMemoryCatalog catalogWithTwoSnapshots() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        catalog.createTable(TableIdentifier.of("db1", "t"), schema, PartitionSpec.unpartitioned());
        append(catalog, "f1", 1L);
        sleepForDistinctSnapshotTimestamp();
        append(catalog, "f2", 2L);
        return catalog;
    }

    private static void sleepForDistinctSnapshotTimestamp() {
        // Ensure snap2 gets a strictly-later commit timestamp than snap1 so rollback_to_timestamp(t2) lands
        // deterministically on snap1 (the latest snapshot strictly older than t2).
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void append(InMemoryCatalog catalog, String file, long records) {
        TableIdentifier id = TableIdentifier.of("db1", "t");
        Table t = catalog.loadTable(id);
        DataFile df = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("s3://b/db1/" + file + ".parquet")
                .withFileSizeInBytes(1024)
                .withRecordCount(records)
                .withFormat(FileFormat.PARQUET)
                .build();
        t.newAppend().appendFile(df).commit();
    }

    /** Minimal {@link ConnectorSession} exposing a time zone (the only field the procedures consult). */
    private static final class TzSession implements ConnectorSession {
        private final String timeZone;

        TzSession(String timeZone) {
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
        public long getCatalogId() {
            return 0;
        }

        @Override
        public String getCatalogName() {
            return "test";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }
    }
}
