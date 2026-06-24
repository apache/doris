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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import java.util.Map;

/**
 * Pins the {@link IcebergProcedureOps} dispatch (P6.4-T03 skeleton + T04 bodies).
 *
 * <p><b>WHY this matters:</b> {@code getSupportedProcedures()} exports the factory's name list and
 * {@code execute()} routes through the factory to a {@link org.apache.doris.connector.iceberg.action.BaseIcebergAction}.
 * T04 makes a known procedure executable end-to-end: the body's SDK mutation + {@code commit()} run inside ONE
 * {@code executeAuthenticated} scope (the auth fix the legacy fe-core actions lacked), then the table's cached
 * metadata is invalidated once at dispatch level (replacing the legacy per-action {@code ExtMetaCacheMgr}
 * call), and the {@link ConnectorSession} is threaded to the body (the {@code rollback_to_timestamp} time
 * zone). The whole path is dormant pre-cutover (iceberg is not {@code PluginDrivenExternalTable} until P6.6).</p>
 */
public class IcebergProcedureOpsTest {

    private static final ConnectorSession SESSION = new TzSession("Asia/Shanghai");

    private static IcebergProcedureOps newOps() {
        // getSupportedProcedures + the unknown-name rejection never touch the catalog/context, so they may
        // be null here; the catalog-backed path is exercised below with an InMemoryCatalog.
        return new IcebergProcedureOps(Collections.emptyMap(), null, null);
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

    // ─────────────────── catalog-backed dispatch (T04) ───────────────────

    @Test
    public void runsBodyInAuthScopeAndInvalidatesTableAfterCommit() {
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
        Assertions.assertEquals(ImmutableList.of("db1.t"), ctx.invalidatedTables,
                "the table is invalidated once at dispatch level after a successful commit");
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
        Assertions.assertEquals(ImmutableList.of("db1.t"), ctx.invalidatedTables);
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
