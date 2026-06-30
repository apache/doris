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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.BranchChange;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.ddl.ConnectorPartitionField;
import org.apache.doris.connector.api.ddl.ConnectorPartitionSpec;
import org.apache.doris.connector.api.ddl.ConnectorSortField;
import org.apache.doris.connector.api.ddl.DropRefChange;
import org.apache.doris.connector.api.ddl.PartitionFieldChange;
import org.apache.doris.connector.api.ddl.TagChange;

import org.apache.iceberg.TableProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Behavior tests for the B1 DDL overrides on {@link IcebergConnectorMetadata} — driven entirely through the
 * {@link RecordingIcebergCatalogOps} seam + {@link RecordingConnectorContext} (no live catalog, no Mockito).
 * Asserts: every remote op runs INSIDE the auth context, the HMS-only properties gate, the force-drop
 * cascade, and that the managed-location cleanup hook is invoked (HMS only) with the location captured
 * BEFORE the drop.
 */
public class IcebergConnectorMetadataDdlTest {

    private static Map<String, String> props(String catalogType) {
        Map<String, String> p = new HashMap<>();
        if (catalogType != null) {
            p.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, catalogType);
        }
        return p;
    }

    private static IcebergConnectorMetadata metadata(RecordingIcebergCatalogOps ops,
            RecordingConnectorContext ctx, String catalogType) {
        return new IcebergConnectorMetadata(ops, props(catalogType), ctx);
    }

    @Test
    public void testSupportsCreateDatabase() {
        Assertions.assertTrue(metadata(new RecordingIcebergCatalogOps(),
                new RecordingConnectorContext(), IcebergConnectorProperties.TYPE_REST).supportsCreateDatabase());
    }

    // ---------- createDatabase ----------

    @Test
    public void testCreateDatabaseHmsWithPropertiesIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        Map<String, String> dbProps = Collections.singletonMap("location", "s3://wh/db");
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_HMS).createDatabase(null, "db1", dbProps);
        Assertions.assertEquals("db1", ops.lastCreateDb);
        Assertions.assertEquals(dbProps, ops.lastCreateDbProps);
        Assertions.assertEquals(1, ctx.authCount, "createDatabase must run inside executeAuthenticated");
    }

    @Test
    public void testCreateDatabaseNonHmsWithPropertiesFailsLoud() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergConnectorMetadata md = metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> md.createDatabase(null, "db1", Collections.singletonMap("k", "v")));
        Assertions.assertTrue(ex.getMessage().contains("rest"));
        // The gate runs BEFORE the auth context — the seam must not be touched.
        Assertions.assertTrue(ops.log.isEmpty(), ops.log.toString());
        Assertions.assertEquals(0, ctx.authCount);
    }

    @Test
    public void testCreateDatabaseNonHmsEmptyPropertiesSucceeds() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST)
                .createDatabase(null, "db1", Collections.emptyMap());
        Assertions.assertEquals("db1", ops.lastCreateDb);
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void testCreateDatabaseAuthFailureWraps() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        IcebergConnectorMetadata md = metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> md.createDatabase(null, "db1", Collections.emptyMap()));
        // failAuth throws WITHOUT running the task -> the seam create must not have run.
        Assertions.assertTrue(ops.log.isEmpty());
    }

    // ---------- dropDatabase ----------

    @Test
    public void testDropDatabaseForceCascadesAndCleansHms() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.tables = Arrays.asList("t1", "t2");
        ops.namespaceLocation = Optional.of("s3://wh/db1");
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_HMS).dropDatabase(null, "db1", false, true);
        // location captured BEFORE drop, then the tables cascade-dropped, then the (empty) view list probed,
        // then the namespace dropped.
        Assertions.assertEquals(Arrays.asList(
                "loadNamespaceLocation:db1",
                "listTableNames:db1",
                "dropTable:db1.t1:purge=true",
                "dropTable:db1.t2:purge=true",
                "listViewNames:db1",
                "dropDatabase:db1"), ops.log);
        // cleanup hook called once with the namespace location + empty child dirs.
        Assertions.assertEquals(Collections.singletonList("s3://wh/db1"), ctx.cleanedLocations);
        Assertions.assertTrue(ctx.cleanedChildDirs.get(0).isEmpty());
    }

    @Test
    public void testDropDatabaseForceCascadesViewsAfterTables() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.tables = Collections.singletonList("t1");
        ops.views = Arrays.asList("v1", "v2");
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST).dropDatabase(null, "db1", false, true);
        // WHY: iceberg VIEWS live in their own namespace (listTableNames subtracts them), so a force drop
        // must cascade them too — AFTER the tables and BEFORE dropNamespace — or the dropDatabase below would
        // fail loud "namespace not empty". MUTATION: dropping the view cascade -> the dropView entries vanish
        // (the namespace would not be empty in production) -> red.
        Assertions.assertEquals(Arrays.asList(
                "listTableNames:db1",
                "dropTable:db1.t1:purge=true",
                "listViewNames:db1",
                "dropView:db1.v1",
                "dropView:db1.v2",
                "dropDatabase:db1"), ops.log);
    }

    @Test
    public void testDropDatabaseNonForceNonHmsNoCascadeNoCleanup() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.tables = Arrays.asList("t1", "t2");
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST).dropDatabase(null, "db1", false, false);
        // No location load (non-HMS), no cascade (non-force), just the namespace drop.
        Assertions.assertEquals(Collections.singletonList("dropDatabase:db1"), ops.log);
        Assertions.assertTrue(ctx.cleanedLocations.isEmpty());
    }

    // ---------- createTable ----------

    @Test
    public void testCreateTableBuildsArtifactsAndCallsSeam() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                .dbName("db1").tableName("t1")
                .columns(Arrays.asList(
                        new ConnectorColumn("id", ConnectorType.of("BIGINT"), "", true, null, false),
                        new ConnectorColumn("name", ConnectorType.of("VARCHAR", 50, 0), "", true, null, false)))
                .partitionSpec(new ConnectorPartitionSpec(ConnectorPartitionSpec.Style.TRANSFORM,
                        Collections.singletonList(
                                new ConnectorPartitionField("id", "bucket", Collections.singletonList(8))),
                        Collections.emptyList()))
                .sortOrder(Collections.singletonList(new ConnectorSortField("id", true, true)))
                .build();
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST).createTable(null, request);

        Assertions.assertEquals("db1", ops.lastCreateTableDb);
        Assertions.assertEquals("t1", ops.lastCreateTableName);
        Assertions.assertNotNull(ops.lastCreateSchema.findField("id"));
        Assertions.assertNotNull(ops.lastCreateSchema.findField("name"));
        Assertions.assertEquals(1, ops.lastCreateSpec.fields().size());
        Assertions.assertEquals("bucket[8]", ops.lastCreateSpec.fields().get(0).transform().toString());
        Assertions.assertNotNull(ops.lastCreateSortOrder);
        Assertions.assertFalse(ops.lastCreateSortOrder.isUnsorted());
        // MOR + format-version defaults applied.
        Assertions.assertEquals("2", ops.lastCreateProps.get(TableProperties.FORMAT_VERSION));
        Assertions.assertEquals("merge-on-read", ops.lastCreateProps.get(TableProperties.DELETE_MODE));
        Assertions.assertEquals(1, ctx.authCount, "createTable must run inside executeAuthenticated");
    }

    @Test
    public void testCreateTableUnsupportedTypeFailsBeforeRemote() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                .dbName("db1").tableName("t1")
                .columns(Collections.singletonList(
                        new ConnectorColumn("t", ConnectorType.of("TINYINT"), "", true, null, false)))
                .build();
        IcebergConnectorMetadata md = metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST);
        Assertions.assertThrows(DorisConnectorException.class, () -> md.createTable(null, request));
        // Schema build is pure + runs before the auth/remote create -> the seam never ran.
        Assertions.assertTrue(ops.log.isEmpty());
        Assertions.assertEquals(0, ctx.authCount);
    }

    // ---------- dropTable ----------

    @Test
    public void testDropTableHmsCapturesLocationAndCleans() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.tableLocation = Optional.of("s3://wh/db1/t1");
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_HMS)
                .dropTable(null, new IcebergTableHandle("db1", "t1"));
        // location captured BEFORE the purge-drop.
        Assertions.assertEquals(Arrays.asList(
                "loadTableLocation:db1.t1",
                "dropTable:db1.t1:purge=true"), ops.log);
        Assertions.assertTrue(ops.lastDropPurge);
        Assertions.assertEquals(Collections.singletonList("s3://wh/db1/t1"), ctx.cleanedLocations);
        Assertions.assertEquals(Arrays.asList("data", "metadata"), ctx.cleanedChildDirs.get(0));
    }

    @Test
    public void testDropTableNonHmsNoLocationNoCleanup() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST)
                .dropTable(null, new IcebergTableHandle("db1", "t1"));
        Assertions.assertEquals(Collections.singletonList("dropTable:db1.t1:purge=true"), ops.log);
        Assertions.assertTrue(ctx.cleanedLocations.isEmpty());
    }

    // ---------- dropView ----------

    @Test
    public void testDropViewRoutesToSeamAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST).dropView(null, "db1", "v1");
        // WHY: PluginDrivenExternalCatalog.dropTable routes a flipped iceberg view here; it must reach the seam
        // with the (db, view) names verbatim, INSIDE the auth context (mirrors legacy performDropView under the
        // executionAuthenticator). MUTATION: dropping the delegation / hoisting it outside the auth wrap -> red.
        Assertions.assertEquals(Collections.singletonList("dropView:db1.v1"), ops.log);
        Assertions.assertEquals("db1", ops.lastDropViewDb);
        Assertions.assertEquals("v1", ops.lastDropViewName);
        Assertions.assertEquals(1, ctx.authCount, "dropView must run inside executeAuthenticated");
    }

    @Test
    public void testDropViewAuthFailureWraps() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        // WHY: like the other write ops, a remote/auth failure must surface as a DorisConnectorException so
        // PluginDrivenExternalCatalog.dropTable can rewrap it as a DdlException; the seam must NOT be reached.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST).dropView(null, "db1", "v1"));
        Assertions.assertTrue(ex.getMessage().contains("Failed to drop Iceberg view"), ex.getMessage());
        Assertions.assertFalse(ops.log.contains("dropView:db1.v1"),
                "the seam must not be reached when the auth wrap throws");
    }

    // ---------- renameTable ----------

    @Test
    public void testRenameTableRoutesByHandleAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST)
                .renameTable(null, new IcebergTableHandle("db1", "t1"), "t2");
        Assertions.assertEquals(Collections.singletonList("renameTable:db1.t1->t2"), ops.log);
        Assertions.assertEquals("db1", ops.lastRenameTableDb);
        Assertions.assertEquals("t1", ops.lastRenameTableOld);
        Assertions.assertEquals("t2", ops.lastRenameTableNew);
        Assertions.assertEquals(1, ctx.authCount, "renameTable must run inside executeAuthenticated");
    }

    @Test
    public void testRenameTableAuthFailureWraps() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST)
                        .renameTable(null, new IcebergTableHandle("db1", "t1"), "t2"));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    // ---------- DLF flavor: every DDL write fails loud BEFORE the seam (legacy IcebergDLFExternalCatalog parity) ----------

    // WHY: a DLF (Aliyun Data Lake Formation) iceberg catalog rejected all DDL writes in master. After the flip
    // the migrated DLFCatalog does NOT override createTable, so without a connector guard CREATE TABLE would
    // actually create a table against the live DLF metastore (DLF write is unvalidated); the other ops degraded
    // to a generic message. Each guard must throw the exact legacy message, before the auth scope and the seam.
    // MUTATION: dropping any guard / weakening isDlfCatalog to non-DLF -> the matching test goes red.
    private static void assertDlfRejects(Consumer<IcebergConnectorMetadata> op, String expectedMessage) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergConnectorMetadata md = metadata(ops, ctx, IcebergConnectorProperties.TYPE_DLF);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class, () -> op.accept(md));
        Assertions.assertEquals(expectedMessage, ex.getMessage());
        Assertions.assertTrue(ops.log.isEmpty(), "DLF guard must fail before the seam: " + ops.log);
        Assertions.assertEquals(0, ctx.authCount, "DLF guard must fail before the auth scope");
    }

    @Test
    public void testDlfCreateDatabaseFailsLoud() {
        assertDlfRejects(md -> md.createDatabase(null, "db1", Collections.emptyMap()),
                "iceberg catalog with dlf type not supports 'create database'");
    }

    @Test
    public void testDlfDropDatabaseFailsLoud() {
        // force=true would otherwise cascade tables/views through the seam — the guard must pre-empt all of it.
        assertDlfRejects(md -> md.dropDatabase(null, "db1", false, true),
                "iceberg catalog with dlf type not supports 'drop database'");
    }

    @Test
    public void testDlfCreateTableFailsLoudBeforeRemote() {
        // a valid column type: the test must fail on the DLF guard, not on type building -> proves the real fix
        // (createTable was the sole op that previously reached the live DLF metastore).
        ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                .dbName("db1").tableName("t1")
                .columns(Collections.singletonList(
                        new ConnectorColumn("id", ConnectorType.of("BIGINT"), "", true, null, false)))
                .build();
        assertDlfRejects(md -> md.createTable(null, request),
                "iceberg catalog with dlf type not supports 'create table'");
    }

    @Test
    public void testDlfDropTableFailsLoud() {
        assertDlfRejects(md -> md.dropTable(null, new IcebergTableHandle("db1", "t1")),
                "iceberg catalog with dlf type not supports 'drop table'");
    }

    @Test
    public void testDlfRenameTableFailsLoud() {
        assertDlfRejects(md -> md.renameTable(null, new IcebergTableHandle("db1", "t1"), "t2"),
                "iceberg catalog with dlf type not supports 'rename table'");
    }

    // ---------- Branch / tag (B4): route by handle, auth-wrap, wrap auth failures ----------

    @Test
    public void testCreateOrReplaceBranchRoutesByHandleAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        BranchChange branch = new BranchChange("b1", true, false, false, 7L, null, null, null);
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST)
                .createOrReplaceBranch(null, new IcebergTableHandle("db1", "t1"), branch);
        Assertions.assertEquals(Collections.singletonList("createOrReplaceBranch:db1.t1:b1"), ops.log);
        Assertions.assertEquals("db1", ops.lastBranchTagDb);
        Assertions.assertEquals("t1", ops.lastBranchTagTable);
        Assertions.assertSame(branch, ops.lastBranch);
        Assertions.assertEquals(1, ctx.authCount, "createOrReplaceBranch must run inside executeAuthenticated");
    }

    @Test
    public void testCreateOrReplaceBranchAuthFailureWraps() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST).createOrReplaceBranch(
                        null, new IcebergTableHandle("db1", "t1"),
                        new BranchChange("b1", true, false, false, null, null, null, null)));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    @Test
    public void testCreateOrReplaceTagRoutesByHandleAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        TagChange tag = new TagChange("v1", true, false, false, 7L, null);
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST)
                .createOrReplaceTag(null, new IcebergTableHandle("db1", "t1"), tag);
        Assertions.assertEquals(Collections.singletonList("createOrReplaceTag:db1.t1:v1"), ops.log);
        Assertions.assertSame(tag, ops.lastTag);
        Assertions.assertEquals(1, ctx.authCount, "createOrReplaceTag must run inside executeAuthenticated");
    }

    @Test
    public void testCreateOrReplaceTagAuthFailureWraps() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST).createOrReplaceTag(
                        null, new IcebergTableHandle("db1", "t1"),
                        new TagChange("v1", true, false, false, null, null)));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    @Test
    public void testDropBranchRoutesByHandleAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        DropRefChange drop = new DropRefChange("b1", true);
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST)
                .dropBranch(null, new IcebergTableHandle("db1", "t1"), drop);
        Assertions.assertEquals(Collections.singletonList("dropBranch:db1.t1:b1"), ops.log);
        Assertions.assertSame(drop, ops.lastDropBranch);
        Assertions.assertEquals(1, ctx.authCount, "dropBranch must run inside executeAuthenticated");
    }

    @Test
    public void testDropTagRoutesByHandleAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        DropRefChange drop = new DropRefChange("v1", false);
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST)
                .dropTag(null, new IcebergTableHandle("db1", "t1"), drop);
        Assertions.assertEquals(Collections.singletonList("dropTag:db1.t1:v1"), ops.log);
        Assertions.assertSame(drop, ops.lastDropTag);
        Assertions.assertEquals(1, ctx.authCount, "dropTag must run inside executeAuthenticated");
    }

    @Test
    public void testDropTagAuthFailureWraps() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST).dropTag(
                        null, new IcebergTableHandle("db1", "t1"), new DropRefChange("v1", false)));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    // ---------- Partition evolution (B5): route by handle, auth-wrap, wrap auth failures ----------

    @Test
    public void testAddPartitionFieldRoutesByHandleAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        PartitionFieldChange change = new PartitionFieldChange("bucket", 8, "id", "id_b",
                null, null, null, null);
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST)
                .addPartitionField(null, new IcebergTableHandle("db1", "t1"), change);
        Assertions.assertEquals(Collections.singletonList("addPartitionField:db1.t1:id"), ops.log);
        Assertions.assertEquals("db1", ops.lastPartitionFieldDb);
        Assertions.assertEquals("t1", ops.lastPartitionFieldTable);
        Assertions.assertSame(change, ops.lastAddPartitionField);
        Assertions.assertEquals(1, ctx.authCount, "addPartitionField must run inside executeAuthenticated");
    }

    @Test
    public void testAddPartitionFieldAuthFailureWraps() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST).addPartitionField(
                        null, new IcebergTableHandle("db1", "t1"),
                        new PartitionFieldChange(null, null, "id", null, null, null, null, null)));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    @Test
    public void testDropPartitionFieldRoutesByHandleAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        PartitionFieldChange change = new PartitionFieldChange(null, null, null, "p_id",
                null, null, null, null);
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST)
                .dropPartitionField(null, new IcebergTableHandle("db1", "t1"), change);
        Assertions.assertEquals(Collections.singletonList("dropPartitionField:db1.t1:p_id"), ops.log);
        Assertions.assertSame(change, ops.lastDropPartitionField);
        Assertions.assertEquals(1, ctx.authCount, "dropPartitionField must run inside executeAuthenticated");
    }

    @Test
    public void testReplacePartitionFieldRoutesByHandleAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        PartitionFieldChange change = new PartitionFieldChange("bucket", 4, "id", "p2",
                "p", null, null, null);
        metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST)
                .replacePartitionField(null, new IcebergTableHandle("db1", "t1"), change);
        Assertions.assertEquals(Collections.singletonList("replacePartitionField:db1.t1:id"), ops.log);
        Assertions.assertSame(change, ops.lastReplacePartitionField);
        Assertions.assertEquals(1, ctx.authCount, "replacePartitionField must run inside executeAuthenticated");
    }

    @Test
    public void testReplacePartitionFieldAuthFailureWraps() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx, IcebergConnectorProperties.TYPE_REST).replacePartitionField(
                        null, new IcebergTableHandle("db1", "t1"),
                        new PartitionFieldChange(null, null, "id", null, "p", null, null, null)));
        Assertions.assertTrue(ops.log.isEmpty());
    }
}
