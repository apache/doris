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
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.ddl.ConnectorPartitionField;
import org.apache.doris.connector.api.ddl.ConnectorPartitionSpec;
import org.apache.doris.connector.api.ddl.ConnectorSortField;

import org.apache.iceberg.TableProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
        // location captured BEFORE drop, then the tables cascade-dropped, then the namespace dropped.
        Assertions.assertEquals(Arrays.asList(
                "loadNamespaceLocation:db1",
                "listTableNames:db1",
                "dropTable:db1.t1:purge=true",
                "dropTable:db1.t2:purge=true",
                "dropDatabase:db1"), ops.log);
        // cleanup hook called once with the namespace location + empty child dirs.
        Assertions.assertEquals(Collections.singletonList("s3://wh/db1"), ctx.cleanedLocations);
        Assertions.assertTrue(ctx.cleanedChildDirs.get(0).isEmpty());
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
}
