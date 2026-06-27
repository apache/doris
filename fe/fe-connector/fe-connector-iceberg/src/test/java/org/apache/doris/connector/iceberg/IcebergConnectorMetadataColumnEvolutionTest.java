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
import org.apache.doris.connector.api.ddl.ConnectorColumnPosition;

import org.apache.iceberg.types.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Behavior tests for the B2 column-evolution overrides on {@link IcebergConnectorMetadata}, driven through the
 * {@link RecordingIcebergCatalogOps} seam + {@link RecordingConnectorContext} (no live catalog, no Mockito).
 * Asserts that each op builds the neutral column PURELY then runs the seam INSIDE the auth context, that the
 * neutral position is forwarded, and that the pre-remote parity guards (non-nullable add, aggregated/auto-inc
 * column, complex-type modify, empty reorder) fail loud BEFORE the seam runs.
 */
public class IcebergConnectorMetadataColumnEvolutionTest {

    private static final IcebergTableHandle HANDLE = new IcebergTableHandle("db1", "t1");

    private static Map<String, String> props() {
        Map<String, String> p = new HashMap<>();
        p.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        return p;
    }

    private static IcebergConnectorMetadata metadata(RecordingIcebergCatalogOps ops, RecordingConnectorContext ctx) {
        return new IcebergConnectorMetadata(ops, props(), ctx);
    }

    private static ConnectorColumn col(String name, String type) {
        return new ConnectorColumn(name, ConnectorType.of(type), "c", true, null, false);
    }

    // ---------- addColumn ----------

    @Test
    public void testAddColumnBuildsTypeAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx).addColumn(null, HANDLE, col("age", "INT"), ConnectorColumnPosition.after("id"));
        Assertions.assertEquals(Collections.singletonList("addColumn:db1.t1:age"), ops.log);
        Assertions.assertEquals("age", ops.lastAddColumn.getName());
        Assertions.assertEquals(Type.TypeID.INTEGER, ops.lastAddColumn.getType().typeId());
        Assertions.assertEquals("c", ops.lastAddColumn.getComment());
        Assertions.assertFalse(ops.lastAddColumnPos.isFirst());
        Assertions.assertEquals("id", ops.lastAddColumnPos.getAfterColumn());
        Assertions.assertEquals(1, ctx.authCount, "addColumn must run inside executeAuthenticated");
    }

    @Test
    public void testAddColumnNullPositionForwardedAsNull() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx).addColumn(null, HANDLE, col("age", "INT"), null);
        Assertions.assertNull(ops.lastAddColumnPos);
    }

    @Test
    public void testAddColumnDefaultLiteralParsed() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorColumn withDefault = new ConnectorColumn("n", ConnectorType.of("INT"), "", true, "42", false);
        metadata(ops, ctx).addColumn(null, HANDLE, withDefault, null);
        Assertions.assertNotNull(ops.lastAddColumn.getDefaultValue());
        Assertions.assertEquals(42, ops.lastAddColumn.getDefaultValue().value());
    }

    @Test
    public void testAddNonNullableColumnFailsBeforeRemote() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorColumn notNull = new ConnectorColumn("age", ConnectorType.of("INT"), "", false, null, false);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).addColumn(null, HANDLE, notNull, null));
        Assertions.assertTrue(ex.getMessage().contains("non-nullable"));
        Assertions.assertTrue(ops.log.isEmpty());
        Assertions.assertEquals(0, ctx.authCount);
    }

    @Test
    public void testAddAggregatedColumnFailsBeforeRemote() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        // isKey=false, isAutoInc=false, isAggregated=true
        ConnectorColumn agg = new ConnectorColumn("s", ConnectorType.of("INT"), "", true, null, false, false, true);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).addColumn(null, HANDLE, agg, null));
        Assertions.assertTrue(ex.getMessage().contains("aggregation"));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    @Test
    public void testAddAutoIncColumnFailsBeforeRemote() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        // isKey=false, isAutoInc=true
        ConnectorColumn autoInc = new ConnectorColumn("s", ConnectorType.of("INT"), "", true, null, false, true);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).addColumn(null, HANDLE, autoInc, null));
        Assertions.assertTrue(ex.getMessage().contains("auto incremental"));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    @Test
    public void testAddColumnAuthFailureWraps() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).addColumn(null, HANDLE, col("age", "INT"), null));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    // ---------- addColumns ----------

    @Test
    public void testAddColumnsBuildsAllAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx).addColumns(null, HANDLE, Arrays.asList(col("a", "INT"), col("b", "STRING")));
        Assertions.assertEquals(Collections.singletonList("addColumns:db1.t1:2"), ops.log);
        Assertions.assertEquals(2, ops.lastAddColumns.size());
        Assertions.assertEquals("a", ops.lastAddColumns.get(0).getName());
        Assertions.assertEquals("b", ops.lastAddColumns.get(1).getName());
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void testAddColumnsRejectsNonNullableMember() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorColumn notNull = new ConnectorColumn("b", ConnectorType.of("INT"), "", false, null, false);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).addColumns(null, HANDLE, Arrays.asList(col("a", "INT"), notNull)));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    // ---------- dropColumn / renameColumn ----------

    @Test
    public void testDropColumnRoutesAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx).dropColumn(null, HANDLE, "age");
        Assertions.assertEquals(Collections.singletonList("dropColumn:db1.t1:age"), ops.log);
        Assertions.assertEquals("age", ops.lastDropColumn);
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void testRenameColumnRoutesAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx).renameColumn(null, HANDLE, "old", "new");
        Assertions.assertEquals(Collections.singletonList("renameColumn:db1.t1:old->new"), ops.log);
        Assertions.assertEquals("old", ops.lastRenameColumnOld);
        Assertions.assertEquals("new", ops.lastRenameColumnNew);
        Assertions.assertEquals(1, ctx.authCount);
    }

    // ---------- modifyColumn ----------

    @Test
    public void testModifyScalarColumnBuildsTypeAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx).modifyColumn(null, HANDLE, col("age", "BIGINT"), ConnectorColumnPosition.FIRST);
        Assertions.assertEquals(Collections.singletonList("modifyColumn:db1.t1:age"), ops.log);
        Assertions.assertEquals(Type.TypeID.LONG, ops.lastModifyColumn.getType().typeId());
        Assertions.assertTrue(ops.lastModifyColumnPos.isFirst());
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void testModifyComplexColumnFailsBeforeRemote() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorColumn arr = new ConnectorColumn("arr",
                ConnectorType.arrayOf(ConnectorType.of("INT")), "", true, null, false);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).modifyColumn(null, HANDLE, arr, null));
        Assertions.assertTrue(ex.getMessage().contains("complex"));
        Assertions.assertTrue(ops.log.isEmpty());
        Assertions.assertEquals(0, ctx.authCount);
    }

    @Test
    public void testModifyAggregatedColumnFailsBeforeRemote() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ConnectorColumn agg = new ConnectorColumn("s", ConnectorType.of("INT"), "", true, null, false, false, true);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).modifyColumn(null, HANDLE, agg, null));
        Assertions.assertTrue(ops.log.isEmpty());
    }

    // ---------- reorderColumns ----------

    @Test
    public void testReorderColumnsRoutesAndIsAuthWrapped() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        metadata(ops, ctx).reorderColumns(null, HANDLE, Arrays.asList("b", "a"));
        Assertions.assertEquals(Collections.singletonList("reorderColumns:db1.t1:[b, a]"), ops.log);
        Assertions.assertEquals(Arrays.asList("b", "a"), ops.lastReorder);
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void testReorderColumnsEmptyFailsBeforeRemote() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).reorderColumns(null, HANDLE, Collections.emptyList()));
        Assertions.assertTrue(ex.getMessage().contains("empty"));
        Assertions.assertTrue(ops.log.isEmpty());
        Assertions.assertEquals(0, ctx.authCount);
    }
}
