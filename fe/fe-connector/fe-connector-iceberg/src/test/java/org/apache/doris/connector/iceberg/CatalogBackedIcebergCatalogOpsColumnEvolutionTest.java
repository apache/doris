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
import org.apache.doris.connector.iceberg.IcebergCatalogOps.CatalogBackedIcebergCatalogOps;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * End-to-end seam tests for the B2 column-evolution methods on {@link CatalogBackedIcebergCatalogOps}, against a
 * REAL iceberg {@link InMemoryCatalog} (no Mockito). Proves the {@code UpdateSchema} build+commit actually
 * mutates the persisted schema (add at FIRST/AFTER/end, drop, rename, modify type/comment/nullability, reorder)
 * and that the validation guards (optional&rarr;required, missing column) fail loud.
 */
public class CatalogBackedIcebergCatalogOpsColumnEvolutionTest {

    private InMemoryCatalog catalog;
    private CatalogBackedIcebergCatalogOps ops;

    @BeforeEach
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        ops = new CatalogBackedIcebergCatalogOps(catalog);
        ops.createDatabase("db1", Collections.emptyMap());
        // id BIGINT (optional), val INT (optional), name VARCHAR (optional, doc "old"), req INT (required)
        Schema schema = IcebergSchemaBuilder.buildSchema(Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("BIGINT"), "", true, null, false),
                new ConnectorColumn("val", ConnectorType.of("INT"), "", true, null, false),
                new ConnectorColumn("name", ConnectorType.of("VARCHAR", 50, 0), "old", true, null, false),
                new ConnectorColumn("req", ConnectorType.of("INT"), "", false, null, false)));
        ops.createTable("db1", "t1", schema, PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
    }

    @AfterEach
    public void tearDown() throws Exception {
        catalog.close();
    }

    private Schema reload() {
        return ops.loadTable("db1", "t1").schema();
    }

    private List<String> columnOrder() {
        return reload().columns().stream().map(Types.NestedField::name).collect(Collectors.toList());
    }

    private static IcebergColumnChange change(String name, Type type, String comment, boolean nullable) {
        return new IcebergColumnChange(name, type, comment, null, nullable);
    }

    // ---------- addColumn ----------

    @Test
    public void testAddColumnAtEnd() {
        ops.addColumn("db1", "t1", change("age", Types.IntegerType.get(), "c", true), null);
        Assertions.assertNotNull(reload().findField("age"));
        Assertions.assertEquals("age", columnOrder().get(columnOrder().size() - 1));
    }

    @Test
    public void testAddColumnFirst() {
        ops.addColumn("db1", "t1", change("age", Types.IntegerType.get(), "c", true),
                ConnectorColumnPosition.FIRST);
        Assertions.assertEquals("age", columnOrder().get(0));
    }

    @Test
    public void testAddColumnAfter() {
        ops.addColumn("db1", "t1", change("age", Types.IntegerType.get(), "c", true),
                ConnectorColumnPosition.after("id"));
        Assertions.assertEquals(Arrays.asList("id", "age"), columnOrder().subList(0, 2));
    }

    // ---------- addColumns ----------

    @Test
    public void testAddColumns() {
        ops.addColumns("db1", "t1", Arrays.asList(
                change("a", Types.IntegerType.get(), "c", true),
                change("b", Types.StringType.get(), "c", true)));
        Assertions.assertNotNull(reload().findField("a"));
        Assertions.assertNotNull(reload().findField("b"));
    }

    // ---------- dropColumn / renameColumn ----------

    @Test
    public void testDropColumn() {
        ops.dropColumn("db1", "t1", "val");
        Assertions.assertNull(reload().findField("val"));
    }

    @Test
    public void testRenameColumn() {
        ops.renameColumn("db1", "t1", "name", "full_name");
        Assertions.assertNull(reload().findField("name"));
        Assertions.assertNotNull(reload().findField("full_name"));
    }

    // ---------- modifyColumn ----------

    @Test
    public void testModifyColumnWidensType() {
        ops.modifyColumn("db1", "t1", change("val", Types.LongType.get(), "c", true), null);
        Assertions.assertEquals(Type.TypeID.LONG, reload().findField("val").type().typeId());
    }

    @Test
    public void testModifyColumnCommentOnly() {
        // VARCHAR maps to iceberg STRING; re-sending the same type with a new doc updates only the comment.
        ops.modifyColumn("db1", "t1", change("name", Types.StringType.get(), "new comment", true), null);
        Assertions.assertEquals("new comment", reload().findField("name").doc());
        Assertions.assertEquals(Type.TypeID.STRING, reload().findField("name").type().typeId());
    }

    @Test
    public void testModifyColumnRequiredToOptional() {
        Assertions.assertFalse(reload().findField("req").isOptional());
        ops.modifyColumn("db1", "t1", change("req", Types.IntegerType.get(), null, true), null);
        Assertions.assertTrue(reload().findField("req").isOptional());
    }

    @Test
    public void testModifyColumnRepositions() {
        ops.modifyColumn("db1", "t1", change("name", Types.StringType.get(), "c", true),
                ConnectorColumnPosition.FIRST);
        Assertions.assertEquals("name", columnOrder().get(0));
    }

    @Test
    public void testModifyColumnOptionalToRequiredFailsLoud() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> ops.modifyColumn("db1", "t1", change("id", Types.LongType.get(), null, false), null));
        Assertions.assertTrue(ex.getMessage().contains("not null"));
        // schema unchanged: id stays optional.
        Assertions.assertTrue(reload().findField("id").isOptional());
    }

    @Test
    public void testModifyMissingColumnFailsLoud() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> ops.modifyColumn("db1", "t1", change("ghost", Types.IntegerType.get(), null, true), null));
        Assertions.assertTrue(ex.getMessage().contains("does not exist"));
    }

    // ---------- reorderColumns ----------

    @Test
    public void testReorderColumns() {
        ops.reorderColumns("db1", "t1", Arrays.asList("name", "req", "id", "val"));
        Assertions.assertEquals(Arrays.asList("name", "req", "id", "val"), columnOrder());
    }
}
