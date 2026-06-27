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
import org.apache.doris.connector.api.ddl.ConnectorSortField;
import org.apache.doris.connector.iceberg.IcebergCatalogOps.CatalogBackedIcebergCatalogOps;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * End-to-end seam tests for the B1 DDL methods on {@link CatalogBackedIcebergCatalogOps}, exercised against a
 * REAL iceberg {@link InMemoryCatalog} (no Mockito). Proves the thin delegations create/drop real namespaces +
 * tables and that the location helpers read back what the catalog persisted.
 */
public class CatalogBackedIcebergCatalogOpsDdlTest {

    private InMemoryCatalog catalog;
    private CatalogBackedIcebergCatalogOps ops;

    @BeforeEach
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        ops = new CatalogBackedIcebergCatalogOps(catalog);
    }

    @AfterEach
    public void tearDown() throws Exception {
        catalog.close();
    }

    private static Schema schema() {
        return IcebergSchemaBuilder.buildSchema(Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("BIGINT"), "", true, null, false),
                new ConnectorColumn("name", ConnectorType.of("VARCHAR", 50, 0), "", true, null, false)));
    }

    @Test
    public void testCreateAndDropDatabase() {
        ops.createDatabase("db1", Collections.emptyMap());
        Assertions.assertTrue(ops.databaseExists("db1"));
        Assertions.assertTrue(ops.listDatabaseNames().contains("db1"));

        ops.dropDatabase("db1");
        Assertions.assertFalse(ops.databaseExists("db1"));
    }

    @Test
    public void testLoadNamespaceLocationReadsBackProperty() {
        ops.createDatabase("db1", Collections.singletonMap("location", "s3://wh/db1"));
        Optional<String> location = ops.loadNamespaceLocation("db1");
        Assertions.assertTrue(location.isPresent());
        Assertions.assertEquals("s3://wh/db1", location.get());
    }

    @Test
    public void testLoadNamespaceLocationAbsentWhenUnset() {
        ops.createDatabase("db1", Collections.emptyMap());
        Assertions.assertFalse(ops.loadNamespaceLocation("db1").isPresent());
    }

    @Test
    public void testCreateAndDropTable() {
        ops.createDatabase("db1", Collections.emptyMap());
        Map<String, String> props = IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap());
        ops.createTable("db1", "t1", schema(), PartitionSpec.unpartitioned(), null, props);

        Assertions.assertTrue(ops.tableExists("db1", "t1"));
        // The created table carries our columns + the MOR defaults applied by IcebergSchemaBuilder.
        Assertions.assertEquals(Type.TypeID.LONG, ops.loadTable("db1", "t1").schema().findField("id").type().typeId());
        Assertions.assertEquals("merge-on-read", ops.loadTable("db1", "t1").properties().get("write.delete.mode"));
        Assertions.assertTrue(ops.loadTableLocation("db1", "t1").isPresent());

        ops.dropTable("db1", "t1", true);
        Assertions.assertFalse(ops.tableExists("db1", "t1"));
    }

    @Test
    public void testCreateTableWithSortOrder() {
        ops.createDatabase("db1", Collections.emptyMap());
        Schema schema = schema();
        SortOrder sortOrder = IcebergSchemaBuilder.buildSortOrder(
                Collections.singletonList(new ConnectorSortField("id", true, true)), schema);
        ops.createTable("db1", "t1", schema, PartitionSpec.unpartitioned(), sortOrder,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));

        // The write order is persisted (the buildTable().withSortOrder() path).
        Assertions.assertFalse(ops.loadTable("db1", "t1").sortOrder().isUnsorted());
    }

    @Test
    public void testCreateTablePartitioned() {
        ops.createDatabase("db1", Collections.emptyMap());
        Schema schema = schema();
        PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("id", 8).build();
        ops.createTable("db1", "t1", schema, spec, null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
        Assertions.assertFalse(ops.loadTable("db1", "t1").spec().isUnpartitioned());
    }

    @Test
    public void testForceDropDatabaseAfterCascade() {
        // Mirror the metadata layer's force path: drop the contained tables, then the namespace.
        ops.createDatabase("db1", Collections.emptyMap());
        ops.createTable("db1", "t1", schema(), PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
        for (String table : ops.listTableNames("db1")) {
            ops.dropTable("db1", table, true);
        }
        ops.dropDatabase("db1");
        Assertions.assertFalse(ops.databaseExists("db1"));
        Assertions.assertFalse(catalog.namespaceExists(Namespace.of("db1")));
    }

    @Test
    public void testDropTablePurgeRemovesIdentifier() {
        ops.createDatabase("db1", Collections.emptyMap());
        ops.createTable("db1", "t1", schema(), PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
        ops.dropTable("db1", "t1", true);
        Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("db1", "t1")));
    }
}
