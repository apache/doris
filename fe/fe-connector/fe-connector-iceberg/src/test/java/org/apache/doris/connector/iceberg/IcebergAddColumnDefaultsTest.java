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

import org.apache.doris.connector.iceberg.IcebergCatalogOps.CatalogBackedIcebergCatalogOps;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPath;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPosition;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

/** Exercises metadata conversion, table-version validation and the real SDK schema commit together. */
public class IcebergAddColumnDefaultsTest {
    private static final IcebergTableHandle HANDLE = new IcebergTableHandle("db", "t");
    private InMemoryCatalog catalog;
    private IcebergConnectorMetadata metadata;

    @BeforeEach
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
        metadata = new IcebergConnectorMetadata(new CatalogBackedIcebergCatalogOps(catalog),
                IcebergCatalogProperties.of(Collections.singletonMap(
                        IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, IcebergCatalogProperties.TYPE_REST)),
                new RecordingConnectorContext());
    }

    @AfterEach
    public void tearDown() throws Exception {
        catalog.close();
    }

    private Table createTable(int formatVersion) {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "s", Types.StructType.of(
                        Types.NestedField.optional(3, "a", Types.IntegerType.get()))),
                Types.NestedField.optional(4, "arr", Types.ListType.ofOptional(5, Types.StructType.of(
                        Types.NestedField.optional(6, "a", Types.IntegerType.get())))),
                Types.NestedField.optional(7, "m", Types.MapType.ofOptional(8, 9, Types.StringType.get(),
                        Types.StructType.of(Types.NestedField.optional(10, "a", Types.IntegerType.get())))));
        return catalog.createTable(TableIdentifier.of("db", "t"), schema, PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", String.valueOf(formatVersion)));
    }

    private Schema reload() {
        return catalog.loadTable(TableIdentifier.of("db", "t")).schema();
    }

    private static ConnectorColumnPath path(String... parts) {
        return ConnectorColumnPath.of(Arrays.asList(parts));
    }

    private static ConnectorColumn column(String name, boolean nullable, String defaultValue) {
        return new ConnectorColumn(name, ConnectorType.of("INT"), "added field", nullable, defaultValue, false);
    }

    private static void assertDefaults(Types.NestedField field, boolean nullable, int value) {
        Assertions.assertEquals(nullable, field.isOptional());
        Assertions.assertEquals(value, field.initialDefault());
        Assertions.assertEquals(value, field.writeDefault());
        Assertions.assertEquals("added field", field.doc());
    }

    @Test
    public void addRequiredColumnPreservesDefaultsAndPosition() {
        createTable(3);
        metadata.addColumn(null, HANDLE, column("required", false, "7"), ConnectorColumnPosition.after("id"));
        Schema schema = reload();
        assertDefaults(schema.findField("required"), false, 7);
        Assertions.assertEquals("required", schema.columns().get(1).name());
        Assertions.assertEquals(10, schema.findField("m.value.a").fieldId());
    }

    @Test
    public void addColumnsPreservesEachFieldsRequirednessAndDefaults() {
        createTable(3);
        metadata.addColumns(null, HANDLE, Arrays.asList(
                column("required", false, "7"), column("optional", true, "8"), column("plain", true, null)));
        assertDefaults(reload().findField("required"), false, 7);
        assertDefaults(reload().findField("optional"), true, 8);
        Assertions.assertTrue(reload().findField("plain").isOptional());
        Assertions.assertNull(reload().findField("plain").initialDefault());
    }

    @Test
    public void addNestedRequiredFieldsUnderStructAndCollectionValues() {
        createTable(3);
        for (ConnectorColumnPath path : Arrays.asList(path("s", "required"),
                path("arr", "element", "required"),
                path("m", "value", "required"))) {
            metadata.addNestedColumn(null, HANDLE, path, column("required", false, "9"),
                    ConnectorColumnPosition.FIRST);
            Schema schema = reload();
            assertDefaults(schema.findField(path.getFullPath()), false, 9);
            Assertions.assertEquals("required", schema.findType(path.getParentPath().getFullPath())
                    .asStructType().fields().get(0).name());
        }
        Assertions.assertTrue(reload().findField("s").isOptional());
        Assertions.assertTrue(reload().findField("arr.element").isOptional());
        Assertions.assertTrue(reload().findField("m.value").isOptional());
    }

    @Test
    public void addNestedOptionalDefaultAndCanonicalizeParentPath() {
        createTable(3);
        metadata.addNestedColumn(null, HANDLE, path("S", "optional"),
                column("optional", true, "11"), ConnectorColumnPosition.after("a"));
        assertDefaults(reload().findField("s.optional"), true, 11);
        Assertions.assertEquals("optional", reload().findType("s").asStructType().fields().get(1).name());
    }

    @Test
    public void addTopLevelAndNestedTypedDefaults() {
        createTable(3);
        ConnectorType[] types = {ConnectorType.of("BOOLEAN"), ConnectorType.of("DATEV2"),
                ConnectorType.of("DATETIMEV2", 6, 0), ConnectorType.of("STRING"),
                ConnectorType.of("DECIMALV3", 10, 2), ConnectorType.of("TIMESTAMPTZ", 6, 0)};
        String[] defaults = {"1", "2024-01-02", "2024-01-02 03:04:05.123456", "hello", "1.50",
                "2024-01-01 19:04:05.123456+00:00"};
        Object[] expected = {true, 19724, 1704164645123456L, "hello", new BigDecimal("1.50"), 1704135845123456L};
        for (int i = 0; i < types.length; i++) {
            String name = "typed_" + i;
            ConnectorColumn column = new ConnectorColumn(name, types[i], "", false, defaults[i], false);
            metadata.addColumn(null, HANDLE, column, null);
            metadata.addNestedColumn(null, HANDLE, path("s", name), column, null);
            for (String fieldName : Arrays.asList(name, "s." + name)) {
                Types.NestedField field = reload().findField(fieldName);
                Assertions.assertTrue(field.isRequired());
                Assertions.assertEquals(expected[i], field.initialDefault());
                Assertions.assertEquals(expected[i], field.writeDefault());
            }
        }
    }

    @Test
    public void requiredWithoutNonNullDefaultFailsWithoutChangingSchema() {
        createTable(3);
        String before = SchemaParser.toJson(reload());
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.addColumn(null, HANDLE, column("required", false, null), null));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.addNestedColumn(null, HANDLE, path("s", "required"),
                        column("required", false, null), null));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.addColumns(null, HANDLE, Arrays.asList(
                        column("valid", true, null), column("required", false, null))));
        Assertions.assertEquals(before, SchemaParser.toJson(reload()));
    }

    @Test
    public void formatOneRejectsDefaultsOnEveryAddPath() {
        assertOldFormatRejectsDefaults(1);
    }

    @Test
    public void formatTwoRejectsDefaultsOnEveryAddPath() {
        assertOldFormatRejectsDefaults(2);
    }

    private void assertOldFormatRejectsDefaults(int formatVersion) {
        createTable(formatVersion);
        String before = SchemaParser.toJson(reload());
        for (boolean nullable : Arrays.asList(false, true)) {
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.addColumn(null, HANDLE, column("added", nullable, "7"), null));
            Assertions.assertTrue(error.getMessage().contains("format-version 3"), error.getMessage());
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.addNestedColumn(null, HANDLE, path("s", "added"),
                            column("added", nullable, "7"), null));
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.addColumns(null, HANDLE, Arrays.asList(
                            column("plain", true, null), column("added", nullable, "7"))));
            Assertions.assertEquals(before, SchemaParser.toJson(reload()));
        }
        metadata.addNestedColumn(null, HANDLE, path("s", "plain"),
                column("plain", true, null), null);
        Assertions.assertTrue(reload().findField("s.plain").isOptional());
    }

    @Test
    public void invalidDefaultAndBatchCollisionLeaveSchemaUnchanged() {
        createTable(3);
        String before = SchemaParser.toJson(reload());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> metadata.addColumn(null, HANDLE, column("bad", false, "invalid"), null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> metadata.addNestedColumn(null, HANDLE, path("s", "bad"),
                        column("bad", false, "invalid"), null));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.addColumns(null, HANDLE, Arrays.asList(
                        column("valid", false, "7"), column("ID", false, "8"))));
        Assertions.assertEquals(before, SchemaParser.toJson(reload()));
    }

    @Test
    public void rejectMapKeyEvolutionAndNonStructParent() {
        createTable(3);
        String before = SchemaParser.toJson(reload());
        for (ConnectorColumnPath path : Arrays.asList(path("m", "key", "added"),
                path("id", "added"))) {
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.addNestedColumn(null, HANDLE, path, column("added", false, "7"), null));
        }
        Assertions.assertEquals(before, SchemaParser.toJson(reload()));
    }

    @Test
    public void newWriterUsesAddedRequiredDefaultAndOldWriterRejectsSchemaDrift() {
        Table table = createTable(3);
        IcebergWriteSchemaContext oldWriter = IcebergWriteSchemaContext.create(
                table, "db.t", Optional.empty(), false, false);
        metadata.addColumn(null, HANDLE, column("required", false, "7"), null);
        table.refresh();
        Assertions.assertThrows(DorisConnectorException.class, () -> oldWriter.validateCurrentSchema(table, false));
        IcebergWriteSchemaContext newWriter = IcebergWriteSchemaContext.create(
                table, "db.t", Optional.empty(), false, false);
        ConnectorColumn added = newWriter.getColumns().stream()
                .filter(col -> col.getName().equals("required")).findFirst().get();
        Assertions.assertFalse(added.isNullable());
        Assertions.assertEquals("7", added.getDefaultValueSql());
    }
}
