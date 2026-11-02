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
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * {@code MODIFY COLUMN} of iceberg BINARY / FIXED columns and nested fields, driven through
 * {@link IcebergConnectorMetadata} into a REAL {@link InMemoryCatalog}. Doris reads BINARY as {@code VARBINARY} and
 * FIXED(n) as {@code VARBINARY(n)}, and iceberg has no type to build for VARBINARY, so a modify that restates how
 * such a column or field is read (to change its nullability, comment or position) must keep the current iceberg
 * type rather than fail with "Unsupported type for Iceberg: VARBINARY". A VARBINARY that restates nothing still
 * fails.
 */
public class IcebergConnectorMetadataBinaryModifyTest {

    private static final IcebergTableHandle HANDLE = new IcebergTableHandle("db1", "t1");
    private static final String UNSUPPORTED = "Unsupported type for Iceberg: VARBINARY";

    private InMemoryCatalog catalog;
    private CatalogBackedIcebergCatalogOps ops;
    private IcebergConnectorMetadata metadata;

    @BeforeEach
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        ops = new CatalogBackedIcebergCatalogOps(catalog);
        ops.createDatabase("db1", Collections.emptyMap());
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "payload", Types.BinaryType.get()),
                Types.NestedField.required(3, "digest", Types.FixedType.ofLength(16), "sha"),
                Types.NestedField.optional(4, "s", Types.StructType.of(
                        Types.NestedField.optional(5, "a", Types.BinaryType.get(), "raw"),
                        Types.NestedField.optional(6, "b", Types.IntegerType.get()))),
                Types.NestedField.optional(7, "arr", Types.ListType.ofRequired(8, Types.FixedType.ofLength(4))),
                Types.NestedField.optional(9, "m", Types.MapType.ofRequired(10, 11,
                        Types.StringType.get(), Types.BinaryType.get())));
        ops.createTable("db1", "t1", schema, PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
        Map<String, String> props = new HashMap<>();
        props.put(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, IcebergCatalogProperties.TYPE_REST);
        metadata = new IcebergConnectorMetadata(ops, IcebergCatalogProperties.of(props),
                new RecordingConnectorContext());
    }

    @AfterEach
    public void tearDown() throws Exception {
        catalog.close();
    }

    private Schema reload() {
        return ops.loadTable("db1", "t1").schema();
    }

    /** The neutral type fe-core sends for a Doris {@code VARBINARY}, which is how iceberg BINARY is read. */
    private static ConnectorType varbinary() {
        return ConnectorType.of("VARBINARY");
    }

    /** The neutral type fe-core sends for a Doris {@code VARBINARY(n)}, which is how iceberg FIXED(n) is read. */
    private static ConnectorType varbinary(int length) {
        return ConnectorType.of("VARBINARY", length, 0);
    }

    /** A nullable column with its nullability stated and its comment omitted. */
    private static ConnectorColumn column(String name, ConnectorType type) {
        return new ConnectorColumn(name, type, "", true, null, false).withSpecified(true, false);
    }

    private static ConnectorColumnPath path(String... parts) {
        return ConnectorColumnPath.of(Arrays.asList(parts));
    }

    @Test
    public void testRestatingBinaryColumnKeepsBinaryAndMovesIt() {
        // MODIFY COLUMN payload VARBINARY NULL FIRST
        metadata.modifyColumn(null, HANDLE, column("payload", varbinary()), ConnectorColumnPosition.FIRST);

        Schema schema = reload();
        Assertions.assertEquals(Types.BinaryType.get(), schema.findField("payload").type());
        Assertions.assertEquals("payload", schema.columns().get(0).name());
    }

    @Test
    public void testRestatingFixedColumnKeepsFixedAndWidensNullability() {
        // MODIFY COLUMN digest VARBINARY(16) NULL COMMENT 'digest'
        ConnectorColumn digest = new ConnectorColumn("digest", varbinary(16), "digest", true, null, false)
                .withSpecified(true, true);
        metadata.modifyColumn(null, HANDLE, digest, null);

        Types.NestedField field = reload().findField("digest");
        Assertions.assertEquals(Types.FixedType.ofLength(16), field.type());
        Assertions.assertTrue(field.isOptional());
        Assertions.assertEquals("digest", field.doc());
    }

    @Test
    public void testRestatingBinaryStructFieldChangesItsComment() {
        // MODIFY COLUMN s.a VARBINARY COMMENT 'bytes'
        ConnectorColumn a = new ConnectorColumn("a", varbinary(), "bytes", true, null, false)
                .withSpecified(false, true);
        metadata.modifyNestedColumn(null, HANDLE, path("s", "a"), a, null);

        Types.NestedField field = reload().findField("s.a");
        Assertions.assertEquals(Types.BinaryType.get(), field.type());
        Assertions.assertEquals("bytes", field.doc());
    }

    @Test
    public void testRestatingBinaryElementAndValueWidensThem() {
        // MODIFY COLUMN arr.element VARBINARY(4) NULL; MODIFY COLUMN m.value VARBINARY NULL
        metadata.modifyNestedColumn(null, HANDLE, path("arr", "element"), column("element", varbinary(4)), null);
        metadata.modifyNestedColumn(null, HANDLE, path("m", "value"), column("value", varbinary()), null);

        Schema schema = reload();
        Types.ListType arr = schema.findField("arr").type().asListType();
        Assertions.assertEquals(Types.FixedType.ofLength(4), arr.elementType());
        Assertions.assertTrue(arr.isElementOptional());
        Types.MapType m = schema.findField("m").type().asMapType();
        Assertions.assertEquals(Types.BinaryType.get(), m.valueType());
        Assertions.assertTrue(m.isValueOptional());
    }

    @Test
    public void testVarbinaryThatRestatesNothingStillFails() {
        Schema before = reload();
        // A VARBINARY is not how the current column or field is read: a length other than FIXED's, a length for an
        // unbounded BINARY, a column or field of another type, or no such column.
        assertFails(() -> metadata.modifyColumn(null, HANDLE, column("payload", varbinary(16)), null));
        assertFails(() -> metadata.modifyColumn(null, HANDLE, column("digest", varbinary(8)), null));
        assertFails(() -> metadata.modifyColumn(null, HANDLE, column("id", varbinary()), null));
        assertFails(() -> metadata.modifyColumn(null, HANDLE, column("s", varbinary()), null));
        assertFails(() -> metadata.modifyColumn(null, HANDLE, column("missing", varbinary()), null));
        assertFails(() -> metadata.modifyNestedColumn(null, HANDLE, path("s", "a"), column("a", varbinary(3)), null));
        assertFails(() -> metadata.modifyNestedColumn(null, HANDLE, path("s", "b"), column("b", varbinary()), null));
        Assertions.assertTrue(before.sameSchema(reload()), "a failed modify must not change the schema");
    }

    private static void assertFails(org.junit.jupiter.api.function.Executable modify) {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class, modify);
        Assertions.assertEquals(UNSUPPORTED, ex.getMessage());
    }
}
