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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * {@link AdbcObjectsReader} against hand-built {@code getObjects} responses.
 *
 * <p>These cases were once driven through the real SQLite driver, and cannot be any more. Reading a driver's
 * result into Arrow also loads arrow-c-data's own JNI shim, which travels inside the arrow-c-data jar as an
 * upstream build: it needs CXXABI_1.3.9, and hosts Doris still supports do not have it (CentOS 7's
 * libstdc++ stops at CXXABI_1.3.7). No property in this repository redirects it, so a driver-backed reader
 * test does not test the reader on such a host -- it fails FE UT. The response shape is what this class is
 * about, and that shape is fixed by the ADBC standard; a fixture expresses it without the driver.
 *
 * <p>Two shapes below are worth knowing came from a live driver rather than from the standard, because a
 * source that differed would be a real finding: SQLite reports a namespace whose {@code db_schema_name} is
 * the empty string (not null, and not a missing entry), and a depth-CATALOGS answer reports the schema list
 * as null. The end-to-end half of that -- that a real driver really answers this way, and that a view stays
 * out of {@code SHOW TABLES} when the source ignores the type filter -- is pinned by the regression suites
 * under {@code regression-test/suites/external_table_p0/adbc}, which have the driver and the cluster.
 */
class AdbcObjectsReaderTest {

    private static final String CATALOG_NAME = "catalog_name";
    private static final String CATALOG_DB_SCHEMAS = "catalog_db_schemas";

    private static final Field TABLE_STRUCT = new Field("item", FieldType.nullable(new ArrowType.Struct()),
            List.of(new Field("table_name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                    new Field("table_type", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

    private static final Field SCHEMA_STRUCT = new Field("item", FieldType.nullable(new ArrowType.Struct()),
            List.of(new Field("db_schema_name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                    new Field("db_schema_tables", FieldType.nullable(new ArrowType.List()),
                            List.of(TABLE_STRUCT))));

    private static final Schema GET_OBJECTS_SCHEMA = new Schema(List.of(
            new Field(CATALOG_NAME, FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field(CATALOG_DB_SCHEMAS, FieldType.nullable(new ArrowType.List()), List.of(SCHEMA_STRUCT))));

    /**
     * A source with no schema layer: SQLite reports catalog "main" and answers the schema entry with the
     * EMPTY STRING, not with null and not by omitting the entry. Reading that as an absent level is the
     * difference between a database named "main" and one named "".
     */
    @Test
    void readsTheNamespaceASourceWithNoSchemaLayerReports() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                ArrowReader reader = response(allocator, catalog("main", schema("", null)))) {
            List<AdbcNamespace> namespaces = AdbcObjectsReader.readNamespaces(reader);

            Assertions.assertEquals(List.of(new AdbcNamespace("main", "")), namespaces);
            Assertions.assertEquals("main", namespaces.get(0).dorisDatabaseName());
        }
    }

    /**
     * The same source asked at depth CATALOGS answers a null schema list, which is the other way a driver
     * says "there is no schema level here". Both must land on the catalog name as the database.
     */
    @Test
    void nullSchemaListStillYieldsTheCatalogAsADatabase() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                ArrowReader reader = response(allocator, catalog("main", null))) {
            List<AdbcNamespace> namespaces = AdbcObjectsReader.readNamespaces(reader);

            Assertions.assertEquals(List.of(new AdbcNamespace("main", "")), namespaces);
            Assertions.assertEquals("main", namespaces.get(0).dorisDatabaseName());
        }
    }

    /** A namespace reported twice is one namespace: a source may repeat a catalog level it shares. */
    @Test
    void namespaceReportedTwiceIsListedOnce() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                ArrowReader reader = response(allocator,
                        catalog("main", schema("", null)),
                        catalog("main", schema("", null)))) {
            Assertions.assertEquals(List.of(new AdbcNamespace("main", "")),
                    AdbcObjectsReader.readNamespaces(reader));
        }
    }

    /**
     * A source that honours the type filter answers with base tables only, and they come back in the order
     * the source listed them.
     */
    @Test
    void readsTableNamesAndHonoursTheTableTypeFilter() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                ArrowReader reader = response(allocator, catalog("main",
                        schema("", table("t1", "TABLE"), table("t2", "BASE TABLE"))))) {
            Assertions.assertEquals(List.of("t1", "t2"),
                    AdbcObjectsReader.readTableNames(reader, new AdbcNamespace("main", "")));
        }
    }

    /**
     * A Doris source is what makes this the load-bearing case: its Flight SQL endpoint recognises only the
     * literal "VIEW" as a type filter and answers everything else -- including the "table" ADBC asks with --
     * by returning every object it has. The response therefore carries objects the connector never asked
     * for, and they have to be dropped by the table_type that came back with them.
     *
     * <p>Dropping an unrecognised type is deliberate, and a view is the reason: a leaked view scans fine
     * through ADBC, so nothing ever looks broken -- the catalog just offers an object DESC and SELECT then
     * fail on. A missing type is kept, because it says nothing about the object.
     */
    @Test
    void viewsAreDroppedEvenWhenTheSourceIgnoresTheTypeFilter() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                ArrowReader reader = response(allocator, catalog("main", schema("",
                        table("t1", "TABLE"), table("v1", "VIEW"), table("v2", "view"),
                        table("s1", "SYSTEM VIEW"), table("a1", "OLAP"), table("n1", null),
                        table("e1", ""), table("b1", "BASE TABLE"))))) {
            List<String> tables = AdbcObjectsReader.readTableNames(reader, new AdbcNamespace("main", ""));

            // t1 first, then the two the source left untyped -- a source that omits the column stays as
            // usable as it was before the filter existed -- and the Doris spelling of a base table.
            Assertions.assertEquals(List.of("t1", "n1", "e1", "b1"), tables);
        }
    }

    /**
     * {@code getObjects} filters are advisory: a driver may answer a narrower request with everything it
     * has. Rows that belong to another namespace must not be listed under the one that was asked for, or a
     * table of a neighbouring database appears in this one -- and the reverse, a namespace that matches
     * neither level, must list nothing rather than everything.
     */
    @Test
    void tablesOfOtherNamespacesAreNotListed() throws Exception {
        Object[] row = catalog("main", schema("", table("t1", "TABLE")),
                schema("public", table("p1", "TABLE")));
        Object[] otherCatalog = catalog("other", schema("", table("o1", "TABLE")));

        try (BufferAllocator allocator = new RootAllocator();
                ArrowReader reader = response(allocator, row, otherCatalog)) {
            Assertions.assertEquals(List.of("t1"),
                    AdbcObjectsReader.readTableNames(reader, new AdbcNamespace("main", "")));
        }
        // A second read: the same rows, asked for the other schema of the same catalog. Its neighbour's
        // table must not come along, and neither may the other catalog's.
        try (BufferAllocator allocator = new RootAllocator();
                ArrowReader reader = response(allocator, row, otherCatalog)) {
            Assertions.assertEquals(List.of("p1"),
                    AdbcObjectsReader.readTableNames(reader, new AdbcNamespace("main", "public")));
        }
        try (BufferAllocator allocator = new RootAllocator();
                ArrowReader reader = response(allocator, row, otherCatalog)) {
            Assertions.assertEquals(List.of(),
                    AdbcObjectsReader.readTableNames(reader, new AdbcNamespace("main", "no_such_schema")));
        }
    }

    @Test
    void theTypeNamesRealSourcesUseAreClassifiedTheWayTheyMean() {
        // A Doris source spells a table "BASE TABLE" -- getting that one wrong does not hide a view, it
        // hides EVERY table of every ADBC catalog pointed at Doris, and the catalog just looks empty.
        Assertions.assertTrue(AdbcObjectsReader.isBaseTable("BASE TABLE"));
        // ...and its materialized views come back under the same name, which is right: those are storage
        // that can be scanned, not a query wearing a table's name.
        Assertions.assertTrue(AdbcObjectsReader.isBaseTable("table"));

        Assertions.assertFalse(AdbcObjectsReader.isBaseTable("VIEW"));
        Assertions.assertFalse(AdbcObjectsReader.isBaseTable("view"));
        // What Doris calls its information_schema tables. Reading one through ADBC is not supported either.
        Assertions.assertFalse(AdbcObjectsReader.isBaseTable("SYSTEM VIEW"));

        // Dropped, deliberately. The forgiving rule -- keep what is not recognised -- is the wrong one here:
        // a leaked view scans fine through ADBC, so it never announces itself. A source whose tables land
        // here lists nothing instead, which does.
        Assertions.assertFalse(AdbcObjectsReader.isBaseTable("OLAP"));

        // Saying nothing is not the same as saying something unrecognised: a source that omits the column
        // stays exactly as usable as it was before this filter existed.
        Assertions.assertTrue(AdbcObjectsReader.isBaseTable(null));
        Assertions.assertTrue(AdbcObjectsReader.isBaseTable(""));
    }

    @Test
    void resultWithoutTheStandardColumnsIsRejectedByName() throws Exception {
        // A driver that answers getObjects with its own shape must fail with something that says so; the
        // alternative is a NullPointerException from deep inside the reader.
        Schema schema = new Schema(List.of(
                new Field("something_else", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
        try (BufferAllocator allocator = new RootAllocator();
                ArrowReader reader = oneRowReader(allocator, schema)) {
            DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                    () -> AdbcObjectsReader.readNamespaces(reader));
            Assertions.assertTrue(e.getMessage().contains("catalog_name"), e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("something_else"), e.getMessage());
        }
    }

    // ========= fixture =========

    /** One catalog row: the name the source reported, and its schema entries (null at depth CATALOGS). */
    private static Object[] catalog(String name, Object[]... schemas) {
        return new Object[] {name, schemas == null ? null : List.of(schemas)};
    }

    /** One schema entry: the name the source reported (SQLite's is the empty string) and its tables. */
    private static Object[] schema(String name, Object[]... tables) {
        return new Object[] {name, tables == null ? null : List.of(tables)};
    }

    /** One table: the name and the {@code table_type} the source reported, either of which may be absent. */
    private static Object[] table(String name, String type) {
        return new Object[] {name, type};
    }

    /**
     * Builds the getObjects response these rows describe and returns it as the reader a driver would have
     * handed over -- written to an IPC stream and read back, so the reader sees real Arrow vectors rather
     * than the fixture's own objects.
     */
    private static ArrowReader response(BufferAllocator allocator, Object[]... rows) throws Exception {
        VectorSchemaRoot root = VectorSchemaRoot.create(GET_OBJECTS_SCHEMA, allocator);
        VarCharVector catalogs = (VarCharVector) root.getVector(CATALOG_NAME);
        ListVector schemas = (ListVector) root.getVector(CATALOG_DB_SCHEMAS);
        StructVector schemaStruct = (StructVector) schemas.getDataVector();
        VarCharVector schemaNames = schemaStruct.getChild("db_schema_name", VarCharVector.class);
        ListVector tables = schemaStruct.getChild("db_schema_tables", ListVector.class);
        StructVector tableStruct = (StructVector) tables.getDataVector();
        VarCharVector tableNames = tableStruct.getChild("table_name", VarCharVector.class);
        VarCharVector tableTypes = tableStruct.getChild("table_type", VarCharVector.class);

        int schemaCount = 0;
        int tableCount = 0;
        for (int row = 0; row < rows.length; row++) {
            catalogs.setSafe(row, utf8((String) rows[row][0]));
            List<Object[]> rowSchemas = asList(rows[row][1]);
            if (rowSchemas == null) {
                schemas.setNull(row);
                continue;
            }
            schemas.startNewValue(row);
            for (Object[] entry : rowSchemas) {
                schemaNames.setSafe(schemaCount, utf8((String) entry[0]));
                schemaStruct.setIndexDefined(schemaCount);
                List<Object[]> entryTables = asList(entry[1]);
                if (entryTables == null) {
                    tables.setNull(schemaCount);
                } else {
                    tables.startNewValue(schemaCount);
                    for (Object[] entryTable : entryTables) {
                        tableNames.setSafe(tableCount, utf8((String) entryTable[0]));
                        if (entryTable[1] == null) {
                            tableTypes.setNull(tableCount);
                        } else {
                            tableTypes.setSafe(tableCount, utf8((String) entryTable[1]));
                        }
                        tableStruct.setIndexDefined(tableCount);
                        tableCount++;
                    }
                    tables.endValue(schemaCount, entryTables.size());
                }
                schemaCount++;
            }
            schemas.endValue(row, rowSchemas.size());
        }

        // Children before parents: a list's setValueCount is what gives its child its count.
        tableNames.setValueCount(tableCount);
        tableTypes.setValueCount(tableCount);
        schemaNames.setValueCount(schemaCount);
        tables.setValueCount(schemaCount);
        schemas.setValueCount(rows.length);
        catalogs.setValueCount(rows.length);
        root.setRowCount(rows.length);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(bytes))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        root.close();
        return new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator);
    }

    private static ArrowReader oneRowReader(BufferAllocator allocator, Schema schema) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(bytes))) {
            VarCharVector vector = (VarCharVector) root.getVector(0);
            vector.allocateNew(1);
            vector.setSafe(0, "x".getBytes(StandardCharsets.UTF_8));
            root.setRowCount(1);
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static List<Object[]> asList(Object value) {
        return value == null ? null : (List<Object[]>) value;
    }
}
