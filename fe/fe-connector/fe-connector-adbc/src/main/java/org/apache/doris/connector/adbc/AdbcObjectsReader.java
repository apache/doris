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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Reads the nested Arrow result of {@code AdbcConnection.getObjects}.
 *
 * <p>The shape is fixed by the ADBC standard schemas and was confirmed against a live driver:
 * <pre>
 * catalog_name: Utf8
 * catalog_db_schemas: List&lt;Struct&lt;
 *     db_schema_name: Utf8,
 *     db_schema_tables: List&lt;Struct&lt;table_name: Utf8, table_type: Utf8, ...&gt;&gt;&gt;&gt;
 * </pre>
 *
 * <p><b>The column layer is deliberately not read here.</b> Its fields are XDBC integer type codes
 * ({@code xdbc_data_type}, {@code xdbc_type_name}, ...), not Arrow types, so deriving Doris column types
 * from them would reintroduce exactly the two-step type translation ADBC exists to avoid -- and it would
 * diverge from the real Arrow arrays BE reads. Column types come from {@code getTableSchema} instead.
 *
 * <p>Values are pulled with {@code getObject}, which materializes the nested structs as maps. That costs an
 * allocation per row, and is the right trade here: metadata listings are small and infrequent, while hand
 * walking list offsets into child vectors is where this kind of code goes wrong silently.
 */
public final class AdbcObjectsReader {

    private static final String CATALOG_NAME = "catalog_name";
    private static final String CATALOG_DB_SCHEMAS = "catalog_db_schemas";
    private static final String DB_SCHEMA_NAME = "db_schema_name";
    private static final String DB_SCHEMA_TABLES = "db_schema_tables";
    private static final String TABLE_NAME = "table_name";
    private static final String TABLE_TYPE = "table_type";

    private AdbcObjectsReader() {
    }

    /**
     * Every {@code (catalog, db_schema)} pair the source reports, in source order and de-duplicated.
     *
     * <p>A catalog with no schema layer still yields one namespace: drivers report it as a single row whose
     * schema list is null (depth CATALOGS) or holds one entry with an empty name.
     */
    public static List<AdbcNamespace> readNamespaces(ArrowReader reader) {
        LinkedHashSet<AdbcNamespace> namespaces = new LinkedHashSet<>();
        forEachCatalogRow(reader, (catalogName, schemas) -> {
            if (schemas == null || schemas.isEmpty()) {
                namespaces.add(new AdbcNamespace(catalogName, null));
                return;
            }
            for (Object schema : schemas) {
                namespaces.add(new AdbcNamespace(catalogName, stringField(schema, DB_SCHEMA_NAME)));
            }
        });
        return new ArrayList<>(namespaces);
    }

    /**
     * Base table names inside {@code namespace}. Other namespaces in the same result are skipped, and so are
     * objects the source itself calls something other than a table, because {@code getObjects} filters are
     * advisory -- a driver may answer a narrower request with everything it has. Returning those would list
     * tables under the wrong database, or offer a view that {@code DESC} and {@code SELECT} then fail on.
     */
    public static List<String> readTableNames(ArrowReader reader, AdbcNamespace namespace) {
        List<String> tables = new ArrayList<>();
        forEachCatalogRow(reader, (catalogName, schemas) -> {
            if (schemas == null) {
                return;
            }
            for (Object schema : schemas) {
                AdbcNamespace current = new AdbcNamespace(catalogName, stringField(schema, DB_SCHEMA_NAME));
                if (!current.equals(namespace)) {
                    continue;
                }
                Object rawTables = mapOf(schema).get(DB_SCHEMA_TABLES);
                if (!(rawTables instanceof List)) {
                    continue;
                }
                for (Object table : (List<?>) rawTables) {
                    String name = stringField(table, TABLE_NAME);
                    if (name != null && !name.isEmpty() && isBaseTable(stringField(table, TABLE_TYPE))) {
                        tables.add(name);
                    }
                }
            }
        });
        return tables;
    }

    /**
     * Whether an object the source reported is a base table, judged by the {@code table_type} it came with
     * rather than by the type filter the request carried.
     *
     * <p>A Doris source is the reason this exists: its Flight SQL endpoint recognises only the literal
     * {@code "VIEW"} as a type filter and answers every other value -- including the {@code "table"} ADBC
     * asks with -- by returning ALL objects. Its {@code table_type} column is still right, so the filtering
     * is done here. Any source that honours the filter simply has nothing left to drop.
     *
     * <p>A type this does not recognise is dropped, because the request asked for base tables and an object
     * the source calls something else is not one. Keeping them would be the more forgiving rule and is the
     * wrong one: a view leaked into the listing SCANS FINE through ADBC, so nothing ever looks broken and
     * the catalog quietly offers objects it does not mean to. A source that spells its tables some third
     * way instead lists nothing at all -- noticed within a minute, and fixed by one name in this method.
     *
     * <p>A missing type is not an unrecognised one, and is kept: it says nothing about the object, and a
     * source that omits the column should stay as usable as it was before this filter existed.
     */
    static boolean isBaseTable(String tableType) {
        if (tableType == null || tableType.trim().isEmpty()) {
            return true;
        }
        String normalized = tableType.trim().toUpperCase(Locale.ROOT);
        // "BASE TABLE" is what a Doris source answers with, and it covers its materialized views too --
        // those are storage Doris can scan, unlike a view, whose rows exist only as a query.
        return normalized.equals("TABLE") || normalized.equals("BASE TABLE");
    }

    private static void forEachCatalogRow(ArrowReader reader, CatalogRowConsumer consumer) {
        try {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            FieldVector catalogVector = requireVector(root, CATALOG_NAME);
            FieldVector schemasVector = requireVector(root, CATALOG_DB_SCHEMAS);
            while (reader.loadNextBatch()) {
                for (int row = 0; row < root.getRowCount(); row++) {
                    Object schemas = schemasVector.getObject(row);
                    consumer.accept(asString(catalogVector.getObject(row)),
                            schemas instanceof List ? (List<?>) schemas : null);
                }
            }
        } catch (DorisConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to read the ADBC getObjects result: " + e.getMessage(), e);
        }
    }

    private static FieldVector requireVector(VectorSchemaRoot root, String name) {
        FieldVector vector = root.getVector(name);
        if (vector == null) {
            throw new DorisConnectorException("The ADBC driver returned a getObjects result without a '"
                    + name + "' column; it does not follow the ADBC standard schema. Columns present: "
                    + root.getSchema().getFields());
        }
        return vector;
    }

    private static Map<?, ?> mapOf(Object struct) {
        if (!(struct instanceof Map)) {
            throw new DorisConnectorException(
                    "The ADBC getObjects result has an unexpected nesting shape at " + struct);
        }
        return (Map<?, ?>) struct;
    }

    private static String stringField(Object struct, String field) {
        return asString(mapOf(struct).get(field));
    }

    /** Arrow hands back {@code Text} for utf8 columns, so the value is stringified rather than cast. */
    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    @FunctionalInterface
    private interface CatalogRowConsumer {
        void accept(String catalogName, List<?> schemas);
    }
}
