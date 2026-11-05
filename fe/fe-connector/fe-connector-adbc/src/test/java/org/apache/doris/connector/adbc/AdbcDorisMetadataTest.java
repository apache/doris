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

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class AdbcDorisMetadataTest {
    @Test
    void nonDorisSourceKeepsStandardMetadataWithDorisSqlDialect() {
        try (RecordingClient client = new RecordingClient("main", List.of())) {
            client.vendorName = "SQLite";
            client.standardMetadata = true;
            AdbcConnectorMetadata metadata = metadata(client);
            Assertions.assertEquals(List.of("main"),
                    Assertions.assertDoesNotThrow(() -> metadata.listDatabaseNames(null)));
            Assertions.assertEquals(List.of("base_table"), metadata.listTableNames(null, "main"));
            Assertions.assertTrue(client.queries.isEmpty());
            Assertions.assertEquals(1, client.vendorProbes);
        }
    }

    @Test
    void dorisSourceStaysScopedWithAnOverriddenSqlDialect() {
        try (RecordingClient client = new RecordingClient("internal", List.of(List.of("db_one")))) {
            client.vendorName = "  dOrIsFE  ";
            AdbcConnectorMetadata metadata = metadata(client, AnsiDialect.NAME);
            Assertions.assertEquals(List.of("db_one"),
                    Assertions.assertDoesNotThrow(() -> metadata.listDatabaseNames(null)));
            client.rows = List.of(List.of("base_table", "BASE TABLE"));
            Assertions.assertEquals(List.of("base_table"), metadata.listTableNames(null, "db_one"));
            Assertions.assertEquals(List.of("SELECT CURRENT_CATALOG()", "SHOW DATABASES FROM `internal`",
                    "SHOW FULL TABLES FROM `internal`.`db_one`"), client.queries);
            Assertions.assertEquals(1, client.vendorProbes);
        }
    }

    @Test
    void unavailableVendorDoesNotTurnAnSqlOverrideIntoSourceIdentification() {
        try (RecordingClient client = new RecordingClient("main", List.of())) {
            client.vendorFailure = new AdbcException("vendor not supported", null,
                    AdbcStatusCode.NOT_IMPLEMENTED, null, 0);
            client.standardMetadata = true;
            AdbcConnectorMetadata metadata = metadata(client);
            Assertions.assertEquals(List.of("main"),
                    Assertions.assertDoesNotThrow(() -> metadata.listDatabaseNames(null)));
            Assertions.assertEquals(List.of("base_table"), metadata.listTableNames(null, "main"));
            Assertions.assertTrue(client.queries.isEmpty());
            Assertions.assertEquals(AnsiDialect.NAME, new AdbcDialectSelector("").select(() -> client).name());
            Assertions.assertEquals(1, client.vendorProbes);
        }
    }

    @Test
    void metadataRetriesVendorAfterATemporaryInfoFailure() {
        try (RecordingClient client = new RecordingClient("internal", List.of(List.of("db_one")))) {
            client.standardMetadata = true;
            client.vendorFailure = new AdbcException("temporary getInfo failure", null,
                    AdbcStatusCode.IO, null, 0);
            AdbcConnectorMetadata metadata = metadata(client);
            Assertions.assertThrows(DorisConnectorException.class, () -> metadata.listDatabaseNames(null));
            // A failed identity lookup must not issue the unscoped RPC that can recurse on a loopback source.
            Assertions.assertEquals(0, client.standardMetadataReads);
            Assertions.assertEquals(1, client.vendorProbes);
            client.vendorFailure = null;
            Assertions.assertEquals(List.of("db_one"), metadata.listDatabaseNames(null));
            Assertions.assertEquals(DorisDialect.NAME, new AdbcDialectSelector("").select(() -> client).name());
            Assertions.assertEquals(2, client.vendorProbes);
            Assertions.assertEquals(0, client.standardMetadataReads);
        }
    }

    @Test
    void metadataRetriesVendorAfterATemporaryConnectionFailure() {
        try (RecordingClient client = new RecordingClient("internal", List.of(List.of("db_one")))) {
            client.standardMetadata = true;
            client.connectionFailure = new DorisConnectorException("temporary connection failure");
            AdbcConnectorMetadata metadata = metadata(client);
            Assertions.assertThrows(DorisConnectorException.class, () -> metadata.listDatabaseNames(null));
            Assertions.assertEquals(0, client.vendorProbes);
            client.connectionFailure = null;
            Assertions.assertEquals(List.of("db_one"), metadata.listDatabaseNames(null));
            Assertions.assertEquals(DorisDialect.NAME, new AdbcDialectSelector("").select(() -> client).name());
            Assertions.assertEquals(1, client.vendorProbes);
            Assertions.assertEquals(0, client.standardMetadataReads);
        }
    }

    @Test
    void automaticDialectRetriesVendorAfterATemporaryFailure() {
        try (RecordingClient client = new RecordingClient("internal", List.of(List.of("db_one")))) {
            client.vendorFailure = new AdbcException("temporary getInfo failure", null,
                    AdbcStatusCode.IO, null, 0);
            AdbcDialectSelector selector = new AdbcDialectSelector("");
            Assertions.assertThrows(DorisConnectorException.class, () -> selector.select(() -> client));
            client.vendorFailure = null;
            Assertions.assertEquals(DorisDialect.NAME, selector.select(() -> client).name());
            Assertions.assertEquals("`demo`", selector.select(() -> client).quoteIdentifier("demo"));
            Assertions.assertEquals(List.of("db_one"), metadata(client).listDatabaseNames(null));
            Assertions.assertEquals(2, client.vendorProbes);
        }
    }

    @Test
    void automaticDialectAndMetadataShareTheVendorProbe() {
        try (RecordingClient client = new RecordingClient("internal", List.of(List.of("db_one")))) {
            AdbcDialectSelector selector = new AdbcDialectSelector("");
            Assertions.assertEquals(DorisDialect.NAME, selector.select(() -> client).name());
            Assertions.assertEquals(List.of("db_one"), metadata(client).listDatabaseNames(null));
            Assertions.assertEquals(1, client.vendorProbes);
        }
    }

    @Test
    void namespacesStayInsideTheCurrentRemoteCatalog() {
        try (RecordingClient client = new RecordingClient("warehouse`one", List.of(
                List.of("db_one"), List.of("db_two")))) {
            Assertions.assertEquals(List.of("db_one", "db_two"), metadata(client).listDatabaseNames(null));
            Assertions.assertEquals(List.of("SELECT CURRENT_CATALOG()", "SHOW DATABASES FROM `warehouse``one`"),
                    client.queries);
            Assertions.assertEquals(2, client.closedStatements);
        }
    }

    @Test
    void namespacesRemainAvailableWithoutSessionOptions() {
        try (RecordingClient client = new RecordingClient("warehouse`one", List.of(List.of("db_one")))) {
            // The native Flight driver reports NOT_FOUND when an older server lacks GetSessionOptions.
            client.currentCatalogFailure = new AdbcException("current catalog not supported", null,
                    AdbcStatusCode.NOT_FOUND, null, 0);
            AdbcConnectorMetadata metadata = metadata(client);
            Assertions.assertEquals(List.of("db_one"),
                    Assertions.assertDoesNotThrow(() -> metadata.listDatabaseNames(null)));
            client.rows = List.of(List.of("base_table", "BASE TABLE"));
            Assertions.assertEquals(List.of("base_table"), metadata.listTableNames(null, "db_one"));
            Assertions.assertEquals(List.of("SELECT CURRENT_CATALOG()", "SHOW DATABASES FROM `warehouse``one`",
                    "SHOW FULL TABLES FROM `warehouse``one`.`db_one`"), client.queries);
            Assertions.assertEquals(3, client.closedStatements);
        }
    }

    @Test
    void missingCurrentCatalogStopsBeforeDiscovery() {
        try (RecordingClient client = new RecordingClient("", List.of(List.of("db_one")))) {
            Assertions.assertThrows(DorisConnectorException.class, () -> metadata(client).listDatabaseNames(null));
            Assertions.assertEquals(List.of("SELECT CURRENT_CATALOG()"), client.queries);
            Assertions.assertEquals(1, client.closedStatements);
        }
    }

    @Test
    void tablesAreScopedAndViewsAreExcluded() {
        try (RecordingClient client = new RecordingClient("internal", List.of(List.of("db`one")))) {
            AdbcConnectorMetadata metadata = metadata(client);
            Assertions.assertEquals(List.of("db`one"), metadata.listDatabaseNames(null));
            client.rows = List.of(List.of("base_table", "BASE TABLE"), List.of("view_table", "VIEW"),
                    List.of("system_table", "SYSTEM VIEW"));
            Assertions.assertEquals(List.of("base_table"), metadata.listTableNames(null, "db`one"));
            Assertions.assertEquals("SHOW FULL TABLES FROM `internal`.`db``one`", client.queries.get(2));
            AdbcTableHandle handle = (AdbcTableHandle) metadata.getTableHandle(null, "db`one", "base_table")
                    .orElseThrow();
            Assertions.assertEquals("internal", handle.getRemoteCatalog());
            Assertions.assertEquals("db`one", handle.getRemoteDbSchema());
            Assertions.assertEquals(3, client.closedStatements);
        }
    }

    @Test
    void freshListingSeesNewDatabases() {
        try (RecordingClient client = new RecordingClient("internal", List.of(List.of("first_db")))) {
            AdbcConnectorMetadata metadata = metadata(client);
            Assertions.assertEquals(List.of("first_db"), metadata.listDatabaseNames(null));
            client.rows = List.of(List.of("first_db"), List.of("new_db"));
            Assertions.assertEquals(List.of("first_db", "new_db"), metadata.listDatabaseNames(null));
        }
    }

    @Test
    void anEmptyCatalogListingDoesNotInventADatabase() {
        try (RecordingClient client = new RecordingClient("internal", List.of())) {
            Assertions.assertEquals(List.of(), metadata(client).listDatabaseNames(null));
        }
    }

    private static AdbcConnectorMetadata metadata(RecordingClient client) {
        return metadata(client, DorisDialect.NAME);
    }

    private static AdbcConnectorMetadata metadata(RecordingClient client, String configuredDialect) {
        AdbcDialectSelector selector = new AdbcDialectSelector(configuredDialect);
        return new AdbcConnectorMetadata(client, new AdbcSchemaStrategy(), () -> selector.select(() -> client),
                new AdbcMetadataCache(Map.of()));
    }

    private static final class RecordingClient extends AdbcClient {
        private final RootAllocator allocator = new RootAllocator();
        private final List<String> queries = new ArrayList<>();
        private final String currentCatalog;
        private AdbcException currentCatalogFailure;
        private List<List<String>> rows;
        private int closedStatements;
        private String vendorName = "DorisFE";
        private AdbcException vendorFailure;
        private DorisConnectorException connectionFailure;
        private boolean standardMetadata;
        private int vendorProbes;
        private int standardMetadataReads;

        private RecordingClient(String currentCatalog, List<List<String>> rows) {
            super(Path.of("unused.so"), "unused.so", null, "grpc://localhost:1", null, null, Map.of());
            this.currentCatalog = currentCatalog;
            this.rows = rows;
        }

        @Override
        public <T> T withConnection(AdbcConnectionCall<T> body) {
            // Connection acquisition may fail before the optional getInfo call even starts.
            if (connectionFailure != null) {
                throw connectionFailure;
            }
            try {
                return body.apply(new AdbcConnection() {
                    @Override
                    public String getCurrentCatalog() throws AdbcException {
                        if (currentCatalogFailure != null) {
                            throw currentCatalogFailure;
                        }
                        return currentCatalog;
                    }

                    @Override
                    public ArrowReader getObjects(GetObjectsDepth depth, String catalogPattern,
                            String dbSchemaPattern, String tableNamePattern, String[] tableTypes,
                            String columnNamePattern) throws AdbcException {
                        standardMetadataReads++;
                        // An unscoped Flight metadata request reenters the catalog being loaded on a loopback source.
                        if (!standardMetadata) {
                            throw new AssertionError("Unscoped GetObjects would recursively load this ADBC catalog");
                        }
                        try {
                            return AdbcObjectsReaderTest.response(allocator, AdbcObjectsReaderTest.catalog("main",
                                    AdbcObjectsReaderTest.schema("", new Object[] {"base_table", "TABLE"},
                                            new Object[] {"view_table", "VIEW"})));
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }

                    @Override
                    public ArrowReader getInfo(int[] infoCodes) throws AdbcException {
                        vendorProbes++;
                        if (vendorFailure != null) {
                            throw vendorFailure;
                        }
                        try {
                            return vendorResponse();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }

                    @Override
                    public AdbcStatement createStatement() {
                        return new AdbcStatement() {
                            @Override
                            public void setSqlQuery(String query) {
                                queries.add(query);
                            }

                            @Override
                            public QueryResult executeQuery() {
                                try {
                                    return new QueryResult(-1, response());
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            }

                            @Override
                            public UpdateResult executeUpdate() {
                                throw new AssertionError("Metadata discovery must not write");
                            }

                            @Override
                            public void prepare() {
                                throw new AssertionError("Metadata discovery needs no prepared statement");
                            }

                            @Override
                            public void close() {
                                closedStatements++;
                            }
                        };
                    }

                    @Override
                    public void close() {
                    }
                });
            } catch (AdbcException e) {
                throw AdbcClient.translate(e, "ADBC operation failed");
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        private ArrowReader vendorResponse() throws IOException {
            Schema schema = new Schema(List.of(Field.nullable("info_name", new ArrowType.Int(32, false)),
                    new Field("info_value", FieldType.nullable(new ArrowType.Union(UnionMode.Dense, new int[] {0})),
                            List.of(Field.nullable("string_value", ArrowType.Utf8.INSTANCE)))));
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                    ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(bytes))) {
                root.allocateNew();
                ((UInt4Vector) root.getVector("info_name")).setSafe(0, AdbcInfoCode.VENDOR_NAME.getValue());
                DenseUnionVector values = (DenseUnionVector) root.getVector("info_value");
                values.setTypeId(0, (byte) 0);
                values.setOffset(0, 0);
                ((VarCharVector) values.getVectorByType((byte) 0)).setSafe(0,
                        vendorName.getBytes(StandardCharsets.UTF_8));
                root.setRowCount(1);
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator);
        }

        private ArrowReader response() throws IOException {
            String query = queries.get(queries.size() - 1);
            int columns = query.startsWith("SHOW FULL TABLES") ? 2 : 1;
            List<List<String>> responseRows = query.equals("SELECT CURRENT_CATALOG()")
                    ? List.of(List.of(currentCatalog)) : rows;
            List<Field> fields = new ArrayList<>();
            for (int column = 0; column < columns; column++) {
                fields.add(new Field("column_" + column, FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
            }
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator);
                    ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(bytes))) {
                writer.start();
                // One row per batch ensures discovery consumes the whole stream.
                for (List<String> row : responseRows) {
                    root.allocateNew();
                    for (int column = 0; column < columns; column++) {
                        ((VarCharVector) root.getVector(column)).setSafe(0,
                                row.get(column).getBytes(StandardCharsets.UTF_8));
                    }
                    root.setRowCount(1);
                    writer.writeBatch();
                }
                writer.end();
            }
            return new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator);
        }

        @Override
        public void close() {
            allocator.close();
        }
    }
}
