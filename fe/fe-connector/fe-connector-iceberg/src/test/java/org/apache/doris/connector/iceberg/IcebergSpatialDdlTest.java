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
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.ddl.ConnectorPartitionField;
import org.apache.doris.connector.spi.ddl.ConnectorPartitionSpec;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

/** Exercises spatial guards through the real connector DDL entry points before remote mutations. */
public class IcebergSpatialDdlTest {
    private static final IcebergTableHandle HANDLE = new IcebergTableHandle("db1", "t1");
    private static final ConnectorType GEOMETRY = ConnectorType.of("GEOMETRY")
            .withSpatialMetadata("EPSG:3857", null);
    private static final ConnectorType GEOGRAPHY = ConnectorType.of("GEOGRAPHY")
            .withSpatialMetadata("OGC:CRS84", "vincenty");

    private static ConnectorColumn column(ConnectorType type) {
        return new ConnectorColumn("shape", type, "", true, null, false);
    }

    private static IcebergConnectorMetadata metadata(IcebergCatalogOps ops,
            Map<String, String> catalogOverrides) {
        Map<String, String> properties = new HashMap<>(catalogOverrides);
        properties.put(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, IcebergCatalogProperties.TYPE_REST);
        return new IcebergConnectorMetadata(ops, IcebergCatalogProperties.of(properties),
                new RecordingConnectorContext());
    }

    private static ConnectorCreateTableRequest request(ConnectorType type, Map<String, String> properties) {
        return ConnectorCreateTableRequest.builder().dbName("db1").tableName("t1")
                .columns(Collections.singletonList(column(type))).properties(properties).build();
    }

    private static RecordingIcebergCatalogOps existingTable(int version, String fileFormat) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("spatial", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, Integer.toString(version));
        properties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat);
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = catalog.createTable(TableIdentifier.of("db1", "t1"),
                new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())),
                org.apache.iceberg.PartitionSpec.unpartitioned(), properties);
        return ops;
    }

    private static RecordingIcebergCatalogOps existingSpatialTable(int version, String fileFormat) {
        return existingSpatialTable(version, fileFormat, Types.GeometryType.crs84());
    }

    private static RecordingIcebergCatalogOps existingSpatialTable(int version, String fileFormat, Type type) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("spatial", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, Integer.toString(version));
        properties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat);
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = catalog.createTable(TableIdentifier.of("db1", "t1"),
                new Schema(Types.NestedField.optional(1, "shape", type)),
                org.apache.iceberg.PartitionSpec.unpartitioned(), properties);
        return ops;
    }

    private static List<ConnectorType> nestedTypes() {
        return Arrays.asList(ConnectorType.arrayOf(GEOMETRY),
                ConnectorType.mapOf(ConnectorType.of("INT"), GEOGRAPHY),
                ConnectorType.structOf(Collections.singletonList("g"), Collections.singletonList(GEOMETRY)));
    }

    @Test
    public void testCreatePreservesSpatialMetadata() {
        for (ConnectorType type : Arrays.asList(GEOMETRY, GEOGRAPHY)) {
            RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
            metadata(ops, Collections.emptyMap()).createTable(null,
                    request(type, Collections.singletonMap(TableProperties.FORMAT_VERSION, "3")));
            Type nativeType = ops.lastCreateSchema.findType("shape");
            Assertions.assertEquals(type.getTypeName().equals("GEOMETRY")
                    ? Types.GeometryType.of("EPSG:3857")
                    : Types.GeographyType.of("OGC:CRS84", EdgeAlgorithm.VINCENTY), nativeType);
        }
    }

    @Test
    public void testCreateRejectsSpatialIdentityPartitionsBeforeRemoteMutation() {
        for (ConnectorType type : Arrays.asList(GEOMETRY, GEOGRAPHY)) {
            RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
            ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                    .dbName("db1").tableName("t1").columns(Collections.singletonList(column(type)))
                    .properties(Map.of(TableProperties.FORMAT_VERSION, "3"))
                    .partitionSpec(identityPartition("shape")).build();
            ValidationException error = Assertions.assertThrows(ValidationException.class,
                    () -> metadata(ops, Collections.emptyMap()).createTable(null, request));
            Assertions.assertTrue(error.getMessage().contains("for transform: identity"),
                    error.getMessage());
            Assertions.assertTrue(ops.log.isEmpty());
        }
    }

    @Test
    public void testCreateAllowsOrdinaryPartitionKeyAndSpatialMapValue() {
        for (ConnectorType type : Arrays.asList(GEOMETRY, GEOGRAPHY)) {
            RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
            ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                    .dbName("db1").tableName("t1")
                    .columns(Arrays.asList(new ConnectorColumn("id", ConnectorType.of("INT"), "", true, null),
                            column(ConnectorType.mapOf(ConnectorType.of("STRING"), type))))
                    .properties(Map.of(TableProperties.FORMAT_VERSION, "3"))
                    .partitionSpec(identityPartition("id")).build();
            metadata(ops, Collections.emptyMap()).createTable(null, request);
            Assertions.assertEquals(ops.lastCreateSchema.findField("id").fieldId(),
                    ops.lastCreateSpec.fields().get(0).sourceId());
            Assertions.assertTrue(IcebergSpatialSchemaValidator.containsSpatial(ops.lastCreateSchema));
        }
    }

    private static ConnectorPartitionSpec identityPartition(String source) {
        return new ConnectorPartitionSpec(ConnectorPartitionSpec.Style.IDENTITY,
                Collections.singletonList(new ConnectorPartitionField(source, "identity", Collections.emptyList())));
    }

    @Test
    public void testCreateRejectsV2AndNonParquetBeforeRemote() {
        for (Map<String, String> properties : Arrays.asList(Collections.<String, String>emptyMap(),
                Map.of(TableProperties.FORMAT_VERSION, "3", TableProperties.DEFAULT_FILE_FORMAT, "orc"),
                Map.of(TableProperties.FORMAT_VERSION, "3", TableProperties.DEFAULT_FILE_FORMAT, "parquet",
                        "write-format", "orc"))) {
            RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata(ops, Collections.emptyMap()).createTable(null, request(GEOMETRY, properties)));
            Assertions.assertTrue(ops.log.isEmpty());
        }
    }

    @Test
    public void testCreateRespectsCatalogPropertyPrecedence() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        metadata(ops, Map.of("table-default.format-version", "3",
                "table-default.write.format.default", "orc",
                "table-override.write.format.default", "parquet"))
                .createTable(null, request(GEOMETRY, Collections.emptyMap()));
        Assertions.assertNotNull(ops.lastCreateSchema);
        RecordingIcebergCatalogOps rejected = new RecordingIcebergCatalogOps();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(rejected, Map.of("table-override.format-version", "2"))
                        .createTable(null, request(GEOMETRY, Map.of(TableProperties.FORMAT_VERSION, "3"))));
        Assertions.assertTrue(rejected.log.isEmpty());
        RecordingIcebergCatalogOps requestWins = new RecordingIcebergCatalogOps();
        metadata(requestWins, Map.of("table-default.format-version", "2",
                "table-default.write.format.default", "orc")).createTable(null,
                request(GEOMETRY, Map.of(TableProperties.FORMAT_VERSION, "3",
                        TableProperties.DEFAULT_FILE_FORMAT, "PARQUET")));
        Assertions.assertNotNull(requestWins.lastCreateSchema);
    }

    @Test
    public void testCreateNestedSpatialRequiresV3Parquet() {
        for (ConnectorType type : nestedTypes()) {
            RecordingIcebergCatalogOps valid = new RecordingIcebergCatalogOps();
            metadata(valid, Collections.emptyMap()).createTable(null,
                    request(type, Map.of(TableProperties.FORMAT_VERSION, "3")));
            Assertions.assertNotNull(valid.lastCreateSchema);
            for (Map<String, String> properties : Arrays.asList(
                    Map.of(TableProperties.FORMAT_VERSION, "2"),
                    Map.of(TableProperties.FORMAT_VERSION, "3", TableProperties.DEFAULT_FILE_FORMAT, "orc"))) {
                RecordingIcebergCatalogOps rejected = new RecordingIcebergCatalogOps();
                Assertions.assertThrows(DorisConnectorException.class,
                        () -> metadata(rejected, Collections.emptyMap()).createTable(null, request(type, properties)));
                Assertions.assertTrue(rejected.log.isEmpty());
            }
        }
    }

    @Test
    public void testAddReadsActualVersionAndFormat() {
        RecordingIcebergCatalogOps ops = existingTable(3, "parquet");
        // Iceberg removes format-version from properties; validation must read BaseTable metadata.
        Assertions.assertFalse(ops.table.properties().containsKey(TableProperties.FORMAT_VERSION));
        metadata(ops, Map.of("table-override.format-version", "2"))
                .addColumn(null, HANDLE, column(GEOMETRY), null);
        Assertions.assertNotNull(ops.lastAddColumn);
        RecordingIcebergCatalogOps bulk = existingTable(3, "PARQUET");
        metadata(bulk, Collections.emptyMap()).addColumns(null, HANDLE,
                Collections.singletonList(column(GEOGRAPHY)));
        Assertions.assertNotNull(bulk.lastAddColumns);
    }

    @Test
    public void testAddAndModifyRejectIncompatibleExistingTable() {
        RecordingIcebergCatalogOps alias = existingTable(3, "parquet");
        alias.table.updateProperties().set("write-format", "orc").commit();
        for (RecordingIcebergCatalogOps ops : Arrays.asList(existingTable(2, "parquet"),
                existingTable(3, "orc"), alias)) {
            IcebergConnectorMetadata md = metadata(ops, Map.of("table-override.format-version", "3"));
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> md.addColumn(null, HANDLE, column(GEOMETRY), null));
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> md.addColumns(null, HANDLE, Collections.singletonList(column(GEOGRAPHY))));
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> md.modifyColumn(null, HANDLE, column(GEOMETRY), null));
            Assertions.assertTrue(ops.log.stream().noneMatch(op -> op.startsWith("add") || op.startsWith("modify")));
        }
    }

    @Test
    public void testNestedAndFlatChangesValidateActualTableCapabilities() {
        for (ConnectorType type : nestedTypes()) {
            RecordingIcebergCatalogOps valid = existingTable(3, "parquet");
            metadata(valid, Collections.emptyMap()).addColumn(null, HANDLE, column(type), null);
            Assertions.assertNotNull(valid.lastAddColumn);
            for (RecordingIcebergCatalogOps invalid : Arrays.asList(existingTable(2, "parquet"),
                    existingTable(3, "orc"))) {
                IcebergConnectorMetadata md = metadata(invalid, Collections.emptyMap());
                ConnectorColumnPath path = ConnectorColumnPath.of(Arrays.asList("parent", "shape"));
                Assertions.assertThrows(DorisConnectorException.class,
                        () -> md.addColumn(null, HANDLE, column(type), null));
                Assertions.assertThrows(DorisConnectorException.class,
                        () -> md.addNestedColumn(null, HANDLE, path, column(type), null));
                Assertions.assertThrows(DorisConnectorException.class,
                        () -> md.modifyNestedColumn(null, HANDLE, path, column(type), null));
                Assertions.assertTrue(invalid.log.stream().noneMatch(op -> op.startsWith("add")
                        || op.startsWith("modify")));
            }
        }
    }

    @Test
    public void testSetPropertiesRejectsOrcForExistingSpatialTable() {
        RecordingIcebergCatalogOps rejected = existingSpatialTable(3, "parquet");
        IcebergConnectorMetadata md = metadata(rejected, Collections.emptyMap());
        Assertions.assertThrows(DorisConnectorException.class,
                () -> md.setTableProperties(null, HANDLE,
                        Collections.singletonMap(TableProperties.DEFAULT_FILE_FORMAT, "orc")));
        Assertions.assertTrue(rejected.log.stream().anyMatch(op -> op.startsWith("loadTable:")));
        Assertions.assertTrue(rejected.log.stream().noneMatch(op -> op.startsWith("setTableProperties:")));

        RecordingIcebergCatalogOps accepted = existingSpatialTable(3, "parquet");
        metadata(accepted, Collections.emptyMap()).setTableProperties(null, HANDLE,
                Collections.singletonMap(TableProperties.DEFAULT_FILE_FORMAT, "parquet"));
        Assertions.assertTrue(accepted.log.stream().anyMatch(op -> op.startsWith("setTableProperties:")));
    }

    @Test
    public void testSpatialPropertyPreflightCleansOnSuccess() {
        for (Type type : Arrays.asList(Types.GeometryType.crs84(), Types.GeographyType.crs84())) {
            AtomicInteger cleanup = new AtomicInteger();
            CatalogBackedIcebergCatalogOps ops = cleanupOps(
                    new Schema(Types.NestedField.optional(1, "shape", type)), 3, Collections.emptyMap(), cleanup);
            metadata(ops, Collections.emptyMap()).setTableProperties(null, HANDLE,
                    Map.of(TableProperties.DEFAULT_FILE_FORMAT, "parquet"));
            Assertions.assertEquals(2, cleanup.get(), "preflight and property mutation each own a table load");
        }
    }

    @Test
    public void testSpatialPropertyPreflightCleansOnReject() {
        for (Type type : Arrays.asList(Types.GeometryType.crs84(), Types.GeographyType.crs84())) {
            AtomicInteger cleanup = new AtomicInteger();
            CatalogBackedIcebergCatalogOps ops = cleanupOps(
                    new Schema(Types.NestedField.optional(1, "shape", type)), 3, Collections.emptyMap(), cleanup);
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata(ops, Collections.emptyMap()).setTableProperties(null, HANDLE,
                            Map.of(TableProperties.DEFAULT_FILE_FORMAT, "orc")));
            Assertions.assertTrue(error.getMessage().contains("require Parquet"), error.getMessage());
            Assertions.assertEquals(1, cleanup.get(), "rejected preflight must release its table without mutation");
            ops.withTable("db1", "t1", table -> {
                Assertions.assertEquals("parquet", table.properties().get(TableProperties.DEFAULT_FILE_FORMAT));
                return null;
            });
        }
    }

    @Test
    public void testOrdinaryFormatPropertyPreflightCleansOnSuccess() {
        AtomicInteger cleanup = new AtomicInteger();
        CatalogBackedIcebergCatalogOps ops = cleanupOps(
                new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())),
                2, Collections.emptyMap(), cleanup);
        metadata(ops, Collections.emptyMap()).setTableProperties(null, HANDLE,
                Map.of(TableProperties.DEFAULT_FILE_FORMAT, "orc"));
        Assertions.assertEquals(2, cleanup.get(), "ordinary-table preflight also owns its load");
        ops.withTable("db1", "t1", table -> {
            Assertions.assertEquals("orc", table.properties().get(TableProperties.DEFAULT_FILE_FORMAT));
            return null;
        });
    }

    @Test
    public void testSpatialColumnPreflightCleansOnSuccessAndReject() {
        for (int version : Arrays.asList(2, 3)) {
            for (ConnectorType type : Arrays.asList(GEOMETRY, GEOGRAPHY)) {
                AtomicInteger cleanup = new AtomicInteger();
                CatalogBackedIcebergCatalogOps ops = cleanupOps(
                        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())),
                        version, Collections.emptyMap(), cleanup);
                IcebergConnectorMetadata md = metadata(ops, Collections.emptyMap());
                if (version == 3) {
                    md.addColumn(null, HANDLE, column(type), null);
                    Assertions.assertEquals(2, cleanup.get(), "preflight and schema mutation each own a load");
                } else {
                    Assertions.assertThrows(DorisConnectorException.class,
                            () -> md.addColumn(null, HANDLE, column(type), null));
                    Assertions.assertEquals(1, cleanup.get(), "rejected version preflight must release its table");
                }
                ops.withTable("db1", "t1", table -> {
                    Assertions.assertEquals(version == 3, table.schema().findField("shape") != null);
                    return null;
                });
            }
        }
    }

    @Test
    public void testSpatialColumnPreflightCleansOnValidatorException() {
        AtomicInteger cleanup = new AtomicInteger();
        CatalogBackedIcebergCatalogOps ops = cleanupOps(
                new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())), 3,
                Map.of("write-format", "invalid-spatial-format"), cleanup);
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, Collections.emptyMap()).addColumn(null, HANDLE, column(GEOMETRY), null));
        Assertions.assertTrue(error.getMessage().contains("Unsupported input format type: invalid-spatial-format"),
                error.getMessage());
        Assertions.assertEquals(1, cleanup.get(), "file-format decoding failure must release the preflight table");
        ops.withTable("db1", "t1", table -> {
            Assertions.assertNull(table.schema().findField("shape"));
            return null;
        });
    }

    @ParameterizedTest(name = "{0}, barrier={1}, mutation={2}")
    @MethodSource("spatialDdlRaces")
    public void testSpatialDdlRejectsObservedIncompatibleMetadata(
            ConnectorType type, String barrier, String mutation) throws Exception {
        boolean format = mutation.startsWith("format");
        Type spatial = IcebergSchemaBuilder.buildColumnType(type);
        Consumer<Table> concurrent = format
                ? table -> table.updateSchema().addColumn("shape", spatial).commit()
                : table -> table.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, "orc").commit();
        try (DdlRace race = new DdlRace(barrier, concurrent)) {
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> applySpatialDdl(metadata(race.ops, Collections.emptyMap()), type, mutation));
            Assertions.assertTrue(error.getMessage().contains("require Parquet"), error.getMessage());
            Assertions.assertTrue(race.changed.get(), "the concurrent metadata change must execute");
            Assertions.assertEquals(2, race.cleanup.get(), "both operation-owned loads must be released");
            Table current = race.catalog.loadTable(DdlRace.ID);
            Assertions.assertEquals(format, IcebergSpatialSchemaValidator.containsSpatial(current.schema()));
            Assertions.assertEquals(format ? org.apache.iceberg.FileFormat.PARQUET
                    : org.apache.iceberg.FileFormat.ORC, IcebergWriterHelper.getFileFormat(current));
        }
    }

    @ParameterizedTest(name = "{0}, barrier={1}, mutation={2}")
    @MethodSource("spatialDdlRaces")
    public void testSpatialDdlKeepsCompatibleConcurrentChanges(
            ConnectorType type, String barrier, String mutation) throws Exception {
        try (DdlRace race = new DdlRace(barrier,
                table -> table.updateSchema().addColumn("neighbor", Types.IntegerType.get()).commit())) {
            applySpatialDdl(metadata(race.ops, Collections.emptyMap()), type, mutation);
            Assertions.assertTrue(race.changed.get());
            Assertions.assertEquals(2, race.cleanup.get());
            Table current = race.catalog.loadTable(DdlRace.ID);
            Assertions.assertNotNull(current.schema().findField("neighbor"));
            Assertions.assertEquals(!mutation.startsWith("format"),
                    IcebergSpatialSchemaValidator.containsSpatial(current.schema()));
            Assertions.assertEquals(mutation.startsWith("format") ? org.apache.iceberg.FileFormat.ORC
                    : org.apache.iceberg.FileFormat.PARQUET, IcebergWriterHelper.getFileFormat(current));
        }
    }

    private static Stream<Arguments> spatialDdlRaces() {
        Stream.Builder<Arguments> cases = Stream.builder();
        for (ConnectorType type : Arrays.asList(GEOMETRY, GEOGRAPHY)) {
            for (String barrier : Arrays.asList("load", "commit")) {
                for (String mutation : Arrays.asList("add", "add_many", "add_nested", "modify",
                        "modify_nested", "format", "format_alias")) {
                    // SchemaUpdate aborts a CAS conflict; PropertiesUpdate alone retries it.
                    if (barrier.equals("load") || mutation.startsWith("format")) {
                        cases.add(Arguments.of(type, barrier, mutation));
                    }
                }
            }
        }
        return cases.build();
    }

    private static void applySpatialDdl(IcebergConnectorMetadata md, ConnectorType type, String mutation) {
        switch (mutation) {
            case "add":
                md.addColumn(null, HANDLE, column(type), null);
                break;
            case "add_many":
                md.addColumns(null, HANDLE, Collections.singletonList(
                        column(ConnectorType.mapOf(ConnectorType.of("STRING"), type))));
                break;
            case "add_nested":
                md.addNestedColumn(null, HANDLE, ConnectorColumnPath.of(Arrays.asList("payload", "shape")),
                        column(ConnectorType.arrayOf(type)), null);
                break;
            case "modify":
                ConnectorType inner = ConnectorType.structOf(Collections.singletonList("label"),
                        Collections.singletonList(ConnectorType.of("STRING")));
                md.modifyColumn(null, HANDLE, new ConnectorColumn("payload",
                        ConnectorType.structOf(Arrays.asList("label", "inner", "shape"),
                                Arrays.asList(ConnectorType.of("STRING"), inner, type)), "", true, null, false), null);
                break;
            case "modify_nested":
                md.modifyNestedColumn(null, HANDLE, ConnectorColumnPath.of(Arrays.asList("payload", "inner")),
                        new ConnectorColumn("inner", ConnectorType.structOf(Arrays.asList("label", "shape"),
                                Arrays.asList(ConnectorType.of("STRING"), type)), "", true, null, false), null);
                break;
            case "format":
            case "format_alias":
                md.setTableProperties(null, HANDLE,
                        Map.of(mutation.equals("format") ? TableProperties.DEFAULT_FILE_FORMAT : "write-format", "orc"));
                break;
            default:
                throw new IllegalArgumentException(mutation);
        }
    }

    /** Real SDK mutators with a deterministic change before the second load or first metadata CAS. */
    private static final class DdlRace implements AutoCloseable {
        private static final TableIdentifier ID = TableIdentifier.of("db1", "t1");
        private final InMemoryCatalog catalog = new InMemoryCatalog();
        private final AtomicBoolean changed = new AtomicBoolean();
        private final AtomicInteger cleanup = new AtomicInteger();
        private final CatalogBackedIcebergCatalogOps ops;

        private DdlRace(String barrier, Consumer<Table> concurrent) {
            catalog.initialize("spatial-ddl-race", Collections.emptyMap());
            catalog.createNamespace(Namespace.of("db1"));
            Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "payload", Types.StructType.of(
                            Types.NestedField.optional(3, "label", Types.StringType.get()),
                            Types.NestedField.optional(4, "inner", Types.StructType.of(
                                    Types.NestedField.optional(5, "label", Types.StringType.get()))))));
            catalog.createTable(ID, schema, org.apache.iceberg.PartitionSpec.unpartitioned(),
                    Map.of(TableProperties.FORMAT_VERSION, "3", TableProperties.DEFAULT_FILE_FORMAT, "parquet"));
            ops = new CatalogBackedIcebergCatalogOps(catalog, false, false, true, Optional.empty(),
                    table -> cleanup::incrementAndGet) {
                private final AtomicInteger loads = new AtomicInteger();

                @Override
                public Table loadTable(String dbName, String tableName) {
                    int load = loads.incrementAndGet();
                    if (load == 2 && barrier.equals("load")) {
                        changed.set(true);
                        concurrent.accept(catalog.loadTable(ID));
                    }
                    Table table = super.loadTable(dbName, tableName);
                    if (load != 2 || !barrier.equals("commit")) {
                        return table;
                    }
                    TableOperations delegate = ((HasTableOperations) table).operations();
                    TableOperations racing = (TableOperations) Proxy.newProxyInstance(
                            TableOperations.class.getClassLoader(), new Class<?>[] {TableOperations.class},
                            (proxy, method, args) -> {
                                if (method.getName().equals("commit") && changed.compareAndSet(false, true)) {
                                    concurrent.accept(catalog.loadTable(ID));
                                    throw new CommitFailedException("Concurrent metadata change in spatial DDL test");
                                }
                                try {
                                    return method.invoke(delegate, args);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                            });
                    BaseTable base = (BaseTable) table;
                    return new BaseTable(racing, base.name(), base.reporter(), base.labels());
                }
            };
        }

        @Override
        public void close() throws Exception {
            catalog.close();
        }
    }

    private static CatalogBackedIcebergCatalogOps cleanupOps(
            Schema schema, int version, Map<String, String> overrides, AtomicInteger cleanup) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("spatial-cleanup", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Map<String, String> properties = new HashMap<>(overrides);
        properties.put(TableProperties.FORMAT_VERSION, Integer.toString(version));
        properties.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
        catalog.createTable(TableIdentifier.of("db1", "t1"), schema,
                org.apache.iceberg.PartitionSpec.unpartitioned(), properties);
        return new CatalogBackedIcebergCatalogOps(catalog, false, false, true, Optional.empty(),
                table -> cleanup::incrementAndGet);
    }

    @Test
    public void testSetPropertiesKeepsExistingWriteFormatPrecedence() {
        for (Type type : Arrays.asList(Types.GeometryType.crs84(), Types.GeographyType.crs84())) {
            RecordingIcebergCatalogOps rejected = existingSpatialTable(3, "parquet", type);
            rejected.table.updateProperties().set("write-format", "orc").commit();
            Map<String, String> original = new HashMap<>(rejected.table.properties());
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata(rejected, Collections.emptyMap()).setTableProperties(null, HANDLE,
                            Map.of(TableProperties.DEFAULT_FILE_FORMAT, "parquet")));
            Assertions.assertTrue(error.getMessage().contains("require Parquet"), error.getMessage());
            Assertions.assertNull(rejected.lastSetProperties);
            Assertions.assertEquals(original, rejected.table.properties());

            Map<String, String> repair = Map.of("write-format", "parquet");
            metadata(rejected, Collections.emptyMap()).setTableProperties(null, HANDLE, repair);
            Assertions.assertEquals(repair, rejected.lastSetProperties);
        }
    }

    @Test
    public void testSetPropertiesAllowsExistingParquetWriteFormat() {
        for (Type type : Arrays.asList(Types.GeometryType.crs84(), Types.GeographyType.crs84())) {
            RecordingIcebergCatalogOps accepted = existingSpatialTable(3, "orc", type);
            accepted.table.updateProperties().set("write-format", "parquet").commit();
            Map<String, String> update = Map.of(TableProperties.DEFAULT_FILE_FORMAT, "orc");
            metadata(accepted, Collections.emptyMap()).setTableProperties(null, HANDLE, update);
            Assertions.assertEquals(update, accepted.lastSetProperties);
            Assertions.assertEquals(org.apache.iceberg.FileFormat.PARQUET,
                    IcebergWriterHelper.getFileFormat(accepted.table));
        }
    }

}
