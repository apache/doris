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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPath;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.ddl.ConnectorPartitionField;
import org.apache.doris.connector.spi.ddl.ConnectorPartitionSpec;

import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private static IcebergConnectorMetadata metadata(RecordingIcebergCatalogOps ops,
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
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("spatial", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, Integer.toString(version));
        properties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat);
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = catalog.createTable(TableIdentifier.of("db1", "t1"),
                new Schema(Types.NestedField.optional(1, "shape", Types.GeometryType.crs84())),
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

}
