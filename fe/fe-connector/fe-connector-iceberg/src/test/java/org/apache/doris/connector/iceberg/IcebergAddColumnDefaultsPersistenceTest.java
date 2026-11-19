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
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPath;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/** Verifies defaults survive real metadata serialization and reloading, including failed ALTERs. */
public class IcebergAddColumnDefaultsPersistenceTest {
    private static final IcebergTableHandle HANDLE = new IcebergTableHandle("db", "t");
    private static final TableIdentifier TABLE = TableIdentifier.of("db", "t");
    @TempDir
    Path tempDir;
    private HadoopCatalog catalog;
    private IcebergConnectorMetadata metadata;

    @BeforeEach
    public void setUp() {
        catalog = new HadoopCatalog(new Configuration(false), tempDir.toString());
        catalog.createNamespace(Namespace.of("db"));
        catalog.createTable(TABLE, new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "s", Types.StructType.of(
                        Types.NestedField.optional(3, "a", Types.IntegerType.get())))),
                PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "3"));
        metadata = new IcebergConnectorMetadata(new CatalogBackedIcebergCatalogOps(catalog),
                IcebergCatalogProperties.of(Collections.singletonMap(
                        IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, IcebergCatalogProperties.TYPE_HADOOP)),
                new RecordingConnectorContext());
    }

    @AfterEach
    public void tearDown() throws Exception {
        catalog.close();
    }

    private Schema reload() {
        return catalog.loadTable(TABLE).schema();
    }

    private static ConnectorColumn column(String name, ConnectorType type, String value) {
        return new ConnectorColumn(name, type, "", false, value, false);
    }

    private static ConnectorColumnPath nested(String name) {
        return ConnectorColumnPath.of(Arrays.asList("s", name));
    }

    private void assertDefaults(String name, Object expected) {
        Schema schema = SchemaParser.fromJson(SchemaParser.toJson(reload()));
        Types.NestedField field = schema.findField(name);
        Assertions.assertTrue(field.isRequired());
        Assertions.assertEquals(expected, field.initialDefault());
        Assertions.assertEquals(expected, field.writeDefault());
    }

    @Test
    public void decimalDefaultsUseDeclaredScaleOnAllAddPaths() {
        ConnectorType type = ConnectorType.of("DECIMALV3", 10, 2);
        String[] inputs = {"0", "1", "1.5", "1.500"};
        String[] expected = {"0.00", "1.00", "1.50", "1.50"};
        for (int i = 0; i < inputs.length; i++) {
            ConnectorColumn column = column("d" + i, type, inputs[i]);
            metadata.addColumn(null, HANDLE, column, null);
            metadata.addNestedColumn(null, HANDLE, nested(column.getName()), column, null);
            metadata.addColumns(null, HANDLE, Collections.singletonList(column("batch" + i, type, inputs[i])));
            assertDefaults(column.getName(), new BigDecimal(expected[i]));
            assertDefaults("s." + column.getName(), new BigDecimal(expected[i]));
            assertDefaults("batch" + i, new BigDecimal(expected[i]));
        }
        String before = SchemaParser.toJson(reload());
        for (String value : Arrays.asList("1.001", "100000000", "1e999")) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> metadata.addColumn(null, HANDLE, column("invalid", type, value), null));
            Assertions.assertEquals(before, SchemaParser.toJson(reload()));
        }
    }

    @Test
    public void decodedStringsAndCanonicalUuidsSurviveMetadataRoundTrip() {
        ConnectorType[] types = {ConnectorType.of("STRING"), ConnectorType.of("UUID"),
                ConnectorType.of("BOOLEAN")};
        String[] inputs = {"owner's \"field\" C:\\tmp\\", "00112233-4455-6677-8899-aabbccddeeff", " false "};
        Object[] expected = {inputs[0], UUID.fromString(inputs[1]), false};
        for (int i = 0; i < types.length; i++) {
            ConnectorColumn column = column("v" + i, types[i], inputs[i]);
            metadata.addColumn(null, HANDLE, column, null);
            metadata.addNestedColumn(null, HANDLE, nested(column.getName()), column, null);
            assertDefaults(column.getName(), expected[i]);
            assertDefaults("s." + column.getName(), expected[i]);
        }
    }

    @Test
    public void zonedDefaultsUseSessionZoneOnEveryAddPath() {
        ConnectorType type = ConnectorType.of("TIMESTAMPTZ", 6, 0);
        String[] zones = {"Asia/Shanghai", "CST", "PRC", "UTC"};
        for (int i = 0; i < zones.length; i++) {
            ConnectorSession session = session(zones[i]);
            long expected = i == 3 ? 1704164645123456L : 1704135845123456L;
            ConnectorColumn column = column("ts" + i, type, "2024-01-02 03:04:05.123456");
            metadata.addColumn(session, HANDLE, column, null);
            metadata.addNestedColumn(session, HANDLE, nested(column.getName()), column, null);
            metadata.addColumns(session, HANDLE, Collections.singletonList(
                    column("batch" + i, type, "2024-01-02 03:04:05.123456+08:00")));
            assertDefaults(column.getName(), expected);
            assertDefaults("s." + column.getName(), expected);
            assertDefaults("batch" + i, 1704135845123456L);
        }
    }

    @Test
    public void zonedDefaultsRoundAfterResolvingTheInstant() {
        String[] zones = {"CET", "CET", "CET", "CET", "CET", "UTC", "UTC", "UTC", "UTC"};
        String[] values = {"2021-10-31 02:59:59.6", "2021-10-31 00:59:59.6+00:00",
                "2021-10-31 02:59:59.9996", "2021-03-28 01:59:59.6", "2021-03-28 02:30:00.6",
                "1969-12-31 23:59:59.5", "1969-12-31 23:59:59.9995", "1969-12-31 23:59:59.4999",
                "1704164645123456"};
        int[] precisions = {0, 0, 3, 0, 0, 0, 3, 0, -1};
        long[] expected = {1635642000000000L, 1635642000000000L, 1635642000000000L,
                1616893200000000L, 1616893201000000L, 0, 0, -1000000L,
                1704164645123456L};
        for (int i = 0; i < values.length; i++) {
            ConnectorSession session = session(zones[i]);
            ConnectorType type = precisions[i] == -1 ? ConnectorType.of("TIMESTAMPTZ")
                    : ConnectorType.of("TIMESTAMPTZ", precisions[i], 0);
            ConnectorColumn column = column("rounded" + i, type, values[i]);
            metadata.addColumn(session, HANDLE, column, null);
            metadata.addNestedColumn(session, HANDLE, nested(column.getName()), column, null);
            metadata.addColumns(session, HANDLE, Collections.singletonList(column("batch" + i, type, values[i])));
            assertDefaults(column.getName(), expected[i]);
            assertDefaults("s." + column.getName(), expected[i]);
            assertDefaults("batch" + i, expected[i]);
        }
    }

    @Test
    public void zonedDefaultsCheckFinalUtcBoundsBeforeEveryAddPath() {
        ConnectorType type = ConnectorType.of("TIMESTAMPTZ", 6, 0);
        metadata.addColumns(session("UTC"), HANDLE, Arrays.asList(
                column("minimum", type, "0001-01-01 00:00:00"),
                column("maximum", type, "9999-12-31 23:59:59.999999")));
        assertDefaults("minimum", -62135596800000000L);
        assertDefaults("maximum", 253402300799999999L);
        String before = SchemaParser.toJson(reload());
        String[] zones = {"-08:00", "+08:00", "UTC", "+08:00", "UTC", "UTC"};
        String[] values = {"9999-12-31 23:59:59", "0000-01-01 00:00:00", "9999-12-31 23:59:59.6",
                "0000-03-01 00:00:00", "0000-02-28 23:59:59.6", "0000-12-31 23:59:59.999999"};
        int[] precisions = {6, 6, 0, 6, 0, 6};
        for (int i = 0; i < values.length; i++) {
            ConnectorSession session = session(zones[i]);
            ConnectorColumn invalid = column("invalid", ConnectorType.of("TIMESTAMPTZ", precisions[i], 0),
                    values[i]);
            IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> metadata.addColumn(session, HANDLE, invalid, null));
            Assertions.assertTrue(error.getMessage().contains("supported UTC range"), error.getMessage());
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> metadata.addNestedColumn(session, HANDLE, nested("invalid"), invalid, null));
            Assertions.assertThrows(IllegalArgumentException.class, () -> metadata.addColumns(session, HANDLE,
                    Arrays.asList(column("valid", ConnectorType.of("INT"), "1"), invalid)));
            Assertions.assertEquals(before, SchemaParser.toJson(reload()));
        }
    }

    @Test
    public void civilTimestampDefaultsCheckPhysicalReadBoundsBeforeEveryAddPath() {
        ConnectorType type = ConnectorType.of("DATETIMEV2", 6, 0);
        metadata.addColumns(session("UTC"), HANDLE, Arrays.asList(
                column("minimum", type, "0001-01-01 00:00:00"),
                column("maximum", type, "9999-12-31 23:59:59.999999")));
        assertDefaults("minimum", -62135596800000000L);
        assertDefaults("maximum", 253402300799999999L);
        String before = SchemaParser.toJson(reload());
        for (String value : Arrays.asList("0000-03-01 00:00:00", "0000-12-31 23:59:59.999999")) {
            ConnectorColumn invalid = column("invalid", type, value);
            IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> metadata.addColumn(null, HANDLE, invalid, null));
            Assertions.assertTrue(error.getMessage().contains("supported range 0001-9999"), error.getMessage());
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> metadata.addNestedColumn(null, HANDLE, nested("invalid"), invalid, null));
            Assertions.assertThrows(IllegalArgumentException.class, () -> metadata.addColumns(null, HANDLE,
                    Arrays.asList(column("valid", ConnectorType.of("INT"), "1"), invalid)));
            Assertions.assertEquals(before, SchemaParser.toJson(reload()));
        }
    }

    @Test
    public void nonFiniteDefaultsLeavePublishedMetadataReadableAndUnchanged() {
        String before = SchemaParser.toJson(reload());
        for (String type : Arrays.asList("FLOAT", "DOUBLE")) {
            for (String value : Arrays.asList("NaN", "Infinity", "-Infinity", "1e999", "-1e999")) {
                ConnectorColumn invalid = column("invalid", ConnectorType.of(type), value);
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> metadata.addColumn(null, HANDLE, invalid, null));
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> metadata.addNestedColumn(null, HANDLE, nested("invalid"), invalid, null));
                Assertions.assertThrows(IllegalArgumentException.class, () -> metadata.addColumns(null, HANDLE,
                        Arrays.asList(column("valid", ConnectorType.of("INT"), "1"), invalid)));
                Assertions.assertEquals(before, SchemaParser.toJson(reload()));
            }
        }
        metadata.addColumns(null, HANDLE, Arrays.asList(
                column("f", ConnectorType.of("FLOAT"), "1.25"),
                column("d", ConnectorType.of("DOUBLE"), "-1.25")));
        assertDefaults("f", 1.25f);
        assertDefaults("d", -1.25d);
    }

    private static ConnectorSession session(String timeZone) {
        return new ConnectorSession() {
            @Override
            public String getQueryId() {
                return "test";
            }

            @Override
            public String getUser() {
                return "test";
            }

            @Override
            public String getTimeZone() {
                return timeZone;
            }

            @Override
            public String getLocale() {
                return "en_US";
            }

            @Override
            public long getCatalogId() {
                return 1;
            }

            @Override
            public String getCatalogName() {
                return "test";
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return null;
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return Collections.emptyMap();
            }
        };
    }
}
