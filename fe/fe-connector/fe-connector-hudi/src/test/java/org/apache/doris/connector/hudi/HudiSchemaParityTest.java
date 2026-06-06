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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Schema-level parity for the SPI Hudi metadata path (P3-T07, batch C).
 *
 * <p>WHY: {@code getTableSchema} derives its column list from the Hudi Avro schema
 * via {@link HudiConnectorMetadata#avroSchemaToColumns}. This must produce the same
 * column set — names, order, Doris types, nullability — and the same per-column
 * Hive type strings ({@code colTypes}) as legacy fe-core
 * {@code HMSExternalTable.initHudiSchema} (:740-753) +
 * {@code HudiUtils.fromAvroHudiTypeToDorisType} / {@code convertAvroToHiveType}.
 * Because no compile path sees both modules (fe-core does not depend on the concrete
 * connector modules), parity is asserted against golden values transcribed from —
 * and annotated with — the legacy contract.</p>
 *
 * <p>COW vs MOR: schema derivation is table-type-agnostic on BOTH sides (neither
 * consults COW/MOR), so a single golden schema covers both; the COW/MOR distinction
 * lives only in scan planning and is pinned separately by {@link HudiTableTypeTest}.</p>
 *
 * <p>Two assertions deliberately encode the P3-T07 column-name-casing fix: the
 * top-level column name is lower-cased (legacy {@code toLowerCase(Locale.ROOT)} at
 * {@code HMSExternalTable.java:745}), while a NESTED struct field name keeps its
 * original case (legacy lowercases only the top-level column). A test that passed
 * with the old raw-case behavior would be wrong.</p>
 */
public class HudiSchemaParityTest {

    // A representative Hudi table schema in Avro JSON (the form Hudi actually stores).
    // Mixed-case top-level names (Id, Name, Addr) and a mixed-case nested field
    // (Street) exercise the casing boundary; the type variety mirrors the legacy
    // type matrix (primitive, decimal, date, timestamp, nullable, array, map, struct).
    private static final String SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"hudi_t\",\"fields\":["
            + "{\"name\":\"Id\",\"type\":\"long\"},"
            + "{\"name\":\"Name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"price\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "\"precision\":10,\"scale\":2}},"
            + "{\"name\":\"event_date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
            + "{\"name\":\"created_at\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
            + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
            + "{\"name\":\"props\",\"type\":{\"type\":\"map\",\"values\":\"int\"}},"
            + "{\"name\":\"Addr\",\"type\":{\"type\":\"record\",\"name\":\"AddrRec\",\"fields\":["
            + "{\"name\":\"Street\",\"type\":\"string\"},{\"name\":\"zip\",\"type\":\"int\"}]}}"
            + "]}";

    // Golden column contract, mirroring legacy initHudiSchema field-by-field.
    private static final List<String> EXPECTED_NAMES = Arrays.asList(
            "id", "name", "price", "event_date", "created_at", "tags", "props", "addr");

    private static final List<ConnectorType> EXPECTED_TYPES = Arrays.asList(
            ConnectorType.of("BIGINT"),
            ConnectorType.of("STRING"),
            ConnectorType.of("DECIMALV3", 10, 2),
            ConnectorType.of("DATEV2"),
            ConnectorType.of("DATETIMEV2", 6, 0),
            ConnectorType.arrayOf(ConnectorType.of("STRING")),
            ConnectorType.mapOf(ConnectorType.of("STRING"), ConnectorType.of("INT")),
            ConnectorType.structOf(Arrays.asList("Street", "zip"),
                    Arrays.asList(ConnectorType.of("STRING"), ConnectorType.of("INT"))));

    // Only the union-typed "Name" field is nullable; the flag must track the union,
    // not be a constant.
    private static final List<Boolean> EXPECTED_NULLABLE = Arrays.asList(
            false, true, false, false, false, false, false, false);

    // Hive type strings = legacy colTypes (convertAvroToHiveType per field).
    private static final List<String> EXPECTED_HIVE_TYPES = Arrays.asList(
            "bigint", "string", "decimal(10,2)", "date", "timestamp",
            "array<string>", "map<string,int>", "struct<Street:string,zip:int>");

    private static Schema schema() {
        return new Schema.Parser().parse(SCHEMA_JSON);
    }

    @Test
    public void testSchemaColumnsMirrorLegacyContract() {
        List<ConnectorColumn> columns = HudiConnectorMetadata.avroSchemaToColumns(schema());
        Assertions.assertEquals(EXPECTED_NAMES.size(), columns.size());
        for (int i = 0; i < columns.size(); i++) {
            ConnectorColumn col = columns.get(i);
            Assertions.assertEquals(EXPECTED_NAMES.get(i), col.getName(), "name[" + i + "]");
            Assertions.assertEquals(EXPECTED_TYPES.get(i), col.getType(), "type[" + i + "]");
            Assertions.assertEquals(EXPECTED_NULLABLE.get(i), col.isNullable(), "nullable[" + i + "]");
        }
    }

    @Test
    public void testColumnTypeStringsMirrorLegacyColTypes() {
        List<Schema.Field> fields = schema().getFields();
        Assertions.assertEquals(EXPECTED_HIVE_TYPES.size(), fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Assertions.assertEquals(EXPECTED_HIVE_TYPES.get(i),
                    HudiTypeMapping.toHiveTypeString(fields.get(i).schema()), "colType[" + i + "]");
        }
    }

    @Test
    public void testTopLevelNameLoweredButNestedStructNamePreserved() {
        List<ConnectorColumn> columns = HudiConnectorMetadata.avroSchemaToColumns(schema());
        ConnectorColumn addr = columns.get(7);
        // top-level "Addr" -> "addr"
        Assertions.assertEquals("addr", addr.getName());
        // nested struct field "Street" keeps its case (legacy lowercases only top-level)
        Assertions.assertEquals(Arrays.asList("Street", "zip"), addr.getType().getFieldNames());
        Assertions.assertEquals("struct<Street:string,zip:int>",
                HudiTypeMapping.toHiveTypeString(schema().getFields().get(7).schema()));
    }
}
