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
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorPartitionField;
import org.apache.doris.connector.api.ddl.ConnectorPartitionSpec;
import org.apache.doris.connector.api.ddl.ConnectorSortField;

import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link IcebergSchemaBuilder} — the string-driven port of the legacy fe-core iceberg
 * create-table conversion (type mapping + partition spec + sort order + default properties). Pure: no
 * catalog, no Mockito.
 */
public class IcebergSchemaBuilderTest {

    private static ConnectorColumn col(String name, ConnectorType type, boolean nullable) {
        // 6-arg form: name, type, comment, nullable, defaultValue, isKey.
        return new ConnectorColumn(name, type, "", nullable, null, false);
    }

    // ---------- buildSchema: scalar type mapping (parity with DorisTypeToIcebergType.atomic) ----------

    @Test
    public void testScalarTypeMapping() {
        Schema schema = IcebergSchemaBuilder.buildSchema(Arrays.asList(
                col("b", ConnectorType.of("BOOLEAN"), true),
                col("i", ConnectorType.of("INT"), true),
                col("l", ConnectorType.of("BIGINT"), true),
                col("f", ConnectorType.of("FLOAT"), true),
                col("d", ConnectorType.of("DOUBLE"), true),
                col("s", ConnectorType.of("VARCHAR", 100, 0), true),
                col("dt", ConnectorType.of("DATEV2"), true),
                col("ts", ConnectorType.of("DATETIMEV2", 6, 0), true)));

        Assertions.assertEquals(Type.TypeID.BOOLEAN, schema.findField("b").type().typeId());
        Assertions.assertEquals(Type.TypeID.INTEGER, schema.findField("i").type().typeId());
        Assertions.assertEquals(Type.TypeID.LONG, schema.findField("l").type().typeId());
        Assertions.assertEquals(Type.TypeID.FLOAT, schema.findField("f").type().typeId());
        Assertions.assertEquals(Type.TypeID.DOUBLE, schema.findField("d").type().typeId());
        // char family collapses to STRING (declared length 100 dropped — legacy parity).
        Assertions.assertEquals(Type.TypeID.STRING, schema.findField("s").type().typeId());
        Assertions.assertEquals(Type.TypeID.DATE, schema.findField("dt").type().typeId());
        // datetime maps to timestamp WITHOUT zone.
        Type tsType = schema.findField("ts").type();
        Assertions.assertEquals(Type.TypeID.TIMESTAMP, tsType.typeId());
        Assertions.assertFalse(((Types.TimestampType) tsType).shouldAdjustToUTC());
    }

    @Test
    public void testDecimalCarriesPrecisionScale() {
        Schema schema = IcebergSchemaBuilder.buildSchema(Collections.singletonList(
                col("price", ConnectorType.of("DECIMAL128", 20, 4), true)));
        Types.DecimalType decimal = (Types.DecimalType) schema.findField("price").type();
        Assertions.assertEquals(20, decimal.precision());
        Assertions.assertEquals(4, decimal.scale());
    }

    @Test
    public void testTimestampTzMapsToWithZone() {
        Schema schema = IcebergSchemaBuilder.buildSchema(Collections.singletonList(
                col("ts", ConnectorType.of("TIMESTAMPTZ", 6, 0), true)));
        Type type = schema.findField("ts").type();
        Assertions.assertEquals(Type.TypeID.TIMESTAMP, type.typeId());
        Assertions.assertTrue(((Types.TimestampType) type).shouldAdjustToUTC());
    }

    @Test
    public void testNullabilityAndFieldIdAndComment() {
        ConnectorColumn nullable = new ConnectorColumn("a", ConnectorType.of("INT"), "the a col", true, null, false);
        ConnectorColumn required = new ConnectorColumn("b", ConnectorType.of("INT"), "", false, null, false);
        Schema schema = IcebergSchemaBuilder.buildSchema(Arrays.asList(nullable, required));
        // Top-level field id == declaration index (legacy DorisTypeToIcebergType root scheme).
        Assertions.assertEquals(0, schema.columns().get(0).fieldId());
        Assertions.assertEquals(1, schema.columns().get(1).fieldId());
        Assertions.assertTrue(schema.findField("a").isOptional());
        Assertions.assertFalse(schema.findField("b").isOptional());
        Assertions.assertEquals("the a col", schema.findField("a").doc());
    }

    @Test
    public void testUnsupportedScalarTypeFailsLoud() {
        // TINYINT is not supported by legacy DorisTypeToIcebergType.atomic -> fail loud.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergSchemaBuilder.buildSchema(Collections.singletonList(
                        col("t", ConnectorType.of("TINYINT"), true))));
        Assertions.assertTrue(ex.getMessage().contains("TINYINT"));
    }

    // ---------- buildSchema: complex types + nested id allocation ----------

    @Test
    public void testComplexTypesAndUniqueNestedIds() {
        ConnectorType arr = ConnectorType.arrayOf(ConnectorType.of("INT"));
        ConnectorType map = ConnectorType.mapOf(ConnectorType.of("VARCHAR", 50, 0), ConnectorType.of("BIGINT"));
        ConnectorType struct = ConnectorType.structOf(
                Arrays.asList("x", "y"), Arrays.asList(ConnectorType.of("INT"), ConnectorType.of("DOUBLE")));
        Schema schema = IcebergSchemaBuilder.buildSchema(Arrays.asList(
                col("arr", arr, true), col("m", map, true), col("st", struct, true)));

        Assertions.assertEquals(Type.TypeID.LIST, schema.findField("arr").type().typeId());
        Assertions.assertEquals(Type.TypeID.INTEGER,
                schema.findField("arr").type().asListType().elementType().typeId());
        Assertions.assertEquals(Type.TypeID.MAP, schema.findField("m").type().typeId());
        Types.StructType st = schema.findField("st").type().asStructType();
        Assertions.assertEquals(2, st.fields().size());
        Assertions.assertEquals("x", st.fields().get(0).name());

        // All field ids (top-level + nested) must be unique — iceberg Schema construction would otherwise
        // throw; assert explicitly so a broken id allocator fails this test, not just downstream.
        long distinct = schema.columns().stream()
                .flatMap(f -> idsOf(f.type()).stream())
                .distinct().count();
        long total = schema.columns().stream().flatMap(f -> idsOf(f.type()).stream()).count();
        Assertions.assertEquals(total, distinct);
    }

    private static List<Integer> idsOf(Type type) {
        java.util.List<Integer> ids = new java.util.ArrayList<>();
        collectIds(type, ids);
        return ids;
    }

    private static void collectIds(Type type, List<Integer> ids) {
        if (type.isListType()) {
            ids.add(type.asListType().elementId());
            collectIds(type.asListType().elementType(), ids);
        } else if (type.isMapType()) {
            ids.add(type.asMapType().keyId());
            ids.add(type.asMapType().valueId());
            collectIds(type.asMapType().keyType(), ids);
            collectIds(type.asMapType().valueType(), ids);
        } else if (type.isStructType()) {
            for (Types.NestedField f : type.asStructType().fields()) {
                ids.add(f.fieldId());
                collectIds(f.type(), ids);
            }
        }
    }

    // ---------- buildPartitionSpec ----------

    private static Schema partSchema() {
        return IcebergSchemaBuilder.buildSchema(Arrays.asList(
                col("id", ConnectorType.of("BIGINT"), true),
                col("name", ConnectorType.of("VARCHAR", 50, 0), true),
                col("ts", ConnectorType.of("DATETIMEV2", 6, 0), true)));
    }

    private static ConnectorPartitionSpec spec(ConnectorPartitionField... fields) {
        return new ConnectorPartitionSpec(
                ConnectorPartitionSpec.Style.TRANSFORM, Arrays.asList(fields), Collections.emptyList());
    }

    @Test
    public void testPartitionTransforms() {
        Schema schema = partSchema();
        PartitionSpec result = IcebergSchemaBuilder.buildPartitionSpec(spec(
                new ConnectorPartitionField("id", "bucket", Collections.singletonList(16)),
                new ConnectorPartitionField("name", "truncate", Collections.singletonList(4)),
                new ConnectorPartitionField("ts", "day", Collections.emptyList())), schema);
        List<String> transforms = new java.util.ArrayList<>();
        result.fields().forEach(f -> transforms.add(f.transform().toString()));
        Assertions.assertTrue(transforms.contains("bucket[16]"), transforms.toString());
        Assertions.assertTrue(transforms.contains("truncate[4]"), transforms.toString());
        Assertions.assertTrue(transforms.contains("day"), transforms.toString());
    }

    @Test
    public void testIdentityPartition() {
        Schema schema = partSchema();
        PartitionSpec result = IcebergSchemaBuilder.buildPartitionSpec(
                new ConnectorPartitionSpec(ConnectorPartitionSpec.Style.IDENTITY,
                        Collections.singletonList(new ConnectorPartitionField("name", "identity",
                                Collections.emptyList())),
                        Collections.emptyList()),
                schema);
        Assertions.assertEquals(1, result.fields().size());
        Assertions.assertEquals("identity", result.fields().get(0).transform().toString());
    }

    @Test
    public void testNullOrEmptyPartitionSpecIsUnpartitioned() {
        Assertions.assertTrue(IcebergSchemaBuilder.buildPartitionSpec(null, partSchema()).isUnpartitioned());
    }

    @Test
    public void testUnsupportedTransformFailsLoud() {
        Schema schema = partSchema();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergSchemaBuilder.buildPartitionSpec(spec(
                        new ConnectorPartitionField("name", "weird_transform", Collections.emptyList())), schema));
    }

    @Test
    public void testBucketMissingArgFailsLoud() {
        Schema schema = partSchema();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergSchemaBuilder.buildPartitionSpec(spec(
                        new ConnectorPartitionField("id", "bucket", Collections.emptyList())), schema));
        Assertions.assertTrue(ex.getMessage().contains("bucket"));
    }

    // ---------- buildSortOrder ----------

    @Test
    public void testSortOrder() {
        Schema schema = partSchema();
        SortOrder order = IcebergSchemaBuilder.buildSortOrder(Arrays.asList(
                new ConnectorSortField("id", true, true),
                new ConnectorSortField("name", false, false)), schema);
        Assertions.assertEquals(2, order.fields().size());
        Assertions.assertEquals(SortDirection.ASC, order.fields().get(0).direction());
        Assertions.assertEquals(NullOrder.NULLS_FIRST, order.fields().get(0).nullOrder());
        Assertions.assertEquals(SortDirection.DESC, order.fields().get(1).direction());
        Assertions.assertEquals(NullOrder.NULLS_LAST, order.fields().get(1).nullOrder());
    }

    @Test
    public void testNullOrEmptySortOrderIsNull() {
        Assertions.assertNull(IcebergSchemaBuilder.buildSortOrder(null, partSchema()));
        Assertions.assertNull(IcebergSchemaBuilder.buildSortOrder(Collections.emptyList(), partSchema()));
    }

    // ---------- buildTableProperties ----------

    @Test
    public void testDefaultPropertiesAppliedWhenAbsent() {
        Map<String, String> props = IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap());
        Assertions.assertEquals("2", props.get(TableProperties.FORMAT_VERSION));
        Assertions.assertEquals("merge-on-read", props.get(TableProperties.DELETE_MODE));
        Assertions.assertEquals("merge-on-read", props.get(TableProperties.UPDATE_MODE));
        Assertions.assertEquals("merge-on-read", props.get(TableProperties.MERGE_MODE));
    }

    @Test
    public void testUserPropertiesPreservedOverDefaults() {
        Map<String, String> in = new HashMap<>();
        in.put(TableProperties.FORMAT_VERSION, "3");
        in.put("custom", "v");
        Map<String, String> props = IcebergSchemaBuilder.buildTableProperties(in);
        Assertions.assertEquals("3", props.get(TableProperties.FORMAT_VERSION));
        Assertions.assertEquals("v", props.get("custom"));
        Assertions.assertEquals("merge-on-read", props.get(TableProperties.DELETE_MODE));
    }
}
