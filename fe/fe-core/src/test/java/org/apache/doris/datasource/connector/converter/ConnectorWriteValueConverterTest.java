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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.catalog.Column;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.UuidLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.UuidType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConnectorWriteValueConverterTest {
    private Column uuidColumn() {
        return ConnectorColumnConverter.convertColumn(new ConnectorColumn("u",
                ConnectorType.of("UUID"), "", true, null)
                .withStringWriteType(ConnectorType.of("UUID")));
    }

    @Test
    void canonicalAndCompactTextProduceNativeUuid() {
        Column column = new Column(uuidColumn());
        VarBinaryLiteral bytes = new VarBinaryLiteral("00112233445566778899AABBCCDDEEFF");
        for (String text : new String[] {"00112233-4455-6677-8899-aabbccddeeff",
                "00112233445566778899AABBCCDDEEFF"}) {
            UuidLiteral result = (UuidLiteral) ConnectorWriteValueConverter.convert(
                    column, new StringLiteral(text));
            Assertions.assertEquals(java.util.UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"),
                    result.getValue());
            Assertions.assertEquals(UuidType.INSTANCE, result.getDataType());
        }
        Assertions.assertSame(bytes, ConnectorWriteValueConverter.convert(column, bytes));
        Assertions.assertSame(NullLiteral.INSTANCE, ConnectorWriteValueConverter.convert(column, NullLiteral.INSTANCE));
        Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorWriteValueConverter.convert(column, new StringLiteral("invalid-uuid")));
    }

    @Test
    void rowExpressionsValidateUuidAndPlainBinaryIsUnchanged() {
        SlotReference value = SlotReference.of("text", StringType.INSTANCE);
        Expression result = ConnectorWriteValueConverter.convert(uuidColumn(), value);
        Assertions.assertInstanceOf(Cast.class, result);
        Assertions.assertEquals(UuidType.INSTANCE, result.getDataType());
        Assertions.assertFalse(result.toSql().toLowerCase().contains("unhex"));
        Assertions.assertTrue(result.toSql().toLowerCase().contains("uuid"));
        Column binary = ConnectorColumnConverter.convertColumn(new ConnectorColumn("b",
                ConnectorType.of("VARBINARY"), "", true, null));
        Assertions.assertSame(value, ConnectorWriteValueConverter.convert(binary, value));
    }

    @Test
    void nestedUuidTextIsConvertedBeforeComplexTypeCoercion() {
        ConnectorType uuid = ConnectorType.of("UUID");
        Column array = ConnectorColumnConverter.convertColumn(new ConnectorColumn("items",
                ConnectorType.arrayOf(uuid), "", true, null)
                .withStringWriteType(ConnectorType.arrayOf(uuid)));
        Expression source = SlotReference.of("items",
                org.apache.doris.nereids.types.ArrayType.of(StringType.INSTANCE));
        Expression result = ConnectorWriteValueConverter.convert(array, source);
        Assertions.assertEquals(org.apache.doris.nereids.types.ArrayType.of(
                UuidType.INSTANCE), result.getDataType());
        Assertions.assertFalse(result.toSql().contains("unhex"));
        Assertions.assertTrue(result.toSql().contains("array_map"));
        Assertions.assertSame(NullLiteral.INSTANCE, ConnectorWriteValueConverter.convert(array, NullLiteral.INSTANCE));

        java.util.List<String> names = java.util.Arrays.asList("u", "text");
        Column struct = ConnectorColumnConverter.convertColumn(new ConnectorColumn("record",
                ConnectorType.structOf(names, java.util.Arrays.asList(uuid, ConnectorType.of("STRING"))),
                "", true, null).withStringWriteType(ConnectorType.structOf(names,
                        java.util.Arrays.asList(uuid, ConnectorType.of("STRING")))));
        Expression record = SlotReference.of("record", org.apache.doris.nereids.types.DataType.fromCatalogType(
                ConnectorColumnConverter.convertColumn(new ConnectorColumn("record",
                        ConnectorType.structOf(names, java.util.Arrays.asList(
                                ConnectorType.of("STRING"), ConnectorType.of("STRING"))), "", true, null)).getType()));
        Expression converted = ConnectorWriteValueConverter.convert(struct, record);
        Assertions.assertEquals(org.apache.doris.nereids.types.DataType.fromCatalogType(struct.getType()),
                converted.getDataType());
        Assertions.assertTrue(converted.nullable());
        Assertions.assertFalse(converted.toSql().contains("unhex"));
    }

    @Test
    void uuidMapKeysAndValuesAndNativeInputsRetainUuidType() {
        ConnectorType uuid = ConnectorType.of("UUID");
        ConnectorType map = ConnectorType.mapOf(uuid, uuid);
        Column column = ConnectorColumnConverter.convertColumn(new ConnectorColumn("m", map, "", true, null)
                .withStringWriteType(map));
        MapLiteral literal = new MapLiteral(java.util.Collections.singletonMap(
                new StringLiteral("00112233-4455-6677-8899-aabbccddeeff"), NullLiteral.INSTANCE));
        Expression nullableMap = ConnectorWriteValueConverter.convert(column, literal);
        Assertions.assertEquals(MapType.of(UuidType.INSTANCE, UuidType.INSTANCE), nullableMap.getDataType());
        Assertions.assertInstanceOf(NullLiteral.class, ((MapLiteral) nullableMap).getValue().values().iterator().next());
        Expression constructor = new CreateMap(new StringLiteral("00112233-4455-6677-8899-aabbccddeeff"),
                new NullLiteral(org.apache.doris.nereids.types.TinyIntType.INSTANCE));
        Assertions.assertEquals(MapType.of(UuidType.INSTANCE, UuidType.INSTANCE),
                ConnectorWriteValueConverter.convert(column, constructor).getDataType());
        Expression source = SlotReference.of("m", MapType.of(StringType.INSTANCE, StringType.INSTANCE));
        Expression converted = ConnectorWriteValueConverter.convert(column, source);
        Assertions.assertEquals(MapType.of(UuidType.INSTANCE, UuidType.INSTANCE), converted.getDataType());
        Assertions.assertTrue(converted.nullable());
        Assertions.assertTrue(converted.toSql().contains("map_entries"));
        Assertions.assertFalse(converted.toSql().contains("map_values"));
        Expression nativeMap = SlotReference.of("m", MapType.of(UuidType.INSTANCE, UuidType.INSTANCE));
        Assertions.assertSame(nativeMap, ConnectorWriteValueConverter.convert(column, nativeMap));
        Expression nativeUuid = SlotReference.of("u", UuidType.INSTANCE);
        Assertions.assertSame(nativeUuid, ConnectorWriteValueConverter.convert(uuidColumn(), nativeUuid));
    }

    @Test
    void volatileStructInputOccursOnceIncludingNullCheck() {
        ConnectorType type = ConnectorType.structOf(java.util.Arrays.asList("u", "text"),
                java.util.Arrays.asList(ConnectorType.of("UUID"), ConnectorType.of("STRING")));
        Column column = ConnectorColumnConverter.convertColumn(new ConnectorColumn("s", type, "", true, null)
                .withStringWriteType(type));
        Expression record = TypeCoercionUtils.processBoundFunction(new CreateNamedStruct(
                new StringLiteral("u"), new StringLiteral("00112233-4455-6677-8899-aabbccddeeff"),
                new StringLiteral("text"), new StringLiteral("first")));
        Expression source = TypeCoercionUtils.processBoundFunction(new If(
                new LessThan(new Random(), new DoubleLiteral(0.5)), record, new NullLiteral(record.getDataType())));
        Expression result = ConnectorWriteValueConverter.convert(column, source);
        Assertions.assertEquals(1, countRandom(result), "Null testing and field extraction must share one evaluation");
        Assertions.assertTrue(result.nullable());
    }

    private int countRandom(Expression expression) {
        return (expression instanceof Random ? 1 : 0)
                + expression.children().stream().mapToInt(this::countRandom).sum();
    }

}
