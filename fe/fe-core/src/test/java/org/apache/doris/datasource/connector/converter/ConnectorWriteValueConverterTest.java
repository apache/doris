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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarBinaryType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConnectorWriteValueConverterTest {
    private Column uuidColumn() {
        return ConnectorColumnConverter.convertColumn(new ConnectorColumn("u",
                ConnectorType.of("VARBINARY", 16, 0), "", true, null)
                .withStringWriteType(ConnectorType.of("UUID")));
    }

    @Test
    void textAndTypedBytesHaveTheSameUuidRepresentation() {
        Column column = new Column(uuidColumn());
        VarBinaryLiteral bytes = new VarBinaryLiteral("00112233445566778899AABBCCDDEEFF");
        for (String text : new String[] {"00112233-4455-6677-8899-aabbccddeeff",
                "00112233445566778899AABBCCDDEEFF"}) {
            VarBinaryLiteral result = (VarBinaryLiteral) ConnectorWriteValueConverter.convert(
                    column, new StringLiteral(text));
            Assertions.assertArrayEquals((byte[]) bytes.getValue(), (byte[]) result.getValue());
            Assertions.assertEquals(VarBinaryType.createVarBinaryType(16), result.getDataType());
        }
        Assertions.assertSame(bytes, ConnectorWriteValueConverter.convert(column, bytes));
        Assertions.assertSame(NullLiteral.INSTANCE, ConnectorWriteValueConverter.convert(column, NullLiteral.INSTANCE));
        Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorWriteValueConverter.convert(column, new StringLiteral("invalid-uuid")));
    }

    @Test
    void rowExpressionsValidateBeforeDecodingAndPlainBinaryIsUnchanged() {
        SlotReference value = SlotReference.of("text", StringType.INSTANCE);
        Expression result = ConnectorWriteValueConverter.convert(uuidColumn(), value);
        Assertions.assertInstanceOf(Cast.class, result);
        Assertions.assertEquals(VarBinaryType.createVarBinaryType(16), result.getDataType());
        Assertions.assertTrue(result.toSql().toLowerCase().contains("unhex"));
        Assertions.assertTrue(result.toSql().toLowerCase().contains("uuid"));
        Column binary = ConnectorColumnConverter.convertColumn(new ConnectorColumn("b",
                ConnectorType.of("VARBINARY"), "", true, null));
        Assertions.assertSame(value, ConnectorWriteValueConverter.convert(binary, value));
    }

    @Test
    void nestedUuidTextIsConvertedBeforeComplexTypeCoercion() {
        ConnectorType bytes = ConnectorType.of("VARBINARY", 16, 0);
        ConnectorType uuid = ConnectorType.of("UUID");
        Column array = ConnectorColumnConverter.convertColumn(new ConnectorColumn("items",
                ConnectorType.arrayOf(bytes), "", true, null)
                .withStringWriteType(ConnectorType.arrayOf(uuid)));
        Expression source = SlotReference.of("items",
                org.apache.doris.nereids.types.ArrayType.of(StringType.INSTANCE));
        Expression result = ConnectorWriteValueConverter.convert(array, source);
        Assertions.assertEquals(org.apache.doris.nereids.types.ArrayType.of(
                VarBinaryType.createVarBinaryType(16)), result.getDataType());
        Assertions.assertTrue(result.toSql().contains("unhex"));
        Assertions.assertTrue(result.toSql().contains("array_map"));
        Assertions.assertSame(NullLiteral.INSTANCE, ConnectorWriteValueConverter.convert(array, NullLiteral.INSTANCE));

        java.util.List<String> names = java.util.Arrays.asList("u", "text");
        Column struct = ConnectorColumnConverter.convertColumn(new ConnectorColumn("record",
                ConnectorType.structOf(names, java.util.Arrays.asList(bytes, ConnectorType.of("STRING"))),
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
        Assertions.assertTrue(converted.toSql().contains("unhex"));
    }

}
