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

package org.apache.doris.connector.trino;

import org.apache.doris.connector.spi.ConnectorType;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Unit tests for {@link TrinoTypeMapping}: every supported Trino SPI type must map to
 * the Doris {@link ConnectorType} name (and precision/scale/children) that the rest of
 * the bridge relies on for schema fidelity; unsupported types must fail loudly.
 */
public class TrinoTypeMappingTest {

    private static String name(io.trino.spi.type.Type type) {
        return TrinoTypeMapping.toConnectorType(type).getTypeName();
    }

    @Test
    public void testIntegerFamilyNames() {
        Assertions.assertEquals("BOOLEAN", name(BooleanType.BOOLEAN));
        Assertions.assertEquals("TINYINT", name(TinyintType.TINYINT));
        Assertions.assertEquals("SMALLINT", name(SmallintType.SMALLINT));
        Assertions.assertEquals("INT", name(IntegerType.INTEGER));
        Assertions.assertEquals("BIGINT", name(BigintType.BIGINT));
    }

    @Test
    public void testRealMapsToFloatAndDoubleToDouble() {
        Assertions.assertEquals("FLOAT", name(RealType.REAL));
        Assertions.assertEquals("DOUBLE", name(DoubleType.DOUBLE));
    }

    @Test
    public void testDecimalCarriesPrecisionAndScale() {
        ConnectorType ct = TrinoTypeMapping.toConnectorType(DecimalType.createDecimalType(18, 4));
        Assertions.assertEquals("DECIMALV3", ct.getTypeName());
        Assertions.assertEquals(18, ct.getPrecision());
        Assertions.assertEquals(4, ct.getScale());
    }

    @Test
    public void testStringFamilyNames() {
        Assertions.assertEquals("CHAR", name(CharType.createCharType(10)));
        Assertions.assertEquals("STRING", name(VarcharType.createVarcharType(20)));
        Assertions.assertEquals("STRING", name(VarcharType.VARCHAR));
        Assertions.assertEquals("STRING", name(VarbinaryType.VARBINARY));
    }

    @Test
    public void testDateMapsToDateV2() {
        Assertions.assertEquals("DATEV2", name(DateType.DATE));
    }

    @Test
    public void testTimestampMapsToDatetimeV2WithPrecision() {
        ConnectorType ct = TrinoTypeMapping.toConnectorType(TimestampType.createTimestampType(3));
        Assertions.assertEquals("DATETIMEV2", ct.getTypeName());
        Assertions.assertEquals(3, ct.getPrecision());
    }

    @Test
    public void testTimestampPrecisionClampedToSix() {
        // Doris DATETIMEV2 supports at most 6 fractional digits; higher Trino precision clamps.
        ConnectorType ct = TrinoTypeMapping.toConnectorType(TimestampType.createTimestampType(9));
        Assertions.assertEquals("DATETIMEV2", ct.getTypeName());
        Assertions.assertEquals(6, ct.getPrecision());
    }

    @Test
    public void testArrayCarriesElementType() {
        ConnectorType ct = TrinoTypeMapping.toConnectorType(new ArrayType(IntegerType.INTEGER));
        Assertions.assertEquals("ARRAY", ct.getTypeName());
        Assertions.assertEquals(1, ct.getChildren().size());
        Assertions.assertEquals("INT", ct.getChildren().get(0).getTypeName());
    }

    @Test
    public void testMapCarriesKeyAndValueTypes() {
        ConnectorType ct = TrinoTypeMapping.toConnectorType(
                new MapType(VarcharType.VARCHAR, BigintType.BIGINT, new TypeOperators()));
        Assertions.assertEquals("MAP", ct.getTypeName());
        Assertions.assertEquals(2, ct.getChildren().size());
        Assertions.assertEquals("STRING", ct.getChildren().get(0).getTypeName());
        Assertions.assertEquals("BIGINT", ct.getChildren().get(1).getTypeName());
    }

    @Test
    public void testStructCarriesFieldTypes() {
        RowType row = RowType.rowType(
                RowType.field("a", IntegerType.INTEGER),
                RowType.field("b", VarcharType.VARCHAR));
        ConnectorType ct = TrinoTypeMapping.toConnectorType(row);
        Assertions.assertEquals("STRUCT", ct.getTypeName());
        Assertions.assertEquals(2, ct.getChildren().size());
        Assertions.assertEquals("INT", ct.getChildren().get(0).getTypeName());
        Assertions.assertEquals("STRING", ct.getChildren().get(1).getTypeName());
        // WHY the names matter: Doris resolves STRUCT sub-field access BY NAME. Drop them here and
        // fe-core invents "col0"/"col1", so DESCRIBE shows fabricated names and every `SELECT s.a`
        // written against the source schema is rejected at analysis time as "field a was not found".
        // The user then has no way at all to reach the field by its real name.
        Assertions.assertEquals(Arrays.asList("a", "b"), ct.getFieldNames());
        Assertions.assertEquals(ct.getChildren().size(), ct.getFieldNames().size());
    }

    @Test
    public void testAnonymousStructFieldsNamedByPosition() {
        // Trino ROWs can be anonymous. Each field still needs a DISTINCT resolvable name; the legacy
        // fe-core mapping named them all "col", which collides as soon as there is more than one.
        ConnectorType ct = TrinoTypeMapping.toConnectorType(
                RowType.anonymousRow(IntegerType.INTEGER, VarcharType.VARCHAR));
        Assertions.assertEquals(Arrays.asList("col0", "col1"), ct.getFieldNames());
    }

    @Test
    public void testNestedStructCarriesInnerFieldNames() {
        // row(a int, b row(c int)) - the recursive path has to carry names too, not just the top level.
        RowType inner = RowType.rowType(RowType.field("c", IntegerType.INTEGER));
        ConnectorType ct = TrinoTypeMapping.toConnectorType(RowType.rowType(
                RowType.field("a", IntegerType.INTEGER),
                RowType.field("b", inner)));
        Assertions.assertEquals(Arrays.asList("a", "b"), ct.getFieldNames());
        ConnectorType nested = ct.getChildren().get(1);
        Assertions.assertEquals("STRUCT", nested.getTypeName());
        Assertions.assertEquals(Collections.singletonList("c"), nested.getFieldNames());
    }

    @Test
    public void testUnknownTypeThrows() {
        // An unmapped Trino type must fail loudly rather than silently produce a wrong type.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TrinoTypeMapping.toConnectorType(UuidType.UUID));
    }
}
