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

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

/**
 * Unit tests for the B2 single-column builders on {@link IcebergSchemaBuilder}: {@link
 * IcebergSchemaBuilder#buildColumnType} (one column's iceberg type) and {@link
 * IcebergSchemaBuilder#parseDefaultLiteral} (a column DEFAULT string -&gt; iceberg literal).
 */
public class IcebergSchemaBuilderColumnTest {

    // ---------- buildColumnType ----------

    @Test
    public void testBuildScalarColumnTypes() {
        Assertions.assertEquals(Type.TypeID.INTEGER,
                IcebergSchemaBuilder.buildColumnType(ConnectorType.of("INT")).typeId());
        Assertions.assertEquals(Type.TypeID.LONG,
                IcebergSchemaBuilder.buildColumnType(ConnectorType.of("BIGINT")).typeId());
        Assertions.assertEquals(Type.TypeID.STRING,
                IcebergSchemaBuilder.buildColumnType(ConnectorType.of("VARCHAR", 50, 0)).typeId());
        Type dec = IcebergSchemaBuilder.buildColumnType(ConnectorType.of("DECIMALV3", 10, 2));
        Assertions.assertEquals(Type.TypeID.DECIMAL, dec.typeId());
        Assertions.assertEquals(10, ((Types.DecimalType) dec).precision());
        Assertions.assertEquals(2, ((Types.DecimalType) dec).scale());
    }

    @Test
    public void testBuildComplexColumnTypes() {
        Type arr = IcebergSchemaBuilder.buildColumnType(ConnectorType.arrayOf(ConnectorType.of("INT")));
        Assertions.assertEquals(Type.TypeID.LIST, arr.typeId());
        Assertions.assertEquals(Type.TypeID.INTEGER, ((Types.ListType) arr).elementType().typeId());

        Type map = IcebergSchemaBuilder.buildColumnType(
                ConnectorType.mapOf(ConnectorType.of("STRING"), ConnectorType.of("INT")));
        Assertions.assertEquals(Type.TypeID.MAP, map.typeId());

        Type struct = IcebergSchemaBuilder.buildColumnType(ConnectorType.structOf(
                java.util.Arrays.asList("a", "b"),
                java.util.Arrays.asList(ConnectorType.of("INT"), ConnectorType.of("STRING"))));
        Assertions.assertEquals(Type.TypeID.STRUCT, struct.typeId());
        Assertions.assertEquals(2, ((Types.StructType) struct).fields().size());
    }

    @Test
    public void testBuildUnsupportedColumnTypeFailsLoud() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergSchemaBuilder.buildColumnType(ConnectorType.of("TINYINT")));
    }

    // ---------- parseDefaultLiteral ----------

    @Test
    public void testParseDefaultLiteralNullReturnsNull() {
        Assertions.assertNull(IcebergSchemaBuilder.parseDefaultLiteral(null, Types.IntegerType.get()));
    }

    @Test
    public void testParseDefaultLiteralByType() {
        Assertions.assertEquals(42,
                IcebergSchemaBuilder.parseDefaultLiteral("42", Types.IntegerType.get()).value());
        Assertions.assertEquals(100L,
                IcebergSchemaBuilder.parseDefaultLiteral("100", Types.LongType.get()).value());
        Assertions.assertEquals("hello",
                IcebergSchemaBuilder.parseDefaultLiteral("hello", Types.StringType.get()).value());
        Assertions.assertEquals(true,
                IcebergSchemaBuilder.parseDefaultLiteral("true", Types.BooleanType.get()).value());
        Assertions.assertEquals(new BigDecimal("1.50"),
                IcebergSchemaBuilder.parseDefaultLiteral("1.50", Types.DecimalType.of(10, 2)).value());
    }

    @Test
    public void testParseDefaultLiteralBadValueFailsLoud() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergSchemaBuilder.parseDefaultLiteral("not-a-number", Types.IntegerType.get()));
    }
}
