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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.types.VariantType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class DeltaLakeTypeMappingTest {
    @Test
    void mapsPrimitiveTypes() {
        Map<DataType, String> types = Map.of(
                BooleanType.BOOLEAN, "BOOLEAN", ByteType.BYTE, "TINYINT",
                ShortType.SHORT, "SMALLINT", IntegerType.INTEGER, "INT", LongType.LONG, "BIGINT",
                FloatType.FLOAT, "FLOAT", DoubleType.DOUBLE, "DOUBLE", DateType.DATE, "DATEV2",
                StringType.STRING, "STRING", BinaryType.BINARY, "STRING");
        types.forEach((source, target) -> Assertions.assertEquals(ConnectorType.of(target), map(source)));
        Assertions.assertEquals(ConnectorType.of("DECIMALV3", 38, 18), map(new DecimalType(38, 18)));
        Assertions.assertEquals(ConnectorType.of("VARBINARY"),
                DeltaLakeTypeMapping.toConnectorType(BinaryType.BINARY, false, true));
    }

    @Test
    void distinguishesInstantAndWallClockTimestamps() {
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 6, 0), map(TimestampType.TIMESTAMP));
        Assertions.assertEquals(ConnectorType.of("TIMESTAMPTZ", 6, 0),
                DeltaLakeTypeMapping.toConnectorType(TimestampType.TIMESTAMP, true, false));
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 6, 0),
                DeltaLakeTypeMapping.toConnectorType(TimestampNTZType.TIMESTAMP_NTZ, true, false));
    }

    @Test
    void lowercasesNestedNamesAndPreservesNullability() {
        StructType nested = new StructType().add("MiXeD", IntegerType.INTEGER, false);
        ConnectorType array = map(new ArrayType(nested, false));
        Assertions.assertFalse(array.isChildNullable(0));
        Assertions.assertEquals("mixed", array.getChildren().get(0).getFieldNames().get(0));
        Assertions.assertFalse(array.getChildren().get(0).isChildNullable(0));
        ConnectorType map = map(new MapType(StringType.STRING, new ArrayType(nested, true), false));
        Assertions.assertFalse(map.isChildNullable(1));
        Assertions.assertEquals("mixed",
                map.getChildren().get(1).getChildren().get(0).getFieldNames().get(0));
    }

    @Test
    void rejectsTypesOutsideM1() {
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> map(VariantType.VARIANT));
        Assertions.assertTrue(error.getMessage().contains("variant"));
        Assertions.assertThrows(DorisConnectorException.class, () -> map(new StringType("SPARK.UTF8_LCASE")));
    }

    private static ConnectorType map(DataType type) {
        return DeltaLakeTypeMapping.toConnectorType(type, false, false);
    }
}
