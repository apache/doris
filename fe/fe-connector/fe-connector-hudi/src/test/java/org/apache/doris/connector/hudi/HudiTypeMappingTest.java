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

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Tests {@link HudiTypeMapping#toHiveTypeString}.
 *
 * <p>WHY: the BE Hudi JNI scanner ({@code HadoopHudiJniScanner}) parses
 * {@code hudi_column_types} as Hive type strings split on {@code '#'}. The FE
 * must therefore emit full Hive type strings carrying precision/scale and
 * subtypes — not Doris type names — or the scanner reads wrong/null columns.
 * These tests pin the exact strings, matching fe-core
 * {@code HudiUtils.convertAvroToHiveType}.</p>
 */
public class HudiTypeMappingTest {

    @Test
    public void testPrimitives() {
        Assertions.assertEquals("boolean", HudiTypeMapping.toHiveTypeString(Schema.create(Schema.Type.BOOLEAN)));
        Assertions.assertEquals("int", HudiTypeMapping.toHiveTypeString(Schema.create(Schema.Type.INT)));
        Assertions.assertEquals("bigint", HudiTypeMapping.toHiveTypeString(Schema.create(Schema.Type.LONG)));
        Assertions.assertEquals("float", HudiTypeMapping.toHiveTypeString(Schema.create(Schema.Type.FLOAT)));
        Assertions.assertEquals("double", HudiTypeMapping.toHiveTypeString(Schema.create(Schema.Type.DOUBLE)));
        Assertions.assertEquals("string", HudiTypeMapping.toHiveTypeString(Schema.create(Schema.Type.STRING)));
    }

    @Test
    public void testDateAndTimestampLogicalTypes() {
        Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        Assertions.assertEquals("date", HudiTypeMapping.toHiveTypeString(date));

        Schema tsMillis = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Assertions.assertEquals("timestamp", HudiTypeMapping.toHiveTypeString(tsMillis));

        Schema tsMicros = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Assertions.assertEquals("timestamp", HudiTypeMapping.toHiveTypeString(tsMicros));
    }

    @Test
    public void testDecimalKeepsPrecisionAndScale() {
        // Directly targets bug (a): getTypeName() previously dropped precision/scale.
        Schema decimal = LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES));
        Assertions.assertEquals("decimal(10,2)", HudiTypeMapping.toHiveTypeString(decimal));

        Schema decimalFixed = LogicalTypes.decimal(38, 18)
                .addToSchema(Schema.createFixed("d", null, null, 16));
        Assertions.assertEquals("decimal(38,18)", HudiTypeMapping.toHiveTypeString(decimalFixed));
    }

    @Test
    public void testArray() {
        Schema arr = Schema.createArray(Schema.create(Schema.Type.INT));
        Assertions.assertEquals("array<int>", HudiTypeMapping.toHiveTypeString(arr));
    }

    @Test
    public void testMap() {
        // Avro maps always have string keys.
        Schema map = Schema.createMap(Schema.create(Schema.Type.LONG));
        Assertions.assertEquals("map<string,bigint>", HudiTypeMapping.toHiveTypeString(map));
    }

    @Test
    public void testStructContainsCommas() {
        // Directly targets bug (b): the comma in struct<...> must survive as a
        // single type string; a comma join+split would shatter it.
        Schema struct = Schema.createRecord("r", null, null, false, Arrays.asList(
                new Schema.Field("a", Schema.create(Schema.Type.INT)),
                new Schema.Field("b", Schema.create(Schema.Type.STRING))));
        Assertions.assertEquals("struct<a:int,b:string>", HudiTypeMapping.toHiveTypeString(struct));
    }

    @Test
    public void testNestedComplexType() {
        Schema struct = Schema.createRecord("r", null, null, false, Arrays.asList(
                new Schema.Field("id", Schema.create(Schema.Type.LONG)),
                new Schema.Field("amount",
                        LogicalTypes.decimal(12, 4).addToSchema(Schema.create(Schema.Type.BYTES)))));
        Schema arrOfStruct = Schema.createArray(struct);
        Assertions.assertEquals("array<struct<id:bigint,amount:decimal(12,4)>>",
                HudiTypeMapping.toHiveTypeString(arrOfStruct));
    }

    @Test
    public void testNullableUnionIsUnwrapped() {
        Schema nullableInt = Schema.createUnion(
                Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
        Assertions.assertEquals("int", HudiTypeMapping.toHiveTypeString(nullableInt));
    }

    @Test
    public void testUnsupportedLogicalTypeFailsLoud() {
        // Matches legacy fail-loud: time types are unsupported.
        Schema timeMillis = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HudiTypeMapping.toHiveTypeString(timeMillis));
    }
}
