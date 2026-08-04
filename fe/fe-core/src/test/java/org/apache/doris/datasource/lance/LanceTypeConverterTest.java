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

package org.apache.doris.datasource.lance;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class LanceTypeConverterTest {

    @Test
    public void testDate32AndDate64MapToDate() {
        Assertions.assertEquals(Type.DATEV2, LanceTypeConverter.toDorisType(
                Field.nullable("date32_col", new ArrowType.Date(DateUnit.DAY))));
        Assertions.assertEquals(Type.DATEV2, LanceTypeConverter.toDorisType(
                Field.nullable("date64_col", new ArrowType.Date(DateUnit.MILLISECOND))));
    }

    @Test
    public void testFloat16IsWidenedToFloat() {
        Assertions.assertEquals(Type.FLOAT, LanceTypeConverter.toDorisType(
                Field.nullable("float16_col", new ArrowType.FloatingPoint(FloatingPointPrecision.HALF))));
    }

    @Test
    public void testBinaryTypesPreserveDorisLengths() {
        Assertions.assertEquals(
                "varbinary(" + ScalarType.MAX_VARBINARY_LENGTH + ")",
                LanceTypeConverter.toDorisType(
                        Field.nullable("binary_col", ArrowType.Binary.INSTANCE)).toSql());
        Assertions.assertEquals(
                "varbinary(" + ScalarType.MAX_VARBINARY_LENGTH + ")",
                LanceTypeConverter.toDorisType(
                        Field.nullable("large_binary_col", ArrowType.LargeBinary.INSTANCE)).toSql());
        Assertions.assertEquals(
                "varbinary(16)",
                LanceTypeConverter.toDorisType(
                        Field.nullable("fixed_size_binary_col",
                                new ArrowType.FixedSizeBinary(16))).toSql());
    }

    @Test
    public void testTime32AndTime64MapToTimeV2() {
        Assertions.assertEquals("time(0)", LanceTypeConverter.toDorisType(
                Field.nullable("time32_s_col", new ArrowType.Time(TimeUnit.SECOND, 32))).toSql());
        Assertions.assertEquals("time(3)", LanceTypeConverter.toDorisType(
                Field.nullable("time32_ms_col", new ArrowType.Time(TimeUnit.MILLISECOND, 32))).toSql());
        Assertions.assertEquals("time(6)", LanceTypeConverter.toDorisType(
                Field.nullable("time64_us_col", new ArrowType.Time(TimeUnit.MICROSECOND, 64))).toSql());
        Assertions.assertEquals("time(6)", LanceTypeConverter.toDorisType(
                Field.nullable("time64_ns_col", new ArrowType.Time(TimeUnit.NANOSECOND, 64))).toSql());
    }

    @Test
    public void testTimestampTimezoneControlsDorisType() {
        Assertions.assertEquals(ScalarType.createDatetimeV2Type(6), LanceTypeConverter.toDorisType(
                Field.nullable("timestamp_us_col",
                        new ArrowType.Timestamp(TimeUnit.MICROSECOND, null))));
        Assertions.assertEquals(ScalarType.createTimeStampTzType(3), LanceTypeConverter.toDorisType(
                Field.nullable("timestamp_ms_utc_col",
                        new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"))));
        Assertions.assertEquals(ScalarType.createTimeStampTzType(6), LanceTypeConverter.toDorisType(
                Field.nullable("timestamp_ns_shanghai_col",
                        new ArrowType.Timestamp(TimeUnit.NANOSECOND, "Asia/Shanghai"))));
    }

    @Test
    public void testUnsignedIntegersAreWidenedLosslessly() {
        Assertions.assertEquals(Type.SMALLINT, LanceTypeConverter.toDorisType(
                Field.nullable("uint8_col", new ArrowType.Int(8, false))));
        Assertions.assertEquals(Type.INT, LanceTypeConverter.toDorisType(
                Field.nullable("uint16_col", new ArrowType.Int(16, false))));
        Assertions.assertEquals(Type.BIGINT, LanceTypeConverter.toDorisType(
                Field.nullable("uint32_col", new ArrowType.Int(32, false))));
        Assertions.assertEquals(Type.LARGEINT, LanceTypeConverter.toDorisType(
                Field.nullable("uint64_col", new ArrowType.Int(64, false))));
    }

    @Test
    public void testExtensionAndDictionaryMarkersAreUnsupported() {
        Field extensionField = new Field(
                "json_col",
                new FieldType(
                        true,
                        ArrowType.Utf8.INSTANCE,
                        null,
                        Collections.singletonMap("ARROW:extension:name", "lance.json")),
                Collections.emptyList());
        Assertions.assertEquals(Type.UNSUPPORTED, LanceTypeConverter.toDorisType(extensionField));

        Field dictionaryField = new Field(
                "dictionary_col",
                new FieldType(
                        true,
                        ArrowType.Utf8.INSTANCE,
                        new DictionaryEncoding(1, false, new ArrowType.Int(16, true))),
                Collections.emptyList());
        Assertions.assertEquals(Type.UNSUPPORTED, LanceTypeConverter.toDorisType(dictionaryField));

        Field blobField = new Field(
                "blob_col",
                new FieldType(
                        true,
                        ArrowType.Struct.INSTANCE,
                        null,
                        Collections.singletonMap("ARROW:extension:name", "lance.blob.v2")),
                Collections.emptyList());
        Assertions.assertEquals(Type.UNSUPPORTED, LanceTypeConverter.toDorisType(blobField));

        Field bfloat16Item = new Field(
                "item",
                new FieldType(
                        true,
                        new ArrowType.FixedSizeBinary(2),
                        null,
                        Collections.singletonMap("ARROW:extension:name", "lance.bfloat16")),
                Collections.emptyList());
        Field bfloat16Vector = new Field(
                "bfloat16_vector_col",
                FieldType.nullable(new ArrowType.FixedSizeList(4)),
                Collections.singletonList(bfloat16Item));
        Assertions.assertEquals(Type.UNSUPPORTED, LanceTypeConverter.toDorisType(bfloat16Vector));
    }
}
