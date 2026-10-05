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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class AdbcTypeMapperTest {

    private static Field leaf(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    private static Field nested(String name, ArrowType type, List<Field> children) {
        return new Field(name, FieldType.nullable(type), children);
    }

    private static ConnectorType map(ArrowType type) {
        return AdbcTypeMapper.toDorisType("c", leaf("c", type));
    }

    private static String rejectionOf(String columnName, Field field) {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> AdbcTypeMapper.toDorisType(columnName, field));
        return e.getMessage();
    }

    @Test
    void booleanMapsToBoolean() {
        Assertions.assertEquals(ConnectorType.of("BOOLEAN"), map(ArrowType.Bool.INSTANCE));
    }

    @Test
    void signedIntegersKeepTheirWidth() {
        Assertions.assertEquals(ConnectorType.of("TINYINT"), map(new ArrowType.Int(8, true)));
        Assertions.assertEquals(ConnectorType.of("SMALLINT"), map(new ArrowType.Int(16, true)));
        Assertions.assertEquals(ConnectorType.of("INT"), map(new ArrowType.Int(32, true)));
        Assertions.assertEquals(ConnectorType.of("BIGINT"), map(new ArrowType.Int(64, true)));
    }

    @Test
    void unsignedIntegersWidenByOneStep() {
        // Doris has no unsigned integers. Keeping the width would wrap every value above the signed
        // maximum into a negative number with no error anywhere -- silent data corruption, which is the
        // one outcome this mapper exists to prevent.
        Assertions.assertEquals(ConnectorType.of("SMALLINT"), map(new ArrowType.Int(8, false)));
        Assertions.assertEquals(ConnectorType.of("INT"), map(new ArrowType.Int(16, false)));
        Assertions.assertEquals(ConnectorType.of("BIGINT"), map(new ArrowType.Int(32, false)));
        Assertions.assertEquals(ConnectorType.of("LARGEINT"), map(new ArrowType.Int(64, false)));
    }

    @Test
    void floatsMapByPrecision() {
        Assertions.assertEquals(ConnectorType.of("FLOAT"),
                map(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)));
        Assertions.assertEquals(ConnectorType.of("FLOAT"),
                map(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
        Assertions.assertEquals(ConnectorType.of("DOUBLE"),
                map(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
    }

    @Test
    void decimalsKeepPrecisionAndScale() {
        Assertions.assertEquals(ConnectorType.of("DECIMALV3", 18, 4),
                map(new ArrowType.Decimal(18, 4, 128)));
        Assertions.assertEquals(ConnectorType.of("DECIMALV3", 60, 10),
                map(new ArrowType.Decimal(60, 10, 256)));
    }

    @Test
    void decimalsBeyondTheDorisLimitAreRejected() {
        // 128-bit decimals cap at 38 digits and 256-bit ones at 76. Clamping instead would truncate the
        // high-order digits of every value in the column.
        String narrow = rejectionOf("amount", leaf("amount", new ArrowType.Decimal(39, 2, 128)));
        Assertions.assertTrue(narrow.contains("amount"), narrow);
        Assertions.assertTrue(narrow.contains("38"), narrow);

        String wide = rejectionOf("amount", leaf("amount", new ArrowType.Decimal(77, 2, 256)));
        Assertions.assertTrue(wide.contains("76"), wide);
    }

    @Test
    void datesMapByUnit() {
        Assertions.assertEquals(ConnectorType.of("DATEV2"), map(new ArrowType.Date(DateUnit.DAY)));
        // DATEMILLI carries a time of day; DATEV2 would drop it.
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 3, 0),
                map(new ArrowType.Date(DateUnit.MILLISECOND)));
    }

    @Test
    void timestampScaleFollowsTheArrowUnit() {
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 0, 0),
                map(new ArrowType.Timestamp(TimeUnit.SECOND, null)));
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 3, 0),
                map(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)));
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 6, 0),
                map(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));
    }

    @Test
    void nanosecondTimestampsTruncateRatherThanFail() {
        // Doris tops out at 6 fractional digits. Rejecting the column would make whole tables unreadable
        // over a precision difference that stored data rarely uses.
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 6, 0),
                map(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)));
    }

    @Test
    void zonedTimestampsMapToTimestamptz() {
        Assertions.assertEquals(ConnectorType.of("TIMESTAMPTZ", 6, 0),
                map(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")));
    }

    @Test
    void allStringAndBinaryVariantsMapToString() {
        // The large_/view variants exist only as physical layouts; BE normalizes them before the serde, so
        // FE must present them identically or DESC would disagree with what a query returns.
        for (ArrowType type : Arrays.asList(
                ArrowType.Utf8.INSTANCE, ArrowType.LargeUtf8.INSTANCE, ArrowType.Utf8View.INSTANCE,
                ArrowType.Binary.INSTANCE, ArrowType.LargeBinary.INSTANCE, ArrowType.BinaryView.INSTANCE,
                new ArrowType.FixedSizeBinary(16))) {
            Assertions.assertEquals(ConnectorType.of("STRING"), map(type), "for " + type);
        }
    }

    @Test
    void listVariantsMapToArrayOfTheElementType() {
        for (ArrowType listType : Arrays.asList(
                ArrowType.List.INSTANCE, ArrowType.LargeList.INSTANCE, new ArrowType.FixedSizeList(4))) {
            Field field = nested("c", listType,
                    List.of(leaf("item", ArrowType.LargeUtf8.INSTANCE)));
            Assertions.assertEquals(ConnectorType.arrayOf(ConnectorType.of("STRING")),
                    AdbcTypeMapper.toDorisType("c", field), "for " + listType);
        }
    }

    @Test
    void structFieldNamesAreLowercasedAtEveryLevel() {
        // BE indexes struct children by lowercase key, so a mixed-case child name crashes it rather than
        // producing a query error. The guard has to apply at every level, not just the top one.
        Field inner = nested("Inner", ArrowType.Struct.INSTANCE,
                List.of(leaf("DeepField", new ArrowType.Int(32, true))));
        Field outer = nested("c", ArrowType.Struct.INSTANCE,
                List.of(leaf("OuterField", ArrowType.Utf8.INSTANCE), inner));

        ConnectorType mapped = AdbcTypeMapper.toDorisType("c", outer);

        Assertions.assertEquals(List.of("outerfield", "inner"), mapped.getFieldNames());
        Assertions.assertEquals(List.of("deepfield"), mapped.getChildren().get(1).getFieldNames());
    }

    @Test
    void mapsUnwrapTheEntriesStruct() {
        // Arrow models a map as list<struct<key,value>>; reading the pair from the top level would give the
        // entries struct instead of the key and value types.
        Field entries = nested("entries", ArrowType.Struct.INSTANCE,
                List.of(leaf("key", ArrowType.Utf8.INSTANCE), leaf("value", new ArrowType.Int(64, true))));
        Field field = nested("c", new ArrowType.Map(false), List.of(entries));

        Assertions.assertEquals(
                ConnectorType.mapOf(ConnectorType.of("STRING"), ConnectorType.of("BIGINT")),
                AdbcTypeMapper.toDorisType("c", field));
    }

    @Test
    void runEndEncodedMapsToItsValueType() {
        Field field = nested("c", new ArrowType.RunEndEncoded(),
                List.of(leaf("run_ends", new ArrowType.Int(32, true)),
                        leaf("values", ArrowType.Utf8.INSTANCE)));
        Assertions.assertEquals(ConnectorType.of("STRING"), AdbcTypeMapper.toDorisType("c", field));
    }

    @Test
    void typesWithNoDorisEquivalentAreRejectedByName() {
        // The message must carry the column name: without it a user facing a wide table has no way to tell
        // which column to cast or drop on the remote side.
        for (ArrowType type : Arrays.asList(
                new ArrowType.Time(TimeUnit.MICROSECOND, 64),
                new ArrowType.Duration(TimeUnit.SECOND),
                new ArrowType.Interval(org.apache.arrow.vector.types.IntervalUnit.DAY_TIME),
                ArrowType.Null.INSTANCE)) {
            String message = rejectionOf("weird_col", leaf("weird_col", type));
            Assertions.assertTrue(message.contains("weird_col"), message);
            Assertions.assertTrue(message.contains(type.toString()), message);
        }
    }

    @Test
    void anUnsupportedTypeNestedInsideAStructNamesThePath() {
        Field field = nested("c", ArrowType.Struct.INSTANCE,
                List.of(leaf("ok", ArrowType.Utf8.INSTANCE),
                        leaf("bad", new ArrowType.Time(TimeUnit.MICROSECOND, 64))));
        String message = rejectionOf("c", field);
        Assertions.assertTrue(message.contains("c.bad"), message);
    }
}
