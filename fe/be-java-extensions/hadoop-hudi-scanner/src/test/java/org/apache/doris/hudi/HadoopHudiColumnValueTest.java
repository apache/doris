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

package org.apache.doris.hudi;

import org.apache.doris.common.jni.vec.ColumnType;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class HadoopHudiColumnValueTest {

    private HadoopHudiColumnValue columnValue;

    @BeforeEach
    public void setUp() {
        columnValue = new HadoopHudiColumnValue(ZoneId.of("UTC"));
    }

    // ===================== Constructor & basic =====================

    @Test
    public void testConstructorAndGetZoneId() {
        ZoneId zone = ZoneId.of("Asia/Shanghai");
        HadoopHudiColumnValue cv = new HadoopHudiColumnValue(zone);
        Assertions.assertEquals(zone, cv.getZoneId());
    }

    @Test
    public void testGetZoneIdUTC() {
        Assertions.assertEquals(ZoneId.of("UTC"), columnValue.getZoneId());
    }

    // ===================== isNull =====================

    @Test
    public void testIsNullWhenNull() {
        columnValue.setRow(null);
        Assertions.assertTrue(columnValue.isNull());
    }

    @Test
    public void testIsNullWhenNotNull() {
        columnValue.setRow(new IntWritable(1));
        Assertions.assertFalse(columnValue.isNull());
    }

    // ===================== getBoolean =====================

    @Test
    public void testGetBooleanTrue() {
        columnValue.setRow(new BooleanWritable(true));
        columnValue.setField(new ColumnType("col", ColumnType.Type.BOOLEAN),
                PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
        Assertions.assertTrue(columnValue.getBoolean());
    }

    @Test
    public void testGetBooleanFalse() {
        columnValue.setRow(new BooleanWritable(false));
        columnValue.setField(new ColumnType("col", ColumnType.Type.BOOLEAN),
                PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
        Assertions.assertFalse(columnValue.getBoolean());
    }

    // ===================== getShort =====================

    @Test
    public void testGetShort() {
        columnValue.setRow(new org.apache.hadoop.hive.serde2.io.ShortWritable((short) 123));
        columnValue.setField(new ColumnType("col", ColumnType.Type.SMALLINT),
                PrimitiveObjectInspectorFactory.writableShortObjectInspector);
        Assertions.assertEquals((short) 123, columnValue.getShort());
    }

    @Test
    public void testGetShortNegative() {
        columnValue.setRow(new org.apache.hadoop.hive.serde2.io.ShortWritable((short) -32000));
        columnValue.setField(new ColumnType("col", ColumnType.Type.SMALLINT),
                PrimitiveObjectInspectorFactory.writableShortObjectInspector);
        Assertions.assertEquals((short) -32000, columnValue.getShort());
    }

    // ===================== getInt =====================

    @Test
    public void testGetInt() {
        columnValue.setRow(new IntWritable(42));
        columnValue.setField(new ColumnType("col", ColumnType.Type.INT),
                PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        Assertions.assertEquals(42, columnValue.getInt());
    }

    @Test
    public void testGetIntZero() {
        columnValue.setRow(new IntWritable(0));
        columnValue.setField(new ColumnType("col", ColumnType.Type.INT),
                PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        Assertions.assertEquals(0, columnValue.getInt());
    }

    // ===================== getLong =====================

    @Test
    public void testGetLong() {
        columnValue.setRow(new LongWritable(123456789L));
        columnValue.setField(new ColumnType("col", ColumnType.Type.BIGINT),
                PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        Assertions.assertEquals(123456789L, columnValue.getLong());
    }

    // ===================== getFloat =====================

    @Test
    public void testGetFloat() {
        columnValue.setRow(new FloatWritable(3.14f));
        columnValue.setField(new ColumnType("col", ColumnType.Type.FLOAT),
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        Assertions.assertEquals(3.14f, columnValue.getFloat(), 0.001f);
    }

    // ===================== getDouble =====================

    @Test
    public void testGetDouble() {
        columnValue.setRow(new DoubleWritable(2.71828));
        columnValue.setField(new ColumnType("col", ColumnType.Type.DOUBLE),
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        Assertions.assertEquals(2.71828, columnValue.getDouble(), 0.00001);
    }

    // ===================== getString =====================

    @Test
    public void testGetString() {
        columnValue.setRow(new Text("hello"));
        columnValue.setField(new ColumnType("col", ColumnType.Type.STRING),
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        Assertions.assertEquals("hello", columnValue.getString());
    }

    @Test
    public void testGetStringEmpty() {
        columnValue.setRow(new Text(""));
        columnValue.setField(new ColumnType("col", ColumnType.Type.STRING),
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        Assertions.assertEquals("", columnValue.getString());
    }

    // ===================== getBytes =====================

    @Test
    public void testGetBytes() {
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        columnValue.setRow(new BytesWritable(data));
        columnValue.setField(new ColumnType("col", ColumnType.Type.BINARY),
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
        byte[] result = columnValue.getBytes();
        Assertions.assertNotNull(result);
    }

    // ===================== getByte (unsupported) =====================

    @Test
    public void testGetByteThrowsUnsupported() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            columnValue.getByte();
        });
    }

    // ===================== getDecimal =====================

    @Test
    public void testGetDecimal() {
        HiveDecimal hd = HiveDecimal.create(new BigDecimal("12345.6789"));
        columnValue.setRow(new org.apache.hadoop.hive.serde2.io.HiveDecimalWritable(hd));
        columnValue.setField(new ColumnType("col", ColumnType.Type.DECIMAL128, 10, 4),
                PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector);
        BigDecimal result = columnValue.getDecimal();
        Assertions.assertEquals(new BigDecimal("12345.6789"), result);
    }

    @Test
    public void testGetDecimalZero() {
        HiveDecimal hd = HiveDecimal.create(BigDecimal.ZERO);
        columnValue.setRow(new org.apache.hadoop.hive.serde2.io.HiveDecimalWritable(hd));
        columnValue.setField(new ColumnType("col", ColumnType.Type.DECIMAL128, 10, 4),
                PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector);
        Assertions.assertEquals(0, columnValue.getDecimal().compareTo(BigDecimal.ZERO));
    }

    // ===================== getDate - DateWritable branch =====================

    @Test
    public void testGetDateWithDateWritable() {
        int epochDay = (int) LocalDate.of(2023, 4, 1).toEpochDay();
        DateWritable dw = new DateWritable(epochDay);
        columnValue.setRow(dw);
        columnValue.setField(new ColumnType("col", ColumnType.Type.DATEV2),
                PrimitiveObjectInspectorFactory.writableDateObjectInspector);

        LocalDate result = columnValue.getDate();
        Assertions.assertEquals(LocalDate.of(2023, 4, 1), result);
    }

    @Test
    public void testGetDateWithDateWritableEpoch() {
        DateWritable dw = new DateWritable(0);
        columnValue.setRow(dw);
        columnValue.setField(new ColumnType("col", ColumnType.Type.DATEV2),
                PrimitiveObjectInspectorFactory.writableDateObjectInspector);

        LocalDate result = columnValue.getDate();
        Assertions.assertEquals(LocalDate.of(1970, 1, 1), result);
    }

    @Test
    public void testGetDateWithDateWritableNegativeDays() {
        DateWritable dw = new DateWritable(-1);
        columnValue.setRow(dw);
        columnValue.setField(new ColumnType("col", ColumnType.Type.DATEV2),
                PrimitiveObjectInspectorFactory.writableDateObjectInspector);

        LocalDate result = columnValue.getDate();
        Assertions.assertEquals(LocalDate.of(1969, 12, 31), result);
    }

    // ===================== getDate - DateObjectInspector branch =====================

    @Test
    public void testGetDateWithDateObjectInspector() {
        Date hiveDate = Date.of(2023, 4, 1);
        DateWritableV2 dw2 = new DateWritableV2(hiveDate);
        columnValue.setRow(dw2);
        columnValue.setField(new ColumnType("col", ColumnType.Type.DATEV2),
                PrimitiveObjectInspectorFactory.writableDateObjectInspector);

        LocalDate result = columnValue.getDate();
        Assertions.assertEquals(LocalDate.of(2023, 4, 1), result);
    }

    @Test
    public void testGetDateWithDateObjectInspectorEpoch() {
        Date hiveDate = Date.of(1970, 1, 1);
        DateWritableV2 dw2 = new DateWritableV2(hiveDate);
        columnValue.setRow(dw2);
        columnValue.setField(new ColumnType("col", ColumnType.Type.DATEV2),
                PrimitiveObjectInspectorFactory.writableDateObjectInspector);

        LocalDate result = columnValue.getDate();
        Assertions.assertEquals(LocalDate.of(1970, 1, 1), result);
    }

    @Test
    public void testGetDateWithDateObjectInspectorFarFuture() {
        Date hiveDate = Date.of(2099, 12, 31);
        DateWritableV2 dw2 = new DateWritableV2(hiveDate);
        columnValue.setRow(dw2);
        columnValue.setField(new ColumnType("col", ColumnType.Type.DATEV2),
                PrimitiveObjectInspectorFactory.writableDateObjectInspector);

        LocalDate result = columnValue.getDate();
        Assertions.assertEquals(LocalDate.of(2099, 12, 31), result);
    }

    // ===================== getDateTime - Timestamp branch =====================

    @Test
    public void testGetDateTimeWithTimestamp() {
        Timestamp ts = Timestamp.valueOf("2023-04-01 12:30:45");
        columnValue.setRow(ts);
        columnValue.setField(new ColumnType("col", ColumnType.Type.DATETIMEV2),
                PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);

        LocalDateTime result = columnValue.getDateTime();
        Assertions.assertNotNull(result);
        // Timestamp.toSqlTimestamp().toLocalDateTime() uses system default timezone,
        // so we verify via the same conversion path instead of hardcoding hour values
        LocalDateTime expected = ts.toSqlTimestamp().toLocalDateTime();
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testGetDateTimeWithTimestampEpoch() {
        Timestamp ts = Timestamp.valueOf("1970-01-01 00:00:00");
        columnValue.setRow(ts);
        columnValue.setField(new ColumnType("col", ColumnType.Type.DATETIMEV2),
                PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);

        LocalDateTime result = columnValue.getDateTime();
        LocalDateTime expected = ts.toSqlTimestamp().toLocalDateTime();
        Assertions.assertEquals(expected, result);
    }

    // ===================== getDateTime - TimestampWritableV2 branch =====================

    @Test
    public void testGetDateTimeWithTimestampWritableV2() {
        Timestamp ts = Timestamp.valueOf("2023-06-15 08:00:00");
        TimestampWritableV2 tw2 = new TimestampWritableV2(ts);
        columnValue.setRow(tw2);
        columnValue.setField(new ColumnType("col", ColumnType.Type.DATETIMEV2),
                PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);

        LocalDateTime result = columnValue.getDateTime();
        Assertions.assertNotNull(result);
    }

    // ===================== getDateTime - TimestampWritable branch =====================

    @Test
    public void testGetDateTimeWithTimestampWritable() {
        java.sql.Timestamp sqlTs = java.sql.Timestamp.valueOf("2023-04-01 12:30:45");
        TimestampWritable tw = new TimestampWritable(sqlTs);
        columnValue.setRow(tw);
        columnValue.setField(new ColumnType("col", ColumnType.Type.DATETIMEV2),
                PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);

        LocalDateTime result = columnValue.getDateTime();
        Assertions.assertEquals(2023, result.getYear());
        Assertions.assertEquals(4, result.getMonthValue());
        Assertions.assertEquals(1, result.getDayOfMonth());
        Assertions.assertEquals(12, result.getHour());
        Assertions.assertEquals(30, result.getMinute());
        Assertions.assertEquals(45, result.getSecond());
    }

    // ===================== getDateTime - LongWritable millis (precision=3) =====================

    @Test
    public void testGetDateTimeWithLongWritableMillis() {
        long millis = LocalDateTime.of(2023, 4, 1, 0, 0, 0)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        columnValue.setRow(new LongWritable(millis));
        ColumnType ct = new ColumnType("col", ColumnType.Type.DATETIMEV2, 3, 0);
        columnValue.setField(ct,
                PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        LocalDateTime result = columnValue.getDateTime();
        Assertions.assertEquals(2023, result.getYear());
        Assertions.assertEquals(4, result.getMonthValue());
        Assertions.assertEquals(1, result.getDayOfMonth());
        Assertions.assertEquals(0, result.getHour());
        Assertions.assertEquals(0, result.getMinute());
        Assertions.assertEquals(0, result.getSecond());
    }

    @Test
    public void testGetDateTimeWithLongWritableMillisNonZero() {
        long millis = LocalDateTime.of(2023, 6, 15, 10, 30, 45, 123000000)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        columnValue.setRow(new LongWritable(millis));
        ColumnType ct = new ColumnType("col", ColumnType.Type.DATETIMEV2, 3, 0);
        columnValue.setField(ct,
                PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        LocalDateTime result = columnValue.getDateTime();
        Assertions.assertEquals(2023, result.getYear());
        Assertions.assertEquals(6, result.getMonthValue());
        Assertions.assertEquals(15, result.getDayOfMonth());
        Assertions.assertEquals(10, result.getHour());
        Assertions.assertEquals(30, result.getMinute());
        Assertions.assertEquals(45, result.getSecond());
    }

    // ===================== getDateTime - LongWritable micros (precision=6) =====================

    @Test
    public void testGetDateTimeWithLongWritableMicros() {
        long micros = LocalDateTime.of(2023, 4, 1, 0, 0, 0)
                .toInstant(ZoneOffset.UTC).toEpochMilli() * 1000;
        columnValue.setRow(new LongWritable(micros));
        ColumnType ct = new ColumnType("col", ColumnType.Type.DATETIMEV2, 6, 0);
        columnValue.setField(ct,
                PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        LocalDateTime result = columnValue.getDateTime();
        Assertions.assertEquals(2023, result.getYear());
        Assertions.assertEquals(4, result.getMonthValue());
        Assertions.assertEquals(1, result.getDayOfMonth());
    }

    @Test
    public void testGetDateTimeWithLongWritableMicrosWithFraction() {
        long epochSeconds = LocalDateTime.of(2023, 6, 15, 10, 30, 45)
                .toEpochSecond(ZoneOffset.UTC);
        long micros = epochSeconds * 1000000 + 123456;
        columnValue.setRow(new LongWritable(micros));
        ColumnType ct = new ColumnType("col", ColumnType.Type.DATETIMEV2, 6, 0);
        columnValue.setField(ct,
                PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        LocalDateTime result = columnValue.getDateTime();
        Assertions.assertEquals(2023, result.getYear());
        Assertions.assertEquals(6, result.getMonthValue());
        Assertions.assertEquals(15, result.getDayOfMonth());
        Assertions.assertEquals(10, result.getHour());
        Assertions.assertEquals(30, result.getMinute());
        Assertions.assertEquals(45, result.getSecond());
        Assertions.assertEquals(123456000, result.getNano());
    }

    // ===================== getDateTime - unsupported precision =====================

    @Test
    public void testGetDateTimeWithUnsupportedPrecision() {
        columnValue.setRow(new LongWritable(12345L));
        ColumnType ct = new ColumnType("col", ColumnType.Type.DATETIMEV2, 9, 0);
        columnValue.setField(ct,
                PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        Assertions.assertThrows(RuntimeException.class, () -> {
            columnValue.getDateTime();
        });
    }

    // ===================== canGetStringAsBytes =====================

    @Test
    public void testCanGetStringAsBytes() {
        Assertions.assertFalse(columnValue.canGetStringAsBytes());
    }

    // ===================== getBigInteger (unsupported) =====================

    @Test
    public void testGetBigIntegerThrowsUnsupported() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            columnValue.getBigInteger();
        });
    }

    // ===================== getStringAsBytes (unsupported) =====================

    @Test
    public void testGetStringAsBytesThrowsUnsupported() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            columnValue.getStringAsBytes();
        });
    }

    // ===================== setRow / setField =====================

    @Test
    public void testSetRowAndSetField() {
        columnValue.setRow(new IntWritable(100));
        columnValue.setField(new ColumnType("col", ColumnType.Type.INT),
                PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        Assertions.assertFalse(columnValue.isNull());
        Assertions.assertEquals(100, columnValue.getInt());
    }

    @Test
    public void testSetRowOverwrite() {
        columnValue.setRow(new IntWritable(1));
        columnValue.setField(new ColumnType("col", ColumnType.Type.INT),
                PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        Assertions.assertEquals(1, columnValue.getInt());

        columnValue.setRow(new IntWritable(2));
        Assertions.assertEquals(2, columnValue.getInt());
    }

    // ===================== getDateTime with different time zones =====================

    @Test
    public void testGetDateTimeMillisWithShanghaiTimeZone() {
        HadoopHudiColumnValue cv = new HadoopHudiColumnValue(ZoneId.of("Asia/Shanghai"));
        long millis = 1680307200000L; // 2023-04-01 00:00:00 UTC
        cv.setRow(new LongWritable(millis));
        ColumnType ct = new ColumnType("col", ColumnType.Type.DATETIMEV2, 3, 0);
        cv.setField(ct, PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        LocalDateTime result = cv.getDateTime();
        Assertions.assertEquals(2023, result.getYear());
        Assertions.assertEquals(4, result.getMonthValue());
        Assertions.assertEquals(1, result.getDayOfMonth());
        Assertions.assertEquals(8, result.getHour());
    }

    @Test
    public void testGetDateTimeMicrosWithShanghaiTimeZone() {
        HadoopHudiColumnValue cv = new HadoopHudiColumnValue(ZoneId.of("Asia/Shanghai"));
        long micros = 1680307200000000L; // 2023-04-01 00:00:00 UTC in micros
        cv.setRow(new LongWritable(micros));
        ColumnType ct = new ColumnType("col", ColumnType.Type.DATETIMEV2, 6, 0);
        cv.setField(ct, PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        LocalDateTime result = cv.getDateTime();
        Assertions.assertEquals(2023, result.getYear());
        Assertions.assertEquals(4, result.getMonthValue());
        Assertions.assertEquals(1, result.getDayOfMonth());
        Assertions.assertEquals(8, result.getHour());
    }

    // ===================== Edge cases =====================

    @Test
    public void testGetDateWithDateWritableMaxDay() {
        int epochDay = (int) LocalDate.of(9999, 12, 31).toEpochDay();
        DateWritable dw = new DateWritable(epochDay);
        columnValue.setRow(dw);
        columnValue.setField(new ColumnType("col", ColumnType.Type.DATEV2),
                PrimitiveObjectInspectorFactory.writableDateObjectInspector);

        LocalDate result = columnValue.getDate();
        Assertions.assertEquals(LocalDate.of(9999, 12, 31), result);
    }

    @Test
    public void testGetLongMaxValue() {
        columnValue.setRow(new LongWritable(Long.MAX_VALUE));
        columnValue.setField(new ColumnType("col", ColumnType.Type.BIGINT),
                PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        Assertions.assertEquals(Long.MAX_VALUE, columnValue.getLong());
    }

    @Test
    public void testGetLongMinValue() {
        columnValue.setRow(new LongWritable(Long.MIN_VALUE));
        columnValue.setField(new ColumnType("col", ColumnType.Type.BIGINT),
                PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        Assertions.assertEquals(Long.MIN_VALUE, columnValue.getLong());
    }

    @Test
    public void testGetIntNegative() {
        columnValue.setRow(new IntWritable(-999));
        columnValue.setField(new ColumnType("col", ColumnType.Type.INT),
                PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        Assertions.assertEquals(-999, columnValue.getInt());
    }

    @Test
    public void testGetDoubleNaN() {
        columnValue.setRow(new DoubleWritable(Double.NaN));
        columnValue.setField(new ColumnType("col", ColumnType.Type.DOUBLE),
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        Assertions.assertTrue(Double.isNaN(columnValue.getDouble()));
    }

    @Test
    public void testGetFloatInfinity() {
        columnValue.setRow(new FloatWritable(Float.POSITIVE_INFINITY));
        columnValue.setField(new ColumnType("col", ColumnType.Type.FLOAT),
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        Assertions.assertTrue(Float.isInfinite(columnValue.getFloat()));
    }

    @Test
    public void testGetStringWithSpecialChars() {
        columnValue.setRow(new Text("hello\tworld\n日本語"));
        columnValue.setField(new ColumnType("col", ColumnType.Type.STRING),
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        Assertions.assertEquals("hello\tworld\n日本語", columnValue.getString());
    }

    @Test
    public void testGetDecimalNegative() {
        HiveDecimal hd = HiveDecimal.create(new BigDecimal("-99999.99"));
        columnValue.setRow(new org.apache.hadoop.hive.serde2.io.HiveDecimalWritable(hd));
        columnValue.setField(new ColumnType("col", ColumnType.Type.DECIMAL128, 10, 2),
                PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector);
        Assertions.assertEquals(new BigDecimal("-99999.99"), columnValue.getDecimal());
    }
}
