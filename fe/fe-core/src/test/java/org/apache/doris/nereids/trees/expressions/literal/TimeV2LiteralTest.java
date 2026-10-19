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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeV2Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TimeV2LiteralTest {

    @Test
    public void testTimeV2LiteralCreate() {
        // without micro second
        TimeV2Literal literal = new TimeV2Literal(TimeV2Type.of(0), "12:12:12");
        String s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12");
        // max val
        literal = new TimeV2Literal(TimeV2Type.of(0), "838:59:59");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "838:59:59");
        // min val
        literal = new TimeV2Literal(TimeV2Type.of(0), "-838:59:59");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "-838:59:59");
        // hour is negative
        literal = new TimeV2Literal(TimeV2Type.of(0), "-00:01:01");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "-00:01:01");
        literal = new TimeV2Literal(-3599000000.0);
        s = literal.getStringValue();
        Assertions.assertEquals(s, "-00:59:59.000000");
        // contail micro second part
        literal = new TimeV2Literal(TimeV2Type.of(1), "12:12:12.121212");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12.1");
        literal = new TimeV2Literal(TimeV2Type.of(2), "12:12:12.121212");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12.12");
        literal = new TimeV2Literal(TimeV2Type.of(3), "12:12:12.121212");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12.121");
        literal = new TimeV2Literal(TimeV2Type.of(4), "12:12:12.121212");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12.1212");
        literal = new TimeV2Literal(TimeV2Type.of(5), "12:12:12.121212");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12.12121");
        literal = new TimeV2Literal(TimeV2Type.of(6), "12:12:12.121212");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12.121212");
        // max val
        literal = new TimeV2Literal(TimeV2Type.of(6), "838:59:59.999999");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "838:59:59.999999");
        // min val
        literal = new TimeV2Literal(TimeV2Type.of(6), "-838:59:59.999999");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "-838:59:59.999999");
        // not string
        literal = new TimeV2Literal(12, 12, 12, 121212, 6, false);
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12.121212");
        // max val
        literal = new TimeV2Literal(838, 59, 59, 999999, 6, false);
        s = literal.getStringValue();
        Assertions.assertEquals(s, "838:59:59.999999");
        // min val
        literal = new TimeV2Literal(838, 59, 59, 999999, 6, true);
        s = literal.getStringValue();
        Assertions.assertEquals(s, "-838:59:59.999999");
        // string without ":"
        literal = new TimeV2Literal(TimeV2Type.of(0), "8385959");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "838:59:59");
        literal = new TimeV2Literal(TimeV2Type.of(0), "-8385959");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "-838:59:59");
        literal = new TimeV2Literal(TimeV2Type.of(0), "120000");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:00:00");
        literal = new TimeV2Literal(TimeV2Type.of(6), "8385959.999999");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "838:59:59.999999");
        literal = new TimeV2Literal(TimeV2Type.of(6), "-8385959.999999");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "-838:59:59.999999");
        // one ":"
        literal = new TimeV2Literal(TimeV2Type.of(0), "12:00");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:00:00");
    }

    @Test
    public void testTimeV2LiteralOutOfRange() {
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(838, 59, 59, 1000000, 6, false);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(838, 59, 60, 999999, 6, false);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(838, 60, 59, 999999, 6, false);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(839, 59, 59, 999999, 6, false);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(838, 59, 59, -1, 6, false);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(838, 59, -1, 999999, 6, false);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(838, -1, 59, 999999, 6, false);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(839, 59, 59, 999999, 6, true);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(3020400000000.0);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(-3020400000000.0);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(TimeV2Type.of(0), "838:59:60");
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(TimeV2Type.of(0), "838:60:59");
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(TimeV2Type.of(0), "839:59:59");
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(TimeV2Type.of(0), "-839:59:59");
        });
    }

    @Test
    public void testUncheckedCast() {
        // to string
        TimeV2Literal literal = new TimeV2Literal(TimeV2Type.of(0), "12:12:12");
        Expression expression = literal.uncheckedCastTo(StringType.INSTANCE);
        Assertions.assertInstanceOf(StringLiteral.class, expression);
        Assertions.assertEquals("12:12:12", ((StringLiteral) expression).value);

        literal = new TimeV2Literal(TimeV2Type.of(0), "0");
        expression = literal.uncheckedCastTo(StringType.INSTANCE);
        Assertions.assertInstanceOf(StringLiteral.class, expression);
        Assertions.assertEquals("00:00:00", ((StringLiteral) expression).value);

        literal = new TimeV2Literal(TimeV2Type.of(3), "0");
        expression = literal.uncheckedCastTo(StringType.INSTANCE);
        Assertions.assertInstanceOf(StringLiteral.class, expression);
        Assertions.assertEquals("00:00:00.000", ((StringLiteral) expression).value);
    }

    @Test
    public void testCastNegativeTimeToDateTimeV2KeepsSign() {
        TimeV2Literal literal = new TimeV2Literal(TimeV2Type.of(0), "-00:00:01");

        DateTimeV2Literal dateTime = (DateTimeV2Literal) literal.uncheckedCastTo(DateTimeV2Type.of(0));

        Assertions.assertEquals(23, dateTime.getHour());
        Assertions.assertEquals(59, dateTime.getMinute());
        Assertions.assertEquals(59, dateTime.getSecond());
    }

    @Test
    public void testFromMysqlBinaryTimeParameter() throws AnalysisException {
        // Protocol::MYSQL_TYPE_TIME, 12 bytes: is_negative, days, hours, minutes, seconds, microseconds
        ByteBuffer data = ByteBuffer.allocate(13).order(ByteOrder.LITTLE_ENDIAN);
        data.put((byte) 12).put((byte) 0).putInt(0).put((byte) 12).put((byte) 34).put((byte) 56).putInt(123456).flip();
        Literal literal = Literal.getLiteralByMysqlType(MysqlColType.MYSQL_TYPE_TIME, false, data);
        TimeV2Literal time = Assertions.assertInstanceOf(TimeV2Literal.class, literal);
        Assertions.assertEquals("12:34:56.123456", time.getStringValue());

        // 8 bytes: no microseconds; negative, and the days fold into the hours
        data = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN);
        data.put((byte) 8).put((byte) 1).putInt(1).put((byte) 1).put((byte) 2).put((byte) 3).flip();
        time = (TimeV2Literal) Literal.getLiteralByMysqlType(MysqlColType.MYSQL_TYPE_TIME2, false, data);
        Assertions.assertEquals("-25:02:03.000000", time.getStringValue());

        // 0 bytes: 00:00:00
        data = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN);
        data.put((byte) 0).flip();
        time = (TimeV2Literal) Literal.getLiteralByMysqlType(MysqlColType.MYSQL_TYPE_TIME, false, data);
        Assertions.assertEquals("00:00:00.000000", time.getStringValue());
    }

    @Test
    public void testBinaryTimeRejectsInvalidFraming() {
        for (MysqlColType type : new MysqlColType[] {MysqlColType.MYSQL_TYPE_TIME, MysqlColType.MYSQL_TYPE_TIME2}) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> Literal.getLiteralByMysqlType(type, false, ByteBuffer.allocate(0)));
            for (int len : new int[] {1, 7, 9, 11, 13, 252, 253, 254, 255}) {
                ByteBuffer data = ByteBuffer.allocate(17).order(ByteOrder.LITTLE_ENDIAN);
                data.put((byte) len).position(17);
                data.flip();
                Assertions.assertThrows(AnalysisException.class,
                        () -> Literal.getLiteralByMysqlType(type, false, data), "length=" + len);
                Assertions.assertEquals(1, data.position(), "invalid length must not consume parameter data");
            }
            for (int len : new int[] {8, 12}) {
                for (int available = 0; available < len; available++) {
                    ByteBuffer data = ByteBuffer.allocate(available + 1).order(ByteOrder.LITTLE_ENDIAN);
                    data.put((byte) len).position(available + 1);
                    data.flip();
                    Assertions.assertThrows(AnalysisException.class,
                            () -> Literal.getLiteralByMysqlType(type, false, data));
                }
            }
        }
    }

    @Test
    public void testBinaryTimeKeepsParameterBoundaries() {
        // The malformed first value used to read the second TIME's length as days=8.
        ByteBuffer malformed = ByteBuffer.wrap(new byte[] {1, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0})
                .order(ByteOrder.LITTLE_ENDIAN);
        Assertions.assertThrows(AnalysisException.class,
                () -> Literal.getLiteralByMysqlType(MysqlColType.MYSQL_TYPE_TIME, false, malformed));
        Assertions.assertEquals(1, malformed.position());

        ByteBuffer valid = ByteBuffer.allocate(13).order(ByteOrder.LITTLE_ENDIAN);
        valid.put((byte) 8).put((byte) 0).putInt(0).put((byte) 12).put((byte) 34).put((byte) 56)
                .putInt(7654321).flip();
        Assertions.assertEquals("12:34:56.000000",
                Literal.getLiteralByMysqlType(MysqlColType.MYSQL_TYPE_TIME, false, valid).getStringValue());
        Assertions.assertEquals(7654321,
                Literal.getLiteralByMysqlType(MysqlColType.MYSQL_TYPE_LONG, false, valid).getValue());
        Assertions.assertFalse(valid.hasRemaining());
    }

    @Test
    public void testBinaryTimeRejectsOutOfRangeDurations() {
        for (MysqlColType type : new MysqlColType[] {MysqlColType.MYSQL_TYPE_TIME, MysqlColType.MYSQL_TYPE_TIME2}) {
            for (int days : new int[] {35, 0x20000000, 0x80000000, 0xffffffff}) {
                ByteBuffer data = binaryTime(0, days, 0, 0, 0, 0);
                Assertions.assertThrows(AnalysisException.class,
                        () -> Literal.getLiteralByMysqlType(type, false, data));
            }
            Assertions.assertThrows(AnalysisException.class,
                    () -> Literal.getLiteralByMysqlType(type, false, binaryTime(0, 34, 23, 0, 0, 0)));
            Assertions.assertThrows(AnalysisException.class,
                    () -> Literal.getLiteralByMysqlType(type, false, binaryTime(0, 0, 0, 0, 0, 1000000)));
            for (int sign : new int[] {2, 255}) {
                Assertions.assertThrows(AnalysisException.class,
                        () -> Literal.getLiteralByMysqlType(type, false, binaryTime(sign, 0, 0, 0, 0, 0)));
            }
        }
    }

    @Test
    public void testBinaryTimeAcceptsMaximumDuration() {
        for (MysqlColType type : new MysqlColType[] {MysqlColType.MYSQL_TYPE_TIME, MysqlColType.MYSQL_TYPE_TIME2}) {
            Assertions.assertEquals("838:59:59.999999",
                    Literal.getLiteralByMysqlType(type, false, binaryTime(0, 34, 22, 59, 59, 999999)).getStringValue());
            Assertions.assertEquals("-838:59:59.999999",
                    Literal.getLiteralByMysqlType(type, false, binaryTime(1, 34, 22, 59, 59, 999999)).getStringValue());
        }
    }

    private static ByteBuffer binaryTime(int sign, int days, int hour, int minute, int second, int microsecond) {
        int len = microsecond == 0 ? 8 : 12;
        ByteBuffer data = ByteBuffer.allocate(len + 1).order(ByteOrder.LITTLE_ENDIAN);
        data.put((byte) len).put((byte) sign).putInt(days).put((byte) hour).put((byte) minute).put((byte) second);
        if (len == 12) {
            data.putInt(microsecond);
        }
        data.flip();
        return data;
    }

}
