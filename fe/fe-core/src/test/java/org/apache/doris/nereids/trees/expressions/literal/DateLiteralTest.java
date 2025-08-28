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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

class DateLiteralTest {
    @Test
    void reject() {
        Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("2022-01-01 01:00:00.000000"));
        Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("2022-01-01 00:01:00.000000"));
        Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("2022-01-01 00:00:01.000000"));
        Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("2022-01-01 00:00:00.000001"));
    }

    @Test
    void testNormalize() {
        String s = DateLiteral.normalize("2021-5").get();
        Assertions.assertEquals("2021-05", s);
        s = DateLiteral.normalize("2021-5-1").get();
        Assertions.assertEquals("2021-05-01", s);
        s = DateLiteral.normalize("2021-5-01").get();
        Assertions.assertEquals("2021-05-01", s);

        s = DateLiteral.normalize("2021-5-01 0:0:0").get();
        Assertions.assertEquals("2021-05-01 00:00:00", s);
        s = DateLiteral.normalize("2021-5-01 0:0:0.001").get();
        Assertions.assertEquals("2021-05-01 00:00:00.001", s);
        s = DateLiteral.normalize("2021-5-01 0:0:0.12345678").get();
        Assertions.assertEquals("2021-05-01 00:00:00.1234567", s);
        s = DateLiteral.normalize("2021-5-1    Asia/Shanghai").get();
        Assertions.assertEquals("2021-05-01Asia/Shanghai", s);
        s = DateLiteral.normalize("2021-5-1 0:0:0.12345678   Asia/Shanghai").get();
        Assertions.assertEquals("2021-05-01 00:00:00.1234567Asia/Shanghai", s);
    }

    @Test
    void testDate() {
        new DateLiteral("220101");
        new DateLiteral("22-01-01");
        new DateLiteral("22-01-1");
        new DateLiteral("22-1-1");

        new DateLiteral("2022-01-01");
        new DateLiteral("2022-01-1");
        new DateLiteral("2022-1-1");
        new DateLiteral("20220101");

        Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("-01-01"));
    }

    @Test
    @Disabled
    void testZone() {
        // new DateLiteral("2022-01-01Z");
        // new DateLiteral("2022-01-01UTC");
        // new DateLiteral("2022-01-01GMT");
        new DateLiteral("2022-01-01UTC+08");
        new DateLiteral("2022-01-01UTC-06");
        new DateLiteral("2022-01-01UTC+08:00");
        new DateLiteral("2022-01-01UTC-06:00");
        new DateLiteral("2022-01-01Europe/London");
    }

    @Test
    @Disabled
    void testOffset() {
        new DateLiteral("2022-01-01+01:00:00");
        new DateLiteral("2022-01-01+01:00");
        new DateLiteral("2022-01-01+01");
        new DateLiteral("2022-01-01+1:0:0");
        new DateLiteral("2022-01-01+1:0");
        new DateLiteral("2022-01-01+1");

        new DateLiteral("2022-01-01-01:00:00");
        new DateLiteral("2022-01-01-01:00");
        new DateLiteral("2022-01-01-1:0:0");
        new DateLiteral("2022-01-01-1:0");
    }

    @Test
    void testIrregularDate() {
        Consumer<DateLiteral> assertFunc = (DateLiteral dateLiteral) -> {
            Assertions.assertEquals("2016-07-02", dateLiteral.toString());
        };
        DateLiteral dateLiteral;

        dateLiteral = new DateLiteral("2016-07-02");
        assertFunc.accept(dateLiteral);

        dateLiteral = new DateLiteral("2016-7-02");
        assertFunc.accept(dateLiteral);
        dateLiteral = new DateLiteral("2016-07-2");
        assertFunc.accept(dateLiteral);
        dateLiteral = new DateLiteral("2016-7-2");
        assertFunc.accept(dateLiteral);

        dateLiteral = new DateLiteral("2016-07-02");
        assertFunc.accept(dateLiteral);
        dateLiteral = new DateLiteral("2016-07-2");
        assertFunc.accept(dateLiteral);
        dateLiteral = new DateLiteral("2016-7-02");
        assertFunc.accept(dateLiteral);
        dateLiteral = new DateLiteral("2016-7-2");
        assertFunc.accept(dateLiteral);
    }

    @Test
    void testWrongPunctuationDate() {
        Assertions.assertThrows(AnalysisException.class, () -> new DateTimeV2Literal("2020€02€01"));
        Assertions.assertThrows(AnalysisException.class, () -> new DateTimeV2Literal("2020【02】01"));
    }

    @Test
    void testPunctuationDate() {
        new DateLiteral("2020!02!01");
        new DateLiteral("2020@02@01");
        new DateLiteral("2020#02#01");
        new DateLiteral("2020$02$01");
        new DateLiteral("2020%02%01");
        new DateLiteral("2020^02^01");
        new DateLiteral("2020&02&01");
        new DateLiteral("2020*02*01");
        new DateLiteral("2020(02(01");
        new DateLiteral("2020)02)01");
        new DateLiteral("2020-02-01");
        new DateLiteral("2020+02+01");
        new DateLiteral("2020=02=01");
        new DateLiteral("2020_02_01");
        new DateLiteral("2020{02{01");
        new DateLiteral("2020}02}01");
        new DateLiteral("2020[02[01");
        new DateLiteral("2020]02]01");
        new DateLiteral("2020|02|01");
        new DateLiteral("2020\\02\\01");
        new DateLiteral("2020:02:01");
        new DateLiteral("2020;02;01");
        new DateLiteral("2020\"02\"01");
        new DateLiteral("2020'02'01");
        new DateLiteral("2020<02<01");
        new DateLiteral("2020>02>01");
        new DateLiteral("2020,02,01");
        new DateLiteral("2020.02.01");
        new DateLiteral("2020?02?01");
        new DateLiteral("2020/02/01");
        new DateLiteral("2020~02~01");
        new DateLiteral("2020`02`01");
    }

    @Test
    void testPunctuationDateTime() {
        new DateLiteral("2020!02!01 00!00!00");
        new DateLiteral("2020@02@01 00@00@00");
        new DateLiteral("2020#02#01 00#00#00");
        new DateLiteral("2020$02$01 00$00$00");
        new DateLiteral("2020%02%01 00%00%00");
        new DateLiteral("2020^02^01 00^00^00");
        new DateLiteral("2020&02&01 00&00&00");
        new DateLiteral("2020*02*01 00*00*00");
        new DateLiteral("2020(02(01 00(00(00");
        new DateLiteral("2020)02)01 00)00)00");
        new DateLiteral("2020-02-01 00-00-00");
        new DateLiteral("2020+02+01 00+00+00");
        new DateLiteral("2020=02=01 00=00=00");
        new DateLiteral("2020_02_01 00_00_00");
        new DateLiteral("2020{02{01 00{00{00");
        new DateLiteral("2020}02}01 00}00}00");
        new DateLiteral("2020[02[01 00[00[00");
        new DateLiteral("2020]02]01 00]00]00");
        new DateLiteral("2020|02|01 00|00|00");
        new DateLiteral("2020\\02\\01 00\\00\\00");
        new DateLiteral("2020:02:01 00:00:00");
        new DateLiteral("2020;02;01 00;00;00");
        new DateLiteral("2020\"02\"01 00\"00\"00");
        new DateLiteral("2020'02'01 00'00'00");
        new DateLiteral("2020<02<01 00<00<00");
        new DateLiteral("2020>02>01 00>00>00");
        new DateLiteral("2020,02,01 00,00,00");
        new DateLiteral("2020.02.01 00.00.00");
        new DateLiteral("2020?02?01 00?00?00");
        new DateLiteral("2020/02/01 00/00/00");
        new DateLiteral("2020~02~01 00~00~00");
        new DateLiteral("2020`02`01 00`00`00");
    }

    @Test
    void testPoint() {
        new DateLiteral("2020.02.01");
        new DateLiteral("2020.02.01 00.00.00");
        new DateTimeV2Literal("2020.02.01 00.00.00.1");
        new DateTimeV2Literal("2020.02.01 00.00.00.000001");
        new DateTimeV2Literal("2020.02.01 00.00.00.0000001");
    }

    @Test
    void testSuffixSpace() {
        new DateLiteral("2016-07-02  ");
        new DateLiteral("2016-07-02 00:00:00  ");
    }

    @Test
    void testUncheckedCastTo() {
        DateLiteral v1 = new DateLiteral(2025, 7, 23);
        Expression expression = v1.uncheckedCastTo(IntegerType.INSTANCE);
        Assertions.assertInstanceOf(IntegerLiteral.class, expression);
        Assertions.assertEquals(20250723, ((IntegerLiteral) expression).getValue().intValue());

        expression = v1.uncheckedCastTo(BigIntType.INSTANCE);
        Assertions.assertInstanceOf(BigIntLiteral.class, expression);
        Assertions.assertEquals(20250723, ((BigIntLiteral) expression).getValue().intValue());

        expression = v1.uncheckedCastTo(LargeIntType.INSTANCE);
        Assertions.assertInstanceOf(LargeIntLiteral.class, expression);
        Assertions.assertEquals(20250723, ((LargeIntLiteral) expression).getValue().intValue());

        expression = v1.uncheckedCastTo(FloatType.INSTANCE);
        Assertions.assertInstanceOf(FloatLiteral.class, expression);
        Assertions.assertEquals(20250723, ((FloatLiteral) expression).getValue().floatValue());

        expression = v1.uncheckedCastTo(DoubleType.INSTANCE);
        Assertions.assertInstanceOf(DoubleLiteral.class, expression);
        Assertions.assertEquals(20250723, ((DoubleLiteral) expression).getValue().doubleValue());

        expression = v1.uncheckedCastTo(DateTimeType.INSTANCE);
        Assertions.assertInstanceOf(DateTimeLiteral.class, expression);
        DateTimeLiteral datetime = (DateTimeLiteral) expression;
        Assertions.assertEquals(2025, datetime.year);
        Assertions.assertEquals(7, datetime.month);
        Assertions.assertEquals(23, datetime.day);
        Assertions.assertEquals(0, datetime.hour);
        Assertions.assertEquals(0, datetime.minute);
        Assertions.assertEquals(0, datetime.second);
        Assertions.assertEquals(0, datetime.microSecond);

        expression = v1.uncheckedCastTo(DateTimeV2Type.MAX);
        Assertions.assertInstanceOf(DateTimeV2Literal.class, expression);
        DateTimeV2Literal datetime2 = (DateTimeV2Literal) expression;
        Assertions.assertEquals(2025, datetime2.year);
        Assertions.assertEquals(7, datetime2.month);
        Assertions.assertEquals(23, datetime2.day);
        Assertions.assertEquals(0, datetime2.hour);
        Assertions.assertEquals(0, datetime2.minute);
        Assertions.assertEquals(0, datetime2.second);
        Assertions.assertEquals(0, datetime2.microSecond);

        expression = v1.uncheckedCastTo(DateV2Type.INSTANCE);
        Assertions.assertInstanceOf(DateV2Literal.class, expression);
        DateV2Literal date2 = (DateV2Literal) expression;
        Assertions.assertEquals(2025, date2.year);
        Assertions.assertEquals(7, date2.month);
        Assertions.assertEquals(23, date2.day);

        DateV2Literal v2 = new DateV2Literal(2025, 7, 23);
        expression = v2.uncheckedCastTo(DateType.INSTANCE);
        Assertions.assertInstanceOf(DateLiteral.class, expression);
        DateLiteral date = (DateLiteral) expression;
        Assertions.assertEquals(2025, date.year);
        Assertions.assertEquals(7, date.month);
        Assertions.assertEquals(23, date.day);
    }
}
