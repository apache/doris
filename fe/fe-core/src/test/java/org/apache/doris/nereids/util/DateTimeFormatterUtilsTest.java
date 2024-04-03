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

package org.apache.doris.nereids.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.function.Consumer;

class DateTimeFormatterUtilsTest {
    @Test
    void test() {
        DateTimeFormatter formatter = DateTimeFormatterUtils.ZONE_FORMATTER;

        formatter.parse("");

        formatter.parse("UTC+01:00");
        formatter.parse("UTC+01:00:00");

        formatter.parse("GMT+01:00");
        formatter.parse("Asia/Shanghai");
        formatter.parse("Z");
    }

    private void assertDatePart(TemporalAccessor dateTime) {
        Assertions.assertEquals(2020, dateTime.get(ChronoField.YEAR));
        Assertions.assertEquals(2, dateTime.get(ChronoField.MONTH_OF_YEAR));
        Assertions.assertEquals(19, dateTime.get(ChronoField.DAY_OF_MONTH));
    }

    @Test
    void testBasicDateTimeFormatter() {
        DateTimeFormatter formatter = DateTimeFormatterUtils.BASIC_FORMATTER_WITHOUT_T;
        TemporalAccessor dateTime = formatter.parse("20200219");
        assertDatePart(dateTime);
        dateTime = formatter.parse("20200219010101");
        assertDatePart(dateTime);
        dateTime = formatter.parse("20200219010101.1");
        assertDatePart(dateTime);
        dateTime = formatter.parse("20200219010101.000001");
        assertDatePart(dateTime);
        dateTime = formatter.parse("20200219010101.1");
        assertDatePart(dateTime);

        formatter = DateTimeFormatterUtils.BASIC_DATE_TIME_FORMATTER;
        dateTime = formatter.parse("20200219T010101");
        assertDatePart(dateTime);
        dateTime = formatter.parse("20200219T010101.1");
        assertDatePart(dateTime);
        dateTime = formatter.parse("20200219T010101.000001");
        assertDatePart(dateTime);
        dateTime = formatter.parse("20200219T010101.1");
        assertDatePart(dateTime);
        dateTime = formatter.parse("20200219T010101.0000001");
        assertDatePart(dateTime);

        // failed case
        DateTimeFormatter withT = DateTimeFormatterUtils.BASIC_DATE_TIME_FORMATTER;
        Assertions.assertThrows(DateTimeParseException.class, () -> withT.parse("20200219 010101"));
        Assertions.assertThrows(DateTimeParseException.class, () -> withT.parse("20200219010101."));
        Assertions.assertThrows(DateTimeParseException.class, () -> withT.parse("20200219010101.0000001"));
        Assertions.assertThrows(DateTimeParseException.class, () -> withT.parse("20200219T010101."));
        DateTimeFormatter withoutT = DateTimeFormatterUtils.BASIC_FORMATTER_WITHOUT_T;
        Assertions.assertThrows(DateTimeParseException.class, () -> withoutT.parse("20200219 010101"));
        Assertions.assertThrows(DateTimeParseException.class, () -> withoutT.parse("20200219010101."));
        Assertions.assertThrows(DateTimeParseException.class, () -> withoutT.parse("20200219T010101."));
        Assertions.assertThrows(DateTimeParseException.class, () -> withoutT.parse("20200219T010101.0000001"));
    }

    @Test
    void testDateTimeFormatter() {
        DateTimeFormatter formatter = DateTimeFormatterUtils.DATE_TIME_FORMATTER;
        TemporalAccessor dateTime = formatter.parse("2020-02-19 01:01:01");
        assertDatePart(dateTime);
        Assertions.assertThrows(DateTimeParseException.class, () -> formatter.parse("2020-02-19T01:01:01"));
        Assertions.assertThrows(DateTimeParseException.class, () -> formatter.parse("2020-02-1901:01:01"));
    }

    @Test
    void testTimeFormatter() {
        // use lambda function to assert time is correct.
        Consumer<TemporalAccessor> assertTime = (dateTime) ->
                Assertions.assertEquals(1, dateTime.get(ChronoField.HOUR_OF_DAY));

        DateTimeFormatter timeFormatter = DateTimeFormatterUtils.TIME_FORMATTER;
        TemporalAccessor dateTime = timeFormatter.parse("01:01:01.000001");
        assertTime.accept(dateTime);
        dateTime = timeFormatter.parse("01:01:01.1");
        assertTime.accept(dateTime);
        dateTime = timeFormatter.parse("01:01:01");
        assertTime.accept(dateTime);
        Assertions.assertThrows(DateTimeParseException.class, () -> timeFormatter.parse("01:01"));
        Assertions.assertThrows(DateTimeParseException.class, () -> timeFormatter.parse("01"));
    }
}
