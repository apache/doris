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

class DateTimeFormatterUtilsTest {
    private void assertDatePart(TemporalAccessor dateTime) {
        Assertions.assertEquals(2020, dateTime.get(ChronoField.YEAR));
        Assertions.assertEquals(2, dateTime.get(ChronoField.MONTH_OF_YEAR));
        Assertions.assertEquals(19, dateTime.get(ChronoField.DAY_OF_MONTH));
    }

    @Test
    void testBasicDateTimeFormatter() {
        DateTimeFormatter formatter = DateTimeFormatterUtils.BASIC_DATE_TIME_FORMATTER;
        TemporalAccessor dateTime = formatter.parse("20200219");
        assertDatePart(dateTime);
        dateTime = formatter.parse("20200219010101");
        assertDatePart(dateTime);
        dateTime = formatter.parse("20200219T010101");
        assertDatePart(dateTime);
        // failed case
        Assertions.assertThrows(DateTimeParseException.class, () -> formatter.parse("20200219 010101"));

        // microsecond
        dateTime = formatter.parse("20200219010101.000001");
        assertDatePart(dateTime);
        dateTime = formatter.parse("20200219T010101.000001");
        assertDatePart(dateTime);
        dateTime = formatter.parse("20200219010101.1");
        assertDatePart(dateTime);
        dateTime = formatter.parse("20200219T010101.1");
        assertDatePart(dateTime);
        Assertions.assertThrows(DateTimeParseException.class, () -> formatter.parse("20200219010101."));
        Assertions.assertThrows(DateTimeParseException.class, () -> formatter.parse("20200219010101.0000001"));
        Assertions.assertThrows(DateTimeParseException.class, () -> formatter.parse("20200219T010101."));
        Assertions.assertThrows(DateTimeParseException.class, () -> formatter.parse("20200219T010101.0000001"));
    }
}
