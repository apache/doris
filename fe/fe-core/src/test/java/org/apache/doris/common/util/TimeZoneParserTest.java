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

package org.apache.doris.common.util;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.trees.expressions.literal.format.DateTimeChecker;
import org.apache.doris.nereids.types.TimeStampTzType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.DateTimeException;

class TimeZoneParserTest {
    @Test
    void sessionBoundaries() {
        for (String prefix : new String[] {"", "UTC", "GMT", "utc", "gmt"}) {
            for (String offset : new String[] {"+14:01", "+14:30", "+14:45", "-12:01", "-12:30",
                    "-12:45", "+15:00", "-13:00", "+08:60"}) {
                Assertions.assertThrows(DateTimeException.class,
                        () -> TimeZoneParser.parse(prefix + offset, TimeZoneParser.Usage.SESSION), prefix + offset);
            }
            for (String offset : new String[] {"+14:00", "-12:00", "+08:01", "+08:15", "-00:30"}) {
                Assertions.assertNotNull(TimeZoneParser.parse(prefix + offset, TimeZoneParser.Usage.SESSION));
            }
        }
    }

    @Test
    void offsetSyntaxCannotBypassValidation() {
        for (TimeZoneParser.Usage usage : TimeZoneParser.Usage.values()) {
            for (String value : new String[] {"UTC8", "GMT8:30", "UTC0", "GMT0:00"}) {
                Assertions.assertThrows(DateTimeException.class,
                        () -> TimeZoneParser.normalize(value, usage), usage + ": " + value);
            }
        }
        for (String value : new String[] {"+00:00:00", "-00:00:00", "+0000", "+00"}) {
            Assertions.assertThrows(DateTimeException.class,
                    () -> TimeZoneParser.normalize(value, TimeZoneParser.Usage.SESSION), value);
        }
        Assertions.assertThrows(DateTimeException.class,
                () -> TimeZoneParser.normalize("+00:00:00", TimeZoneParser.Usage.DATETIME));
        Assertions.assertEquals("+00:00",
                TimeZoneParser.normalize("+00:00:00", TimeZoneParser.Usage.TIMESTAMPTZ));
        for (String value : new String[] {"UT", "ut", "Z", "z"}) {
            Assertions.assertEquals("UTC", TimeZoneParser.normalize(value, TimeZoneParser.Usage.DATETIME));
        }
    }

    @Test
    void namesAndSyntax() throws Exception {
        Assertions.assertEquals("UTC", TimeUtils.checkTimeZoneValidAndStandardize("utc"));
        Assertions.assertThrows(DateTimeException.class,
                () -> TimeUtils.checkTimeZoneValidAndStandardize("asia/shanghai"));
        for (String value : new String[] {"+8", "+08", "+0800", "+8:00"}) {
            Assertions.assertEquals("+08:00", TimeZoneParser.normalize(value, TimeZoneParser.Usage.DATETIME));
        }
        for (String value : new String[] {"+14:30", "-14:45", "+08:01", "+08:15", "+08:00:01"}) {
            Assertions.assertThrows(DateTimeException.class,
                    () -> TimeZoneParser.normalize(value, TimeZoneParser.Usage.DATETIME));
        }
        Assertions.assertEquals("-14:00", TimeZoneParser.normalize("-14:00", TimeZoneParser.Usage.DATETIME));
        Assertions.assertEquals("+08:05:43",
                TimeZoneParser.normalize("+08:05:43", TimeZoneParser.Usage.TIMESTAMPTZ));
        Assertions.assertEquals("+18:00",
                TimeZoneParser.normalize("+18:00", TimeZoneParser.Usage.TIMESTAMPTZ));
        for (String value : new String[] {"+18:01", "+19:00", "-18:01", "-19:00"}) {
            Assertions.assertThrows(DateTimeException.class,
                    () -> TimeZoneParser.normalize(value, TimeZoneParser.Usage.TIMESTAMPTZ));
        }
    }

    @Test
    void literalSuffixes() {
        Assertions.assertTrue(DateTimeChecker.hasTimeZone("2024-01-02 12:00:00 America/Port-au-Prince"));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone("2024-01-02 12:00:00 Etc/GMT+3"));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone("2024-01-02 12:00:00+0800"));
        Assertions.assertDoesNotThrow(() -> new DateTimeV2Literal("2024-01-02 12:00:00-14:00"));
        Assertions.assertDoesNotThrow(() -> new DateTimeV2Literal("2024-01-02 12:00:00+8:30"));
        Assertions.assertDoesNotThrow(() -> new DateTimeV2Literal("2024-01-02 12:00:00 UTC+8"));
        Assertions.assertDoesNotThrow(() -> new TimestampTzLiteral("1900-01-01 08:05:43+08:05:43"));
        Assertions.assertDoesNotThrow(() -> new TimestampTzLiteral("20240102120000+0800"));
        for (String suffix : new String[] {"UTC", "CST", "Asia/Shanghai",
                "GMT0", "EST5EDT", "W-SU"}) {
            Assertions.assertTrue(DateTimeChecker.hasTimeZone("2024-01-02 12:00:00 " + suffix), suffix);
            Assertions.assertTrue(DateTimeChecker.hasTimeZone("20240102120000" + suffix), suffix);
            Assertions.assertEquals(new TimestampTzLiteral("2024-01-02 12:00:00 " + suffix).getValue(),
                    new TimestampTzLiteral("20240102120000" + suffix).getValue(), suffix);
            Assertions.assertEquals(new TimestampTzLiteral("2024-01-02 12:00:00 " + suffix).getValue(),
                    new TimestampTzLiteral("20240102T120000" + suffix).getValue(), suffix);
            Assertions.assertEquals(new TimestampTzLiteral("2024-01-02 12:00:00 " + suffix).getValue(),
                    TimestampTzLiteral.fromTimeZone(TimeStampTzType.SYSTEM_DEFAULT,
                            "20240102120000" + suffix, "+08:00").getValue(), suffix);
        }
        Assertions.assertThrows(AnalysisException.class,
                () -> new DateTimeV2Literal("2024-01-02 12:00:00+08:01"));
        Assertions.assertThrows(AnalysisException.class,
                () -> new TimestampTzLiteral("2024-01-02 12:00:00 GMT-13:00"));
    }
}
