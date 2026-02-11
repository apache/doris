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

package org.apache.doris.nereids.trees.expressions.literal.format;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DateTimeCheckerTest {

    @Test
    public void testDateValidate() {
        String literal;
        literal = "20241012";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "2024-10-12";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "2024-1-1";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "2024101";
        Assertions.assertFalse(DateTimeChecker.isValidDateTime(literal));
        literal = "202410123";
        Assertions.assertFalse(DateTimeChecker.isValidDateTime(literal));
        literal = "2024-101";
        Assertions.assertFalse(DateTimeChecker.isValidDateTime(literal));
    }

    @Test
    public void testDateTimeValidate() {
        String literal;
        literal = "20241012010203";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "20241012T010203";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "241012010203";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "241012T010203";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "2024-01-01 01:02:03";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "2024-01-01T01:02:03";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "2024-01-01 01:02";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "2024-01-01 01";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "2024-1-1 1:2:3";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "2024-01-01  ";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
    }

    @Test
    public void testDateTimeWithNanoSecondValidate() {
        String literal;
        literal = "2024-01-01 01:02:03.123456";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "2024-01-01 01:02:03.1";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "2024-01-01 01:02:03.123";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "2024-01-01 01:02:03.";
        Assertions.assertFalse(DateTimeChecker.isValidDateTime(literal));
    }

    @Test
    public void testTimestamptzValidate() {
        String literal;
        literal = "2024-01-01 01:02:03Z";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone(literal));
        literal = "2024-01-01 01:02:03+08:00";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone(literal));
        literal = "2024-01-01 01:02:03-05:00";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone(literal));
        literal = "2024-01-01 01:02:03UTC+08:00";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone(literal));
        literal = "2024-01-01 01:02:03Europe/London";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone(literal));
        literal = "2024-01-01 01:02:03America/New_York";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone(literal));
        literal = "2024-01-01 01:02:03  +08:00";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone(literal));
        literal = "2024-01-01 01:02:03+08";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone(literal));
        literal = "2024-01-01";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        Assertions.assertFalse(DateTimeChecker.hasTimeZone(literal));
        literal = "2024-01-01 01:02:03";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        Assertions.assertFalse(DateTimeChecker.hasTimeZone(literal));
    }

    @Test
    public void testInvalidDateTime() {
        String literal;
        literal = "2024-ab-01";
        Assertions.assertFalse(DateTimeChecker.isValidDateTime(literal));
        literal = "2024-01-01 ab:cd:ef";
        Assertions.assertFalse(DateTimeChecker.isValidDateTime(literal));
        literal = "";
        Assertions.assertFalse(DateTimeChecker.isValidDateTime(literal));
        literal = "2024@01@01";
        Assertions.assertFalse(DateTimeChecker.isValidDateTime(literal));
        literal = "2024/01/01";
        Assertions.assertFalse(DateTimeChecker.isValidDateTime(literal));
    }

    @Test
    public void testTrimming() {
        String literal;
        literal = "  2024-01-01 01:02:03  ";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "  2024-01-01";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
        literal = "2024-01-01  ";
        Assertions.assertTrue(DateTimeChecker.isValidDateTime(literal));
    }

    @Test
    public void testCompactDateTimeWithTimeZone() {
        Assertions.assertTrue(DateTimeChecker.hasTimeZone("20241012010203Z"));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone("20241012010203z"));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone("20241012010203.1145141Zulu"));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone("20241012010203zulu"));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone("20241012010203+08:00"));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone("241012010203+08:00"));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone("2024-01-01 12:00:00 uTc"));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone("2024-01-01 12:00:00     Gmt"));

        Assertions.assertTrue(DateTimeChecker.hasTimeZone("20251010111213.123456+08:00"));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone("20251010111213.123Z"));
        Assertions.assertTrue(DateTimeChecker.hasTimeZone("241010111213.123+08:00"));

        Assertions.assertFalse(DateTimeChecker.hasTimeZone("20241012010203"));
        Assertions.assertFalse(DateTimeChecker.hasTimeZone("2024-01-01 12:00:00"));
        Assertions.assertFalse(DateTimeChecker.hasTimeZone("20241012"));
        Assertions.assertFalse(DateTimeChecker.hasTimeZone("2024-10-12"));
        Assertions.assertFalse(DateTimeChecker.hasTimeZone("25-10-12"));
    }
}
