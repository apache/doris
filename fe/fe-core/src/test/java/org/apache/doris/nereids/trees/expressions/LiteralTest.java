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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;

import org.junit.jupiter.api.Test;

class LiteralTest {

    @Test
    void testDate() {
        new DateLiteral("2016-07-02");

        new DateLiteral("2016-7-02");
        new DateLiteral("2016-07-2");
        new DateLiteral("2016-7-2");

        new DateLiteral("2016-07-02");
        new DateLiteral("2016-07-2");
        new DateLiteral("2016-7-02");
        new DateLiteral("2016-7-2");
    }

    @Test
    void testDateTime() {
        new DateLiteral("2016-07-02 01:01:00");

        new DateLiteral("2016-7-02 01:01:00");
        new DateLiteral("2016-07-2 01:01:00");
        new DateLiteral("2016-7-2 01:01:00");

        new DateLiteral("2016-07-02 1:01:00");
        new DateLiteral("2016-07-02 01:1:00");
        new DateLiteral("2016-07-02 01:01:0");
        new DateLiteral("2016-07-02 1:1:00");
        new DateLiteral("2016-07-02 1:01:0");
        new DateLiteral("2016-07-02 10:1:0");
        new DateLiteral("2016-07-02 1:1:0");

        new DateLiteral("2016-7-2 1:1:0");
        new DateLiteral("2016-7-02 1:01:0");
        new DateLiteral("2016-07-2 1:1:0");
        new DateLiteral("2016-7-02 01:01:0");
        new DateLiteral("2016-7-2 01:1:0");
    }

    @Test
    void testDateTimeHour() {
        new DateTimeV2Literal("2016-07-02 01");
        new DateTimeV2Literal("2016-07-02 1");

        new DateTimeV2Literal("2016-7-02 1");
        new DateTimeV2Literal("2016-7-02 01");

        new DateTimeV2Literal("2016-07-2 1");
        new DateTimeV2Literal("2016-07-2 01");

        new DateTimeV2Literal("2016-7-2 1");
        new DateTimeV2Literal("2016-7-2 01");
    }

    @Test
    void testDateTimeHourMinute() {
        new DateTimeV2Literal("2016-07-02 01:01");
        new DateTimeV2Literal("2016-07-02 1:01");
        new DateTimeV2Literal("2016-07-02 01:1");
        new DateTimeV2Literal("2016-07-02 1:1");

        new DateTimeV2Literal("2016-7-02 01:01");
        new DateTimeV2Literal("2016-7-02 1:01");
        new DateTimeV2Literal("2016-7-02 01:1");
        new DateTimeV2Literal("2016-7-02 1:1");

        new DateTimeV2Literal("2016-07-2 01:01");
        new DateTimeV2Literal("2016-07-2 1:01");
        new DateTimeV2Literal("2016-07-2 01:1");
        new DateTimeV2Literal("2016-07-2 1:1");

        new DateTimeV2Literal("2016-7-2 01:01");
        new DateTimeV2Literal("2016-7-2 1:01");
        new DateTimeV2Literal("2016-7-2 01:1");
        new DateTimeV2Literal("2016-7-2 1:1");
    }

    @Test
    void testDateTimeHourMinuteSecond() {
        new DateTimeV2Literal("2016-07-02 01:01:01");
        new DateTimeV2Literal("2016-07-02 1:01:01");
        new DateTimeV2Literal("2016-07-02 01:1:01");
        new DateTimeV2Literal("2016-07-02 1:1:01");
        new DateTimeV2Literal("2016-07-02 01:01:1");
        new DateTimeV2Literal("2016-07-02 1:01:1");
        new DateTimeV2Literal("2016-07-02 01:1:1");
        new DateTimeV2Literal("2016-07-02 1:1:1");

        new DateTimeV2Literal("2016-7-02 01:01:01");
        new DateTimeV2Literal("2016-7-02 1:01:01");
        new DateTimeV2Literal("2016-7-02 01:1:01");
        new DateTimeV2Literal("2016-7-02 1:1:01");
        new DateTimeV2Literal("2016-7-02 01:01:1");
        new DateTimeV2Literal("2016-7-02 1:01:1");
        new DateTimeV2Literal("2016-7-02 01:1:1");
        new DateTimeV2Literal("2016-7-02 1:1:1");

        new DateTimeV2Literal("2016-07-2 01:01:01");
        new DateTimeV2Literal("2016-07-2 1:01:01");
        new DateTimeV2Literal("2016-07-2 01:1:01");
        new DateTimeV2Literal("2016-07-2 1:1:01");
        new DateTimeV2Literal("2016-07-2 01:01:1");
        new DateTimeV2Literal("2016-07-2 1:01:1");
        new DateTimeV2Literal("2016-07-2 01:1:1");
        new DateTimeV2Literal("2016-07-2 1:1:1");

        new DateTimeV2Literal("2016-7-2 01:01:01");
        new DateTimeV2Literal("2016-7-2 1:01:01");
        new DateTimeV2Literal("2016-7-2 01:1:01");
        new DateTimeV2Literal("2016-7-2 1:1:01");
        new DateTimeV2Literal("2016-7-2 01:01:1");
        new DateTimeV2Literal("2016-7-2 1:01:1");
        new DateTimeV2Literal("2016-7-2 01:1:1");
        new DateTimeV2Literal("2016-7-2 1:1:1");
    }

    @Test
    void testDateTimeHourMinuteSecondMicrosecond() {
        new DateTimeV2Literal("2016-07-02 01:01:01.1");
        new DateTimeV2Literal("2016-07-02 1:01:01.1");
        new DateTimeV2Literal("2016-07-02 01:1:01.1");
        new DateTimeV2Literal("2016-07-02 1:1:01.1");
        new DateTimeV2Literal("2016-07-02 01:01:1.1");
        new DateTimeV2Literal("2016-07-02 1:01:1.1");
        new DateTimeV2Literal("2016-07-02 01:1:1.1");
        new DateTimeV2Literal("2016-07-02 1:1:1.1");

        new DateTimeV2Literal("2016-7-02 01:01:01.1");
        new DateTimeV2Literal("2016-7-02 1:01:01.1");
        new DateTimeV2Literal("2016-7-02 01:1:01.1");
        new DateTimeV2Literal("2016-7-02 1:1:01.1");
        new DateTimeV2Literal("2016-7-02 01:01:1.1");
        new DateTimeV2Literal("2016-7-02 1:01:1.1");
        new DateTimeV2Literal("2016-7-02 01:1:1.1");
        new DateTimeV2Literal("2016-7-02 1:1:1.1");

        new DateTimeV2Literal("2016-07-2 01:01:01.1");
        new DateTimeV2Literal("2016-07-2 1:01:01.1");
        new DateTimeV2Literal("2016-07-2 01:1:01.1");
        new DateTimeV2Literal("2016-07-2 1:1:01.1");
        new DateTimeV2Literal("2016-07-2 01:01:1.1");
        new DateTimeV2Literal("2016-07-2 1:01:1.1");
        new DateTimeV2Literal("2016-07-2 01:1:1.1");
        new DateTimeV2Literal("2016-07-2 1:1:1.1");

        new DateTimeV2Literal("2016-7-2 01:01:01.1");
        new DateTimeV2Literal("2016-7-2 1:01:01.1");
        new DateTimeV2Literal("2016-7-2 01:1:01.1");
        new DateTimeV2Literal("2016-7-2 1:1:01.1");
        new DateTimeV2Literal("2016-7-2 01:01:1.1");
        new DateTimeV2Literal("2016-7-2 1:01:1.1");
        new DateTimeV2Literal("2016-7-2 01:1:1.1");
        new DateTimeV2Literal("2016-7-2 1:1:1.1");

        // Testing with microsecond of length 2
        new DateTimeV2Literal("2016-07-02 01:01:01.12");
        new DateTimeV2Literal("2016-7-02 01:01:01.12");

        // Testing with microsecond of length 3
        new DateTimeV2Literal("2016-07-02 01:01:01.123");
        new DateTimeV2Literal("2016-7-02 01:01:01.123");

        // Testing with microsecond of length 4
        new DateTimeV2Literal("2016-07-02 01:01:01.1234");
        new DateTimeV2Literal("2016-7-02 01:01:01.1234");

        // Testing with microsecond of length 5
        new DateTimeV2Literal("2016-07-02 01:01:01.12345");
        new DateTimeV2Literal("2016-7-02 01:01:01.12345");

        // Testing with microsecond of length 6
        new DateTimeV2Literal("2016-07-02 01:01:01.123456");
        new DateTimeV2Literal("2016-7-02 01:01:01.123456");
    }
}
