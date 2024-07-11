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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

class DateTimeExtractAndTransformTest {
    @Test
    void testSpecialDateWeeks() {
        // test week/yearweek for 0000-01-01/02
        LocalDateTime time1 = LocalDateTime.of(0, 1, 1, 0, 0, 0, 0);
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time1, 0));
        Assertions.assertEquals(new TinyIntLiteral((byte) 0), DateTimeExtractAndTransform.week(time1, 1));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time1, 2));
        Assertions.assertEquals(new TinyIntLiteral((byte) 52), DateTimeExtractAndTransform.week(time1, 3));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time1, 4));
        Assertions.assertEquals(new TinyIntLiteral((byte) 0), DateTimeExtractAndTransform.week(time1, 5));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time1, 6));
        Assertions.assertEquals(new TinyIntLiteral((byte) 52), DateTimeExtractAndTransform.week(time1, 7));

        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time1, 0));
        Assertions.assertEquals(new TinyIntLiteral((byte) 0), DateTimeExtractAndTransform.yearWeek(time1, 1));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time1, 2));
        Assertions.assertEquals(new TinyIntLiteral((byte) 52), DateTimeExtractAndTransform.yearWeek(time1, 3));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time1, 4));
        Assertions.assertEquals(new TinyIntLiteral((byte) 0), DateTimeExtractAndTransform.yearWeek(time1, 5));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time1, 6));
        Assertions.assertEquals(new TinyIntLiteral((byte) 52), DateTimeExtractAndTransform.yearWeek(time1, 7));

        LocalDateTime time2 = LocalDateTime.of(0, 1, 2, 0, 0, 0, 0);
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 0));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 1));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 2));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 3));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 4));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 5));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 6));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 7));

        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 0));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 1));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 2));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 3));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 4));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 5));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 6));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 7));
    }
}
