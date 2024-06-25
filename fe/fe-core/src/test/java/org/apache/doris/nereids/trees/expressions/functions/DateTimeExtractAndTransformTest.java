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
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

class DateTimeExtractAndTransformTest {
    @Test
    void testWeekMode2Function() {
        LocalDateTime localDateTime = LocalDateTime.of(0, 1, 1, 0, 0, 0, 0);
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(localDateTime, 2));
        LocalDateTime localDateTime2 = localDateTime.plusDays(1);
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(localDateTime2, 2));
        LocalDateTime localDateTime3 = LocalDateTime.of(0, 1, 9, 0, 0, 0, 0);
        Assertions.assertEquals(new TinyIntLiteral((byte) 2), DateTimeExtractAndTransform.week(localDateTime3, 2));

        LocalDateTime localDateTime4 = LocalDateTime.of(0, 12, 30, 0, 0, 0, 0);
        Assertions.assertEquals(new TinyIntLiteral((byte) 52), DateTimeExtractAndTransform.week(localDateTime4, 2));
        LocalDateTime localDateTime5 = localDateTime4.plusDays(1);
        Assertions.assertEquals(new TinyIntLiteral((byte) 53), DateTimeExtractAndTransform.week(localDateTime5, 2));
    }

    @Test
    void testYearWeekMode2Function() {
        LocalDateTime localDateTime = LocalDateTime.of(0, 1, 1, 0, 0, 0, 0);
        Assertions.assertEquals(new IntegerLiteral(1), DateTimeExtractAndTransform.yearWeek(localDateTime, 2));
        LocalDateTime localDateTime2 = localDateTime.plusDays(1);
        Assertions.assertEquals(new IntegerLiteral(1), DateTimeExtractAndTransform.yearWeek(localDateTime2, 2));
        LocalDateTime localDateTime3 = LocalDateTime.of(0, 1, 9, 0, 0, 0, 0);
        Assertions.assertEquals(new IntegerLiteral(2), DateTimeExtractAndTransform.yearWeek(localDateTime3, 2));

        LocalDateTime localDateTime4 = LocalDateTime.of(0, 12, 30, 0, 0, 0, 0);
        Assertions.assertEquals(new IntegerLiteral(52), DateTimeExtractAndTransform.yearWeek(localDateTime4, 2));
        LocalDateTime localDateTime5 = localDateTime4.plusDays(1);
        Assertions.assertEquals(new IntegerLiteral(53), DateTimeExtractAndTransform.yearWeek(localDateTime5, 2));
    }
}
