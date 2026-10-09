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

package org.apache.doris.nereids.trees.expressions.functions.executable;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ExpressionEvaluator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TQueryGlobals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

class DateTimeAcquireTest {
    @Test
    void testNowPrecisionUsesMatchingLiteralType() {
        Expression microsecond = DateTimeAcquire.now(new IntegerLiteral(6));
        Assertions.assertInstanceOf(DateTimeV2Literal.class, microsecond);

        for (int precision = 7; precision <= 9; precision++) {
            Expression evaluated = ExpressionEvaluator.INSTANCE.eval(new Now(new IntegerLiteral(precision)));
            Assertions.assertInstanceOf(TimeStampNsLiteral.class, evaluated);
            TimeStampNsLiteral timestampNs = (TimeStampNsLiteral) evaluated;
            long factor = (long) Math.pow(10, 9 - precision);
            Assertions.assertEquals(0, timestampNs.getNanoSecond() % factor);
        }
    }

    @Test
    void testCurrentTimestampNanosecondPrecision() {
        Expression evaluated = DateTimeAcquire.currentTimestamp(new IntegerLiteral(7));
        Assertions.assertInstanceOf(TimeStampNsLiteral.class, evaluated);
        TimeStampNsLiteral timestampNs = (TimeStampNsLiteral) evaluated;
        Assertions.assertEquals(0, timestampNs.getNanoSecond() % 100);
    }

    @Test
    void testCurrentTimestampUsesStatementStartTime() {
        Instant statementStart = Instant.parse("2024-02-29T12:34:56.123456789Z");
        ConnectContext context = Mockito.mock(ConnectContext.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setTimeZone("UTC");
        Mockito.when(context.getStartTimeInstant()).thenReturn(statementStart);
        Mockito.when(context.getSessionVariable()).thenReturn(sessionVariable);
        try (MockedStatic<ConnectContext> mockedContext = Mockito.mockStatic(ConnectContext.class)) {
            mockedContext.when(ConnectContext::get).thenReturn(context);
            TimeStampNsLiteral expected = TimeStampNsLiteral.fromJavaDateType(
                    LocalDateTime.ofInstant(statementStart, ZoneId.of("UTC")));

            Assertions.assertEquals(expected, DateTimeAcquire.now(new IntegerLiteral(9)));
            Assertions.assertEquals(expected, DateTimeAcquire.currentTimestamp(new IntegerLiteral(9)));

            LocalDateTime localStart = LocalDateTime.ofInstant(statementStart, ZoneId.of("UTC"));
            DateTimeV2Literal expectedSecond = DateTimeV2Literal.fromJavaDateType(localStart, 0);
            DateV2Literal expectedDate = (DateV2Literal) DateV2Literal.fromJavaDateType(localStart);
            TimeV2Literal expectedTime = (TimeV2Literal) TimeV2Literal.fromJavaDateType(localStart);
            TimeV2Literal expectedMicrosecondTime = (TimeV2Literal) TimeV2Literal.fromJavaDateType(localStart, 6);

            Assertions.assertEquals(expectedSecond, DateTimeAcquire.localTime());
            Assertions.assertEquals(expectedSecond, DateTimeAcquire.localTimestamp());
            Assertions.assertEquals(expectedDate, DateTimeAcquire.curDate());
            Assertions.assertEquals(expectedDate, DateTimeAcquire.currentDate());
            Assertions.assertEquals(expectedTime, DateTimeAcquire.curTime());
            Assertions.assertEquals(expectedTime, DateTimeAcquire.currentTime());
            Assertions.assertEquals(expectedMicrosecondTime,
                    DateTimeAcquire.curTime(new TinyIntLiteral((byte) 6)));
            Assertions.assertEquals(expectedMicrosecondTime,
                    DateTimeAcquire.currentTime(new TinyIntLiteral((byte) 6)));
            Assertions.assertEquals(statementStart.getEpochSecond(),
                    ((BigIntLiteral) DateTimeAcquire.unixTimestamp()).getValue());
            Assertions.assertEquals(expectedSecond, DateTimeAcquire.utcTimestamp());
            Assertions.assertEquals(expected, DateTimeAcquire.utcTimestamp(new IntegerLiteral(9)));

            TQueryGlobals queryGlobals = CoordinatorContext.createQueryGlobals(context);
            Assertions.assertEquals(statementStart.toEpochMilli(), queryGlobals.getTimestampMs());
            Assertions.assertEquals(statementStart.getNano(), queryGlobals.getNanoSeconds());
        }
    }
}
