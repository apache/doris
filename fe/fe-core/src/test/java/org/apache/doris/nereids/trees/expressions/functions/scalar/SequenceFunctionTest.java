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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.parser.ParserTestBase;
import org.apache.doris.nereids.rules.analysis.DatetimeFunctionBinder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for sequence function parsing.
 */
public class SequenceFunctionTest extends ParserTestBase {

    @Test
    public void testSequenceWithOneIntArg() {
        parseExpression("sequence(5)")
                .assertEquals(parseExpression("sequence(5)").getExpression());
    }

    @Test
    public void testSequenceWithTwoIntArgs() {
        parseExpression("sequence(1, 5)")
                .assertEquals(parseExpression("sequence(1, 5)").getExpression());
    }

    @Test
    public void testSequenceWithThreeIntArgs() {
        parseExpression("sequence(1, 10, 2)")
                .assertEquals(parseExpression("sequence(1, 10, 2)").getExpression());
    }

    @Test
    public void testSequenceWithDatetimeArgs() {
        parseExpression(
                "sequence(cast('2024-01-01' as datetime), cast('2024-01-10' as datetime))")
                .assertEquals(parseExpression(
                        "sequence(CAST('2024-01-01' AS DATETIMEV2(0)), CAST('2024-01-10' AS DATETIMEV2(0)))")
                        .getExpression());
    }

    @Test
    public void testSequenceWithDatetimeAndInterval() {
        parseExpression(
                "sequence(cast('2024-01-01' as datetime), cast('2024-01-10' as datetime), 1)")
                .assertEquals(parseExpression(
                        "sequence(CAST('2024-01-01' AS DATETIMEV2(0)), CAST('2024-01-10' AS DATETIMEV2(0)), 1)")
                        .getExpression());
    }

    @Test
    public void testSequenceDayUnit() {
        String sql = "sequence(cast('2024-01-01' as datetime), cast('2024-01-10' as datetime), INTERVAL 1 day)";
        Assertions.assertEquals(
                "sequence_day_unit(cast('2024-01-01' as DATETIMEV2(0)), cast('2024-01-10' as DATETIMEV2(0)), 1)",
                DatetimeFunctionBinder.INSTANCE.bind(
                        (UnboundFunction) parseExpression(sql).getExpression()).toSql());
    }

    @Test
    public void testSequenceHourUnit() {
        String sql = "sequence(cast('2024-01-01' as datetime), cast('2024-01-10' as datetime), INTERVAL 1 hour)";
        Assertions.assertEquals(
                "sequence_hour_unit(cast('2024-01-01' as DATETIMEV2(0)), cast('2024-01-10' as DATETIMEV2(0)), 1)",
                DatetimeFunctionBinder.INSTANCE.bind(
                        (UnboundFunction) parseExpression(sql).getExpression()).toSql());
    }

    @Test
    public void testSequenceMinuteUnit() {
        String sql = "sequence(cast('2024-01-01' as datetime), cast('2024-01-10' as datetime), INTERVAL 1 minute)";
        Assertions.assertEquals(
                "sequence_minute_unit(cast('2024-01-01' as DATETIMEV2(0)), cast('2024-01-10' as DATETIMEV2(0)), 1)",
                DatetimeFunctionBinder.INSTANCE.bind(
                        (UnboundFunction) parseExpression(sql).getExpression()).toSql());
    }

    @Test
    public void testSequenceSecondUnit() {
        String sql = "sequence(cast('2024-01-01' as datetime), cast('2024-01-10' as datetime), INTERVAL 1 second)";
        Assertions.assertEquals(
                "sequence_second_unit(cast('2024-01-01' as DATETIMEV2(0)), cast('2024-01-10' as DATETIMEV2(0)), 1)",
                DatetimeFunctionBinder.INSTANCE.bind(
                        (UnboundFunction) parseExpression(sql).getExpression()).toSql());
    }

    @Test
    public void testSequenceMonthUnit() {
        String sql = "sequence(cast('2024-01-01' as datetime), cast('2024-12-31' as datetime), INTERVAL 1 month)";
        Assertions.assertEquals(
                "sequence_month_unit(cast('2024-01-01' as DATETIMEV2(0)), cast('2024-12-31' as DATETIMEV2(0)), 1)",
                DatetimeFunctionBinder.INSTANCE.bind(
                        (UnboundFunction) parseExpression(sql).getExpression()).toSql());
    }

    @Test
    public void testSequenceQuarterUnit() {
        String sql = "sequence(cast('2024-01-01' as datetime), cast('2024-12-31' as datetime), INTERVAL 1 quarter)";
        Assertions.assertEquals(
                "sequence_quarter_unit(cast('2024-01-01' as DATETIMEV2(0)), cast('2024-12-31' as DATETIMEV2(0)), 1)",
                DatetimeFunctionBinder.INSTANCE.bind(
                        (UnboundFunction) parseExpression(sql).getExpression()).toSql());
    }

    @Test
    public void testSequenceYearUnit() {
        String sql = "sequence(cast('2024-01-01' as datetime), cast('2026-12-31' as datetime), INTERVAL 1 year)";
        Assertions.assertEquals(
                "sequence_year_unit(cast('2024-01-01' as DATETIMEV2(0)), cast('2026-12-31' as DATETIMEV2(0)), 1)",
                DatetimeFunctionBinder.INSTANCE.bind(
                        (UnboundFunction) parseExpression(sql).getExpression()).toSql());
    }

    @Test
    public void testSequenceWeekUnit() {
        String sql = "sequence(cast('2024-01-01' as datetime), cast('2024-02-01' as datetime), INTERVAL 1 week)";
        Assertions.assertEquals(
                "sequence_week_unit(cast('2024-01-01' as DATETIMEV2(0)), cast('2024-02-01' as DATETIMEV2(0)), 1)",
                DatetimeFunctionBinder.INSTANCE.bind(
                        (UnboundFunction) parseExpression(sql).getExpression()).toSql());
    }

    @Test
    public void testSequenceWithMultipleDays() {
        String sql = "sequence(cast('2024-01-01' as datetime), cast('2024-01-10' as datetime), INTERVAL 2 day)";
        Assertions.assertEquals(
                "sequence_day_unit(cast('2024-01-01' as DATETIMEV2(0)), cast('2024-01-10' as DATETIMEV2(0)), 2)",
                DatetimeFunctionBinder.INSTANCE.bind(
                        (UnboundFunction) parseExpression(sql).getExpression()).toSql());
    }

    @Test
    public void testSequenceInSelect() {
        parseExpression("sequence(1, 5)")
                .assertEquals(parseExpression("sequence(1, 5)").getExpression());
    }

    @Test
    public void testSequenceWithColumn() {
        parseExpression("sequence(id, id + 10)")
                .assertEquals(parseExpression("sequence(`id`, `id` + 10)").getExpression());
    }
}
