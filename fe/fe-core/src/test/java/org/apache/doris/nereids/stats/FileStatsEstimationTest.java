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

package org.apache.doris.nereids.stats;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.FileType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.statistics.model.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;

class FileStatsEstimationTest {
    private final SlotReference file = new SlotReference("f", FileType.INSTANCE);
    private final SlotReference otherFile = new SlotReference("g", FileType.INSTANCE);
    private final SlotReference condition = new SlotReference("c", BooleanType.INSTANCE);

    @Test
    void testUnionAllPreservesCountsAndPayloadBytesAcrossThreeChildren() {
        SlotReference thirdFile = new SlotReference("h", FileType.INSTANCE);
        LogicalUnion union = new LogicalUnion(Qualifier.ALL, ImmutableList.of(file),
                ImmutableList.of(ImmutableList.of(file), ImmutableList.of(otherFile), ImmutableList.of(thirdFile)),
                ImmutableList.of(), true, ImmutableList.of(new DummyPlan(), new DummyPlan(), new DummyPlan()));
        Statistics result = new StatsCalculator(null).computeUnion(union, ImmutableList.of(
                statistics(100, file, fileStats(100, 100, 0)),
                statistics(100, otherFile, fileStats(100, 0, 1000)),
                statistics(50, thirdFile, fileStats(50, 10, 400))));
        Assertions.assertEquals(250, result.getRowCount());
        assertFileStats(result.findColumnStatistics(file), 250, 110, 1400, 5.6);
        Assertions.assertEquals(110, new FilterEstimation().estimate(new IsNull(file), result).getRowCount());
    }

    @Test
    void testUnionAllEmptyAndAllNullPayloadsStayZero() {
        LogicalUnion union = new LogicalUnion(Qualifier.ALL, ImmutableList.of(file),
                ImmutableList.of(ImmutableList.of(file), ImmutableList.of(otherFile)),
                ImmutableList.of(), true, ImmutableList.of(new DummyPlan(), new DummyPlan()));
        for (double rows : new double[] {0, 100}) {
            Statistics result = new StatsCalculator(null).computeUnion(union, ImmutableList.of(
                    statistics(0, file, fileStats(0, 0, 0)),
                    statistics(rows, otherFile, fileStats(rows, rows, 0))));
            assertFileStats(result.findColumnStatistics(file), rows, rows, 0, 0);
        }
    }

    @Test
    void testUnionAllScalesBytesToInputRowsAndKeepsUnknownUnavailable() {
        LogicalUnion union = new LogicalUnion(Qualifier.ALL, ImmutableList.of(file),
                ImmutableList.of(ImmutableList.of(file), ImmutableList.of(otherFile)),
                ImmutableList.of(), true, ImmutableList.of(new DummyPlan(), new DummyPlan()));
        // A filtered child can retain the original column count and dataSize, but its average is per row.
        Statistics result = new StatsCalculator(null).computeUnion(union, ImmutableList.of(
                statistics(20, file, fileStats(100, 0, 1000)),
                statistics(10, otherFile, fileStats(100, 0, 2000))));
        assertFileStats(result.findColumnStatistics(file), 30, 0, 400, 400.0 / 30);
        result = new StatsCalculator(null).computeUnion(union, ImmutableList.of(
                statistics(20, file, fileStats(20, 0, 200)),
                statistics(10, otherFile, ColumnStatistic.UNKNOWN)));
        Assertions.assertTrue(result.findColumnStatistics(file).isUnKnown);
        Assertions.assertTrue(result.findColumnStatistics(file).ndvUnavailable);
    }

    @Test
    void testUnionAllTypedNullConstantKeepsNullCount() {
        LogicalUnion union = new LogicalUnion(Qualifier.ALL, ImmutableList.of(file),
                ImmutableList.of(ImmutableList.of(file)),
                ImmutableList.of(ImmutableList.of(new Alias(new NullLiteral(FileType.INSTANCE), "n"))),
                true, ImmutableList.of(new DummyPlan()));
        Statistics result = new StatsCalculator(null).computeUnion(union, new ArrayList<>(ImmutableList.of(
                statistics(10, file, fileStats(10, 2, 80)))));
        assertFileStats(result.findColumnStatistics(file), 11, 3, 80, 80.0 / 11);
    }

    @Test
    void testIsNullFileProjectionAndGroupingUseBooleanStatistics() {
        Statistics input = statistics(100, file, fileStats(100, 23, 770));
        IsNull predicate = new IsNull(file);
        ColumnStatistic result = ExpressionEstimation.estimate(predicate, input);
        Assertions.assertFalse(result.ndvUnavailable);
        Assertions.assertEquals(2, result.ndv);
        Assertions.assertEquals(0, result.numNulls);
        Assertions.assertEquals(0, result.minValue);
        Assertions.assertEquals(1, result.maxValue);
        Assertions.assertEquals(1, result.avgSizeByte);
        Assertions.assertEquals(100, result.dataSize);
        Assertions.assertEquals(2, StatsCalculator.estimateGroupByRowCount(ImmutableList.of(predicate), input));
        SlotReference projected = new SlotReference("is_null", BooleanType.INSTANCE);
        Statistics project = statistics(100, projected, result);
        Assertions.assertEquals(0, new FilterEstimation().estimate(new IsNull(projected), project).getRowCount());
        Assertions.assertEquals(23, new FilterEstimation().estimate(predicate, input).getRowCount());
    }

    @Test
    void testIsNullFileUniformAndEmptyInputs() {
        for (double[] sample : new double[][] {{100, 0, 1, 0, 0}, {100, 100, 1, 1, 1}, {0, 0, 0, 0, 0}}) {
            Statistics input = statistics(sample[0], file, fileStats(sample[0], sample[1], 0));
            ColumnStatistic result = ExpressionEstimation.estimate(new IsNull(file), input);
            Assertions.assertEquals(sample[2], result.ndv);
            Assertions.assertEquals(sample[3], result.minValue);
            Assertions.assertEquals(sample[4], result.maxValue);
            Assertions.assertEquals(0, result.numNulls);
            Assertions.assertFalse(result.ndvUnavailable);
        }
    }

    @Test
    void testFileConditionalsKeepAllNullAndPayloadFacts() {
        for (double[] sample : new double[][] {{100, 0, 0}, {23, 770, 7.7}, {0, 1000, 10}}) {
            Statistics input = twoFiles(fileStats(100, sample[0], sample[1]),
                    fileStats(100, sample[0], sample[1]));
            for (Expression expression : ImmutableList.of(new If(condition, file, otherFile),
                    new CaseWhen(ImmutableList.of(new WhenClause(condition, file)), otherFile))) {
                ColumnStatistic result = ExpressionEstimation.estimate(expression, input);
                assertFileStats(result, 100, sample[0], sample[1], sample[2]);
                Statistics project = statistics(100, file, result);
                Assertions.assertEquals(sample[0],
                        new FilterEstimation().estimate(new IsNull(file), project).getRowCount());
            }
        }
    }

    @Test
    void testFileConditionalsWeightPayloadAndIncludeImplicitNullElse() {
        Statistics input = twoFiles(fileStats(100, 20, 800), fileStats(100, 60, 400));
        // Unknown Boolean conditions use the existing 50% filter estimate.
        assertFileStats(ExpressionEstimation.estimate(new If(condition, file, otherFile), input),
                100, 40, 600, 6);
        assertFileStats(ExpressionEstimation.estimate(
                new CaseWhen(ImmutableList.of(new WhenClause(condition, file))), input), 100, 60, 400, 4);
        CaseWhen multiple = new CaseWhen(ImmutableList.of(new WhenClause(condition, file),
                new WhenClause(new SlotReference("d", BooleanType.INSTANCE), otherFile)));
        // Sequential branch probabilities: 1/2, 1/4, and 1/4 implicit NULL.
        assertFileStats(ExpressionEstimation.estimate(multiple, input), 100, 50, 500, 5);
    }

    @Test
    void testFileConditionalsUseCollectedConditionSelectivity() {
        Statistics input = twoFiles(fileStats(100, 20, 800), fileStats(100, 60, 400));
        SlotReference key = new SlotReference("k", IntegerType.INSTANCE);
        input.addColumnStats(key, new ColumnStatisticBuilder(100).setNdv(4).setNumNulls(25)
                .setMinValue(0).setMaxValue(3).setAvgSizeByte(4).build());
        // The condition is TRUE on 25 rows: 25% of the first payload and 75% of the second.
        IsNull predicate = new IsNull(key);
        assertFileStats(ExpressionEstimation.estimate(new If(predicate, file, otherFile), input),
                100, 50, 500, 5);
        assertFileStats(ExpressionEstimation.estimate(
                new CaseWhen(ImmutableList.of(new WhenClause(predicate, file)), otherFile), input),
                100, 50, 500, 5);
    }

    @Test
    void testFileConditionalsOnEmptyInput() {
        Statistics input = statistics(0, file, fileStats(0, 0, 0));
        for (Expression expression : ImmutableList.of(new If(condition, file, otherFile),
                new CaseWhen(ImmutableList.of(new WhenClause(condition, file))))) {
            assertFileStats(ExpressionEstimation.estimate(expression, input), 0, 0, 0, 0);
        }
    }

    @Test
    void testFileConditionalsRespectLiteralConditionsAndTypedNulls() {
        Statistics input = twoFiles(fileStats(100, 23, 770), fileStats(100, 100, 0));
        NullLiteral nullFile = new NullLiteral(FileType.INSTANCE);
        assertFileStats(ExpressionEstimation.estimate(new If(BooleanLiteral.TRUE, file, otherFile), input),
                100, 23, 770, 7.7);
        for (Expression predicate : ImmutableList.of(BooleanLiteral.FALSE, new NullLiteral(BooleanType.INSTANCE))) {
            assertFileStats(ExpressionEstimation.estimate(new If(predicate, file, nullFile), input),
                    100, 100, 0, 0);
        }
        CaseWhen expression = new CaseWhen(ImmutableList.of(new WhenClause(BooleanLiteral.FALSE, file),
                new WhenClause(BooleanLiteral.TRUE, nullFile)), file);
        assertFileStats(ExpressionEstimation.estimate(expression, input), 100, 100, 0, 0);
    }

    @Test
    void testFileConditionalsDoNotTreatUnknownBranchAsKnown() {
        Statistics input = twoFiles(fileStats(100, 23, 770), ColumnStatistic.UNKNOWN);
        ColumnStatistic result = ExpressionEstimation.estimate(new If(condition, file, otherFile), input);
        Assertions.assertTrue(result.isUnKnown);
        Assertions.assertTrue(result.ndvUnavailable);
        // An unreachable unknown branch cannot erase the selected branch's known facts.
        assertFileStats(ExpressionEstimation.estimate(new If(BooleanLiteral.TRUE, file, otherFile), input),
                100, 23, 770, 7.7);
    }

    private Statistics twoFiles(ColumnStatistic first, ColumnStatistic second) {
        return new Statistics(100, new HashMap<>(ImmutableMap.of(file, first, otherFile, second)));
    }

    private static Statistics statistics(double rows, Expression expression, ColumnStatistic column) {
        return new Statistics(rows, new HashMap<>(ImmutableMap.of(expression, column)));
    }

    private static ColumnStatistic fileStats(double rows, double nulls, double bytes) {
        return new ColumnStatisticBuilder(rows).setNdvUnavailable(true).setNumNulls(nulls)
                .setDataSize(bytes).setAvgSizeByte(rows == 0 ? 0 : bytes / rows).build();
    }

    private static void assertFileStats(ColumnStatistic result, double rows, double nulls,
            double bytes, double average) {
        Assertions.assertFalse(result.isUnKnown);
        Assertions.assertTrue(result.ndvUnavailable);
        Assertions.assertEquals(0, result.ndv);
        Assertions.assertEquals(rows, result.count);
        Assertions.assertEquals(nulls, result.numNulls, 1e-9);
        Assertions.assertEquals(bytes, result.dataSize, 1e-9);
        Assertions.assertEquals(average, result.avgSizeByte, 1e-9);
        Assertions.assertNull(result.minExpr);
        Assertions.assertNull(result.maxExpr);
        Assertions.assertNull(result.hotValues);
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, result.minValue);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, result.maxValue);
    }
}
