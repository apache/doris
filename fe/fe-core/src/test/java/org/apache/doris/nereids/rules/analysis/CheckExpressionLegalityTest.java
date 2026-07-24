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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.CheckLegalityAfterRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Percentile;
import org.apache.doris.nereids.trees.expressions.functions.agg.PercentileApprox;
import org.apache.doris.nereids.trees.expressions.functions.agg.PercentileArray;
import org.apache.doris.nereids.trees.expressions.functions.agg.WindowFunnelV2;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CheckExpressionLegalityTest implements MemoPatternMatchSupported {
    @Test
    public void testAvg() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class, "Can not find the compatibility function signature", () -> {
                    PlanChecker.from(connectContext)
                            .analyze("select avg(id) from (select to_bitmap(1) id) tbl");
                });
    }

    @Test
    public void testBitmapCount() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class, "argument should be of BITMAP type", () -> {
                    PlanChecker.from(connectContext)
                            .analyze("select bitmap_count(id) from (select 1 id) tbl");
                });
    }

    @Test
    public void testArraySortLambdaArgumentCount() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "the lambda must be a binary comparator lambda", () -> {
                    PlanChecker.from(connectContext)
                            .analyze("select array_sort(x -> x, [1, 2, 3])");
                });
    }

    @Test
    public void testCountDistinctBitmap() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        PlanChecker.from(connectContext)
                .analyze("select count(distinct id) from (select to_bitmap(1) id) tbl")
                .matches(logicalAggregate().when(agg ->
                    agg.getOutputExpressions().get(0).child(0) instanceof Count
                ))
                .rewrite()
                .matches(logicalAggregate().when(agg ->
                    agg.getOutputExpressions().get(0).child(0) instanceof BitmapUnionCount
                ));

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "COUNT DISTINCT could not process type", () ->
                        PlanChecker.from(connectContext)
                                .analyze("select count(distinct id) from (select to_bitmap(1) id) tbl")
                                .applyBottomUp(new ExpressionRewrite(CheckLegalityAfterRewrite.INSTANCE))
        );
    }

    @Test
    public void testWindowFunnelV2TooManyConditions() {
        List<Expression> arguments = ImmutableList.<Expression>builder()
                .add(new IntegerLiteral(10))
                .add(new StringLiteral("default"))
                .add(new DateTimeLiteral("2022-02-28 00:00:00"))
                .addAll(ImmutableList.copyOf(java.util.Collections.nCopies(
                        WindowFunnelV2.MAX_EVENT_CONDITIONS + 1, BooleanLiteral.TRUE)))
                .build();
        WindowFunnelV2 expression = new WindowFunnelV2(arguments.get(0), arguments.get(1),
                arguments.get(2), arguments.get(3), arguments.subList(4, arguments.size())
                        .toArray(new Expression[0]));

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "supports at most " + WindowFunnelV2.MAX_EVENT_CONDITIONS + " event conditions",
                expression::checkLegalityBeforeTypeCoercion);
    }

    @Test
    public void testPercentileChecksAfterRewrite() {
        Percentile percentile = new Percentile(false, true,
                new IntegerLiteral(1), new org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral(1.2));
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "percentile quantile must be in [0, 1]", percentile::checkLegalityAfterRewrite);
    }

    @Test
    public void testPercentileArrayChecksAfterRewrite() {
        PercentileArray percentileArray = new PercentileArray(false, new IntegerLiteral(1),
                new org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral(
                        ImmutableList.of(
                                new org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral(0.1),
                                new org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral(1.2))));
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "percentile_array quantile must be in [0, 1]", percentileArray::checkLegalityAfterRewrite);
    }

    @Test
    public void testPercentileApproxChecksAfterRewrite() {
        PercentileApprox percentileApprox = new PercentileApprox(false, true,
                new IntegerLiteral(1), new org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral(1.2));
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "percentile_approx quantile must be in [0, 1]",
                percentileApprox::checkLegalityAfterRewrite);
    }
}
