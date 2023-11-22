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
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Test;

public class CheckExpressionLegalityTest implements MemoPatternMatchSupported {
    @Test
    public void testAvg() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class, "avg requires a numeric parameter", () -> {
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
}
