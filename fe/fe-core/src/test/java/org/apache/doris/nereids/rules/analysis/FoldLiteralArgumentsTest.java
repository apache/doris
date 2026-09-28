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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapExprCalculate;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapExprCalculateCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.SequenceCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.SequenceMatch;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopN;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopNArray;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopNWeighted;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayApply;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RegexpReplace;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RegexpReplaceOne;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sha2;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SplitByRegexp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Tokenize;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Uniform;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UtcTime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UtcTimestamp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WidthBucket;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class FoldLiteralArgumentsTest {

    @Test
    public void testScalarFunctions() {
        assertLiteral(analyze("select sha2('abc', 200 + 56)", Sha2.class).child(1));
        assertLiteral(analyze("select split_by_regexp('a,b,c', ',', 1 + 1)", SplitByRegexp.class).child(2));
        assertLiteral(analyze("select regexp_replace('abc', 'a', 'b', concat('', ''))",
                RegexpReplace.class).child(3));
        assertLiteral(analyze("select regexp_replace_one('abc', 'a', 'b', concat('', ''))",
                RegexpReplaceOne.class).child(3));
        assertLiteral(analyze("select tokenize('x', concat('\"parser\"=\"', 'english\"'))",
                Tokenize.class).child(1));
        // 2 + 3 folds to a SMALLINT literal, narrowed to the TINYINT parameter by a cast
        Expression bucketNum = analyze("select width_bucket(1.5, 0, 10, 2 + 3)", WidthBucket.class).child(3);
        Assertions.assertInstanceOf(Cast.class, bucketNum, bucketNum.toSql());
        assertLiteral(bucketNum.child(0));
        assertLiteral(analyze("select array_apply([1, 2, 3], concat('>', '='), 2)", ArrayApply.class).child(1));
        assertLiteral(analyze("select date_trunc(cast('2024-03-15 10:00:00' as datetime), concat('mon', 'th'))",
                DateTrunc.class).child(1));
        assertLiteral(analyze("select date_trunc(concat('ye', 'ar'), cast('2024-03-15 10:00:00' as datetime))",
                DateTrunc.class).child(0));
        // a string date value is not folded when the time unit is already a literal,
        // so the return type is still derived from a non-literal string
        DateTrunc stringValue = analyze("select date_trunc(concat('2024-03-15', ' 10:00:00'), 'month')",
                DateTrunc.class);
        Assertions.assertFalse(stringValue.child(0) instanceof Literal, stringValue.toSql());
        Assertions.assertEquals(DateTimeV2Type.of(6), stringValue.getDataType());

        Random rand = analyze("select rand(1 + 1)", Random.class);
        assertLiteral(rand.child(0));
        Random random = analyze("select random(1, 5 + 5)", Random.class);
        assertLiteral(random.child(0));
        assertLiteral(random.child(1));
        Uniform uniform = analyze("select uniform(1, 5 + 5, k) from (select 1 k) t", Uniform.class);
        assertLiteral(uniform.child(0));
        assertLiteral(uniform.child(1));

        assertLiteral(analyze("select now(1 + 2)", Now.class).child(0));
        assertLiteral(analyze("select utc_timestamp(1 + 2)", UtcTimestamp.class).child(0));
        UtcTime utcTime = analyze("select utc_time(1 + 2)", UtcTime.class);
        assertLiteral(utcTime.child(0));
        Assertions.assertEquals(TimeV2Type.of(3), utcTime.getDataType());
    }

    @Test
    public void testAggregateFunctions() {
        String table = " from (select 1 k, cast('2024-01-01' as datetime) dt, 'a' s) t";
        assertLiteral(analyze("select sequence_match(concat('(?1)', '(?2)'), dt, k = 1, k = 2)" + table,
                SequenceMatch.class).child(0));
        assertLiteral(analyze("select sequence_count(concat('(?1)', '(?2)'), dt, k = 1, k = 2)" + table,
                SequenceCount.class).child(0));
        assertLiteral(analyze("select orthogonal_bitmap_expr_calculate(to_bitmap(k), cast(k as varchar),"
                + " concat('1', '&2'))" + table, OrthogonalBitmapExprCalculate.class).child(2));
        assertLiteral(analyze("select orthogonal_bitmap_expr_calculate_count(to_bitmap(k), cast(k as varchar),"
                + " concat('1', '&2'))" + table, OrthogonalBitmapExprCalculateCount.class).child(2));
        assertLiteral(analyze("select topn(s, 1 + 1)" + table, TopN.class).child(1));
        assertLiteral(analyze("select topn_array(s, 1 + 1)" + table, TopNArray.class).child(1));
        assertLiteral(analyze("select topn_weighted(s, k, 1 + 1)" + table, TopNWeighted.class).child(2));
    }

    @Test
    public void testTopNWithoutFoldConstant() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().debugSkipFoldConstant = true;
        PlanChecker.from(connectContext)
                .analyze("select topn(s, 1 + 1) from (select 'a' s) t")
                .rewrite();
    }

    @Test
    public void testFoldedValueIsStillChecked() {
        assertAnalysisError("select sha2('abc', 200 + 100)", "sha2 functions only support digest length of");
        assertAnalysisError("select split_by_regexp('a,b,c', ',', 0 - 1)", "must be a positive constant");
        assertAnalysisError("select array_apply([1, 2, 3], concat('>', '>'), 2)", "op support =, >=, <=, >, <, !=");
        assertAnalysisError("select date_trunc(cast('2024-03-15 10:00:00' as datetime), concat('mon', 'x'))",
                "date_trunc function time unit param only support argument is");
        assertAnalysisError("select now(3 + 7)", "Precision of NOW must be between 0 and");
        assertAnalysisError("select embed(concat('no_such_', 'resource'), 'x')",
                "AI resource 'no_such_resource' does not exist");
        assertAnalysisError("select ai_agg(concat('no_such_', 'resource'), s, concat('ta', 'sk'))"
                + " from (select 'a' s) t", "AI resource 'no_such_resource' does not exist");
    }

    @Test
    public void testNonConstantArgumentIsStillRejected() {
        assertAnalysisError("select sha2('abc', k) from (select 256 k) t",
                "the second parameter of sha2 must be a literal");
        assertAnalysisError("select sha2('abc', connection_id())", "the second parameter of sha2 must be a literal");
        // a constant expression FE cannot fold (crc32 has no FE executor)
        assertAnalysisError("select sha2('abc', 256 + crc32(''))", "the second parameter of sha2 must be a literal");
        assertAnalysisError("select rand(k) from (select 1 k) t", "The param of rand function must be literal");
        // a BIGINT precision would keep a narrowing cast to INT, so it is not folded
        assertAnalysisError("select utc_time(cast(3 as bigint))", "UTC_TIME scale argument must be a constant literal");
        // the date value is not folded when the time unit is a non-string literal
        assertAnalysisError("select date_trunc(concat('2024-03-15', ' 10:00:00'), null)", "must be a string constant");
        assertAnalysisError("select array_apply([1, 2, 3], s, 2) from (select '>' s) t",
                "array_apply(arr, op, val): op support const value only.");
        assertAnalysisError("select orthogonal_bitmap_expr_calculate(to_bitmap(k), cast(k as varchar), s)"
                + " from (select 1 k, '1' s) t", "must be a string literal");
    }

    private static <T extends BoundFunction> T analyze(String sql, Class<T> functionClass) {
        Plan plan = PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql).getPlan();
        List<Expression> functions = new ArrayList<>();
        plan.foreach(node -> {
            for (Expression expression : ((Plan) node).getExpressions()) {
                functions.addAll(expression.collectToList(functionClass::isInstance));
            }
        });
        Assertions.assertEquals(1, functions.size(), sql);
        return functionClass.cast(functions.get(0));
    }

    private static void assertLiteral(Expression argument) {
        Assertions.assertInstanceOf(Literal.class, argument, argument.toSql());
    }

    private static void assertAnalysisError(String sql, String message) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql), sql);
        Assertions.assertTrue(exception.getMessage().contains(message), exception.getMessage());
    }
}
