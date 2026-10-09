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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AIAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapExprCalculate;
import org.apache.doris.nereids.trees.expressions.functions.agg.OrthogonalBitmapExprCalculateCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.SequenceCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.SequenceMatch;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopN;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopNArray;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopNWeighted;
import org.apache.doris.nereids.trees.expressions.functions.ai.AISummarize;
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
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Constant expressions as the arguments that functions require to be constant.
 * prepareBeforeTypeCoercion folds only the arguments whose values the signature depends on. The value of any
 * other argument is validated without replacing the argument: before type coercion for a scalar function, which
 * constant folding may remove, and after the rewrite for an aggregate function.
 */
public class ConstantFunctionArgumentTest {

    @Test
    public void testPrepareFoldsArgumentsTheSignatureDependsOn() {
        Now now = analyze("select now(1 + 2)", Now.class);
        assertLiteral(now.child(0));
        Assertions.assertEquals(DateTimeV2Type.of(3), now.getDataType());
        UtcTimestamp utcTimestamp = analyze("select utc_timestamp(1 + 2)", UtcTimestamp.class);
        assertLiteral(utcTimestamp.child(0));
        Assertions.assertEquals(DateTimeV2Type.of(3), utcTimestamp.getDataType());
        UtcTime utcTime = analyze("select utc_time(1 + 2)", UtcTime.class);
        assertLiteral(utcTime.child(0));
        Assertions.assertEquals(TimeV2Type.of(3), utcTime.getDataType());

        // without a date argument, the literal time unit tells which argument is the date value
        DateTrunc stringColumn = analyze("select date_trunc(s, concat('mon', 'th')) from (select '2024-03-15' s) t",
                DateTrunc.class);
        assertLiteral(stringColumn.child(1));
        DateTrunc stringColumnUnitFirst = analyze("select date_trunc(concat('ye', 'ar'), s)"
                + " from (select '2024-03-15' s) t", DateTrunc.class);
        assertLiteral(stringColumnUnitFirst.child(0));
        // a string date value is not folded when the time unit is already a literal,
        // so the return type is still derived from a non-literal string
        DateTrunc stringValue = analyze("select date_trunc(concat('2024-03-15', ' 10:00:00'), 'month')",
                DateTrunc.class);
        assertNotLiteral(stringValue.child(0));
        Assertions.assertEquals(DateTimeV2Type.of(6), stringValue.getDataType());
        // when both arguments are foldable strings, only the time unit is folded, so the return type is the same
        // as with a literal time unit
        DateTrunc bothFoldable = analyze("select date_trunc(concat('2024-01-01 01:02:03+08:00', ''),"
                + " concat('mon', 'th'))", DateTrunc.class);
        assertNotLiteral(bothFoldable.child(0));
        assertLiteral(bothFoldable.child(1));
        Assertions.assertEquals(analyze("select date_trunc(concat('2024-01-01 01:02:03+08:00', ''), 'month')",
                DateTrunc.class).getDataType(), bothFoldable.getDataType());
        DateTrunc bothFoldableUnitFirst = analyze("select date_trunc(concat('mon', 'th'),"
                + " concat('2024-01-01 01:02:03+08:00', ''))", DateTrunc.class);
        assertLiteral(bothFoldableUnitFirst.child(0));
        assertNotLiteral(bothFoldableUnitFirst.child(1));

        // beside a date argument the other argument is the time unit, so the signature does not need its value
        DateTrunc typedDate = analyze("select date_trunc(cast('2024-03-15 10:00:00' as datetime),"
                + " concat('mon', 'th'))", DateTrunc.class);
        assertNotLiteral(typedDate.child(1));
        Assertions.assertEquals(DateTimeV2Type.of(0), typedDate.getDataType());
        assertNotLiteral(analyze("select date_trunc(concat('ye', 'ar'), TIMESTAMP '2024-03-15 10:00:00')",
                DateTrunc.class).child(0));
        assertNotLiteral(analyze("select date_trunc(DATE '2024-03-15', concat('mon', 'th'))",
                DateTrunc.class).child(1));

        assertAnalysisError("select now(3 + 7)", "Precision of NOW must be between 0 and");
    }

    @Test
    public void testScalarFunctionArgumentsAreNotFoldedByAnalysis() {
        // only the rewrite folds an argument whose value the signature does not depend on
        assertFoldedByRewrite("select sha2('abc', 200 + 56)", Sha2.class, 1);
        assertFoldedByRewrite("select split_by_regexp('a,b,c', ',', 1 + 1)", SplitByRegexp.class, 2);
        assertFoldedByRewrite("select regexp_replace('abc', 'a', 'b', concat('', ''))", RegexpReplace.class, 3);
        assertFoldedByRewrite("select regexp_replace_one('abc', 'a', 'b', concat('', ''))",
                RegexpReplaceOne.class, 3);
        assertFoldedByRewrite("select tokenize('x', concat('\"parser\"=\"', 'english\"'))", Tokenize.class, 1);
        assertFoldedByRewrite("select width_bucket(1.5, 0, 10, 2 + 3)", WidthBucket.class, 3);
        assertFoldedByRewrite("select array_apply([1, 2, 3], concat('>', '='), 2)", ArrayApply.class, 1);
        assertFoldedByRewrite("select rand(1 + 1)", Random.class, 0);
        assertFoldedByRewrite("select random(1, 5 + 5)", Random.class, 1);
        assertFoldedByRewrite("select uniform(1, 5 + 5, crc32('x'))", Uniform.class, 1);
    }

    @Test
    public void testScalarFunctionValueIsCheckedBeforeTypeCoercion() {
        assertAnalysisError("select sha2('abc', 200 + 100)", "sha2 functions only support digest length of");
        assertAnalysisError("select sha2('abc', cast(null as int))",
                "sha2 functions only support digest length of");
        assertAnalysisError("select split_by_regexp('a,b,c', ',', 0 - 1)", "must be a positive constant");
        // a typed NULL is an integral constant, but not a positive one
        assertAnalysisError("select split_by_regexp('a,b,c', ',', cast(null as int))", "must be a positive constant");
        assertAnalysisError("select array_apply([1, 2, 3], concat('>', '>'), 2)", "op support =, >=, <=, >, <, !=");
        assertAnalysisError("select tokenize('x', concat('par', 'ser'))",
                "tokenize second argument must be properties format");
        assertAnalysisError("select date_trunc(cast('2024-03-15 10:00:00' as datetime), concat('mon', 'x'))",
                "date_trunc function time unit param only support argument is");
        assertAnalysisError("select date_trunc(concat('mon', 'x'), DATE '2024-03-15')",
                "date_trunc function time unit param only support argument is");
        assertAnalysisError("select embed(concat('no_such_', 'resource'), 'x')",
                "AI resource 'no_such_resource' does not exist");

        // constant folding removes these functions, so no check after the rewrite would see the arguments
        assertAnalysisError("select sha2(null, 200 + 100)", "sha2 functions only support digest length of");
        assertAnalysisError("select split_by_regexp(null, ',', 0 - 1)", "must be a positive constant");
        assertAnalysisError("select array_apply(null, concat('>', '>'), 2)", "op support =, >=, <=, >, <, !=");
        assertAnalysisError("select tokenize(null, concat('par', 'ser'))",
                "tokenize second argument must be properties format");
        assertAnalysisError("select date_trunc(cast(null as datetime), concat('mon', 'x'))",
                "date_trunc function time unit param only support argument is");
        assertAnalysisError("select embed(concat('no_such_', 'resource'), null)",
                "AI resource 'no_such_resource' does not exist");
    }

    @Test
    public void testAggregateFunctionValueIsCheckedAfterRewrite() {
        String table = " from (select 1 k, cast('2024-01-01' as datetime) dt, 'a' s) t";
        assertFoldedByRewrite("select sequence_match(concat('(?1)', '(?2)'), dt, k = 1, k = 2)" + table,
                SequenceMatch.class, 0);
        assertFoldedByRewrite("select sequence_count(concat('(?1)', '(?2)'), dt, k = 1, k = 2)" + table,
                SequenceCount.class, 0);
        assertFoldedByRewrite("select orthogonal_bitmap_expr_calculate(to_bitmap(k), cast(k as varchar),"
                + " concat('1', '&2'))" + table, OrthogonalBitmapExprCalculate.class, 2);
        assertFoldedByRewrite("select orthogonal_bitmap_expr_calculate_count(to_bitmap(k), cast(k as varchar),"
                + " concat('1', '&2'))" + table, OrthogonalBitmapExprCalculateCount.class, 2);
        // a STRING formula is accepted before the type coercion casts it to VARCHAR
        assertFoldedByRewrite("select orthogonal_bitmap_expr_calculate(to_bitmap(k), cast(k as varchar),"
                + " concat(cast('1' as string), cast('&2' as string)))" + table,
                OrthogonalBitmapExprCalculate.class, 2);
        assertFoldedByRewrite("select orthogonal_bitmap_expr_calculate_count(to_bitmap(k), cast(k as varchar),"
                + " concat(cast('1' as string), cast('&2' as string)))" + table,
                OrthogonalBitmapExprCalculateCount.class, 2);
        // a formula that is not a string is cast to VARCHAR by the type coercion
        assertFoldedByRewrite("select orthogonal_bitmap_expr_calculate(to_bitmap(k), cast(k as varchar),"
                + " 1 + 1)" + table, OrthogonalBitmapExprCalculate.class, 2);
        assertFoldedByRewrite("select topn(s, 1 + 1)" + table, TopN.class, 1);
        assertFoldedByRewrite("select topn_array(s, 1 + 1)" + table, TopNArray.class, 1);
        assertFoldedByRewrite("select topn_weighted(s, k, 1 + 1)" + table, TopNWeighted.class, 2);

        assertRewriteError("select topn(s, 1 - 1)" + table, "must be a constant positive integer");
        assertRewriteError("select topn_array(s, 1 - 1)" + table, "must be a constant positive integer");
        assertRewriteError("select topn_weighted(s, k, 1 - 1)" + table, "must be a constant positive integer");
        assertRewriteError("select sequence_match(concat('(?1)', '(?9)'), dt, k = 1, k = 2)" + table,
                "Event number 9 is out of range");
        assertRewriteError("select sequence_count(concat('(?1)', '(?9)'), dt, k = 1, k = 2)" + table,
                "Event number 9 is out of range");
        // a typed NULL pattern folds to a NULL literal, which must be rejected here: BE's nullable aggregate
        // wrapper would otherwise skip every row for a NULL pattern instead of raising an error
        assertRewriteError("select sequence_match(cast(null as string), dt, k = 1, k = 2)" + table,
                "must be string constant, but it is null");
        assertRewriteError("select sequence_count(cast(null as string), dt, k = 1, k = 2)" + table,
                "must be string constant, but it is null");
        assertRewriteError("select ai_agg(concat('no_such_', 'resource'), s, concat('ta', 'sk'))" + table,
                "AI resource 'no_such_resource' does not exist");
        // the state combinator checks its nested function at the same points
        PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze("select topn_state(s, 1 + 1)" + table).rewrite();
        PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze("select sequence_match_state(concat('(?1)', '(?2)'), dt, k = 1, k = 2)" + table).rewrite();
        assertRewriteError("select topn_state(s, 1 - 1)" + table, "must be a constant positive integer");
        assertRewriteError("select sequence_match_state(concat('(?1)', '(?9)'), dt, k = 1, k = 2)" + table,
                "Event number 9 is out of range");
        // a literal is still validated by the analysis
        assertAnalysisError("select sequence_match('(?1)(?9)', dt, k = 1, k = 2)" + table,
                "Event number 9 is out of range");
        assertAnalysisError("select ai_agg('no_such_resource', s, 'task')" + table,
                "AI resource 'no_such_resource' does not exist");
    }

    @Test
    public void testWithoutFoldConstant() {
        // the rewrite does not fold the arguments, which are left to BE like the constants FE cannot fold
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().debugSkipFoldConstant = true;
        String table = " from (select 1 k, cast('2024-01-01' as datetime) dt, 'a' s) t";
        for (String sql : new String[] {
                "select topn(s, 1 + 1)" + table,
                "select sequence_match(concat('(?1)', '(?2)'), dt, k = 1, k = 2)" + table,
                "select sha2(s, 200 + 56)" + table,
                "select date_trunc(dt, concat('mon', 'th'))" + table}) {
            PlanChecker.from(connectContext).analyze(sql).rewrite();
        }
    }

    @Test
    public void testNonConstantArgumentIsStillRejected() {
        assertAnalysisError("select sha2('abc', k) from (select 256 k) t",
                "the second parameter of sha2 must be a constant");
        assertAnalysisError("select rand(k) from (select 1 k) t", "The param of rand function must be constant");
        // a literal of another type is still rejected, instead of being cast and left to BE
        assertAnalysisError("select sha2('abc', 'abc')", "the second parameter of sha2 must be an integer");
        assertAnalysisError("select split_by_regexp('a,b,c', ',', 1.5)", "must be a positive constant");
        // so is a constant of another type, whether FE folds it or leaves it to BE, before the type coercion
        // casts it to the INT signature
        assertAnalysisError("select sha2('abc', 255.5 + 0.5)", "the second parameter of sha2 must be an integer");
        assertAnalysisError("select sha2('abc', 256.0 + crc32(''))",
                "the second parameter of sha2 must be an integer");
        assertAnalysisError("select split_by_regexp('a,b,c', ',', 1.5 + 0.5)", "must be a positive constant");
        assertAnalysisError("select split_by_regexp('a,b,c', ',', 2.0 + crc32(''))", "must be a positive constant");
        assertAnalysisError("select tokenize('x', 5)", "tokenize second argument must be string literal");
        assertAnalysisError("select array_apply([1, 2, 3], 1, 2)", "op support const value only");
        assertAnalysisError("select sequence_match(1, dt, k = 1) from (select 1 k, now() dt) t",
                "function must be string constant");
        assertAnalysisError("select sequence_match(s, dt, k = 1) from (select 1 k, now() dt, '(?1)' s) t",
                "function must be string constant");
        // a BIGINT precision would keep a narrowing cast to INT, so it is not folded
        assertAnalysisError("select utc_time(cast(3 as bigint))", "UTC_TIME scale argument must be a constant literal");
        // the date value is not folded when the time unit is a non-string literal
        assertAnalysisError("select date_trunc(concat('2024-03-15', ' 10:00:00'), null)", "must be a string constant");
        assertAnalysisError("select array_apply([1, 2, 3], s, 2) from (select '>' s) t",
                "array_apply(arr, op, val): op support const value only.");
        assertAnalysisError("select orthogonal_bitmap_expr_calculate(to_bitmap(k), cast(k as varchar), s)"
                + " from (select 1 k, '1' s) t", "must be a string constant");
        assertAnalysisError("select embed(s, 'x') from (select 'resource' s) t",
                "AI Function must accept literal for the resource name");
        assertAnalysisError("select ai_agg(s, s, 'task') from (select 'resource' s) t",
                "AI_AGG must accept literal for the resource name");
        assertAnalysisError("select ai_agg('resource', s, s) from (select 'task' s) t",
                "AI_AGG must accept literal for the task");
    }

    @Test
    public void testConstantFeCannotFoldIsLeftToBe() {
        // crc32 has no FE executor, so crc32('') = 0 is a constant only BE can evaluate
        assertNotLiteral(analyzeAndRewrite("select sha2('abc', 256 + crc32(''))", Sha2.class).child(1));
        assertNotLiteral(analyzeAndRewrite("select width_bucket(1.5, 0, 10, 2 + crc32(''))",
                WidthBucket.class).child(3));
        assertNotLiteral(analyzeAndRewrite("select split_by_regexp('a,b,c', ',', 1 + crc32(''))",
                SplitByRegexp.class).child(2));
        assertNotLiteral(analyzeAndRewrite("select tokenize('x', lpad('\"parser\"=\"english\"', 19, ' '))",
                Tokenize.class).child(1));
        assertNotLiteral(analyzeAndRewrite("select regexp_replace('abc', 'a', 'b', lpad('', 1, ' '))",
                RegexpReplace.class).child(3));
        assertNotLiteral(analyzeAndRewrite("select regexp_replace_one('abc', 'a', 'b', lpad('', 1, ' '))",
                RegexpReplaceOne.class).child(3));
        assertNotLiteral(analyzeAndRewrite("select array_apply([1, 2, 3], lpad('=', 2, '>'), 2)",
                ArrayApply.class).child(1));
        assertNotLiteral(analyzeAndRewrite("select date_trunc(cast('2024-03-15 10:00:00' as datetime),"
                + " lpad('ay', 3, 'd'))", DateTrunc.class).child(1));
        assertNotLiteral(analyzeAndRewrite("select date_trunc(lpad('ay', 3, 'd'), DATE '2024-03-15')",
                DateTrunc.class).child(0));
        // beside a nonconstant VARCHAR date value, a constant unit FE cannot fold to a literal is still the
        // time unit, in both argument orders; BE validates the evaluated unit
        assertNotLiteral(analyzeAndRewrite(
                "select date_trunc(s, lpad('mo', 2, 'nth')) from (select '2024-03-15' s) t",
                DateTrunc.class).child(1));
        assertNotLiteral(analyzeAndRewrite(
                "select date_trunc(lpad('mo', 2, 'nth'), s) from (select '2024-03-15' s) t",
                DateTrunc.class).child(0));
        // beside a constant string date value too, in both argument orders, and the string date value selects
        // the same signature as beside a literal time unit, including the timezone type
        DateTrunc stringDate = analyze("select date_trunc('2024-03-15 10:00:00', lpad('nth', 5, 'mo'))",
                DateTrunc.class);
        assertNotLiteral(stringDate.child(1));
        Assertions.assertEquals(analyze("select date_trunc('2024-03-15 10:00:00', 'month')", DateTrunc.class)
                .getDataType(), stringDate.getDataType());
        DateTrunc stringDateUnitFirst = analyze("select date_trunc(lpad('nth', 5, 'mo'), '2024-03-15 10:00:00')",
                DateTrunc.class);
        assertNotLiteral(stringDateUnitFirst.child(0));
        Assertions.assertEquals(stringDate.getDataType(), stringDateUnitFirst.getDataType());
        DateTrunc zonedStringDate = analyze("select date_trunc('2024-01-01 01:02:03+08:00', lpad('nth', 5, 'mo'))",
                DateTrunc.class);
        Assertions.assertInstanceOf(TimeStampTzType.class, zonedStringDate.getDataType());
        Assertions.assertEquals(analyze("select date_trunc('2024-01-01 01:02:03+08:00', 'month')",
                DateTrunc.class).getDataType(), zonedStringDate.getDataType());
        Assertions.assertEquals(zonedStringDate.getDataType(), analyze(
                "select date_trunc(lpad('nth', 5, 'mo'), '2024-01-01 01:02:03+08:00')", DateTrunc.class)
                .getDataType());
        // a string date value FE can fold is kept unfolded, like beside a literal time unit
        DateTrunc foldableStringDate = analyze("select date_trunc(concat('2024-03-15', ' 10:00:00'),"
                + " lpad('nth', 5, 'mo'))", DateTrunc.class);
        assertNotLiteral(foldableStringDate.child(0));
        assertNotLiteral(foldableStringDate.child(1));
        Assertions.assertEquals(DateTimeV2Type.of(6), foldableStringDate.getDataType());
        assertNotLiteral(analyze("select date_trunc(lpad('nth', 5, 'mo'), concat('2024-03-15', ' 10:00:00'))",
                DateTrunc.class).child(1));
        // the time unit value FE can evaluate beside a string date value is still validated
        assertAnalysisError("select date_trunc('2024-03-15 10:00:00', concat('mon', 'x'))",
                "date_trunc function time unit param only support argument is");
        assertNotLiteral(analyzeAndRewrite("select rand(1 + crc32(''))", Random.class).child(0));
        Uniform uniform = analyzeAndRewrite("select uniform(1 + crc32(''), 10 + crc32(''), crc32('x'))",
                Uniform.class);
        assertNotLiteral(uniform.child(0));
        assertNotLiteral(uniform.child(1));

        String table = " from (select 1 k, cast('2024-01-01' as datetime) dt, 'a' s) t";
        assertNotLiteral(analyzeAndRewrite("select sequence_match(lpad('(?2)', 8, '(?1)'), dt, k = 1, k = 2)"
                + table, SequenceMatch.class).child(0));
        assertNotLiteral(analyzeAndRewrite("select sequence_count(lpad('(?2)', 8, '(?1)'), dt, k = 1, k = 2)"
                + table, SequenceCount.class).child(0));
        assertNotLiteral(analyzeAndRewrite("select orthogonal_bitmap_expr_calculate(to_bitmap(k),"
                + " cast(k as varchar), lpad('&2', 3, '1'))" + table, OrthogonalBitmapExprCalculate.class).child(2));
        assertNotLiteral(analyzeAndRewrite("select orthogonal_bitmap_expr_calculate_count(to_bitmap(k),"
                + " cast(k as varchar), lpad('&2', 3, '1'))" + table,
                OrthogonalBitmapExprCalculateCount.class).child(2));
        assertNotLiteral(analyzeAndRewrite("select topn(s, 1 + crc32(''))" + table, TopN.class).child(1));
        assertNotLiteral(analyzeAndRewrite("select topn_array(s, 1 + crc32(''))" + table, TopNArray.class)
                .child(1));
        assertNotLiteral(analyzeAndRewrite("select topn_weighted(s, k, 1 + crc32(''))" + table,
                TopNWeighted.class).child(2));

        // FE does not fold an illegal time unit, for example one pushed into an IF branch, and leaves it to BE
        Expression illegalUnit = FoldConstantRuleOnFE.evaluateWithoutContext(
                new DateTrunc(new DateTimeV2Literal("2024-03-15 10:00:00"), new VarcharLiteral("xx")));
        Assertions.assertInstanceOf(DateTrunc.class, illegalUnit, illegalUnit.toSql());

        // the precision determines the return type, so FE must know its value
        assertAnalysisError("select now(1 + crc32(''))", "NOW precision argument must be a constant literal");
        assertAnalysisError("select utc_time(1 + crc32(''))", "UTC_TIME scale argument must be a constant literal");
        // which argument is the time unit is unknown when neither argument is a date or a literal
        assertAnalysisError("select date_trunc(lpad('ay', 3, 'd'), concat('2024-03-15', crc32('')))",
                "must be a string constant");
        // FE resolves the AI resource, so it must know the resource name
        assertAnalysisError("select embed(lpad('resource', 9, 'x'), 'x')",
                "AI Function must accept literal for the resource name");
        assertRewriteError("select ai_agg(lpad('resource', 9, 'x'), s, 'task')" + table,
                "AI_AGG must accept literal for the resource name");
    }

    @Test
    public void testAiFunctionResolvesTheResourceNameItEvaluates() throws Exception {
        CreateResourceCommand command = new CreateResourceCommand(new CreateResourceInfo(true, false,
                "constant_argument_ai_resource", ImmutableMap.of("type", "ai",
                "ai.endpoint", "https://ai.example.com/v1/chat/completions", "ai.provider_type", "openai",
                "ai.api_key", "key", "ai.model_name", "model", "ai.validity_check", "false")));
        command.getInfo().analyzeResourceType();
        Env.getCurrentEnv().getResourceMgr().createResource(Resource.fromCommand(command), true);
        String resourceName = "concat('constant_argument_', 'ai_resource')";
        String table = " from (select 1 k, 'a' s) t";

        // without constant folding, the check after the rewrite evaluates the resource name and the task itself,
        // and BE reads the values it evaluates from the first row
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().debugSkipFoldConstant = true;
        String aiAggSql = "select ai_agg(" + resourceName + ", s, concat('ta', 'sk'))" + table;
        AIAgg aiAgg = findFunction(aiAggSql, PlanChecker.from(connectContext).analyze(aiAggSql).rewrite().getPlan(),
                AIAgg.class);
        assertNotLiteral(aiAgg.child(0));
        assertNotLiteral(aiAgg.child(2));
        String aiSummarizeSql = "select ai_summarize(" + resourceName + ", s)" + table;
        assertNotLiteral(findFunction(aiSummarizeSql,
                PlanChecker.from(connectContext).analyze(aiSummarizeSql).rewrite().getPlan(), AISummarize.class)
                .child(0));
        assertRewriteError(connectContext, "select ai_agg(concat('no_such_', 'resource'), s, concat('ta', 'sk'))"
                + table, "AI resource 'no_such_resource' does not exist");
        // FE resolves the resource, so it must still know the resource name, and the task must still fold on FE
        assertRewriteError(connectContext, "select ai_agg(lpad('resource', 9, 'x'), s, 'task')" + table,
                "AI_AGG must accept literal for the resource name");
        assertRewriteError(connectContext, "select ai_agg(" + resourceName + ", s, lpad('task', 5, 'x'))" + table,
                "AI_AGG must accept literal for the task");
    }

    /** the analysis keeps the argument as written, and the rewrite folds it to a literal */
    private static <T extends BoundFunction> void assertFoldedByRewrite(String sql, Class<T> functionClass,
            int argumentIndex) {
        assertNotLiteral(analyze(sql, functionClass).child(argumentIndex));
        assertLiteral(analyzeAndRewrite(sql, functionClass).child(argumentIndex));
    }

    private static <T extends BoundFunction> T analyze(String sql, Class<T> functionClass) {
        return findFunction(sql, PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql).getPlan(),
                functionClass);
    }

    private static <T extends BoundFunction> T analyzeAndRewrite(String sql, Class<T> functionClass) {
        return findFunction(sql,
                PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql).rewrite().getPlan(),
                functionClass);
    }

    private static <T extends BoundFunction> T findFunction(String sql, Plan plan, Class<T> functionClass) {
        List<Expression> functions = new ArrayList<>();
        plan.foreach(node -> {
            for (Expression expression : ((Plan) node).getExpressions()) {
                functions.addAll(expression.collectToList(functionClass::isInstance));
            }
        });
        Assertions.assertEquals(1, functions.size(), sql);
        return functionClass.cast(functions.get(0));
    }

    private static void assertNotLiteral(Expression argument) {
        Assertions.assertFalse(argument instanceof Literal, argument.toSql());
    }

    private static void assertLiteral(Expression argument) {
        Assertions.assertInstanceOf(Literal.class, argument, argument.toSql());
    }

    private static void assertRewriteError(String sql, String message) {
        assertRewriteError(MemoTestUtils.createConnectContext(), sql, message);
    }

    private static void assertRewriteError(ConnectContext connectContext, String sql, String message) {
        PlanChecker analyzed = PlanChecker.from(connectContext).analyze(sql);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, analyzed::rewrite, sql);
        Assertions.assertTrue(exception.getMessage().contains(message), exception.getMessage());
    }

    private static void assertAnalysisError(String sql, String message) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql), sql);
        Assertions.assertTrue(exception.getMessage().contains(message), exception.getMessage());
    }
}
