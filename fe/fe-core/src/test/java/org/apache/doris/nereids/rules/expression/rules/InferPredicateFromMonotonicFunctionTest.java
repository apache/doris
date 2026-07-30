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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Date;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateFormat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FromUnixtime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HourCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Left;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeekCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeekFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Year;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsAdd;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InferPredicateFromMonotonicFunctionTest extends ExpressionRewriteTestHelper {

    private final SlotReference strCol = new SlotReference("s", StringType.INSTANCE, true);
    private final SlotReference intCol = new SlotReference("i", IntegerType.INSTANCE, true);
    private final SlotReference dtCol = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT, true);
    private final SlotReference bigintCol = new SlotReference("ts", BigIntType.INSTANCE, true);
    private final SlotReference tstzCol = new SlotReference("tstz", TimeStampTzType.SYSTEM_DEFAULT, true);

    @BeforeEach
    void setUp() {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().enableInferPredicateFromMonotonicFunction = true;
        ctx.setThreadLocalInfo();
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(InferPredicateFromMonotonicFunction.INSTANCE)));
    }

    private Substring substr(Expression src, int pos, int len) {
        return new Substring(src, new IntegerLiteral(pos), new IntegerLiteral(len));
    }

    // substring(s, 1, 10) >= '2026-03-26'  ==>  and(orig, s >= '2026-03-26')
    @Test
    void testSubstringGeDerivesLowerBound() {
        GreaterThanEqual orig = new GreaterThanEqual(substr(strCol, 1, 10), new VarcharLiteral("2026-03-26"));
        Expression expected = new And(orig, new GreaterThanEqual(strCol, new VarcharLiteral("2026-03-26")));
        assertRewrite(orig, expected);
    }

    // substring(s, 1, 10) > '2026-03-26'  ==>  and(orig, s > '2026-03-26')
    @Test
    void testSubstringGtDerivesLowerBound() {
        GreaterThan orig = new GreaterThan(substr(strCol, 1, 10), new VarcharLiteral("2026-03-26"));
        Expression expected = new And(orig, new GreaterThan(strCol, new VarcharLiteral("2026-03-26")));
        assertRewrite(orig, expected);
    }

    // left(s, 10) >= '2026-03-26'  ==>  and(orig, s >= '2026-03-26')
    @Test
    void testLeftGeDerivesLowerBound() {
        GreaterThanEqual orig = new GreaterThanEqual(
                new Left(strCol, new IntegerLiteral(10)), new VarcharLiteral("2026-03-26"));
        Expression expected = new And(orig, new GreaterThanEqual(strCol, new VarcharLiteral("2026-03-26")));
        assertRewrite(orig, expected);
    }

    // const longer than prefix window: substring(s, 1, 4) >= '2026-03-26' cannot derive a sound bound
    @Test
    void testConstLongerThanPrefixNotRewritten() {
        GreaterThanEqual orig = new GreaterThanEqual(substr(strCol, 1, 4), new VarcharLiteral("2026-03-26"));
        assertRewrite(orig, orig);
    }

    // position != 1 is not a prefix, must not rewrite
    @Test
    void testSubstringPositionNotOneNotRewritten() {
        GreaterThanEqual orig = new GreaterThanEqual(substr(strCol, 2, 10), new VarcharLiteral("2026-03-26"));
        assertRewrite(orig, orig);
    }

    // upper-bound comparisons have no sound bare-column bound from a prefix
    @Test
    void testLessThanNotRewritten() {
        LessThan orig = new LessThan(substr(strCol, 1, 10), new VarcharLiteral("2026-03-26"));
        assertRewrite(orig, orig);
    }

    // non-string source column must not be rewritten. Type coercion wraps the int column as
    // substring(cast(i as varchar), 1, 10), whose source is a Cast (not a bare Slot), so the rule
    // must leave it unchanged.
    @Test
    void testNonStringColumnNotRewritten() {
        GreaterThanEqual orig = new GreaterThanEqual(substr(intCol, 1, 10), new VarcharLiteral("2026"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // idempotency: running the rule twice must not append a duplicate derived predicate
    @Test
    void testIdempotent() {
        GreaterThanEqual orig = new GreaterThanEqual(substr(strCol, 1, 10), new VarcharLiteral("2026-03-26"));
        Expression once = executor.rewrite(typeCoercion(orig), context);
        Expression twice = executor.rewrite(once, context);
        Assertions.assertEquals(once, twice);
    }

    // session switch off: no inference happens
    @Test
    void testDisabledBySessionVar() {
        ConnectContext.get().getSessionVariable().enableInferPredicateFromMonotonicFunction = false;
        GreaterThanEqual orig = new GreaterThanEqual(substr(strCol, 1, 10), new VarcharLiteral("2026-03-26"));
        assertRewrite(orig, orig);
    }

    // inside a conjunction, the derived predicate is appended and the original conjuncts kept
    @Test
    void testInsideConjunctionAppendsDerived() {
        GreaterThanEqual prefix = new GreaterThanEqual(substr(strCol, 1, 10), new VarcharLiteral("2026-03-26"));
        EqualTo other = new EqualTo(intCol, new IntegerLiteral(1));
        And input = new And(prefix, other);
        Expression rewritten = executor.rewrite(typeCoercion(input), context);
        // original conjuncts must survive
        Assertions.assertTrue(rewritten.toString().contains("2026-03-26"));
        // derived bare-column predicate must be present
        Assertions.assertTrue(rewritten.getArguments().stream()
                .anyMatch(e -> e instanceof GreaterThanEqual && e.child(0).equals(strCol)));
    }

    // ---- date floor family: date_trunc / date / to_date satisfy f(col) <= col ----

    // date(dt) >= '2026-03-26'  ==>  and(orig, dt >= '2026-03-26')  (floor property, any constant)
    @Test
    void testDateDerivesLowerBound() {
        GreaterThanEqual orig = new GreaterThanEqual(new Date(dtCol), new VarcharLiteral("2026-03-26"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "should append a derived predicate");
        Assertions.assertTrue(rewritten.getArguments().stream()
                .anyMatch(e -> e instanceof GreaterThanEqual && e.child(0).equals(dtCol)),
                "derived bare datetime-column lower bound must be present");
    }

    // to_date(dt) > '2026-03-26'  ==>  and(orig, dt > '2026-03-26')
    @Test
    void testToDateDerivesLowerBound() {
        GreaterThan orig = new GreaterThan(new ToDate(dtCol), new VarcharLiteral("2026-03-26"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream()
                .anyMatch(e -> e instanceof GreaterThan && e.child(0).equals(dtCol)));
    }

    // date_trunc(dt, 'day') >= '2026-03-26'  ==>  and(orig, dt >= '2026-03-26')
    @Test
    void testDateTruncDerivesLowerBound() {
        GreaterThanEqual orig = new GreaterThanEqual(
                new DateTrunc(dtCol, new VarcharLiteral("day")), new VarcharLiteral("2026-03-26"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream()
                .anyMatch(e -> e instanceof GreaterThanEqual && e.child(0).equals(dtCol)));
    }

    // upper-bound comparison on a floor function: date(dt) < c has no sound bare-column bound
    @Test
    void testDateLessThanNotRewritten() {
        LessThan orig = new LessThan(new Date(dtCol), new VarcharLiteral("2026-03-26"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // fixed-offset zone (no transitions at all): from_unixtime is a pure affine bijection everywhere,
    // so the guard's isFixedOffset fast path lets from_unixtime(ts) >= c derive ts >= unix_timestamp(c).
    @Test
    void testFromUnixtimeFixedOffsetZoneDerives() {
        ConnectContext.get().getSessionVariable().setTimeZone("UTC");
        GreaterThanEqual orig = new GreaterThanEqual(
                new FromUnixtime(bigintCol), new VarcharLiteral("2026-03-26 00:00:00"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "fixed-offset zone should derive an epoch lower bound");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(bigintCol) && e.child(1) instanceof Literal),
                "derived bare epoch-column lower bound must be present");
    }

    // ---- substring/left equality -> two-sided range [c, succ(c)) ----

    // substring(s,1,10) = '2026-03-26'  ==>  and(orig, s >= '2026-03-26' AND s < '2026-03-27')
    // succ('2026-03-26') increments the last byte '6'(0x36)->'7'(0x37) => '2026-03-27'
    @Test
    void testSubstringEqDerivesTwoSidedRange() {
        EqualTo orig = new EqualTo(substr(strCol, 1, 10), new VarcharLiteral("2026-03-26"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "should append derived bounds");
        // lower bound s >= '2026-03-26' present
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(strCol)
                        && ((VarcharLiteral) e.child(1)).getStringValue().equals("2026-03-26")),
                "lower bound s >= '2026-03-26' expected");
        // upper bound s < '2026-03-27' present (byte successor)
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThan && e.child(0).equals(strCol)
                        && ((VarcharLiteral) e.child(1)).getStringValue().equals("2026-03-27")),
                "upper bound s < '2026-03-27' (byte successor) expected");
    }

    // left(s,3) = 'abc'  ==>  and(orig, s >= 'abc' AND s < 'abd')
    @Test
    void testLeftEqDerivesTwoSidedRange() {
        EqualTo orig = new EqualTo(new Left(strCol, new IntegerLiteral(3)), new VarcharLiteral("abc"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThan && e.child(0).equals(strCol)
                        && ((VarcharLiteral) e.child(1)).getStringValue().equals("abd")),
                "upper bound s < 'abd' expected");
    }

    // const longer than window: substring(s,1,4) = '2026-03-26' cannot derive a sound range
    @Test
    void testSubstringEqConstLongerThanWindowNotRewritten() {
        EqualTo orig = new EqualTo(substr(strCol, 1, 4), new VarcharLiteral("2026-03-26"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // date(dt) = c  =>  and(orig, dt >= c AND dt < c + 1 day). preimage two-sided range: the lower
    // bound is the floor property, the upper bound is the constant-folded floorUpperBound (+1 day).
    @Test
    void testDateEqDerivesTwoSidedRange() {
        EqualTo orig = new EqualTo(new Date(dtCol), new VarcharLiteral("2026-03-26"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "date(dt)=c should derive a two-sided range");
        // lower bound dt >= c
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol)),
                "lower bound dt >= c expected");
        // upper bound dt < c + 1 day (folded literal, not an unfolded DaysAdd expression)
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThan && e.child(0).equals(dtCol) && e.child(1) instanceof Literal),
                "upper bound dt < folded(c + 1 day) expected");
    }

    // non-ASCII constant: succ() byte successor is unsafe (signed/unsigned byte order diverge for
    // bytes >= 0x80), so only the lower bound is emitted, no upper bound.
    @Test
    void testSubstringEqNonAsciiConstOnlyLowerBound() {
        EqualTo orig = new EqualTo(substr(strCol, 1, 2), new VarcharLiteral("中文"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "should still append the lower bound");
        // lower bound present
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(strCol)),
                "lower bound s >= '中文' expected");
        // NO upper bound (LessThan) for non-ASCII, to stay safe under signed/unsigned byte order
        Assertions.assertTrue(rewritten.getArguments().stream().noneMatch(e -> e instanceof LessThan),
                "no upper bound for non-ASCII constant");
    }

    // a prefix constant whose last byte is 0x7F must not yield a 0x80 byte (invalid UTF-8 that
    // new String() would mangle into U+FFFD). The 0x7F is stripped and the preceding 'a'(0x61) is
    // bumped to 'b': substr(s,1,2) = 'a'+0x7F  ->  s >= <const> AND s < 'b'. s < 'b' is a sound
    // superset (any s beginning 'a...' is < 'b') and is clean ASCII.
    @Test
    void testSubstringEqTrailing0x7fCleanSuccessor() {
        String cst = "a" + (char) 0x7f;
        EqualTo orig = new EqualTo(substr(strCol, 1, 2), new VarcharLiteral(cst));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThan && e.child(0).equals(strCol)
                        && ((VarcharLiteral) e.child(1)).getStringValue().equals("b")),
                "clean-ASCII upper bound s < 'b' expected (0x7F stripped, no U+FFFD mangling)");
        char replacement = (char) 0xFFFD;
        Assertions.assertTrue(rewritten.getArguments().stream().noneMatch(e ->
                e instanceof LessThan
                        && ((VarcharLiteral) e.child(1)).getStringValue().indexOf(replacement) >= 0),
                "no mangled U+FFFD successor may be emitted");
    }

    // a prefix constant that is entirely 0x7F has no byte incrementable within clean ASCII
    // (0x7F+1 = 0x80 leaves ASCII), so only the lower bound is emitted -- no upper bound.
    @Test
    void testSubstringEqAll0x7fOnlyLowerBound() {
        String cst = "" + (char) 0x7f + (char) 0x7f;
        EqualTo orig = new EqualTo(substr(strCol, 1, 2), new VarcharLiteral(cst));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "should still append the lower bound");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(strCol)),
                "lower bound present");
        Assertions.assertTrue(rewritten.getArguments().stream().noneMatch(e -> e instanceof LessThan),
                "no upper bound when the whole constant is 0x7F");
    }

    // date_trunc with a floor unit ('day') derives lower bound; a hypothetical non-floor unit would
    // be rejected by the unit whitelist (guarded in extractDateFloorSource).
    @Test
    void testDateTruncFloorUnitDerives() {
        GreaterThanEqual orig = new GreaterThanEqual(
                new DateTrunc(dtCol, new VarcharLiteral("month")), new VarcharLiteral("2026-03-01"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol)));
    }

    // year(dt) op Y: year() is globally monotonic and timezone-free, so year(dt) = Y has the EXACT
    // preimage dt in [Y-01-01, (Y+1)-01-01). All of =, >, >=, <, <= derive a bare-column bound.
    // year(dt) = 2024  =>  and(orig, dt >= 2024-01-01 AND dt < 2025-01-01)
    @Test
    void testYearEqDerivesTwoSidedRange() {
        EqualTo orig = new EqualTo(new Year(dtCol), new SmallIntLiteral((short) 2024));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "year(dt)=Y should derive a two-sided range");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol) && e.child(1) instanceof Literal),
                "lower bound dt >= Y-01-01 expected");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThan && e.child(0).equals(dtCol) && e.child(1) instanceof Literal),
                "upper bound dt < (Y+1)-01-01 expected");
    }

    // the derived two-sided range is flattened into the enclosing conjunction: the result is a single
    // flat AND of comparison predicates (no nested And(lower, upper)) with no duplicate conjuncts.
    @Test
    void testEqDerivationProducesFlatConjuncts() {
        EqualTo orig = new EqualTo(new Year(dtCol), new SmallIntLiteral((short) 2024));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().noneMatch(e -> e instanceof And),
                "no child may itself be a nested And -- the range must be flattened");
        Assertions.assertEquals(rewritten.getArguments().size(),
                rewritten.getArguments().stream().distinct().count(), "no duplicate conjuncts");
    }

    // year(dt) >= 2024  =>  and(orig, dt >= 2024-01-01)
    @Test
    void testYearGeDerivesLowerBound() {
        GreaterThanEqual orig = new GreaterThanEqual(new Year(dtCol), new SmallIntLiteral((short) 2024));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol) && e.child(1) instanceof Literal),
                "lower bound dt >= Y-01-01 expected");
    }

    // year(dt) > 2024  =>  and(orig, dt >= 2025-01-01)   (strict '>' maps to the next-year boundary)
    @Test
    void testYearGtDerivesNextYearLowerBound() {
        GreaterThan orig = new GreaterThan(new Year(dtCol), new SmallIntLiteral((short) 2024));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol) && e.child(1) instanceof Literal),
                "lower bound dt >= (Y+1)-01-01 expected");
    }

    // year(dt) < 2024  =>  and(orig, dt < 2024-01-01)
    @Test
    void testYearLtDerivesUpperBound() {
        LessThan orig = new LessThan(new Year(dtCol), new SmallIntLiteral((short) 2024));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThan && e.child(0).equals(dtCol) && e.child(1) instanceof Literal),
                "upper bound dt < Y-01-01 expected");
    }

    // year(dt) <= 2024  =>  and(orig, dt < 2025-01-01)   ('<=' maps to the next-year boundary, exclusive)
    @Test
    void testYearLeDerivesNextYearUpperBound() {
        LessThanEqual orig = new LessThanEqual(new Year(dtCol), new SmallIntLiteral((short) 2024));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThan && e.child(0).equals(dtCol) && e.child(1) instanceof Literal),
                "upper bound dt < (Y+1)-01-01 expected");
    }

    // boundary: year(dt) = 9999 has no upper bound (10000-01-01 is out of the valid year range), so
    // fail open and derive only the sound lower half dt >= 9999-01-01 (no two-sided AND upper bound).
    @Test
    void testYearEqAtMaxYearDerivesLowerBoundOnly() {
        EqualTo orig = new EqualTo(new Year(dtCol), new SmallIntLiteral((short) 9999));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol) && e.child(1) instanceof Literal),
                "lower bound dt >= 9999-01-01 expected");
        Assertions.assertTrue(rewritten.getArguments().stream().noneMatch(e ->
                e instanceof LessThan && e.child(0).equals(dtCol)),
                "no upper bound must be derived at the max year (10000-01-01 is out of range)");
    }

    // year() of a non-slot (an expression, not a bare column) is not rewritten: only a bare
    // DateV2/DateTimeV2 slot argument can be reverse-mapped to a column range.
    @Test
    void testYearOfNonSlotNotRewritten() {
        EqualTo orig = new EqualTo(new Year(new DayFloor(dtCol)), new SmallIntLiteral((short) 2024));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // integration: day_floor is a floor function, so the isFloor-driven rule handles it with NO rule
    // change -- day_floor(dt) >= c derives the lower bound dt >= c.
    @Test
    void testDayFloorGeDerivesLowerBound() {
        GreaterThanEqual orig = new GreaterThanEqual(new DayFloor(dtCol), new VarcharLiteral("2026-03-26"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol)));
    }

    // integration: day_floor(dt) = c derives the two-sided range dt >= c AND dt < c + 1 day.
    @Test
    void testDayFloorEqDerivesTwoSidedRange() {
        EqualTo orig = new EqualTo(new DayFloor(dtCol), new VarcharLiteral("2026-03-26"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol)));
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThan && e.child(0).equals(dtCol) && e.child(1) instanceof Literal));
    }

    // date_format(dt, '%Y-%m-%d') >= '2026-03-26'  =>  and(orig, dt >= str_to_date('2026-03-26','%Y-%m-%d'))
    @Test
    void testDateFormatGeDerivesLowerBound() {
        GreaterThanEqual orig = new GreaterThanEqual(
                new DateFormat(dtCol, new VarcharLiteral("%Y-%m-%d")), new VarcharLiteral("2026-03-26"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol) && e.child(1) instanceof Literal));
    }

    // non-whitelist format (day-first '%d-%m-%Y' is NOT lexicographically time-ordered) must not push
    @Test
    void testDateFormatNonWhitelistFmtNotRewritten() {
        GreaterThanEqual orig = new GreaterThanEqual(
                new DateFormat(dtCol, new VarcharLiteral("%d-%m-%Y")), new VarcharLiteral("26-03-2026"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // various whitelist patterns each derive a bare-column lower bound (or fail open safely)
    @Test
    void testDateFormatDayPatternDerives() {
        GreaterThanEqual orig = new GreaterThanEqual(
                new DateFormat(dtCol, new VarcharLiteral("yyyy-MM-dd")), new VarcharLiteral("2026-03-26"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol) && e.child(1) instanceof Literal));
    }

    // ---- from_unixtime(epochCol) op c  =>  epochCol op unix_timestamp(c)  (relaxed, DST-guarded) ----

    // In a zone with no DST near the constant (Asia/Shanghai, last transition 1991), from_unixtime is
    // strictly monotonic around a modern constant, so from_unixtime(ts) >= '2026-03-26 00:00:00'
    // derives a bare epoch lower bound ts >= unix_timestamp('2026-03-26 00:00:00'). Original kept.
    @Test
    void testFromUnixtimeGeDerivesEpochLowerBoundNoDstZone() {
        ConnectContext.get().getSessionVariable().setTimeZone("Asia/Shanghai");
        GreaterThanEqual orig = new GreaterThanEqual(
                new FromUnixtime(bigintCol), new VarcharLiteral("2026-03-26 00:00:00"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "should append a derived epoch lower bound");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(bigintCol) && e.child(1) instanceof Literal),
                "derived bare epoch-column lower bound ts >= <folded epoch> must be present");
    }

    // strict '>' is handled too: from_unixtime(ts) > c  =>  ts >= unix_timestamp(c). The derived op
    // MUST be '>=' (not '>'): from_unixtime renders the canonical 'yyyy-MM-dd HH:mm:ss', so at the
    // boundary ts = X the rendered string is strictly greater than a shorter/equal constant and the
    // original '>' is TRUE there -- a derived strict '>' would drop that matching row.
    @Test
    void testFromUnixtimeGtDerivesEpochLowerBoundNoDstZone() {
        ConnectContext.get().getSessionVariable().setTimeZone("Asia/Shanghai");
        GreaterThan orig = new GreaterThan(
                new FromUnixtime(bigintCol), new VarcharLiteral("2026-03-26 00:00:00"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(bigintCol) && e.child(1) instanceof Literal),
                "strict '>' must derive a NON-strict '>=' epoch bound so the boundary row is not dropped");
        Assertions.assertTrue(rewritten.getArguments().stream().noneMatch(e ->
                e instanceof GreaterThan && e.child(0).equals(bigintCol)),
                "must NOT derive a strict '>' bare-epoch bound");
    }

    // Finding regression: with a SHORTER-than-canonical constant like '2026-03-26' (no time part),
    // from_unixtime(ts) > '2026-03-26' at ts = unix_timestamp('2026-03-26 00:00:00') renders
    // '2026-03-26 00:00:00', which is string-greater than '2026-03-26', so the original is TRUE at the
    // boundary. The derived bound must be '>=' to keep that row.
    @Test
    void testFromUnixtimeGtShortConstantDerivesNonStrictBound() {
        ConnectContext.get().getSessionVariable().setTimeZone("Asia/Shanghai");
        GreaterThan orig = new GreaterThan(new FromUnixtime(bigintCol), new VarcharLiteral("2026-03-26"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(bigintCol) && e.child(1) instanceof Literal),
                "short constant '>' must still derive a non-strict '>=' epoch bound");
        Assertions.assertTrue(rewritten.getArguments().stream().noneMatch(e ->
                e instanceof GreaterThan && e.child(0).equals(bigintCol)),
                "must NOT derive a strict '>' bare-epoch bound");
    }

    // The guard is PER-PREDICATE, not per-zone: even in a recurring-DST zone (America/New_York), a
    // constant far from any offset transition derives soundly. c = '2026-03-26 00:00:00' sits ~18 days
    // after the 2026-03-08 spring-forward and long before the 2026-11-01 fall-back, so the guard band
    // (X - 2days, X) is clean and from_unixtime is a strict affine bijection there -> derive ts >= X.
    @Test
    void testFromUnixtimeDerivesInDstZoneAwayFromTransition() {
        ConnectContext.get().getSessionVariable().setTimeZone("America/New_York");
        GreaterThanEqual orig = new GreaterThanEqual(
                new FromUnixtime(bigintCol), new VarcharLiteral("2026-03-26 00:00:00"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "constant away from any DST transition should derive");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(bigintCol) && e.child(1) instanceof Literal),
                "derived bare epoch-column lower bound must be present even in a DST zone");
    }

    // When an offset transition falls inside the guard band just below the constant's epoch, the
    // bare-epoch lower bound is unsound (a nearby transition lets an epoch below X still render a local
    // time >= c), so the rule must NOT rewrite. c = '2026-11-01 12:00:00' in America/New_York sits only
    // ~11h after the 2026-11-01 02:00 fall-back transition, which is well inside the 2-day guard band.
    @Test
    void testFromUnixtimeNotDerivedNearDstTransition() {
        ConnectContext.get().getSessionVariable().setTimeZone("America/New_York");
        GreaterThanEqual orig = new GreaterThanEqual(
                new FromUnixtime(bigintCol), new VarcharLiteral("2026-11-01 12:00:00"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // upper-bound comparison from_unixtime(ts) < c has no sound bare-epoch bound from a monotone map
    // in this relaxed direction, so it is not inferred (mirrors the prefix upper-bound rule).
    @Test
    void testFromUnixtimeLessThanNotRewritten() {
        ConnectContext.get().getSessionVariable().setTimeZone("Asia/Shanghai");
        LessThan orig = new LessThan(
                new FromUnixtime(bigintCol), new VarcharLiteral("2026-03-26 00:00:00"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // a 2-arg from_unixtime(ts, fmt) with a NON-monotonic format must not derive an epoch bound: the
    // output is compared as a string, and '%Y-%d-%m' is not textual==chronological, so 2024-01-31
    // renders '2024-31-01' >= '2024-02-01' while its epoch is below unix_timestamp('2024-02-01') -- a
    // bare epoch bound would wrongly drop it. Mirrors the monoFormat gate in FromUnixtime.isMonotonic.
    @Test
    void testFromUnixtimeNonMonotonicFormatNotRewritten() {
        ConnectContext.get().getSessionVariable().setTimeZone("Asia/Shanghai");
        GreaterThanEqual orig = new GreaterThanEqual(
                new FromUnixtime(bigintCol, new VarcharLiteral("%Y-%d-%m")), new VarcharLiteral("2024-02-01"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // ---- months_add / years_add: monotone non-decreasing (end-of-month clamp only makes plateaus),
    //      NOT a floor. Relaxed lower bound: f(col,k) op c  ==>  col >= inverse_sub(c,k). ----

    // months_add(dt, 2) >= '2026-03-26' => and(orig, dt >= months_sub('2026-03-26', 2) = '2026-01-26')
    @Test
    void testMonthsAddGeDerivesLowerBound() {
        GreaterThanEqual orig = new GreaterThanEqual(
                new MonthsAdd(dtCol, new IntegerLiteral(2)), new VarcharLiteral("2026-03-26 00:00:00"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "months_add(dt,k)>=c should derive a lower bound");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol) && e.child(1) instanceof Literal),
                "derived bare-column lower bound dt >= months_sub(c,k) must be present");
    }

    // strict '>' still emits '>=' (plateaus mean '>' cannot tighten to '>'): superset, never drops a row
    @Test
    void testMonthsAddGtDerivesGeLowerBound() {
        GreaterThan orig = new GreaterThan(
                new MonthsAdd(dtCol, new IntegerLiteral(1)), new VarcharLiteral("2026-03-26 00:00:00"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol) && e.child(1) instanceof Literal),
                "even for '>' the derived bound is '>=' (plateau cannot be tightened)");
    }

    // negative k: months_add(dt, -3) >= '2026-03-26' => dt >= months_sub(c, -3) = months_add(c, 3)
    @Test
    void testMonthsAddNegativeKDerivesLowerBound() {
        GreaterThanEqual orig = new GreaterThanEqual(
                new MonthsAdd(dtCol, new IntegerLiteral(-3)), new VarcharLiteral("2026-03-26 00:00:00"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol) && e.child(1) instanceof Literal));
    }

    // years_add(dt, 1) >= '2026-02-28' => dt >= years_sub('2026-02-28', 1). Leap-clamp constant is safe
    // for the lower bound (the round-trip only undershoots col, keeping it a sound superset).
    @Test
    void testYearsAddGeDerivesLowerBound() {
        GreaterThanEqual orig = new GreaterThanEqual(
                new YearsAdd(dtCol, new IntegerLiteral(1)), new VarcharLiteral("2026-02-28 00:00:00"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol) && e.child(1) instanceof Literal));
    }

    // upper bound must be excluded: months_add(col,k) <= c does NOT imply col <= inverse(c,k)
    // (col=2024-01-30 satisfies months_add(.,1)=2024-02-29 <= 2024-02-29 but col > months_sub(c,1)).
    @Test
    void testMonthsAddLessThanNotRewritten() {
        LessThan orig = new LessThan(
                new MonthsAdd(dtCol, new IntegerLiteral(1)), new VarcharLiteral("2026-03-26 00:00:00"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // TimeStampTz column is excluded: it carries a timezone, so calendar-add is no longer a pure
    // wall-clock operation. isDateLikeType() is true for TimeStampTz, so the rule must whitelist
    // DateV2/DateTimeV2 explicitly. months_add(tstz, 1) >= c must not rewrite.
    @Test
    void testMonthsAddTimestampTzNotRewritten() {
        SlotReference tstzCol = new SlotReference("tstz", TimeStampTzType.SYSTEM_DEFAULT, true);
        GreaterThanEqual orig = new GreaterThanEqual(
                new MonthsAdd(tstzCol, new IntegerLiteral(1)), new VarcharLiteral("2026-03-26 00:00:00"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // non-literal k must not rewrite (cannot fold the inverse bound)
    @Test
    void testMonthsAddNonLiteralKNotRewritten() {
        GreaterThanEqual orig = new GreaterThanEqual(
                new MonthsAdd(dtCol, intCol), new VarcharLiteral("2026-03-26 00:00:00"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // ---- ceil family: f(col) >= col, so col <= c is derived from f(col) <= c (upper bound). ----

    // day_ceil(dt) <= c  =>  and(orig, dt <= c). Mirror of the floor lower bound.
    @Test
    void testDayCeilLeDerivesUpperBound() {
        LessThanEqual orig = new LessThanEqual(new DayCeil(dtCol), new VarcharLiteral("2026-03-26"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "day_ceil(dt)<=c should derive an upper bound");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThanEqual && e.child(0).equals(dtCol)),
                "derived bare-column upper bound dt <= c must be present");
    }

    // hour_ceil(dt) < c  =>  and(orig, dt < c)
    @Test
    void testHourCeilLtDerivesUpperBound() {
        LessThan orig = new LessThan(new HourCeil(dtCol), new VarcharLiteral("2026-03-26 05:00:00"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And);
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThan && e.child(0).equals(dtCol)));
    }

    // day_ceil(dt) = c  =>  and(orig, dt > c - 1 day AND dt <= c). preimage two-sided range: the upper
    // bound is the ceil property, the lower bound is the constant-folded ceilLowerBound (c - 1 day).
    @Test
    void testDayCeilEqDerivesTwoSidedRange() {
        EqualTo orig = new EqualTo(new DayCeil(dtCol), new VarcharLiteral("2026-03-26"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "day_ceil(dt)=c should derive a two-sided range");
        // upper bound dt <= c
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThanEqual && e.child(0).equals(dtCol)),
                "upper bound dt <= c expected");
        // lower bound dt > c - 1 day (folded literal, exclusive)
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThan && e.child(0).equals(dtCol) && e.child(1) instanceof Literal),
                "exclusive lower bound dt > folded(c - 1 day) expected");
    }

    // lower-bound comparison on a ceil function: day_ceil(dt) >= c has no sound bare-column bound
    // (col can be anywhere <= f(col); f(col) >= c says nothing about a lower bound on col). Not inferred.
    @Test
    void testDayCeilGeNotRewritten() {
        GreaterThanEqual orig = new GreaterThanEqual(new DayCeil(dtCol), new VarcharLiteral("2026-03-26"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // ceil with an origin (3rd arg) is excluded: the month/quarter/year day-of-month clamp can make
    // the ceil round DOWN, breaking f(col) >= col, so no bound may be derived. day_ceil is additive
    // and would be safe, but the rule uniformly rejects any origin form for simplicity/safety.
    @Test
    void testDayCeilWithOriginNotRewritten() {
        LessThanEqual orig = new LessThanEqual(
                new DayCeil(dtCol, new IntegerLiteral(1), new VarcharLiteral("2020-01-01 00:00:00")),
                new VarcharLiteral("2026-03-26"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // a non-positive period is invalid (BE throws for period < 1 at runtime): deriving a bare-column
    // bound could prune the scan to empty and silently turn that runtime error into a wrong empty
    // result, so no bound may be derived for period <= 0. Covers both the inequality path (day_ceil <=)
    // and the equality preimage path (day_floor =), and both zero and negative periods.
    @Test
    void testDayCeilZeroPeriodNotRewritten() {
        LessThanEqual orig = new LessThanEqual(
                new DayCeil(dtCol, new IntegerLiteral(0)), new VarcharLiteral("2026-03-26"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    @Test
    void testDayFloorNegativePeriodNotRewritten() {
        EqualTo orig = new EqualTo(
                new DayFloor(dtCol, new IntegerLiteral(-3)), new VarcharLiteral("2026-03-26"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // day_floor(dt, origin) with a user origin (arity-2, 2nd arg date-like) IS rewritten: FLOOR always
    // rounds to a bucket boundary <= dt regardless of origin, so f(dt) <= dt still holds and the
    // lower-bound reversal dt >= c stays sound. (Unlike ceil, where an origin can round below col.)
    @Test
    void testDayFloorWithOriginDerivesLowerBound() {
        GreaterThanEqual orig = new GreaterThanEqual(
                new DayFloor(dtCol, new VarcharLiteral("2020-06-01 00:00:00")),
                new VarcharLiteral("2026-03-26"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "floor-with-origin should still derive a lower bound");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol)),
                "derived bare-column lower bound dt >= c must be present");
    }

    // day_floor(dt, period, origin) (arity-3, positive literal period) IS rewritten too -- origin is
    // fine for floor, and the literal period is valid.
    @Test
    void testDayFloorWithPeriodAndOriginDerivesLowerBound() {
        GreaterThanEqual orig = new GreaterThanEqual(
                new DayFloor(dtCol, new IntegerLiteral(7), new VarcharLiteral("2020-06-01 00:00:00")),
                new VarcharLiteral("2026-03-26"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "floor with period+origin should derive a lower bound");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol)),
                "derived bare-column lower bound dt >= c must be present");
    }

    // day_floor(dt, dynamicPeriodColumn): a non-literal period cannot be validated at plan time (BE
    // rejects period < 1 at runtime); deriving a bound could prune to empty and mask that error, so
    // it is not rewritten. Uses intCol (an integer column) as the period.
    @Test
    void testDayFloorDynamicPeriodNotRewritten() {
        GreaterThanEqual orig = new GreaterThanEqual(
                new DayFloor(dtCol, intCol), new VarcharLiteral("2026-03-26"));
        Expression coerced = typeCoercion(orig);
        Assertions.assertEquals(coerced, executor.rewrite(coerced, context));
    }

    // ---- week_floor / week_ceil: the only floor/ceil family that lacked Monotonic; now wired in via
    // the same isFloor()/isCeil() interface, so the rule handles them with NO rule change. Week buckets
    // off the shared FIRST_DAY origin (BE daynr()/7), reversible through WeeksAdd/WeeksSub. ----

    // week_floor(dt) >= c derives the lower bound dt >= c (floor property f(col) <= col).
    @Test
    void testWeekFloorGeDerivesLowerBound() {
        GreaterThanEqual orig = new GreaterThanEqual(new WeekFloor(dtCol), new VarcharLiteral("2026-03-23"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "week_floor(dt)>=c should derive a lower bound");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol)),
                "derived bare-column lower bound dt >= c must be present");
    }

    // week_floor(dt) = c derives the two-sided range dt >= c AND dt < c + 1 week (WeeksAdd bucket).
    @Test
    void testWeekFloorEqDerivesTwoSidedRange() {
        EqualTo orig = new EqualTo(new WeekFloor(dtCol), new VarcharLiteral("2026-03-23"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "week_floor(dt)=c should derive a two-sided range");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(dtCol)),
                "lower bound dt >= c expected");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThan && e.child(0).equals(dtCol) && e.child(1) instanceof Literal),
                "upper bound dt < folded(c + 1 week) expected");
    }

    // week_ceil(dt) <= c derives the upper bound dt <= c (ceil property f(col) >= col).
    @Test
    void testWeekCeilLeDerivesUpperBound() {
        LessThanEqual orig = new LessThanEqual(new WeekCeil(dtCol), new VarcharLiteral("2026-03-23"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "week_ceil(dt)<=c should derive an upper bound");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThanEqual && e.child(0).equals(dtCol)),
                "derived bare-column upper bound dt <= c must be present");
    }

    // week_ceil(dt) = c derives the two-sided range dt > c - 1 week (WeeksSub bucket) AND dt <= c.
    @Test
    void testWeekCeilEqDerivesTwoSidedRange() {
        EqualTo orig = new EqualTo(new WeekCeil(dtCol), new VarcharLiteral("2026-03-23"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "week_ceil(dt)=c should derive a two-sided range");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThanEqual && e.child(0).equals(dtCol)),
                "upper bound dt <= c expected");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThan && e.child(0).equals(dtCol) && e.child(1) instanceof Literal),
                "exclusive lower bound dt > folded(c - 1 week) expected");
    }

    // TimeStampTz + floor equality: BE truncates in SESSION-LOCAL time, so a DST fall-back day's UTC
    // bucket is 25h, but floorUpperBound adds a fixed 24h UTC bucket. That UTC upper bound would prune
    // the fall-back day's last hour, so for a TSTZ column the '=' preimage must keep ONLY the sound
    // lower bound tstz >= c and emit NO upper bound. (Reachability confirmed: day_floor(tstz) stays a
    // bare TSTZ slot through type coercion.)
    @Test
    void testDayFloorTimestampTzEqKeepsOnlyLowerBound() {
        EqualTo orig = new EqualTo(new DayFloor(tstzCol), new VarcharLiteral("2024-11-03 00:00:00"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "should still derive the sound lower bound");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(tstzCol)),
                "sound lower bound tstz >= c must be present");
        Assertions.assertTrue(rewritten.getArguments().stream().noneMatch(e ->
                e instanceof LessThan && e.child(0).equals(tstzCol)),
                "the fixed-UTC-bucket upper bound must NOT be emitted for a TSTZ column");
    }

    // TimeStampTz + ceil equality: mirror of the floor case. The fixed-UTC-bucket lower bound would
    // prune the fall-back day's first hour, so keep ONLY the sound upper bound tstz <= c.
    @Test
    void testDayCeilTimestampTzEqKeepsOnlyUpperBound() {
        EqualTo orig = new EqualTo(new DayCeil(tstzCol), new VarcharLiteral("2024-11-03 00:00:00"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "should still derive the sound upper bound");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof LessThanEqual && e.child(0).equals(tstzCol)),
                "sound upper bound tstz <= c must be present");
        Assertions.assertTrue(rewritten.getArguments().stream().noneMatch(e ->
                e instanceof GreaterThan && e.child(0).equals(tstzCol)),
                "the fixed-UTC-bucket lower bound must NOT be emitted for a TSTZ column");
    }

    // The single-sided bounds stay sound for TSTZ (floor(col) <= col on the instant axis, zone
    // independent), so day_floor(tstz) >= c still derives tstz >= c -- only the '=' opposite bound is
    // dropped, not the whole TSTZ path.
    @Test
    void testDayFloorTimestampTzGeStillDerives() {
        GreaterThanEqual orig = new GreaterThanEqual(
                new DayFloor(tstzCol), new VarcharLiteral("2024-11-03 00:00:00"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "lower-bound comparison stays sound for TSTZ");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(tstzCol)),
                "tstz >= c must be derived (sound on the instant axis)");
    }

    // date_trunc(tstz, 'day') = c: date_trunc reaches the floor '=' branch by a DIFFERENT path than
    // day_floor -- it is a string-unit DateTrunc admitted via isFloor()'s unit whitelist, not a
    // DateCeilFloorMonotonic. The isTimeStampTzSlot guard sits at the branch entry (after
    // extractDateFloorSource), so it covers this path too: the fixed-UTC-bucket upper bound
    // (date_trunc's floorUpperBound adds a whole UTC day/week/... ) is unsound on a DST fall-back day
    // and must be dropped, keeping only the sound lower bound tstz >= c.
    @Test
    void testDateTruncTimestampTzEqKeepsOnlyLowerBound() {
        EqualTo orig = new EqualTo(
                new DateTrunc(tstzCol, new VarcharLiteral("day")), new VarcharLiteral("2024-11-03 00:00:00"));
        Expression rewritten = executor.rewrite(typeCoercion(orig), context);
        Assertions.assertTrue(rewritten instanceof And, "should still derive the sound lower bound");
        Assertions.assertTrue(rewritten.getArguments().stream().anyMatch(e ->
                e instanceof GreaterThanEqual && e.child(0).equals(tstzCol)),
                "sound lower bound tstz >= c must be present");
        Assertions.assertTrue(rewritten.getArguments().stream().noneMatch(e ->
                e instanceof LessThan && e.child(0).equals(tstzCol)),
                "the fixed-UTC-bucket upper bound must NOT be emitted for a TSTZ column");
    }
}
