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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DORIS-29047 (NaN) and its signed-zero sibling: a FLOAT/DOUBLE predicate pushed to iceberg must not prune a
 * file that holds rows Doris considers matching. A pruned file never becomes a split, so BE never sees those
 * rows and cannot filter them back in — the query silently returns too few rows rather than failing.
 *
 * <p>Two oracles, because either one alone misses the bug:
 * <ul>
 *   <li>{@link InclusiveMetricsEvaluator} over hand-built file metrics — the actual pruning decision, and a
 *       direct reproduction of the reported queries (metrics shaped exactly as spark / iceberg-java write
 *       them: NaN never reaches the bounds, {@code -0.0} stays {@code -0.0}).</li>
 *   <li>The row-level {@link Evaluator} over the same converted expression — pins that the expression is
 *       EQUIVALENT to the Doris predicate rather than merely wider. Equivalence is what makes NOT / AND / OR
 *       compose: iceberg's RewriteNot turns {@code not(or(gt, isNaN))} into {@code and(ltEq, notNaN)}, which
 *       is exactly Doris's {@code NOT(d > v)} over a NaN row.</li>
 * </ul>
 */
public class IcebergPredicateConverterFloatSemanticsTest {

    private static final int D_ID = 1;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(D_ID, "d", Types.DoubleType.get()),
            Types.NestedField.optional(2, "f", Types.FloatType.get()),
            Types.NestedField.optional(3, "i", Types.IntegerType.get()));

    // The iceberg spec forbids NaN as a bound ("NaNs are not permitted as lower or upper bounds"), so a NaN
    // shows up only in nan_value_counts; -0.0 survives in the bounds because they use the IEEE total order.
    private static final DataFile NAN_ONLY = file("nan_only", 1, 1L, null, null);
    private static final DataFile NAN_MIXED = file("nan_mixed", 2, 1L, 1.0d, 1.0d);
    private static final DataFile NEG_ZERO = file("negzero", 1, 0L, -0.0d, -0.0d);
    private static final DataFile POS_ZERO = file("poszero", 1, 0L, 0.0d, 0.0d);
    private static final DataFile PLAIN = file("plain", 2, 0L, 10.0d, 20.0d);
    // Doris's own writer reports no NaN count at all (IcebergWriterHelper passes a null nanValueCounts), so
    // nothing in the metadata can rule NaN out and such a file must survive every float range predicate.
    private static final DataFile UNKNOWN_NAN = file("doris_written", 2, null, 1.0d, 1.0d);
    // Same bounds as NAN_MIXED and UNKNOWN_NAN, but the writer states there is no NaN -- the only difference
    // that may bring pruning back.
    private static final DataFile NO_NAN_ONE_VALUE = file("one_value", 2, 0L, 1.0d, 1.0d);

    /**
     * The reported query: {@code WHERE d > 0} / {@code d >= 0} returned nothing over a single NaN row.
     *
     * <p>The four cases here are the deterministic counterpart of the {@code float_prune_nan_only}
     * assertions in the {@code test_iceberg_float_predicate_pushdown} regression suite, whose fixture
     * carries exactly these metrics.
     */
    @Test
    public void nanOnlyFileSurvivesRangePredicates() {
        Assertions.assertTrue(mayMatch(single(cmp("d", ConnectorComparison.Operator.GT, 0.0d)), NAN_ONLY));
        Assertions.assertTrue(mayMatch(single(cmp("d", ConnectorComparison.Operator.GE, 0.0d)), NAN_ONLY));
        // NaN satisfies neither, so the file is still pruned.
        Assertions.assertFalse(mayMatch(single(cmp("d", ConnectorComparison.Operator.LT, 0.0d)), NAN_ONLY));
        Assertions.assertFalse(mayMatch(single(cmp("d", ConnectorComparison.Operator.EQ, 0.0d)), NAN_ONLY));
    }

    /**
     * The half that a "whole file is NaN" special case would miss: NaN hides outside the bounds, so a file
     * holding {1.0, NaN} is pruned by the bounds alone for {@code d > 5}.
     */
    @Test
    public void nanMixedFileSurvivesRangePredicateAboveItsBounds() {
        Assertions.assertTrue(mayMatch(single(cmp("d", ConnectorComparison.Operator.GT, 5.0d)), NAN_MIXED));
        Assertions.assertTrue(mayMatch(single(cmp("d", ConnectorComparison.Operator.GE, 5.0d)), NAN_MIXED));
        Assertions.assertTrue(mayMatch(single(cmp("d", ConnectorComparison.Operator.GT, 5.0d)), UNKNOWN_NAN));
    }

    /**
     * {@code !=} / {@code NOT IN} prune through {@code uniqueValue}, whose NaN guard only fires when the file
     * actually reports a NaN count — so a file written without one is pruned as if its single bound value
     * were its only value.
     */
    @Test
    public void notEqualAndNotInSurviveOnFilesThatMayHoldNaN() {
        Assertions.assertTrue(mayMatch(single(cmp("d", ConnectorComparison.Operator.NE, 1.0d)), UNKNOWN_NAN));
        Assertions.assertTrue(mayMatch(single(notIn("d", 1.0d)), UNKNOWN_NAN));
    }

    /**
     * The fix must not blanket-disable pruning: a file that reports zero NaNs is still pruned, which is what
     * keeps spark / flink / iceberg-java written tables fully prunable.
     */
    @Test
    public void rangePredicatesStillPruneFilesWithoutNaN() {
        Assertions.assertFalse(mayMatch(single(cmp("d", ConnectorComparison.Operator.GT, 100.0d)), PLAIN));
        Assertions.assertFalse(mayMatch(single(cmp("d", ConnectorComparison.Operator.GE, 100.0d)), PLAIN));
        Assertions.assertFalse(mayMatch(single(cmp("d", ConnectorComparison.Operator.LT, 1.0d)), PLAIN));
        Assertions.assertFalse(mayMatch(single(cmp("d", ConnectorComparison.Operator.EQ, 1.0d)), PLAIN));
        Assertions.assertFalse(mayMatch(single(notIn("d", 1.0d)), NO_NAN_ONE_VALUE));
        Assertions.assertFalse(
                mayMatch(single(cmp("d", ConnectorComparison.Operator.NE, 1.0d)), NO_NAN_ONE_VALUE));
    }

    /**
     * Signed zero: Doris reads {@code -0.0 == 0.0} (IEEE) while iceberg orders {@code -0.0} strictly before
     * {@code +0.0}, so a bound at the wrong zero drops the other one in both directions.
     */
    @Test
    public void signedZeroFilesSurviveZeroPredicates() {
        Assertions.assertTrue(mayMatch(single(cmp("d", ConnectorComparison.Operator.EQ, 0.0d)), NEG_ZERO));
        Assertions.assertTrue(mayMatch(single(cmp("d", ConnectorComparison.Operator.GE, 0.0d)), NEG_ZERO));
        Assertions.assertTrue(mayMatch(single(in("d", 0.0d)), NEG_ZERO));
        Assertions.assertTrue(mayMatch(single(cmp("d", ConnectorComparison.Operator.EQ, -0.0d)), POS_ZERO));
        Assertions.assertTrue(mayMatch(single(cmp("d", ConnectorComparison.Operator.LE, -0.0d)), POS_ZERO));
        // Still precise: widening the bound to cover both zeros must not start keeping unrelated files, and
        // -0.0 is neither > 0 nor < 0, so both still prune its file (the regression suite asserts the same
        // four outcomes over the identically-shaped float_prune_negzero fixture).
        Assertions.assertFalse(mayMatch(single(cmp("d", ConnectorComparison.Operator.GT, 0.0d)), NEG_ZERO));
        Assertions.assertFalse(mayMatch(single(cmp("d", ConnectorComparison.Operator.LT, 0.0d)), NEG_ZERO));
        Assertions.assertFalse(mayMatch(single(cmp("d", ConnectorComparison.Operator.EQ, 0.0d)), PLAIN));
        Assertions.assertFalse(mayMatch(single(cmp("d", ConnectorComparison.Operator.LE, -0.0d)), PLAIN));
    }

    /**
     * {@code WHERE d > 0} carries an INT literal, not a double one (Nereids types the bare {@code 0} as an
     * integer), so the zero handling has to see through the literal's Java type.
     */
    @Test
    public void integerZeroLiteralOnFloatingColumnIsStillAZero() {
        Expression ge = single(new ConnectorComparison(ConnectorComparison.Operator.GE, col("d"),
                new ConnectorLiteral(ConnectorType.of("INT"), 0L)));
        Assertions.assertTrue(mayMatch(ge, NEG_ZERO), "d >= 0 must keep a -0.0-only file");
        Assertions.assertTrue(mayMatch(ge, NAN_ONLY), "d >= 0 must keep a NaN-only file");
    }

    /** A FLOAT column takes the same path — the reconciliation keys off the iceberg column type. */
    @Test
    public void floatColumnGetsTheSameTreatment() {
        Expression gt = single(new ConnectorComparison(ConnectorComparison.Operator.GT,
                col("f"), new ConnectorLiteral(ConnectorType.of("FLOAT"), 0.0d)));
        Assertions.assertEquals(Expression.Operation.OR, gt.op());
        Assertions.assertEquals(Expression.Operation.IS_NAN, ((Or) gt).right().op());
    }

    /**
     * A NaN literal is reachable ({@code WHERE d > cast('nan' as double)} — Nereids' DoubleLiteral parses the
     * {@code nan} spelling) and iceberg refuses it outright: {@code Expressions.*(col, NaN)} throws "Cannot
     * create expression literal from NaN". It must become the unary isNaN/notNaN instead of escaping as a
     * planning failure.
     */
    @Test
    public void nanLiteralMapsToUnaryPredicatesInsteadOfThrowing() {
        Assertions.assertEquals(Expression.Operation.IS_NAN,
                single(cmp("d", ConnectorComparison.Operator.EQ, Double.NaN)).op());
        Assertions.assertEquals(Expression.Operation.IS_NAN,
                single(cmp("d", ConnectorComparison.Operator.GE, Double.NaN)).op());
        Assertions.assertEquals(Expression.Operation.NOT_NAN,
                single(cmp("d", ConnectorComparison.Operator.NE, Double.NaN)).op());
        Assertions.assertEquals(Expression.Operation.NOT_NAN,
                single(cmp("d", ConnectorComparison.Operator.LT, Double.NaN)).op());
        Assertions.assertEquals(Expression.Operation.FALSE,
                single(cmp("d", ConnectorComparison.Operator.GT, Double.NaN)).op());
        Assertions.assertEquals(Expression.Operation.NOT_NULL,
                single(cmp("d", ConnectorComparison.Operator.LE, Double.NaN)).op());
        // `d = NaN` keeps exactly the files that may hold a NaN.
        Assertions.assertTrue(mayMatch(single(cmp("d", ConnectorComparison.Operator.EQ, Double.NaN)), NAN_ONLY));
        Assertions.assertFalse(mayMatch(single(cmp("d", ConnectorComparison.Operator.EQ, Double.NaN)), PLAIN));
    }

    /** IN / NOT IN carry both problems: a listed NaN cannot be an iceberg literal, a listed zero is two points. */
    @Test
    public void inListHandlesNaNAndSignedZero() {
        Assertions.assertEquals(Expression.Operation.IS_NAN, single(in("d", Double.NaN)).op());
        Assertions.assertEquals(Expression.Operation.NOT_NAN, single(notIn("d", Double.NaN)).op());
        Expression mixed = single(in("d", 1.0d, Double.NaN));
        Assertions.assertTrue(mayMatch(mixed, NAN_MIXED));
        Assertions.assertFalse(mayMatch(mixed, PLAIN));
    }

    /** Non-floating columns keep the plain 1:1 mapping — no isNaN arm, no zero expansion. */
    @Test
    public void nonFloatingColumnsAreUnchanged() {
        Assertions.assertEquals(Expression.Operation.GT,
                single(intCmp(ConnectorComparison.Operator.GT)).op());
        Assertions.assertEquals(Expression.Operation.EQ,
                single(intCmp(ConnectorComparison.Operator.EQ)).op());
        Assertions.assertEquals(Expression.Operation.LT_EQ,
                single(intCmp(ConnectorComparison.Operator.LE)).op());
    }

    /**
     * A NOT must not smuggle the bug back in through the FILE evaluator. Row-level equivalence is not enough
     * here: iceberg's RewriteNot lowers {@code not(lessThan(d, 5))} to a bare {@code gtEq(d, 5)}, whose
     * metrics evaluator prunes a {1.0, NaN} file even though Doris's {@code NOT(d < 5)} matches the NaN row.
     * The leaves therefore carry the NaN half on both sides — {@code OR isNaN} where NaN matches,
     * {@code AND notNaN} where it does not — so De Morgan turns one into the other.
     */
    @Test
    public void negatedPredicatesStillKeepFilesHoldingNaN() {
        // NOT(d < 5) / NOT(d <= 5): Doris matches the NaN row, so the file must survive.
        Assertions.assertTrue(mayMatch(single(not(cmp("d", ConnectorComparison.Operator.LT, 5.0d))), NAN_MIXED));
        Assertions.assertTrue(mayMatch(single(not(cmp("d", ConnectorComparison.Operator.LE, 5.0d))), NAN_MIXED));
        Assertions.assertTrue(mayMatch(single(not(cmp("d", ConnectorComparison.Operator.LE, 0.0d))), NAN_ONLY));
        // NOT(d = 1.0) / NOT(d IN (1.0)) over a file whose metrics cannot rule NaN out.
        Assertions.assertTrue(mayMatch(single(not(cmp("d", ConnectorComparison.Operator.EQ, 1.0d))), UNKNOWN_NAN));
        Assertions.assertTrue(mayMatch(single(not(in("d", 1.0d))), UNKNOWN_NAN));
        // Still precise in the other direction: NOT(d = 0) over a -0.0-only file matches nothing, so the
        // file is pruned rather than blanket-kept.
        Assertions.assertFalse(mayMatch(single(not(cmp("d", ConnectorComparison.Operator.EQ, 0.0d))), NEG_ZERO));
        // And NOT(d > 100) must still prune a file that lies entirely above 100 with no NaN.
        Assertions.assertFalse(mayMatch(single(not(cmp("d", ConnectorComparison.Operator.GE, 1.0d))), PLAIN));
    }

    /**
     * The equivalence oracle. iceberg's row-level {@link Evaluator} orders floats the way Doris does (NaN
     * greatest), and the signed-zero expansion makes {@code = 0} match both zeros there too, so the converted
     * expression must agree with Doris row for row — and its negation must be the exact complement.
     */
    @Test
    public void convertedExpressionMatchesDorisRowSemantics() {
        double[] rows = {Double.NaN, -0.0d, 0.0d, 1.0d, -1.0d};
        double[] lits = {0.0d, -0.0d, 1.0d};
        List<ConnectorComparison.Operator> ops = Arrays.asList(
                ConnectorComparison.Operator.EQ, ConnectorComparison.Operator.NE,
                ConnectorComparison.Operator.GT, ConnectorComparison.Operator.GE,
                ConnectorComparison.Operator.LT, ConnectorComparison.Operator.LE);
        for (ConnectorComparison.Operator op : ops) {
            for (double lit : lits) {
                Expression expr = single(cmp("d", op, lit));
                Expression negated = single(not(cmp("d", op, lit)));
                for (double row : rows) {
                    String where = "d " + op + " " + lit + " over row " + row;
                    boolean expected = dorisMatches(op, row, lit);
                    Assertions.assertEquals(expected, evalRow(expr, row), where);
                    Assertions.assertEquals(!expected, evalRow(negated, row), "NOT " + where);
                }
            }
        }
    }

    // ───────────────────────────────── Doris float semantics (the oracle) ─────────────────────────────────

    // be/src/common/compare.h: NaN equals NaN and is greater than everything else; zeros compare by IEEE, so
    // -0.0 == 0.0.
    private static int dorisCompare(double left, double right) {
        if (Double.isNaN(left)) {
            return Double.isNaN(right) ? 0 : 1;
        }
        if (Double.isNaN(right)) {
            return -1;
        }
        return Double.compare(left == 0.0d ? 0.0d : left, right == 0.0d ? 0.0d : right);
    }

    private static boolean dorisMatches(ConnectorComparison.Operator op, double row, double lit) {
        int cmp = dorisCompare(row, lit);
        switch (op) {
            case EQ:
                return cmp == 0;
            case NE:
                return cmp != 0;
            case GT:
                return cmp > 0;
            case GE:
                return cmp >= 0;
            case LT:
                return cmp < 0;
            case LE:
                return cmp <= 0;
            default:
                throw new IllegalArgumentException("unexpected operator " + op);
        }
    }

    // ───────────────────────────────────────────── plumbing ──────────────────────────────────────────────

    private static Expression single(ConnectorExpression expr) {
        List<Expression> out = new IcebergPredicateConverter(SCHEMA, ZoneOffset.UTC).convert(expr);
        Assertions.assertEquals(1, out.size(), "expected exactly one pushed predicate for " + expr);
        return out.get(0);
    }

    private static ConnectorColumnRef col(String name) {
        return new ConnectorColumnRef(name, ConnectorType.of("UNKNOWN"));
    }

    private static ConnectorComparison cmp(String colName, ConnectorComparison.Operator op, double value) {
        return new ConnectorComparison(op, col(colName), ConnectorLiteral.ofDouble(value));
    }

    private static ConnectorNot not(ConnectorExpression operand) {
        return new ConnectorNot(operand);
    }

    private static ConnectorComparison intCmp(ConnectorComparison.Operator op) {
        return new ConnectorComparison(op, col("i"), new ConnectorLiteral(ConnectorType.of("INT"), 0L));
    }

    private static ConnectorIn in(String colName, double... values) {
        return connectorIn(colName, false, values);
    }

    private static ConnectorIn notIn(String colName, double... values) {
        return connectorIn(colName, true, values);
    }

    private static ConnectorIn connectorIn(String colName, boolean negated, double... values) {
        List<ConnectorExpression> items = new ArrayList<>();
        for (double value : values) {
            items.add(ConnectorLiteral.ofDouble(value));
        }
        return new ConnectorIn(col(colName), items, negated);
    }

    private static boolean mayMatch(Expression expr, DataFile dataFile) {
        return new InclusiveMetricsEvaluator(SCHEMA, expr).eval(dataFile);
    }

    private static boolean evalRow(Expression expr, double value) {
        return new Evaluator(SCHEMA.asStruct(), expr).eval(new DoubleRow(value));
    }

    private static DataFile file(String name, long records, Long nanCount, Double lower, Double upper) {
        Map<Integer, Long> valueCounts = new HashMap<>();
        valueCounts.put(D_ID, records);
        Map<Integer, Long> nullCounts = new HashMap<>();
        nullCounts.put(D_ID, 0L);
        Map<Integer, Long> nanCounts = null;
        if (nanCount != null) {
            nanCounts = new HashMap<>();
            nanCounts.put(D_ID, nanCount);
        }
        Metrics metrics = new Metrics(records, null, valueCounts, nullCounts, nanCounts,
                bound(lower), bound(upper));
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/fake/" + name + ".parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1024L)
                .withRecordCount(records)
                .withMetrics(metrics)
                .build();
    }

    private static Map<Integer, ByteBuffer> bound(Double value) {
        if (value == null) {
            return null;
        }
        Map<Integer, ByteBuffer> bounds = new HashMap<>();
        bounds.put(D_ID, Conversions.toByteBuffer(Types.DoubleType.get(), value));
        return bounds;
    }

    // Minimal StructLike over SCHEMA; only "d" (position 0) is ever read.
    private static final class DoubleRow implements StructLike {
        private final Double value;

        private DoubleRow(Double value) {
            this.value = value;
        }

        @Override
        public int size() {
            return 3;
        }

        @Override
        public <T> T get(int pos, Class<T> javaClass) {
            return javaClass.cast(pos == 0 ? value : null);
        }

        @Override
        public <T> void set(int pos, T newValue) {
            throw new UnsupportedOperationException();
        }
    }
}
