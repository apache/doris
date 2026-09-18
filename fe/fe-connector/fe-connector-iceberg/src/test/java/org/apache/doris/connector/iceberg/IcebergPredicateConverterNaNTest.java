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
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorBetween;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * DORIS-29047: Doris orders NaN above every other floating-point value (be/src/common/compare.h), so
 * {@code d > 0} and {@code d >= 0} are true for NaN. Iceberg's metrics/manifest evaluators instead assume NaN
 * never satisfies a range comparison: NaN is excluded from lower/upper bounds and an all-NaN file is pruned
 * outright. Pushing a bare {@code greaterThan} therefore prunes files that hold matching NaN rows, and BE never
 * sees the split — the rows are silently lost.
 *
 * <p>These tests assert the SEMANTICS (what {@link InclusiveMetricsEvaluator} does to a file's metrics), not the
 * rendered expression string: a string assertion cannot tell a correct rewrite from one that also destroys the
 * pruning of NaN-free files. Each range case therefore pins three metrics shapes — an all-NaN file, a
 * {1.0, NaN} file whose bounds omit the NaN, and a NaN-free file that must STILL be pruned.</p>
 */
public class IcebergPredicateConverterNaNTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "c_int", Types.IntegerType.get()),
            Types.NestedField.required(4, "c_float", Types.FloatType.get()),
            Types.NestedField.required(5, "c_double", Types.DoubleType.get()));

    private static final int DOUBLE_ID = 5;
    private static final int FLOAT_ID = 4;

    private static IcebergPredicateConverter converter() {
        return new IcebergPredicateConverter(SCHEMA, ZoneOffset.UTC);
    }

    private static ConnectorColumnRef col(String name) {
        return new ConnectorColumnRef(name, ConnectorType.of("UNKNOWN"));
    }

    private static ConnectorLiteral doubleLit(double value) {
        return new ConnectorLiteral(ConnectorType.of("DOUBLE"), value);
    }

    private static ConnectorLiteral floatLit(double value) {
        return new ConnectorLiteral(ConnectorType.of("FLOAT"), value);
    }

    private static ConnectorComparison cmp(ConnectorComparison.Operator op, String colName, ConnectorLiteral lit) {
        return new ConnectorComparison(op, col(colName), lit);
    }

    /** The single pushed predicate for {@code expr}; fails loudly when the converter dropped it. */
    private static Expression pushed(ConnectorExpression expr) {
        List<Expression> out = converter().convert(expr);
        Assertions.assertEquals(1, out.size(), "expected exactly one pushed predicate for " + expr);
        return out.get(0);
    }

    /**
     * A data file carrying only the metrics the evaluators consult. {@code nanCount == null} models a writer that
     * reports no nan_value_counts at all (Doris' own iceberg writer, see IcebergWriterHelper), the shape where a
     * hidden NaN is completely invisible in the metadata.
     */
    private static DataFile file(int fieldId, Type type, long rows, Long nanCount, Object lower, Object upper) {
        Map<Integer, Long> nans = nanCount == null ? null : Collections.singletonMap(fieldId, nanCount);
        Map<Integer, ByteBuffer> lowers = lower == null ? null
                : Collections.singletonMap(fieldId, Conversions.toByteBuffer(type, lower));
        Map<Integer, ByteBuffer> uppers = upper == null ? null
                : Collections.singletonMap(fieldId, Conversions.toByteBuffer(type, upper));
        Metrics metrics = new Metrics(rows, null, Collections.singletonMap(fieldId, rows),
                Collections.singletonMap(fieldId, 0L), nans, lowers, uppers);
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/tmp/doris-29047-" + fieldId + "-" + rows + "-" + nanCount + "-" + upper + ".parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(64)
                .withRecordCount(rows)
                .withMetrics(metrics)
                .build();
    }

    private static boolean mightMatch(Expression expr, DataFile dataFile) {
        return new InclusiveMetricsEvaluator(SCHEMA, expr).eval(dataFile);
    }

    /** All-NaN file: Iceberg's containsNaNsOnly shortcut prunes it for any range predicate. */
    private static DataFile allNaNDouble() {
        return file(DOUBLE_ID, Types.DoubleType.get(), 1L, 1L, null, null);
    }

    /** {1.0, NaN} written by a writer that reports nan counts (Spark/Flink): the bounds still omit the NaN. */
    private static DataFile mixedNaNDouble() {
        return file(DOUBLE_ID, Types.DoubleType.get(), 2L, 1L, 1.0d, 1.0d);
    }

    /** {1.0, NaN} written by a writer that reports NO nan counts (Doris' writer): the NaN is invisible. */
    private static DataFile mixedNaNDoubleWithoutNaNCount() {
        return file(DOUBLE_ID, Types.DoubleType.get(), 2L, null, 1.0d, 1.0d);
    }

    /** A NaN-free file below the literal: pruning it is the whole point of pushdown and must be preserved. */
    private static DataFile nanFreeDouble() {
        return file(DOUBLE_ID, Types.DoubleType.get(), 1L, 0L, 1.0d, 1.0d);
    }

    private static void assertKeepsNaNFilesAndStillPrunesNaNFreeFile(Expression expr) {
        Assertions.assertTrue(mightMatch(expr, allNaNDouble()), "all-NaN file must not be pruned: " + expr);
        Assertions.assertTrue(mightMatch(expr, mixedNaNDouble()), "{1.0, NaN} file must not be pruned: " + expr);
        Assertions.assertTrue(mightMatch(expr, mixedNaNDoubleWithoutNaNCount()),
                "{1.0, NaN} file without nan counts must not be pruned: " + expr);
        Assertions.assertFalse(mightMatch(expr, nanFreeDouble()),
                "a NaN-free file below the literal must still be pruned: " + expr);
    }

    /**
     * {@code c_double > 5} keeps every file that may hold a NaN, because Doris returns NaN rows for it.
     * MUTATION: pushing a bare {@code greaterThan} prunes the two NaN files -> red.
     */
    @Test
    public void greaterThanOnDoubleKeepsFilesHoldingNaN() {
        assertKeepsNaNFilesAndStillPrunesNaNFreeFile(
                pushed(cmp(ConnectorComparison.Operator.GT, "c_double", doubleLit(5.0d))));
    }

    /** {@code c_double >= 5}: same, NaN satisfies GE in Doris too. */
    @Test
    public void greaterThanOrEqualOnDoubleKeepsFilesHoldingNaN() {
        assertKeepsNaNFilesAndStillPrunesNaNFreeFile(
                pushed(cmp(ConnectorComparison.Operator.GE, "c_double", doubleLit(5.0d))));
    }

    /** A FLOAT column behaves like DOUBLE (the guard keys off the iceberg column type, not the literal). */
    @Test
    public void greaterThanOnFloatKeepsFilesHoldingNaN() {
        Expression expr = pushed(cmp(ConnectorComparison.Operator.GT, "c_float", floatLit(5.0d)));
        Assertions.assertTrue(mightMatch(expr, file(FLOAT_ID, Types.FloatType.get(), 1L, 1L, null, null)),
                "all-NaN float file must not be pruned: " + expr);
        Assertions.assertTrue(mightMatch(expr, file(FLOAT_ID, Types.FloatType.get(), 2L, 1L, 1.0f, 1.0f)),
                "{1.0, NaN} float file must not be pruned: " + expr);
        Assertions.assertFalse(mightMatch(expr, file(FLOAT_ID, Types.FloatType.get(), 1L, 0L, 1.0f, 1.0f)),
                "a NaN-free float file below the literal must still be pruned: " + expr);
    }

    /**
     * {@code c_double > 0.5} arrives as a BigDecimal literal from Nereids (an unsuffixed decimal), and
     * {@code c_double > '0'} as a coerced double; both must get the same treatment as a DOUBLE literal.
     */
    @Test
    public void greaterThanWithDecimalLiteralKeepsFilesHoldingNaN() {
        assertKeepsNaNFilesAndStillPrunesNaNFreeFile(pushed(cmp(ConnectorComparison.Operator.GT, "c_double",
                new ConnectorLiteral(ConnectorType.of("DECIMALV3", 2, 1), new java.math.BigDecimal("5.0")))));
    }

    /** Integral columns keep the plain range predicate — no is_nan arm, no lost pruning. */
    @Test
    public void rangePredicateOnIntColumnIsUnchanged() {
        Expression gt = pushed(cmp(ConnectorComparison.Operator.GT, "c_int",
                new ConnectorLiteral(ConnectorType.of("INT"), 5L)));
        Assertions.assertEquals(Expression.Operation.GT, gt.op(), gt.toString());
    }

    /**
     * LT/LE are left alone: Doris evaluates {@code NaN < v} and {@code NaN <= v} as false, exactly what
     * Iceberg's pruning already assumes. Adding an is_nan arm here would keep NaN files for nothing.
     */
    @Test
    public void lessThanOnDoubleIsUnchanged() {
        Expression lt = pushed(cmp(ConnectorComparison.Operator.LT, "c_double", doubleLit(5.0d)));
        Assertions.assertEquals(Expression.Operation.LT, lt.op(), lt.toString());
        Assertions.assertFalse(mightMatch(lt, allNaNDouble()), "LT must still prune an all-NaN file");

        Expression le = pushed(cmp(ConnectorComparison.Operator.LE, "c_double", doubleLit(5.0d)));
        Assertions.assertEquals(Expression.Operation.LT_EQ, le.op(), le.toString());
        Assertions.assertFalse(mightMatch(le, allNaNDouble()), "LE must still prune an all-NaN file");
    }

    /**
     * {@code NOT (c_double < 5)} is true for NaN in Doris. Iceberg's RewriteNot turns {@code not(lt)} into
     * {@code gtEq} during binding, which prunes the NaN files again — so the converter must negate in Doris
     * semantics itself (floats are totally ordered there) and let the GE path add the is_nan arm.
     * MUTATION: emitting {@code Expressions.not(lessThan(...))} -> red.
     */
    @Test
    public void notLessThanOnDoubleKeepsFilesHoldingNaN() {
        assertKeepsNaNFilesAndStillPrunesNaNFreeFile(
                pushed(new ConnectorNot(cmp(ConnectorComparison.Operator.LT, "c_double", doubleLit(5.0d)))));
    }

    /** {@code NOT (c_double <= 5)} negates to GT, which likewise keeps NaN files. */
    @Test
    public void notLessThanOrEqualOnDoubleKeepsFilesHoldingNaN() {
        assertKeepsNaNFilesAndStillPrunesNaNFreeFile(
                pushed(new ConnectorNot(cmp(ConnectorComparison.Operator.LE, "c_double", doubleLit(5.0d)))));
    }

    /** {@code NOT (c_double > 5)} is false for NaN in Doris, so the negated LE may keep pruning NaN files. */
    @Test
    public void notGreaterThanOnDoublePrunesNaNFiles() {
        Expression expr = pushed(new ConnectorNot(cmp(ConnectorComparison.Operator.GT, "c_double",
                doubleLit(5.0d))));
        Assertions.assertFalse(mightMatch(expr, allNaNDouble()), "NOT(d > 5) must still prune an all-NaN file");
    }

    /**
     * A NOT over a compound node touching a float column is not pushed at all: Iceberg's De Morgan rewrite would
     * turn the inner {@code c_double < 5} into {@code c_double >= 5} with no is_nan arm. BE still filters.
     */
    @Test
    public void notOverCompoundWithFloatColumnIsNotPushed() {
        ConnectorExpression expr = new ConnectorNot(new ConnectorAnd(Arrays.asList(
                cmp(ConnectorComparison.Operator.LT, "c_double", doubleLit(5.0d)),
                cmp(ConnectorComparison.Operator.EQ, "c_int", new ConnectorLiteral(ConnectorType.of("INT"), 1L)))));
        Assertions.assertTrue(converter().convert(expr).isEmpty(), "NOT over a compound float node must be dropped");
    }

    /**
     * Iceberg refuses to build a literal from NaN ("Cannot create expression literal from NaN"), and the scan
     * planner does not catch it, so a NaN literal must never reach {@code Expressions.*}. The comparisons that
     * have an exact iceberg form are mapped; the rest are dropped for BE to evaluate.
     */
    @Test
    public void nanLiteralComparisonsMapToNaNPredicates() {
        ConnectorLiteral nan = doubleLit(Double.NaN);
        Assertions.assertEquals(Expression.Operation.IS_NAN,
                pushed(cmp(ConnectorComparison.Operator.EQ, "c_double", nan)).op());
        Assertions.assertEquals(Expression.Operation.IS_NAN,
                pushed(cmp(ConnectorComparison.Operator.EQ_FOR_NULL, "c_double", nan)).op());
        Assertions.assertEquals(Expression.Operation.IS_NAN,
                pushed(cmp(ConnectorComparison.Operator.GE, "c_double", nan)).op());
        Assertions.assertEquals(Expression.Operation.NOT_NAN,
                pushed(cmp(ConnectorComparison.Operator.NE, "c_double", nan)).op());
        Assertions.assertEquals(Expression.Operation.NOT_NAN,
                pushed(cmp(ConnectorComparison.Operator.LT, "c_double", nan)).op());
        // d > NaN is never true and d <= NaN is always true for non-null rows: no exact narrowing form, drop.
        Assertions.assertTrue(converter().convert(cmp(ConnectorComparison.Operator.GT, "c_double", nan)).isEmpty());
        Assertions.assertTrue(converter().convert(cmp(ConnectorComparison.Operator.LE, "c_double", nan)).isEmpty());
    }

    /**
     * {@code d IN (1.0, NaN)} must not throw (Expressions.in rejects a NaN literal) AND must keep the NaN rows:
     * Doris compares NaN = NaN as true, so the NaN element matches exactly the NaN rows. The pruning of a file
     * that holds neither 1.0 nor a NaN has to survive.
     */
    @Test
    public void inWithNaNElementKeepsNaNFilesAndStillPrunes() {
        Expression expr = pushed(new ConnectorIn(col("c_double"),
                Arrays.asList(doubleLit(1.0d), doubleLit(Double.NaN)), false));
        Assertions.assertEquals(Expression.Operation.OR, expr.op(), expr.toString());
        Assertions.assertTrue(mightMatch(expr, allNaNDouble()), "all-NaN file matches the NaN element: " + expr);
        Assertions.assertTrue(mightMatch(expr, mixedNaNDouble()), "{1.0, NaN} file matches both: " + expr);
        Assertions.assertTrue(mightMatch(expr, nanFreeDouble()), "{1.0} file matches the 1.0 element: " + expr);
        Assertions.assertFalse(mightMatch(expr, file(DOUBLE_ID, Types.DoubleType.get(), 1L, 0L, 8.0d, 8.0d)),
                "a file with neither 1.0 nor NaN must still be pruned: " + expr);
    }

    /**
     * {@code d NOT IN (1.0, NaN)} is false for a NaN row in Doris, so an all-NaN file can be pruned — via the
     * notNaN arm. The {1.0, NaN} file must still be read: iceberg cannot rule it out from bounds that both
     * omit the NaN and carry the excluded value.
     */
    @Test
    public void notInWithNaNElementPrunesOnlyTheAllNaNFile() {
        Expression expr = pushed(new ConnectorIn(col("c_double"),
                Arrays.asList(doubleLit(1.0d), doubleLit(Double.NaN)), true));
        Assertions.assertEquals(Expression.Operation.AND, expr.op(), expr.toString());
        Assertions.assertFalse(mightMatch(expr, allNaNDouble()), "an all-NaN file cannot match NOT IN: " + expr);
        Assertions.assertTrue(mightMatch(expr, mixedNaNDouble()), "{1.0, NaN} file must be read: " + expr);
    }

    private static IcebergPredicateConverter converter(IcebergPredicateConverter.Mode mode) {
        return new IcebergPredicateConverter(SCHEMA, ZoneOffset.UTC, mode);
    }

    private static Expression pushed(IcebergPredicateConverter.Mode mode, ConnectorExpression expr) {
        List<Expression> out = converter(mode).convert(expr);
        Assertions.assertEquals(1, out.size(), mode + " expected exactly one pushed predicate for " + expr);
        return out.get(0);
    }

    /**
     * O5-2 write-time conflict detection runs the same predicate over the files a concurrent commit added. A
     * conflict filter that prunes a NaN-holding file MISSES a real conflict (e.g. {@code DELETE ... WHERE d > 0}
     * racing an insert of NaN rows), so conflict mode needs the same isNaN arm as scan mode.
     */
    @Test
    public void conflictModeGreaterThanKeepsFilesHoldingNaN() {
        assertKeepsNaNFilesAndStillPrunesNaNFreeFile(pushed(IcebergPredicateConverter.Mode.CONFLICT,
                cmp(ConnectorComparison.Operator.GT, "c_double", doubleLit(5.0d))));
        assertKeepsNaNFilesAndStillPrunesNaNFreeFile(pushed(IcebergPredicateConverter.Mode.CONFLICT,
                cmp(ConnectorComparison.Operator.GE, "c_double", doubleLit(5.0d))));
    }

    /** Conflict mode has its own comparison/IN matrix, so it needs its own NaN-literal guard. */
    @Test
    public void conflictModeNaNLiteralDoesNotThrow() {
        ConnectorLiteral nan = doubleLit(Double.NaN);
        IcebergPredicateConverter.Mode conflict = IcebergPredicateConverter.Mode.CONFLICT;
        Assertions.assertEquals(Expression.Operation.IS_NAN,
                pushed(conflict, cmp(ConnectorComparison.Operator.EQ, "c_double", nan)).op());
        Assertions.assertDoesNotThrow(() ->
                converter(conflict).convert(cmp(ConnectorComparison.Operator.GT, "c_double", nan)));
        Assertions.assertDoesNotThrow(() -> converter(conflict).convert(new ConnectorBetween(col("c_double"),
                doubleLit(1.0d), nan)));

        // A conflict filter that cannot match a concurrently written NaN file misses a real conflict, so the
        // NaN element has to widen the filter here too -- not silently vanish with the whole IN.
        Expression in = pushed(conflict,
                new ConnectorIn(col("c_double"), Arrays.asList(doubleLit(1.0d), nan), false));
        Assertions.assertEquals(Expression.Operation.OR, in.op(), in.toString());
        Assertions.assertTrue(mightMatch(in, allNaNDouble()), "conflict IN must keep an all-NaN file: " + in);
    }

    /**
     * The NOT bailout must not swallow negations iceberg can represent exactly. {@code d IS NOT NULL} reaches
     * the converter as {@code Not(IsNull)}, and REWRITE mode is all-or-nothing with no BE residual, so dropping
     * it makes {@code RewriteDataFilePlanner} reject the whole {@code rewrite_data_files} WHERE — a regression
     * against the pre-fix behaviour, where the form lowered to an exact {@code not(isNull)}.
     */
    @Test
    public void rewriteModeNotIsNullOnFloatColumnIsStillPushed() {
        Expression expr = pushed(IcebergPredicateConverter.Mode.REWRITE,
                new ConnectorNot(new ConnectorIsNull(col("c_double"), false)));
        Assertions.assertEquals(Expression.Operation.NOT, expr.op(), expr.toString());
    }

    /**
     * Only a negated LT/LE (or BETWEEN) on a float column loses the is_nan arm. A NOT over a compound node whose
     * float comparison is GT/GE is exact — iceberg's De Morgan rewrite negates the whole {@code (range or
     * is_nan)} arm into {@code (ltEq and notNaN)} — so it must keep being pushed.
     */
    @Test
    public void notOverCompoundWithoutNegatedFloatRangeIsPushed() {
        ConnectorExpression expr = new ConnectorNot(new ConnectorAnd(Arrays.asList(
                cmp(ConnectorComparison.Operator.GT, "c_double", doubleLit(5.0d)),
                cmp(ConnectorComparison.Operator.EQ, "c_int", new ConnectorLiteral(ConnectorType.of("INT"), 1L)))));
        Assertions.assertEquals(Expression.Operation.NOT, pushed(expr).op());
    }

    /** rewrite_data_files scopes which files get compacted; NaN rows match {@code d > 5} in Doris, so keep them. */
    @Test
    public void rewriteModeGreaterThanKeepsFilesHoldingNaN() {
        assertKeepsNaNFilesAndStillPrunesNaNFreeFile(pushed(IcebergPredicateConverter.Mode.REWRITE,
                cmp(ConnectorComparison.Operator.GT, "c_double", doubleLit(5.0d))));
    }

    /** A NaN bound in a rewrite BETWEEN must be dropped, not thrown (the planner turns a drop into a clean error). */
    @Test
    public void rewriteModeNaNBetweenBoundDoesNotThrow() {
        Assertions.assertDoesNotThrow(() -> converter(IcebergPredicateConverter.Mode.REWRITE)
                .convert(new ConnectorBetween(col("c_double"), doubleLit(1.0d), doubleLit(Double.NaN))));
    }
}
