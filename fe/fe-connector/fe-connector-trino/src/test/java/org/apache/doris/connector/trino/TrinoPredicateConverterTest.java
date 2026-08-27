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

package org.apache.doris.connector.trino;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;

/**
 * Unit tests for {@link TrinoPredicateConverter}: Doris {@code ConnectorExpression}
 * pushdown trees must convert to the exact Trino {@link TupleDomain} that preserves
 * filter semantics, and must degrade to {@code TupleDomain.all()} (no pushdown, full
 * scan) rather than fail when an expression cannot be represented.
 */
public class TrinoPredicateConverterTest {

    // A column handle is an opaque marker to the converter; it ends up as the key of
    // the produced TupleDomain. equals/hashCode by name so expected/actual compare equal.
    private static final class MockColumnHandle implements ColumnHandle {
        private final String name;

        private MockColumnHandle(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MockColumnHandle && name.equals(((MockColumnHandle) o).name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    private static final Map<String, ColumnHandle> HANDLES = ImmutableMap.of(
            "c_int", new MockColumnHandle("c_int"),
            "c_bigint", new MockColumnHandle("c_bigint"),
            "c_str", new MockColumnHandle("c_str"),
            "c_bool", new MockColumnHandle("c_bool"));

    private static final Map<String, ColumnMetadata> METAS = ImmutableMap.of(
            "c_int", new ColumnMetadata("c_int", IntegerType.INTEGER),
            "c_bigint", new ColumnMetadata("c_bigint", BigintType.BIGINT),
            "c_str", new ColumnMetadata("c_str", VarcharType.createVarcharType(64)),
            "c_bool", new ColumnMetadata("c_bool", BooleanType.BOOLEAN));

    private static final TrinoPredicateConverter CONVERTER = new TrinoPredicateConverter(HANDLES, METAS);

    private static ConnectorColumnRef col(String name) {
        // The Doris type here is unused by the converter; it resolves the Trino type
        // from the column metadata map. A placeholder keeps the ref construction simple.
        return new ConnectorColumnRef(name, ConnectorType.of("INT"));
    }

    private static Type type(String name) {
        return METAS.get(name).getType();
    }

    private static Domain singleValue(String colName, Object value) {
        return Domain.create(ValueSet.ofRanges(Range.equal(type(colName), value)), false);
    }

    private static TupleDomain<ColumnHandle> expect(String colName, Domain domain) {
        return TupleDomain.withColumnDomains(ImmutableMap.of(HANDLES.get(colName), domain));
    }

    @Test
    public void testEqProducesSingleValueDomain() {
        // c_int = 5  ->  domain pinned to the single value 5
        ConnectorComparison cmp = new ConnectorComparison(
                ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(5));
        Assertions.assertEquals(expect("c_int", singleValue("c_int", 5L)), CONVERTER.convert(cmp));
    }

    @Test
    public void testBooleanEqKeepsBooleanValue() {
        // c_bool = true  ->  boolean literals are passed through unchanged (not coerced to long)
        ConnectorComparison cmp = new ConnectorComparison(
                ConnectorComparison.Operator.EQ, col("c_bool"), ConnectorLiteral.ofBoolean(true));
        Assertions.assertEquals(expect("c_bool", singleValue("c_bool", true)), CONVERTER.convert(cmp));
    }

    @Test
    public void testLessThanProducesOpenUpperRange() {
        // c_int < 10  ->  (-inf, 10)
        ConnectorComparison cmp = new ConnectorComparison(
                ConnectorComparison.Operator.LT, col("c_int"), ConnectorLiteral.ofInt(10));
        Domain expected = Domain.create(ValueSet.ofRanges(Range.lessThan(type("c_int"), 10L)), false);
        Assertions.assertEquals(expect("c_int", expected), CONVERTER.convert(cmp));
    }

    @Test
    public void testGreaterOrEqualProducesClosedLowerRange() {
        // c_bigint >= 100  ->  [100, +inf)
        ConnectorComparison cmp = new ConnectorComparison(
                ConnectorComparison.Operator.GE, col("c_bigint"), ConnectorLiteral.ofLong(100L));
        Domain expected = Domain.create(
                ValueSet.ofRanges(Range.greaterThanOrEqual(type("c_bigint"), 100L)), false);
        Assertions.assertEquals(expect("c_bigint", expected), CONVERTER.convert(cmp));
    }

    @Test
    public void testNotEqualExcludesValue() {
        // c_int != 7  ->  everything except 7
        ConnectorComparison cmp = new ConnectorComparison(
                ConnectorComparison.Operator.NE, col("c_int"), ConnectorLiteral.ofInt(7));
        Domain expected = Domain.create(
                ValueSet.all(type("c_int")).subtract(ValueSet.ofRanges(Range.equal(type("c_int"), 7L))),
                false);
        Assertions.assertEquals(expect("c_int", expected), CONVERTER.convert(cmp));
    }

    @Test
    public void testVarcharEqEncodesAsSlice() {
        // c_str = 'hello'  ->  string literal must be encoded as a Trino Slice
        ConnectorComparison cmp = new ConnectorComparison(
                ConnectorComparison.Operator.EQ, col("c_str"), ConnectorLiteral.ofString("hello"));
        Assertions.assertEquals(
                expect("c_str", singleValue("c_str", Slices.utf8Slice("hello"))),
                CONVERTER.convert(cmp));
    }

    @Test
    public void testVarcharDatetimeComparisonDegradesToAll() {
        // CAST(c_str AS DATETIME) >= TIMESTAMP '2026-08-24 00:00:00' reaches this converter as
        // c_str >= <DATETIME literal> because fe-core unwraps CastExpr. Encoding the datetime as a
        // VARCHAR domain would change datetime comparison into lexicographic comparison and can lose rows.
        ConnectorComparison cmp = new ConnectorComparison(
                ConnectorComparison.Operator.GE, col("c_str"),
                ConnectorLiteral.ofDatetime(LocalDateTime.of(2026, 8, 24, 0, 0)));
        Assertions.assertEquals(TupleDomain.<ColumnHandle>all(), CONVERTER.convert(cmp));
    }

    @Test
    public void testAndSkipsIncompatibleVarcharDatetimeComparison() {
        // An incompatible conjunct widens the pushed predicate by being omitted, while compatible conjuncts
        // remain useful. Doris keeps and re-evaluates the original filter after the source scan.
        ConnectorAnd and = new ConnectorAnd(Arrays.asList(
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(5)),
                new ConnectorComparison(ConnectorComparison.Operator.GE, col("c_str"),
                        ConnectorLiteral.ofDatetime(LocalDateTime.of(2026, 8, 24, 0, 0)))));
        Assertions.assertEquals(expect("c_int", singleValue("c_int", 5L)), CONVERTER.convert(and));
    }

    @Test
    public void testVarcharNullSafeEqualityKeepsOnlyNullDomain() {
        ConnectorComparison cmp = new ConnectorComparison(
                ConnectorComparison.Operator.EQ_FOR_NULL, col("c_str"),
                ConnectorLiteral.ofNull(ConnectorType.of("VARCHAR")));
        Assertions.assertEquals(expect("c_str", Domain.onlyNull(type("c_str"))), CONVERTER.convert(cmp));
    }

    @Test
    public void testInProducesMultiValueDomain() {
        // c_int IN (1, 2, 3)  ->  domain of the three discrete values
        ConnectorIn in = new ConnectorIn(col("c_int"),
                Arrays.asList(ConnectorLiteral.ofInt(1), ConnectorLiteral.ofInt(2), ConnectorLiteral.ofInt(3)),
                false);
        Domain expected = Domain.create(ValueSet.ofRanges(
                Range.equal(type("c_int"), 1L),
                Range.equal(type("c_int"), 2L),
                Range.equal(type("c_int"), 3L)), false);
        Assertions.assertEquals(expect("c_int", expected), CONVERTER.convert(in));
    }

    @Test
    public void testNotInExcludesValues() {
        // c_int NOT IN (1, 2)  ->  everything except 1 and 2
        ConnectorIn in = new ConnectorIn(col("c_int"),
                Arrays.asList(ConnectorLiteral.ofInt(1), ConnectorLiteral.ofInt(2)), true);
        Domain expected = Domain.create(ValueSet.all(type("c_int")).subtract(ValueSet.ofRanges(
                Range.equal(type("c_int"), 1L), Range.equal(type("c_int"), 2L))), false);
        Assertions.assertEquals(expect("c_int", expected), CONVERTER.convert(in));
    }

    @Test
    public void testIsNullProducesOnlyNullDomain() {
        // c_str IS NULL  ->  only-null domain
        ConnectorIsNull isNull = new ConnectorIsNull(col("c_str"), false);
        Assertions.assertEquals(expect("c_str", Domain.onlyNull(type("c_str"))), CONVERTER.convert(isNull));
    }

    @Test
    public void testIsNotNullProducesNotNullDomain() {
        // c_str IS NOT NULL  ->  not-null domain
        ConnectorIsNull isNotNull = new ConnectorIsNull(col("c_str"), true);
        Assertions.assertEquals(expect("c_str", Domain.notNull(type("c_str"))), CONVERTER.convert(isNotNull));
    }

    @Test
    public void testAndIntersectsAcrossColumns() {
        // c_int = 5 AND c_bigint = 100  ->  both columns constrained in one TupleDomain
        ConnectorAnd and = new ConnectorAnd(Arrays.asList(
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(5)),
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_bigint"),
                        ConnectorLiteral.ofLong(100L))));
        TupleDomain<ColumnHandle> expected = TupleDomain.withColumnDomains(ImmutableMap.of(
                HANDLES.get("c_int"), singleValue("c_int", 5L),
                HANDLES.get("c_bigint"), singleValue("c_bigint", 100L)));
        Assertions.assertEquals(expected, CONVERTER.convert(and));
    }

    @Test
    public void testOrUnionsSameColumn() {
        // c_int = 1 OR c_int = 2  ->  union into a two-value domain on c_int
        ConnectorOr or = new ConnectorOr(Arrays.asList(
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(1)),
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(2))));
        Domain expected = Domain.create(ValueSet.ofRanges(
                Range.equal(type("c_int"), 1L), Range.equal(type("c_int"), 2L)), false);
        Assertions.assertEquals(expect("c_int", expected), CONVERTER.convert(or));
    }

    @Test
    public void testThreeWayOrKeepsEveryArm() {
        // c_int = 1 OR c_int = 2 OR c_int = 3.
        // WHY this matters: ConnectorOr carries a FLATTENED N-ary list (fe-core's converters flatten
        // nested ORs before handing them over), so folding only the first two arms would push
        // `c_int IN (1, 2)` to the source. The source then never returns the c_int = 3 rows, and BE
        // re-evaluation cannot add rows back - the query silently loses rows.
        ConnectorOr or = new ConnectorOr(Arrays.asList(
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(1)),
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(2)),
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(3))));
        Domain expected = Domain.create(ValueSet.ofRanges(
                Range.equal(type("c_int"), 1L),
                Range.equal(type("c_int"), 2L),
                Range.equal(type("c_int"), 3L)), false);
        Assertions.assertEquals(expect("c_int", expected), CONVERTER.convert(or));
    }

    @Test
    public void testFourWayOrKeepsEveryArm() {
        // Four arms, so the fix cannot be "read one more arm" - it has to fold the whole list.
        ConnectorOr or = new ConnectorOr(Arrays.asList(
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(1)),
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(2)),
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(3)),
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(4))));
        Domain expected = Domain.create(ValueSet.ofRanges(
                Range.equal(type("c_int"), 1L),
                Range.equal(type("c_int"), 2L),
                Range.equal(type("c_int"), 3L),
                Range.equal(type("c_int"), 4L)), false);
        Assertions.assertEquals(expect("c_int", expected), CONVERTER.convert(or));
    }

    @Test
    public void testCrossColumnOrDegradesToAll() {
        // c_int = 1 OR c_bigint = 2 OR c_str = 'x'.
        // A column-wise union keeps a column only when EVERY arm constrains it; here no column does,
        // so the correct result is "no pushdown". Locks down that we never invent a constraint that
        // holds in only some arms just to have something to push.
        ConnectorOr or = new ConnectorOr(Arrays.asList(
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(1)),
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_bigint"),
                        ConnectorLiteral.ofLong(2L)),
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_str"),
                        ConnectorLiteral.ofString("x"))));
        Assertions.assertEquals(TupleDomain.<ColumnHandle>all(), CONVERTER.convert(or));
    }

    @Test
    public void testOrWithUntranslatableArmDegradesToAll() {
        // c_int = 1 OR c_int = 2 OR <bare column ref, which the converter cannot translate>.
        // OR is all-or-nothing: swallowing the failing arm would leave `c_int IN (1, 2)`, which is
        // NARROWER than the user's predicate and loses rows. Dropping the whole pushdown is the only
        // safe degradation. This is the opposite policy from AND, where skipping a conjunct only
        // widens what is pushed and BE re-evaluation recovers exactness.
        ConnectorOr or = new ConnectorOr(Arrays.asList(
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(1)),
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), ConnectorLiteral.ofInt(2)),
                col("c_bool")));
        Assertions.assertEquals(TupleDomain.<ColumnHandle>all(), CONVERTER.convert(or));
    }

    @Test
    public void testNullExpressionDegradesToAll() {
        // A null filter must not be pushed down: scan everything.
        Assertions.assertEquals(TupleDomain.<ColumnHandle>all(), CONVERTER.convert(null));
    }

    @Test
    public void testUnsupportedExpressionDegradesToAll() {
        // A bare column reference is not a predicate; convert() must swallow the failure
        // and return all() so the query still runs (just without pushdown).
        ConnectorExpression unsupported = col("c_int");
        Assertions.assertEquals(TupleDomain.<ColumnHandle>all(), CONVERTER.convert(unsupported));
    }
}
