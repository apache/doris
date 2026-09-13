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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorLike;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.StartsWith;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * P5-T07 — pins the parity-correct predicate-pushdown contract of
 * {@link PaimonPredicateConverter}: NTZ pushes with fixed-UTC semantics (matching legacy
 * {@code PaimonValueConverter} and paimon's UTC-interpreted stored stats), while LTZ / FLOAT /
 * CHAR are deliberately NOT pushed (left to BE-side filtering) to avoid source-side false pruning.
 *
 * <p>The converter only takes a {@link RowType} — no catalog — so every case is fully offline.
 * The paimon {@code DataType} of the column (not the {@link ConnectorType} on the literal) drives
 * the conversion, so the literal's connector type is incidental here.
 */
public class PaimonPredicateConverterTest {

    private static final ConnectorType ANY = ConnectorType.of("INT");

    /** Builds `col = literal` over a single-column RowType of the given paimon type. */
    private static List<Predicate> convertEq(
            RowType rowType, String colName, Object literalValue) {
        PaimonPredicateConverter converter = new PaimonPredicateConverter(rowType);
        ConnectorComparison cmp = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef(colName, ANY),
                new ConnectorLiteral(ANY, literalValue));
        return converter.convert(cmp);
    }

    @Test
    public void ntzPushedWithUtcSemantics() {
        RowType rowType = RowType.builder().field("ts", DataTypes.TIMESTAMP()).build();
        LocalDateTime literal = LocalDateTime.of(2021, 3, 14, 1, 59, 26);

        List<Predicate> predicates = convertEq(rowType, "ts", literal);

        // WHY: a TIMESTAMP_WITHOUT_TIME_ZONE comparison against a wall-clock literal MUST be
        // pushed — dropping it would forfeit all file/partition pruning on NTZ columns.
        // MUTATION: returning null for the TIMESTAMP_WITHOUT_TIME_ZONE root -> size 0 -> red.
        Assertions.assertEquals(1, predicates.size(),
                "an NTZ equality predicate must be pushed (one leaf produced)");

        // WHY: the pushed literal must be the wall clock interpreted in UTC, because paimon's
        // stored min/max stats for a zone-free column are computed by reading the wall clock as
        // UTC; any other zone shifts the epoch-millis vs the stored stats and false-prunes files
        // (silent data loss). MUTATION: switching ZoneOffset.UTC -> a non-UTC zone (e.g. the
        // session zone) shifts this value -> assertion red.
        long expectedMillis = literal.toInstant(ZoneOffset.UTC).toEpochMilli();
        LeafPredicate leaf = (LeafPredicate) predicates.get(0);
        Assertions.assertEquals(Timestamp.fromEpochMillis(expectedMillis), leaf.literals().get(0),
                "NTZ literal must be the wall clock converted via fixed UTC (legacy GMT parity)");
    }

    @Test
    public void ltzNotPushed() {
        RowType rowType = RowType.builder()
                .field("ts", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()).build();
        LocalDateTime literal = LocalDateTime.of(2021, 3, 14, 1, 59, 26);

        List<Predicate> predicates = convertEq(rowType, "ts", literal);

        // WHY: legacy never pushed TIMESTAMP WITH LOCAL TIME ZONE (PaimonValueConverter has no
        // visit(LocalZonedTimestampType) -> defaultMethod -> null). Pushing it via a fixed zone is
        // an instant mismatch under non-UTC sessions, risking false pruning, so the conjunct must
        // be dropped and left to BE-side filtering. MUTATION: re-merging the LTZ case into the NTZ
        // branch (so it produces a predicate) -> size 1 -> red.
        Assertions.assertTrue(predicates.isEmpty(),
                "an LTZ predicate must NOT be pushed (dropped to BE-side filtering)");
    }

    @Test
    public void floatNotPushed() {
        RowType rowType = RowType.builder().field("f", DataTypes.FLOAT()).build();

        List<Predicate> predicates = convertEq(rowType, "f", 1.5d);

        // WHY: the FLOAT root deliberately returns null (not pushed) — pushing a float literal
        // risks precision-mismatch false pruning at the source. MUTATION: returning a value for
        // the FLOAT root -> size 1 -> red.
        Assertions.assertTrue(predicates.isEmpty(),
                "a FLOAT predicate must NOT be pushed");
    }

    @Test
    public void charNotPushed() {
        RowType rowType = RowType.builder().field("c", DataTypes.CHAR(4)).build();

        List<Predicate> predicates = convertEq(rowType, "c", "abc");

        // WHY: the CHAR root deliberately returns null (not pushed) — CHAR's blank-padding
        // semantics differ from an unpadded literal, so pushing risks under-matching at the source.
        // MUTATION: returning a value for the CHAR root -> size 1 -> red.
        Assertions.assertTrue(predicates.isEmpty(),
                "a CHAR predicate must NOT be pushed");
    }

    @Test
    public void intControlIsPushed() {
        RowType rowType = RowType.builder().field("id", DataTypes.INT()).build();

        List<Predicate> predicates = convertEq(rowType, "id", 42);

        // WHY: control — proves the converter still pushes ordinary predicates and that the
        // NTZ/LTZ/FLOAT/CHAR degrade above is type-specific, not a global "drop everything" bug.
        // MUTATION: a converter change that drops all conjuncts (e.g. convert() always returning
        // empty) would make this red while the negative cases stay green, distinguishing the two.
        Assertions.assertEquals(1, predicates.size(),
                "an INT equality predicate must still be pushed (degrade is type-specific)");
        LeafPredicate leaf = (LeafPredicate) predicates.get(0);
        Assertions.assertEquals(42, leaf.literals().get(0),
                "the INT literal must be carried through unchanged");
    }

    // ---------- null-safe equality (<=>) ----------

    @Test
    public void eqForNullWithNonNullLiteralPushesEqual() {
        RowType rowType = RowType.builder().field("id", DataTypes.INT()).build();

        List<Predicate> predicates = convert(rowType,
                ConnectorComparison.Operator.EQ_FOR_NULL, "id", ConnectorLiteral.ofInt(5));

        // WHY: `id <=> 5` and `id = 5` select exactly the same rows (<=> yields false, never unknown,
        // when id is null, and paimon's Equal likewise never matches nulls). Pushing IS NULL instead -
        // which is what the port from fe-core did - is not a narrowing but an INVERSION: paimon prunes
        // away every data file that holds id = 5 at planning time, and the BE-side residual filter can
        // only drop rows from what was read, never bring pruned files back. The query returns 0 rows,
        // with no error, no warning and a smaller partition count in EXPLAIN.
        // MUTATION: restore `case EQ_FOR_NULL: return builder.isNull(idx)` -> red.
        Assertions.assertEquals(1, predicates.size(),
                "`id <=> 5` must be pushed as an equality predicate");
        LeafPredicate leaf = (LeafPredicate) predicates.get(0);
        Assertions.assertSame(Equal.INSTANCE, leaf.function(),
                "`id <=> <non-null>` must translate to Equal, never IsNull");
        Assertions.assertEquals(5, leaf.literals().get(0));
    }

    @Test
    public void eqForNullWithNullLiteralPushesIsNull() {
        RowType rowType = RowType.builder().field("id", DataTypes.INT()).build();

        List<Predicate> predicates = convert(rowType,
                ConnectorComparison.Operator.EQ_FOR_NULL, "id", ConnectorLiteral.ofNull(ANY));

        // `id <=> NULL` is exactly IS NULL - the one case where the null-safe operator does translate.
        Assertions.assertEquals(1, predicates.size());
        LeafPredicate leaf = (LeafPredicate) predicates.get(0);
        Assertions.assertSame(IsNull.INSTANCE, leaf.function());
        Assertions.assertTrue(leaf.literals().isEmpty());
    }

    @Test
    public void plainEqWithNullLiteralNotPushed() {
        RowType rowType = RowType.builder().field("id", DataTypes.INT()).build();

        List<Predicate> predicates = convert(rowType,
                ConnectorComparison.Operator.EQ, "id", ConnectorLiteral.ofNull(ANY));

        // `id = NULL` is unknown for every row. It is neither Equal nor IsNull, so the only correct
        // action is to decline the pushdown.
        Assertions.assertTrue(predicates.isEmpty(),
                "`id = NULL` must not be pushed as anything");
    }

    @Test
    public void eqForNullOnNonPushableTypeNotPushed() {
        RowType rowType = RowType.builder().field("f", DataTypes.FLOAT()).build();

        List<Predicate> predicates = convert(rowType,
                ConnectorComparison.Operator.EQ_FOR_NULL, "f", new ConnectorLiteral(ANY, 1.5d));

        // WHY this case is separate: FLOAT is deliberately not pushed, so the value conversion fails
        // and we land in the same "no value" branch as a genuine null literal. Guarding on the operator
        // ALONE would turn `f <=> 1.5` into IS NULL and recreate the very bug this batch fixes, just on
        // another column type. The guard must also require that the literal really is null.
        // MUTATION: relax the guard to `operator == EQ_FOR_NULL` without `literal.isNull()` -> red.
        Assertions.assertTrue(predicates.isEmpty(),
                "`f <=> 1.5` on a non-pushable type must be declined, not turned into IS NULL");
    }

    // ---------- LIKE ----------

    private static List<Predicate> convertLike(String pattern) {
        RowType rowType = RowType.builder().field("s", DataTypes.STRING()).build();
        return new PaimonPredicateConverter(rowType).convert(new ConnectorLike(
                ConnectorLike.Operator.LIKE,
                new ConnectorColumnRef("s", ANY),
                new ConnectorLiteral(ANY, pattern)));
    }

    private static void assertPrefixPushed(String pattern, String expectedPrefix) {
        List<Predicate> predicates = convertLike(pattern);
        Assertions.assertEquals(1, predicates.size(), "LIKE '" + pattern + "' must be pushed");
        LeafPredicate leaf = (LeafPredicate) predicates.get(0);
        Assertions.assertSame(StartsWith.INSTANCE, leaf.function());
        Assertions.assertEquals(BinaryString.fromString(expectedPrefix), leaf.literals().get(0));
    }

    private static void assertNotPushed(String pattern) {
        // WHY declining is the required answer rather than a missed optimization: this predicate drives
        // paimon's partition and data-file pruning at planning time AND the BE-side JNI row filter. A
        // prefix that is stricter than the user's pattern makes paimon skip files that hold matching
        // rows, and nothing downstream can read them back - the query silently returns fewer rows.
        Assertions.assertTrue(convertLike(pattern).isEmpty(),
                "LIKE '" + pattern + "' cannot be proven equivalent to a prefix match, so it must "
                        + "not be pushed (declining is slow but correct; narrowing loses rows)");
    }

    @Test
    public void likeTrailingWildcardPushesPrefix() {
        assertPrefixPushed("abc%", "abc");
        assertPrefixPushed("abc%%", "abc");
    }

    @Test
    public void likeSingleCharWildcardNotPushed() {
        // '_' matches any ONE character, so 'a_c%' must also match "abc1". Treating it as a literal
        // underscore prunes away every file that only holds "abc..." values.
        assertNotPushed("a_c%");
        assertNotPushed("a\\_c%");
    }

    @Test
    public void likeEscapedWildcardNotPushed() {
        // 'a\%%' means "starts with the literal a%". The raw text carries the backslash, so pushing it
        // verbatim asks paimon for values starting with "a\%" - typically matching nothing at all.
        assertNotPushed("a\\%%");
    }

    @Test
    public void likeInnerWildcardNotPushed() {
        // 'a%b%' does not start with '%' and does end with '%', which is exactly the shape the old
        // check accepted; it then pushed "a%b" as a literal prefix.
        assertNotPushed("a%b%");
    }

    @Test
    public void likeNonPrefixShapesStayUnpushed() {
        // Regression guard on the shapes that were already declined, so the tightening did not
        // accidentally start pushing them.
        assertNotPushed("%abc%");
        assertNotPushed("%abc");
        assertNotPushed("abc");
        assertNotPushed("%");
        assertNotPushed("%%");
    }

    /** Builds `col <op> literal` over a single-column RowType. */
    private static List<Predicate> convert(RowType rowType, ConnectorComparison.Operator op,
            String colName, ConnectorLiteral literal) {
        return new PaimonPredicateConverter(rowType).convert(new ConnectorComparison(
                op, new ConnectorColumnRef(colName, ANY), literal));
    }
}
