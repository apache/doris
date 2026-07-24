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

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
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
}
