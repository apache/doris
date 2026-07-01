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

package org.apache.doris.planner;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ScanNodePartitionTypeNormalizationTest {

    // ---- isSamePrimitiveFamily ----

    @Test
    void testSamePrimitiveFamilySameType() {
        Assertions.assertTrue(ScanNode.isSamePrimitiveFamily(
                ScalarType.INT, ScalarType.INT));
        Assertions.assertTrue(ScanNode.isSamePrimitiveFamily(
                ScalarType.INT, ScalarType.createType(PrimitiveType.INT)));
    }

    @Test
    void testSamePrimitiveFamilyFixedPoint() {
        Assertions.assertTrue(ScanNode.isSamePrimitiveFamily(
                ScalarType.TINYINT, ScalarType.BIGINT));
        Assertions.assertTrue(ScanNode.isSamePrimitiveFamily(
                ScalarType.SMALLINT, ScalarType.LARGEINT));
        Assertions.assertTrue(ScanNode.isSamePrimitiveFamily(
                ScalarType.INT, ScalarType.BIGINT));
    }

    @Test
    void testSamePrimitiveFamilyDateTypes() {
        Assertions.assertTrue(ScanNode.isSamePrimitiveFamily(
                ScalarType.DATE, ScalarType.DATETIME));
        Assertions.assertTrue(ScanNode.isSamePrimitiveFamily(
                ScalarType.DATEV2, ScalarType.DATETIMEV2));
        Assertions.assertTrue(ScanNode.isSamePrimitiveFamily(
                ScalarType.DATE, ScalarType.DATETIMEV2));
    }

    @Test
    void testSamePrimitiveFamilyStringTypes() {
        Assertions.assertTrue(ScanNode.isSamePrimitiveFamily(
                ScalarType.STRING, ScalarType.createVarcharType(65533)));
        Assertions.assertTrue(ScanNode.isSamePrimitiveFamily(
                ScalarType.createCharType(10), ScalarType.createVarcharType(20)));
    }

    @Test
    void testSamePrimitiveFamilyDecimalTypes() {
        Assertions.assertTrue(ScanNode.isSamePrimitiveFamily(
                ScalarType.createDecimalV3Type(10, 2),
                ScalarType.createDecimalV3Type(18, 4)));
    }

    @Test
    void testDifferentFamily() {
        Assertions.assertFalse(ScanNode.isSamePrimitiveFamily(
                ScalarType.INT, ScalarType.STRING));
        Assertions.assertFalse(ScanNode.isSamePrimitiveFamily(
                ScalarType.INT, ScalarType.DATE));
        Assertions.assertFalse(ScanNode.isSamePrimitiveFamily(
                ScalarType.STRING, ScalarType.BOOLEAN));
        Assertions.assertFalse(ScanNode.isSamePrimitiveFamily(
                ScalarType.DATE, ScalarType.STRING));
    }

    // ---- normalizePartitionFilterLiteral ----

    @Test
    void testNormalizeSameTypeReturnsSame() throws AnalysisException {
        // Use a value that fits INT (not TINYINT) so the types match
        IntLiteral original = new IntLiteral(128, Type.INT);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(original, ScalarType.INT);
        // When types match exactly, the same object should be returned
        Assertions.assertSame(original, result);
    }

    @Test
    void testNormalizeIntToBigInt() throws AnalysisException {
        // CAST(k AS BIGINT) = 1 on INT column → BIGINT literal → normalize to INT
        IntLiteral bigIntLiteral = new IntLiteral(1, Type.BIGINT);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(bigIntLiteral, ScalarType.INT);
        Assertions.assertEquals(ScalarType.INT, result.getType());
        Assertions.assertEquals(1, result.getLongValue());
    }

    @Test
    void testNormalizeBigIntToInt() throws AnalysisException {
        IntLiteral intLiteral = new IntLiteral(42, Type.INT);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(intLiteral, Type.BIGINT);
        Assertions.assertEquals(Type.BIGINT, result.getType());
        Assertions.assertEquals(42, result.getLongValue());
    }

    @Test
    void testNormalizeStringToVarchar() throws AnalysisException {
        StringLiteral strLiteral = new StringLiteral("hello");
        // StringLiteral default is STRING type
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(
                strLiteral, ScalarType.createVarcharType(65533));
        Assertions.assertTrue(result.getType().isStringType());
        Assertions.assertEquals("hello", result.getStringValue());
    }

    @Test
    void testNormalizeDifferentFamilyNoChange() throws AnalysisException {
        IntLiteral intLiteral = new IntLiteral(1);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(intLiteral, ScalarType.STRING);
        // Not same family → returned unchanged
        Assertions.assertSame(intLiteral, result);
    }

    // ---- normalizePartitionFilterLiteral: cross-type date normalization ----
    //
    // normalizePartitionFilterLiteral handles date-type family mismatches
    // (e.g. CAST predicates that produce a DATETIME literal for a DATE
    // partition column).  Only up-converts are performed:
    //
    //   DATE / DATEV2 → DATETIME / DATETIMEV2 / TIMESTAMPTZ  (safe: adds midnight)
    //
    // Down-converts (DATETIME → DATE) are intentionally skipped because
    // stripping time changes the value and can cause incorrect pruning for
    // range predicates.  For example, on a DATEV2 partition column with
    // partition [2024-01-01, 2024-01-02), the predicate
    //   CAST(k AS DATETIMEV2) < '2024-01-01 12:00:00'
    // can match rows in that partition because the DATE value casts to
    // midnight.  Normalising the bound to DATEV2 '2024-01-01' and building
    // Range.lessThan() would incorrectly exclude the partition.  By
    // returning the original literal unchanged, the cross-type comparison
    // produces CONVERT_FAILURE — no pruning — which is conservative.

    @Test
    void testNormalizeDateToDateTimeV2() throws AnalysisException {
        // DATE -> DATETIMEV2: string re-parse "2024-01-15" as DATETIMEV2
        // should succeed (DATETIMEV2 accepts date-only strings).
        DateLiteral dateLiteral = new DateLiteral(2024, 1, 15, Type.DATE);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(dateLiteral, Type.DATETIMEV2);
        // Result must be DATETIMEV2 so it can compare against DATETIMEV2 partition bounds.
        Assertions.assertEquals(Type.DATETIMEV2, result.getType());
        Assertions.assertTrue(result instanceof DateLiteral);
        DateLiteral resultDate = (DateLiteral) result;
        Assertions.assertEquals(2024, resultDate.getYear());
        Assertions.assertEquals(1, resultDate.getMonth());
        Assertions.assertEquals(15, resultDate.getDay());
        // DATE -> DATETIMEV2 cast produces 00:00:00 time.
        Assertions.assertEquals(0, resultDate.getHour());
        Assertions.assertEquals(0, resultDate.getMinute());
        Assertions.assertEquals(0, resultDate.getSecond());
    }

    @Test
    void testNormalizeDateTimeV2ToDateV2_DownConvertRejected() throws AnalysisException {
        // DATETIMEV2 → DATEV2: down-convert is unsafe because stripping
        // the time component changes the value.  For a predicate such as
        // CAST(k AS DATETIMEV2) < '2024-06-15 12:30:45' on a DATEV2
        // partition column, normalising to DATEV2 '2024-06-15' and
        // building Range.lessThan() would incorrectly exclude the
        // partition [2024-06-15, 2024-06-16).  The helper therefore
        // returns the original DATETIMEV2 literal unchanged; the
        // cross-type comparison will produce CONVERT_FAILURE which is
        // safer than incorrect pruning.
        DateLiteral datetimeLiteral = new DateLiteral(2024, 6, 15, 12, 30, 45, 0, Type.DATETIMEV2);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(datetimeLiteral, Type.DATEV2);
        // Down-convert is rejected — original returned unchanged.
        Assertions.assertEquals(Type.DATETIMEV2, result.getType());
        Assertions.assertSame(datetimeLiteral, result,
                "down-convert DATETIMEV2->DATEV2 must return original literal");
    }

    @Test
    void testNormalizeDatetimeToDate_DownConvertRejected() throws AnalysisException {
        // DATETIME → DATE: down-convert is unsafe (same rationale as above).
        DateLiteral datetimeLiteral = new DateLiteral(2024, 12, 31, 23, 59, 59, Type.DATETIME);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(datetimeLiteral, Type.DATE);
        // Down-convert is rejected — original returned unchanged.
        Assertions.assertEquals(Type.DATETIME, result.getType());
        Assertions.assertSame(datetimeLiteral, result);
    }

    @Test
    void testNormalizeDateToDatetime() throws AnalysisException {
        // DATE -> DATETIME: string re-parse "2024-01-15" as DATETIME
        // should succeed (DATETIME accepts date-only strings).
        DateLiteral dateLiteral = new DateLiteral(2024, 1, 15, Type.DATE);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(dateLiteral, Type.DATETIME);
        Assertions.assertEquals(Type.DATETIME, result.getType());
        Assertions.assertTrue(result instanceof DateLiteral);
        DateLiteral resultDate = (DateLiteral) result;
        Assertions.assertEquals(2024, resultDate.getYear());
        Assertions.assertEquals(1, resultDate.getMonth());
        Assertions.assertEquals(15, resultDate.getDay());
        Assertions.assertEquals(0, resultDate.getHour());
        Assertions.assertEquals(0, resultDate.getMinute());
        Assertions.assertEquals(0, resultDate.getSecond());
    }

    @Test
    void testNormalizeDateTimeV2ToDateV2_WithTime_DownConvertRejected() throws AnalysisException {
        // DATETIMEV2 with non-midnight time → DATEV2: unsafe down-convert.
        DateLiteral dtV2 = new DateLiteral(2024, 6, 15, 12, 30, 45, 0, Type.DATETIMEV2);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(dtV2, Type.DATEV2);
        // Down-convert is rejected — original returned unchanged.
        Assertions.assertEquals(Type.DATETIMEV2, result.getType());
        Assertions.assertSame(dtV2, result);
    }

    @Test
    void testNormalizeDatetimeV2ToDate_Midnight_DownConvertRejected() throws AnalysisException {
        // DATETIMEV2 midnight → DATE: still unsafe because the helper
        // does not know the operator — even midnight can cause incorrect
        // pruning for ≤ predicates (e.g. ≤ '2024-06-14 00:00:00' would
        // become ≤ '2024-06-14' which is the identical boundary, but see
        // the operator-agnostic comment above).
        DateLiteral dtV2 = new DateLiteral(2024, 6, 15, 0, 0, 0, 0, Type.DATETIMEV2);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(dtV2, Type.DATE);
        // Down-convert is rejected — original returned unchanged.
        Assertions.assertEquals(Type.DATETIMEV2, result.getType());
        Assertions.assertSame(dtV2, result);
    }

    @Test
    void testNormalizeDatetimeV2ToDatetimeV2_ExactSame() throws AnalysisException {
        // Same type: returned unchanged (early return).
        DateLiteral dtV2 = new DateLiteral(2024, 6, 15, 12, 30, 45, 123456, Type.DATETIMEV2);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(dtV2, Type.DATETIMEV2);
        Assertions.assertSame(dtV2, result);
    }

    @Test
    void testNormalizeDateV2ToDate_SameFamily() throws AnalysisException {
        // DATEV2 -> DATE: both are date types, cross normalization works.
        DateLiteral dateV2Lit = new DateLiteral(2024, 3, 20, Type.DATEV2);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(dateV2Lit, Type.DATE);
        Assertions.assertEquals(Type.DATE, result.getType());
        Assertions.assertTrue(result instanceof DateLiteral);
        DateLiteral resultDate = (DateLiteral) result;
        Assertions.assertEquals(2024, resultDate.getYear());
        Assertions.assertEquals(3, resultDate.getMonth());
        Assertions.assertEquals(20, resultDate.getDay());
    }

    @Test
    void testNormalizeDatetimeV2WithMicrosToDateV2_DownConvertRejected() throws AnalysisException {
        // DATETIMEV2 with microseconds → DATEV2: unsafe down-convert.
        DateLiteral dtV2 = new DateLiteral(2024, 6, 15, 23, 59, 59, 999999, Type.DATETIMEV2);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(dtV2, Type.DATEV2);
        // Down-convert is rejected — original returned unchanged.
        Assertions.assertEquals(Type.DATETIMEV2, result.getType());
        Assertions.assertSame(dtV2, result);
    }

    /**
     * Verifies that DATETIMEV2 → DATEV2 down-convert is rejected so that
     * a less-than range predicate on a DATEV2 partition column does not
     * incorrectly exclude the partition whose lower bound equals the
     * (stripped) date value.
     *
     * <p>Scenario: DATEV2 partition column k with partition
     * [2024-01-01, 2024-01-02).  Predicate:
     * CAST(k AS DATETIMEV2) &lt; DATETIMEV2 '2024-01-01 12:00:00'.
     * If the DATETIMEV2 bound were normalised to DATEV2 '2024-01-01',
     * Range.lessThan(DATEV2 '2024-01-01') would exclude the partition
     * that starts at 2024-01-01 — incorrect pruning.
     */
    @Test
    void testDatePredicateRangeIncorrectPruningPrevented() throws AnalysisException {
        // Partition column: DATEV2, partition [2024-01-01, 2024-01-02)
        // Filter: CAST(k AS DATETIMEV2) < DATETIMEV2 '2024-01-01 12:00:00'
        DateLiteral filterBound = new DateLiteral(2024, 1, 1, 12, 0, 0, Type.DATETIMEV2);
        LiteralExpr normalized = ScanNode.normalizePartitionFilterLiteral(
                filterBound, Type.DATEV2);

        // Down-convert must be rejected: returning DATETIMEV2 unchanged.
        // The cross-type comparison produces CONVERT_FAILURE — no pruning —
        // which is conservative.
        Assertions.assertEquals(Type.DATETIMEV2, normalized.getType(),
                "DATETIMEV2→DATEV2 down-convert must be rejected; "
                + "cross-type pruning failure is safer than incorrect exclusion");
        Assertions.assertSame(filterBound, normalized);

        // The partition lower bound (DATEV2 '2024-01-01') would compare
        // against the filter bound — since types differ, ColumnBound.compareTo
        // rejects the cross-type comparison, producing CONVERT_FAILURE (safe).
        DateLiteral partitionLower = new DateLiteral(2024, 1, 1, Type.DATEV2);
        Assertions.assertEquals(-1, filterBound.compareLiteral(partitionLower),
                "cross-type compareLiteral must reject comparison");
    }

    // ---- Integration: ColumnBound.compareTo after normalization ----

    @Test
    void testCastPredicateColumnBoundComparison() throws AnalysisException {
        // Scenario: CAST(k AS BIGINT) = 1 on INT column k
        // Filter bound: BIGINT literal from the predicate
        IntLiteral filterLiteral = new IntLiteral(1, Type.BIGINT);
        // Partition bound: INT literal from the partition key
        IntLiteral partitionLiteral = new IntLiteral(1, Type.INT);

        // Without normalization, CompareLiteral rejects cross-type comparison
        Assertions.assertEquals(-1, filterLiteral.compareLiteral(partitionLiteral));

        // After normalization, comparison works
        LiteralExpr normalized = ScanNode.normalizePartitionFilterLiteral(
                filterLiteral, ScalarType.INT);
        Assertions.assertEquals(0, normalized.compareLiteral(partitionLiteral));
    }

    @Test
    void testCastPredicateColumnBoundRange() throws AnalysisException {
        // INT column, predicate CAST(k AS BIGINT) = 1
        IntLiteral filterLiteral = new IntLiteral(1, Type.BIGINT);
        LiteralExpr normalized = ScanNode.normalizePartitionFilterLiteral(
                filterLiteral, ScalarType.INT);

        ColumnBound filterBound = ColumnBound.of(normalized);
        // Partition range bounds from INT column
        ColumnBound partBound = ColumnBound.of(new IntLiteral(1, Type.INT));

        // They should be equal after normalization
        Assertions.assertEquals(0, filterBound.compareTo(partBound));
    }

    @Test
    void testDataTypeMismatchRangeEnclosure() throws AnalysisException {
        // Partition bounds: INT [3, 7]
        ColumnBound lower = ColumnBound.of(new IntLiteral(3, Type.INT));
        ColumnBound upper = ColumnBound.of(new IntLiteral(7, Type.INT));

        // Filter: CAST(k AS BIGINT) = 5 on INT column
        IntLiteral filterLiteral = new IntLiteral(5, Type.BIGINT);
        LiteralExpr normalized = ScanNode.normalizePartitionFilterLiteral(
                filterLiteral, ScalarType.INT);
        ColumnBound filterBound = ColumnBound.of(normalized);

        // Filter bound should be inside the partition range
        Assertions.assertTrue(filterBound.compareTo(lower) > 0);
        Assertions.assertTrue(filterBound.compareTo(upper) < 0);
    }

    @Test
    void testInPredicateNormalization() throws AnalysisException {
        // IN predicate values should also be normalized
        IntLiteral inValue = new IntLiteral(100, Type.BIGINT);
        LiteralExpr normalized = ScanNode.normalizePartitionFilterLiteral(inValue, ScalarType.INT);
        Assertions.assertEquals(ScalarType.INT, normalized.getType());
        Assertions.assertEquals(100, normalized.getLongValue());
    }
}
