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
    // After the fix, normalizePartitionFilterLiteral constructs target-typed
    // DateLiteral instances directly from the source literal's fields when
    // string-based re-parsing fails.  This is correct because:
    //
    // 1. PartitionKey.compareLiteralExpr (and therefore ColumnBound.compareTo)
    //    now rejects cross-type comparisons (e.g. DATETIMEV2 vs DATEV2).
    //    Every bound MUST be typed identically to the partition column.
    //
    // 2. For partition pruning, a conservative approximation is safe: we may
    //    include partitions that don't actually match, but we must NEVER
    //    exclude a partition that could match.  Converting a DATETIME literal
    //    to DATE (stripping the time component) is conservative because the
    //    DATE partition covers the entire day — the partition may contain rows
    //    matching the original DATETIME predicate.  Converting DATE to
    //    DATETIME (adding 00:00:00) is exact for the DATE → DATETIME cast
    //    semantics.
    //
    // 3. Cross-type date predicates arise from CAST expressions, e.g.
    //    CAST(k AS DATETIMEV2) = DATETIMEV2 '2024-01-01 00:00:00' on a
    //    DATEV2 column.  CastExpr.canHashPartition() returns true for
    //    same-family casts, so getSlotBinding() accepts them; without
    //    normalization the DATETIMEV2 literal would fail to compare against
    //    DATEV2 partition bounds.

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
    void testNormalizeDateTimeV2ToDateType() throws AnalysisException {
        // DATETIMEV2 -> DATEV2: string re-parse "2024-06-15 12:30:45" as
        // DATEV2 fails (DATEV2 rejects time component).  The catch block
        // constructs a DATEV2 literal from the date fields, stripping the
        // time.  This is conservative for partition pruning: the DATEV2
        // partition on 2024-06-15 covers the entire day.
        DateLiteral datetimeLiteral = new DateLiteral(2024, 6, 15, 12, 30, 45, 0, Type.DATETIMEV2);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(datetimeLiteral, Type.DATEV2);
        // Result must be DATEV2 so it can compare against DATEV2 partition bounds.
        Assertions.assertEquals(Type.DATEV2, result.getType());
        Assertions.assertTrue(result instanceof DateLiteral);
        DateLiteral resultDate = (DateLiteral) result;
        Assertions.assertEquals(2024, resultDate.getYear());
        Assertions.assertEquals(6, resultDate.getMonth());
        Assertions.assertEquals(15, resultDate.getDay());
    }

    @Test
    void testNormalizeDatetimeToDate() throws AnalysisException {
        // DATETIME -> DATE: string re-parse fails (DATE rejects time part).
        // The catch block constructs a DATE literal, stripping the time.
        DateLiteral datetimeLiteral = new DateLiteral(2024, 12, 31, 23, 59, 59, Type.DATETIME);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(datetimeLiteral, Type.DATE);
        Assertions.assertEquals(Type.DATE, result.getType());
        Assertions.assertTrue(result instanceof DateLiteral);
        DateLiteral resultDate = (DateLiteral) result;
        Assertions.assertEquals(2024, resultDate.getYear());
        Assertions.assertEquals(12, resultDate.getMonth());
        Assertions.assertEquals(31, resultDate.getDay());
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
    void testNormalizeDateTimeV2ToDateV2_WithTime() throws AnalysisException {
        // DATETIMEV2 with non-midnight time -> DATEV2.  Time is stripped.
        // Example: CAST(k AS DATETIMEV2) = '2024-06-15 12:30:45' on a DATEV2
        // partition column.  The DATEV2 partition '2024-06-15' covers the
        // entire day, so including it is conservative (safe).
        DateLiteral dtV2 = new DateLiteral(2024, 6, 15, 12, 30, 45, 0, Type.DATETIMEV2);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(dtV2, Type.DATEV2);
        Assertions.assertEquals(Type.DATEV2, result.getType());
        Assertions.assertTrue(result instanceof DateLiteral);
        DateLiteral resultDate = (DateLiteral) result;
        Assertions.assertEquals(2024, resultDate.getYear());
        Assertions.assertEquals(6, resultDate.getMonth());
        Assertions.assertEquals(15, resultDate.getDay());
    }

    @Test
    void testNormalizeDatetimeV2ToDate_Midnight() throws AnalysisException {
        // DATETIMEV2 midnight -> DATE.  Even midnight gets correctly
        // converted to a DATE literal.
        DateLiteral dtV2 = new DateLiteral(2024, 6, 15, 0, 0, 0, 0, Type.DATETIMEV2);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(dtV2, Type.DATE);
        Assertions.assertEquals(Type.DATE, result.getType());
        Assertions.assertTrue(result instanceof DateLiteral);
        DateLiteral resultDate = (DateLiteral) result;
        Assertions.assertEquals(2024, resultDate.getYear());
        Assertions.assertEquals(6, resultDate.getMonth());
        Assertions.assertEquals(15, resultDate.getDay());
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
    void testNormalizeDatetimeV2WithMicrosToDateV2() throws AnalysisException {
        // DATETIMEV2 with microseconds -> DATEV2: microseconds and time
        // stripped, only date preserved.  Conservative for pruning.
        DateLiteral dtV2 = new DateLiteral(2024, 6, 15, 23, 59, 59, 999999, Type.DATETIMEV2);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(dtV2, Type.DATEV2);
        Assertions.assertEquals(Type.DATEV2, result.getType());
        Assertions.assertTrue(result instanceof DateLiteral);
        DateLiteral resultDate = (DateLiteral) result;
        Assertions.assertEquals(2024, resultDate.getYear());
        Assertions.assertEquals(6, resultDate.getMonth());
        Assertions.assertEquals(15, resultDate.getDay());
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
