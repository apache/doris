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

    @Test
    void testNormalizeDateToDateTimeV2() throws AnalysisException {
        // DATE -> DATETIMEV2: when createLiteral fails (date-only string can't
        // be parsed as datetime), the normalization returns the original value
        // unchanged. This is by design: the original literal still has the
        // correct value even if the type isn't a perfect match.
        DateLiteral dateLiteral = new DateLiteral(2024, 1, 1, Type.DATE);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(dateLiteral, Type.DATETIMEV2);
        // Falls back to original because createLiteral("2024-01-01", DATETIMEV2) fails
        Assertions.assertEquals(Type.DATE, result.getType());
        Assertions.assertEquals("2024-01-01", result.getStringValue());
    }

    @Test
    void testNormalizeDateTimeV2ToDateType() throws AnalysisException {
        // DATETIMEV2 -> DATEV2: createLiteral rejects datetime string for
        // DATEV2 type (date type can't have time part), so normalization
        // falls back to original.
        DateLiteral datetimeLiteral = new DateLiteral(2024, 1, 1, 0, 0, 0, Type.DATETIMEV2);
        LiteralExpr result = ScanNode.normalizePartitionFilterLiteral(datetimeLiteral, Type.DATEV2);
        // Falls back to original because createLiteral rejects datetime string for date types
        Assertions.assertEquals(Type.DATETIMEV2, result.getType());
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
