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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IdentityHashInternalTest {

    private static final SlotReference INT_COLUMN = new SlotReference(
            "c1", IntegerType.INSTANCE, false, ImmutableList.of());
    private static final SlotReference STRING_COLUMN = new SlotReference(
            "s1", StringType.INSTANCE, false, ImmutableList.of());

    @Test
    public void testValidPositiveIntegerLiteralPasses() {
        for (Expression bucketCount : ImmutableList.of(new IntegerLiteral(1),
                new IntegerLiteral(8), new IntegerLiteral(Integer.MAX_VALUE))) {
            IdentityHashInternal expr = new IdentityHashInternal(INT_COLUMN, STRING_COLUMN, bucketCount);
            Assertions.assertNotNull(TypeCoercionUtils.processBoundFunction(expr));
            Assertions.assertDoesNotThrow(expr::checkLegalityAfterRewrite);
        }
    }

    @Test
    public void testRejectsInvalidBucketCountsDuringBinding() {
        for (Expression bucketCount : ImmutableList.of(INT_COLUMN, new StringLiteral("8"),
                new NullLiteral(), new IntegerLiteral(0), new IntegerLiteral(-8),
                new BigIntLiteral(Integer.MAX_VALUE + 1L),
                // Above BIGINT: LargeIntLiteral must be rejected with full precision, not
                // truncated to a positive long (2^64+1 truncates to 1).
                new LargeIntLiteral(java.math.BigInteger.valueOf(2).pow(64).add(java.math.BigInteger.ONE)))) {
            IdentityHashInternal expr = new IdentityHashInternal(INT_COLUMN, bucketCount);
            Assertions.assertThrows(AnalysisException.class, () -> TypeCoercionUtils.processBoundFunction(expr));
            Assertions.assertThrows(AnalysisException.class, expr::checkLegalityAfterRewrite);
        }
        IdentityHashInternal oneArgument = new IdentityHashInternal(new IntegerLiteral(8));
        Assertions.assertThrows(AnalysisException.class, () -> TypeCoercionUtils.processBoundFunction(oneArgument));
    }

    @Test
    public void testRejectsUnsupportedDistributionTypes() {
        for (DataType type : ImmutableList.of(JsonType.INSTANCE, ArrayType.of(IntegerType.INSTANCE),
                BitmapType.INSTANCE, HllType.INSTANCE)) {
            IdentityHashInternal expr = new IdentityHashInternal(new SlotReference("v", type), new IntegerLiteral(8));
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> TypeCoercionUtils.processBoundFunction(expr));
            Assertions.assertTrue(exception.getMessage().contains("unsupported distribution argument type"));
            Assertions.assertThrows(AnalysisException.class, expr::checkLegalityAfterRewrite);
        }
    }

    @Test
    public void testRejectsFloatingPointDistributionTypes() {
        // Mirror HashDistributionInfo.checkDistributionColumnType: FLOAT/DOUBLE can never be a
        // distribution column because their FE pruning bytes (FloatLiteral.getHashValue -> UTF-8
        // string) differ from the BE routing bytes (IEEE-754 little-endian), so the debug
        // function must not model a routing that cannot exist.
        for (DataType type : ImmutableList.of(FloatType.INSTANCE, DoubleType.INSTANCE)) {
            IdentityHashInternal expr = new IdentityHashInternal(new SlotReference("v", type), new IntegerLiteral(8));
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> TypeCoercionUtils.processBoundFunction(expr));
            Assertions.assertTrue(exception.getMessage().contains("unsupported distribution argument type"),
                    type + " must be rejected: " + exception.getMessage());
            Assertions.assertThrows(AnalysisException.class, expr::checkLegalityAfterRewrite);
        }
    }

    @Test
    public void testPreservesLegacyDistributionTypes() {
        for (DataType type : ImmutableList.of(DateType.INSTANCE, DateTimeType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT)) {
            IdentityHashInternal expr = new IdentityHashInternal(new SlotReference("v", type), new IntegerLiteral(8));
            Expression bound = TypeCoercionUtils.processBoundFunction(expr);
            Assertions.assertEquals(type, bound.child(0).getDataType());
        }
    }

    @Test
    public void testSqlBindingRejectsMalformedCallsWithoutRewriting() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        for (boolean skipFold : ImmutableList.of(false, true)) {
            context.getSessionVariable().debugSkipFoldConstant = skipFold;
            for (String arguments : ImmutableList.of("8", "1, NULL", "1, CAST(NULL AS BIGINT)",
                    "1, CAST(NULL AS INT)", "1, '8'", "1, 0", "1, -1", "1, 2147483648",
                    "1, 18446744073709551617", "1, -18446744073709551615",
                    "CAST('{\"x\":1}' AS JSON), 8", "CAST(NULL AS JSON), 8", "[1, 2], 8")) {
                String sql = "SELECT identity_hash_internal(" + arguments + ")";
                Assertions.assertThrows(AnalysisException.class, () -> PlanChecker.from(context).analyze(sql), sql);
            }
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(context)
                    .analyze("SELECT identity_hash_internal(CAST(NULL AS INT), 'a', 8)"));
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(context)
                    .analyze("SELECT identity_hash_internal(NULL, 8)"));
        }
    }
}
