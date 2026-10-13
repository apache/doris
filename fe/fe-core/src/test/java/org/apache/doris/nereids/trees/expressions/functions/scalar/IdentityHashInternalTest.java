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
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The trailing bucket count of identity_hash_internal must be a positive integer literal.
 * These checks reject malformed calls at analysis time, before the expression reaches BE,
 * whose implementation expects the modulus as a constant column. Non-constant or non-positive
 * counts previously crashed BE (DCHECK abort in debug, nullptr dereference / division by
 * zero in release).
 */
public class IdentityHashInternalTest {

    private static final SlotReference INT_COLUMN = new SlotReference(
            "c1", IntegerType.INSTANCE, false, ImmutableList.of());
    private static final SlotReference STRING_COLUMN = new SlotReference(
            "s1", StringType.INSTANCE, false, ImmutableList.of());

    @Test
    public void testValidPositiveIntegerLiteralPasses() {
        for (Expression bucketCount : ImmutableList.of(new IntegerLiteral(1),
                new IntegerLiteral(8), new IntegerLiteral(Integer.MAX_VALUE))) {
            Expression expr = new IdentityHashInternal(INT_COLUMN, bucketCount);
            // positive integer literal must be accepted
            Assertions.assertNotNull(expr.withChildren(ImmutableList.of(INT_COLUMN, bucketCount)));
        }
    }

    @Test
    public void testValidMultiColumnWithLiteralPasses() {
        IdentityHashInternal expr = new IdentityHashInternal(INT_COLUMN, STRING_COLUMN,
                new IntegerLiteral(8));
        Assertions.assertNotNull(expr.withChildren(
                ImmutableList.of(INT_COLUMN, STRING_COLUMN, new IntegerLiteral(8))));
    }

    @Test
    public void testRejectsNonLiteralBucketCount() {
        // the trailing argument is a column, not a literal
        IdentityHashInternal expr = new IdentityHashInternal(INT_COLUMN, INT_COLUMN);
        Assertions.assertThrows(AnalysisException.class,
                () -> expr.withChildren(ImmutableList.of(INT_COLUMN, INT_COLUMN)),
                "non-literal bucket count must be rejected");
    }

    @Test
    public void testRejectsStringLiteralBucketCount() {
        IdentityHashInternal expr = new IdentityHashInternal(INT_COLUMN, new StringLiteral("8"));
        Assertions.assertThrows(AnalysisException.class,
                () -> expr.withChildren(ImmutableList.of(INT_COLUMN, new StringLiteral("8"))),
                "string literal bucket count must be rejected");
    }

    @Test
    public void testRejectsZeroBucketCount() {
        IdentityHashInternal expr = new IdentityHashInternal(INT_COLUMN, new IntegerLiteral(0));
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> expr.withChildren(ImmutableList.of(INT_COLUMN, new IntegerLiteral(0))),
                "zero bucket count must be rejected");
        Assertions.assertTrue(e.getMessage().contains("positive integer"), e.getMessage());
    }

    @Test
    public void testRejectsNegativeBucketCount() {
        IdentityHashInternal expr = new IdentityHashInternal(INT_COLUMN, new IntegerLiteral(-8));
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> expr.withChildren(ImmutableList.of(INT_COLUMN, new IntegerLiteral(-8))),
                "negative bucket count must be rejected");
        Assertions.assertTrue(e.getMessage().contains("positive integer"), e.getMessage());
    }

    @Test
    public void testRejectsBucketCountOverflowingUint32() {
        // BE's modulus is uint32_t; anything above Integer.MAX_VALUE cannot be a bucket count.
        Expression overflow = new BigIntLiteral(Integer.MAX_VALUE + 1L);
        IdentityHashInternal expr = new IdentityHashInternal(INT_COLUMN, overflow);
        Assertions.assertThrows(AnalysisException.class,
                () -> expr.withChildren(ImmutableList.of(INT_COLUMN, overflow)),
                "bucket count above Integer.MAX_VALUE must be rejected");
    }

    @Test
    public void testSignatureStillAcceptsAnyDataColumns() {
        // The leading distribution columns keep AnyDataType varArgs: the check only constrains
        // the trailing bucket count, so the function still parses with any typed columns.
        IdentityHashInternal expr = new IdentityHashInternal(INT_COLUMN, new IntegerLiteral(8));
        Assertions.assertNotNull(expr.withChildren(
                ImmutableList.of(INT_COLUMN, new IntegerLiteral(8))));
        Assertions.assertTrue(expr.getSignatures().get(0).hasVarArgs,
                "identity_hash_internal must stay variadic");
    }
}
