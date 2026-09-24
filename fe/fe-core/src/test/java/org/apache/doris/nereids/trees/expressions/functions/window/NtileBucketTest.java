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

package org.apache.doris.nereids.trees.expressions.functions.window;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

public class NtileBucketTest {

    private static final String POSITIVE_INTEGER_MESSAGE =
            "The bucket parameter of NTILE must be a constant positive integer";

    @Test
    public void testLiteralBucketIsAccepted() {
        Ntile ntile = new Ntile(new IntegerLiteral(3));

        Assertions.assertDoesNotThrow(ntile::checkLegalityBeforeTypeCoercion);
    }

    @Test
    public void testFoldableConstantExpressionIsAccepted() {
        Ntile add = new Ntile(new Add(new IntegerLiteral(1), new IntegerLiteral(1)));
        Ntile cast = new Ntile(new Cast(new Subtract(new IntegerLiteral(5), new IntegerLiteral(2)),
                BigIntType.INSTANCE));

        Assertions.assertDoesNotThrow(add::checkLegalityBeforeTypeCoercion);
        Assertions.assertDoesNotThrow(cast::checkLegalityBeforeTypeCoercion);
    }

    @Test
    public void testConstantExpressionFoldedToNonPositiveIsRejected() {
        Ntile zero = new Ntile(new Subtract(new IntegerLiteral(1), new IntegerLiteral(1)));
        Ntile negative = new Ntile(new Subtract(new IntegerLiteral(1), new IntegerLiteral(2)));

        AnalysisException zeroException = Assertions.assertThrows(
                AnalysisException.class, zero::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(zeroException.getMessage().contains(POSITIVE_INTEGER_MESSAGE));
        AnalysisException negativeException = Assertions.assertThrows(
                AnalysisException.class, negative::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(negativeException.getMessage().contains(POSITIVE_INTEGER_MESSAGE));
    }

    @Test
    public void testNonPositiveLiteralIsRejected() {
        Ntile zero = new Ntile(new IntegerLiteral(0));
        Ntile negative = new Ntile(new IntegerLiteral(-1));

        AnalysisException zeroException = Assertions.assertThrows(
                AnalysisException.class, zero::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(zeroException.getMessage().contains(POSITIVE_INTEGER_MESSAGE));
        AnalysisException negativeException = Assertions.assertThrows(
                AnalysisException.class, negative::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(negativeException.getMessage().contains(POSITIVE_INTEGER_MESSAGE));
    }

    @Test
    public void testConstantExpressionFoldedToNullIsRejected() {
        Ntile nullBucket = new Ntile(new Cast(new StringLiteral("abc"), IntegerType.INSTANCE));
        Ntile nullLiteral = new Ntile(new NullLiteral(IntegerType.INSTANCE));

        AnalysisException castException = Assertions.assertThrows(
                AnalysisException.class, nullBucket::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(castException.getMessage().contains(POSITIVE_INTEGER_MESSAGE));
        AnalysisException nullException = Assertions.assertThrows(
                AnalysisException.class, nullLiteral::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(nullException.getMessage().contains(POSITIVE_INTEGER_MESSAGE));
    }

    @Test
    public void testNonConstantBucketIsRejected() {
        Ntile ntile = new Ntile(new SlotReference("k1", IntegerType.INSTANCE));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, ntile::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(exception.getMessage().contains("The bucket of NTILE must be a constant value"));
    }

    @Test
    public void testNonIntegralBucketIsRejected() {
        Ntile ntile = new Ntile(new DoubleLiteral(2.0));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, ntile::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(exception.getMessage().contains("The bucket of NTILE must be a integer"));
    }

    @Test
    public void testLiteralBucketIsAcceptedAfterRewrite() {
        Ntile ntile = new Ntile(new IntegerLiteral(3));

        Assertions.assertDoesNotThrow(ntile::checkLegalityAfterRewrite);
    }

    @Test
    public void testUnfoldedBucketIsRejectedAfterRewrite() {
        Ntile unfolded = new Ntile(new Add(new IntegerLiteral(1), new IntegerLiteral(1)));
        Ntile zero = new Ntile(new IntegerLiteral(0));

        AnalysisException unfoldedException = Assertions.assertThrows(
                AnalysisException.class, unfolded::checkLegalityAfterRewrite);
        Assertions.assertTrue(unfoldedException.getMessage().contains(POSITIVE_INTEGER_MESSAGE));
        AnalysisException zeroException = Assertions.assertThrows(
                AnalysisException.class, zero::checkLegalityAfterRewrite);
        Assertions.assertTrue(zeroException.getMessage().contains(POSITIVE_INTEGER_MESSAGE));
    }

    @Test
    public void testLargeIntBucketIsRejected() {
        Ntile ntile = new Ntile(new LargeIntLiteral(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, ntile::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(exception.getMessage().contains(
                "The bucket of NTILE must be an integer within the range of BIGINT"));
    }
}
