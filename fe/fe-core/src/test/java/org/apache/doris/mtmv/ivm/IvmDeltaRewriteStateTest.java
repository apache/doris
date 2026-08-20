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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.LargeIntType;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IvmDeltaRewriteStateTest extends IvmDeltaTestBase {

    @Test
    void testSequenceEncodesRefreshVersionAndDeltaIndex() {
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(
                ImmutableMap.of(), false, 7L, BigIntType.INSTANCE);

        for (int i = 0; i < 5; i++) {
            state.nextDeltaIndex();
        }
        Assertions.assertEquals((7L << 11) | (5L << 1) | 1,
                ((BigIntLiteral) state.toSequence(state.nextDeltaIndex())).getValue());
    }

    @Test
    void testLargeIntSequenceEncodesRefreshVersionAndDeltaIndex() {
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(
                ImmutableMap.of(), false, 7L, LargeIntType.INSTANCE);

        for (int i = 0; i < 3; i++) {
            state.nextDeltaIndex();
        }
        LargeIntLiteral sequence = (LargeIntLiteral) state.toSequence(state.nextDeltaIndex());
        Assertions.assertEquals(java.math.BigInteger.valueOf(7).shiftLeft(75)
                .or(java.math.BigInteger.valueOf(3).shiftLeft(65)).or(java.math.BigInteger.ONE), sequence.getValue());
    }

    @Test
    void testSequenceRejectsTooManyDeltaScans() {
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(
                ImmutableMap.of(), false, 1L, BigIntType.INSTANCE);

        for (int i = 0; i < 1024; i++) {
            state.nextDeltaIndex();
        }
        IvmException exception = Assertions.assertThrows(IvmException.class, state::nextDeltaIndex);
        Assertions.assertTrue(exception.getMessage().contains("too many delta scans"));
    }

    @Test
    void testExcludedTableDoesNotCreateDeltaScan() {
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(
                ImmutableMap.of(), false, 1L, BigIntType.INSTANCE);

        Assertions.assertTrue(state.isExcluded(buildScan()));
        Assertions.assertFalse(state.createDeltaScan(buildScan()).isPresent());
    }
}
