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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

class IvmSequenceCalculatorTest {

    @Test
    void testBigIntSequence() {
        IvmSequenceCalculator calculator = IvmSequenceCalculator.create(7, BigIntType.INSTANCE);

        BigIntLiteral negative = (BigIntLiteral) calculator.encode(3, BigInteger.ZERO, false);
        BigIntLiteral positive = (BigIntLiteral) calculator.encode(3, BigInteger.ZERO, true);

        Assertions.assertEquals((7L << 11) | (3L << 1), negative.getValue());
        Assertions.assertEquals((7L << 11) | (3L << 1) | 1, positive.getValue());
        Assertions.assertTrue(positive.getValue() > negative.getValue());
    }

    @Test
    void testLargeIntSequence() {
        IvmSequenceCalculator calculator = IvmSequenceCalculator.create(7, LargeIntType.INSTANCE);
        BigInteger binlogSequence = BigInteger.ONE.shiftLeft(63);

        LargeIntLiteral sequence = (LargeIntLiteral) calculator.encode(3, binlogSequence, true);
        BigInteger expected = BigInteger.valueOf(7).shiftLeft(75)
                .or(BigInteger.valueOf(3).shiftLeft(65))
                .or(binlogSequence.shiftLeft(1))
                .or(BigInteger.ONE);

        Assertions.assertEquals(expected, sequence.getValue());
    }

    @Test
    void testLargeIntSequenceUsesFullEncodingRange() {
        long maxRefreshVersion = Long.MAX_VALUE >>> 11;
        IvmSequenceCalculator calculator = IvmSequenceCalculator.create(
                maxRefreshVersion, LargeIntType.INSTANCE);

        LargeIntLiteral sequence = (LargeIntLiteral) calculator.encode(
                1023, BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE), true);

        Assertions.assertEquals(BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE),
                sequence.getValue());
    }

    @Test
    void testSequenceRejectsValuesOutsideEncodingRanges() {
        long maxRefreshVersion = Long.MAX_VALUE >>> 11;
        IvmSequenceCalculator.create(maxRefreshVersion, LargeIntType.INSTANCE);

        IvmException refreshVersionException = Assertions.assertThrows(IvmException.class,
                () -> IvmSequenceCalculator.create(maxRefreshVersion + 1, LargeIntType.INSTANCE));
        Assertions.assertTrue(refreshVersionException.getMessage().contains("refresh version"));

        IvmSequenceCalculator calculator = IvmSequenceCalculator.create(1, LargeIntType.INSTANCE);
        IvmException deltaIndexException = Assertions.assertThrows(IvmException.class,
                () -> calculator.encode(1024, BigInteger.ZERO, true));
        Assertions.assertTrue(deltaIndexException.getMessage().contains("delta index"));

        BigInteger maxBinlogSequence = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
        calculator.encode(0, maxBinlogSequence, true);
        IvmException binlogSequenceException = Assertions.assertThrows(IvmException.class,
                () -> calculator.encode(0, BigInteger.ONE.shiftLeft(64), true));
        Assertions.assertTrue(binlogSequenceException.getMessage().contains("binlog sequence"));

        IvmSequenceCalculator bigIntCalculator = IvmSequenceCalculator.create(1, BigIntType.INSTANCE);
        IvmException unsupportedBinlogException = Assertions.assertThrows(IvmException.class,
                () -> bigIntCalculator.encode(0, BigInteger.ONE, true));
        Assertions.assertTrue(unsupportedBinlogException.getMessage().contains("does not support binlog"));
    }
}
