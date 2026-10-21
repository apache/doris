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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.LargeIntType;

import java.math.BigInteger;

/**
 * Encodes IVM delta ordering into the MTMV sequence column.
 *
 * <p>BIGINT encodes {@code (sequence prefix, delta index, op)}. LARGEINT additionally encodes
 * a 64-bit changelog sequence: {@code (sequence prefix, delta index, changelog sequence, op)}.
 */
abstract class IvmSequenceCalculator {
    static final int DELTA_INDEX_BITS = 10;
    private static final int BIGINT_LOW_BITS = DELTA_INDEX_BITS + 1;
    private static final int LARGEINT_CHANGELOG_BITS = 64;
    private static final int LARGEINT_DELTA_INDEX_SHIFT = LARGEINT_CHANGELOG_BITS + 1;
    private static final int LARGEINT_SEQUENCE_PREFIX_SHIFT =
            LARGEINT_DELTA_INDEX_SHIFT + DELTA_INDEX_BITS;
    private static final int MAX_DELTA_INDEX = 1 << DELTA_INDEX_BITS;
    private static final BigInteger MAX_CHANGELOG_SEQUENCE =
            BigInteger.ONE.shiftLeft(LARGEINT_CHANGELOG_BITS).subtract(BigInteger.ONE);

    final long sequencePrefix;

    private IvmSequenceCalculator(long sequencePrefix) {
        this.sequencePrefix = sequencePrefix;
    }

    static IvmSequenceCalculator create(long sequencePrefix, DataType sequenceType) {
        if (sequenceType.equals(BigIntType.INSTANCE)) {
            return new BigIntSequenceCalculator(sequencePrefix);
        }
        if (sequenceType.equals(LargeIntType.INSTANCE)) {
            return new LargeIntSequenceCalculator(sequencePrefix);
        }
        throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                "unsupported IVM sequence type: " + sequenceType.simpleString());
    }

    final Expression encodeByDmlFactor(Expression dmlFactor, int deltaIndex) {
        return new If(new GreaterThan(dmlFactor, new TinyIntLiteral((byte) 0)),
                encode(deltaIndex, BigInteger.ZERO, true),
                encode(deltaIndex, BigInteger.ZERO, false));
    }

    abstract Literal encode(int deltaIndex, BigInteger changelogSequence, boolean positive);

    final void checkDeltaIndex(int deltaIndex) {
        if (deltaIndex < 0 || deltaIndex >= MAX_DELTA_INDEX) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM delta index exceeds the sequence encoding range: " + deltaIndex);
        }
    }

    final void checkChangelogSequence(BigInteger changelogSequence) {
        if (changelogSequence.signum() < 0 || changelogSequence.compareTo(MAX_CHANGELOG_SEQUENCE) > 0) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM changelog sequence exceeds the LARGEINT encoding range: " + changelogSequence);
        }
    }

    private static class BigIntSequenceCalculator extends IvmSequenceCalculator {
        private BigIntSequenceCalculator(long sequencePrefix) {
            super(sequencePrefix);
            if (sequencePrefix < 0 || sequencePrefix > (Long.MAX_VALUE >>> BIGINT_LOW_BITS)) {
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM sequence prefix exceeds the BIGINT sequence encoding range: " + sequencePrefix);
            }
        }

        @Override
        Literal encode(int deltaIndex, BigInteger changelogSequence, boolean positive) {
            checkDeltaIndex(deltaIndex);
            if (!changelogSequence.equals(BigInteger.ZERO)) {
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "BIGINT IVM sequence does not support changelog sequence");
            }
            long sequence = (sequencePrefix << BIGINT_LOW_BITS)
                    | ((long) deltaIndex << 1) | (positive ? 1 : 0);
            return new BigIntLiteral(sequence);
        }
    }

    private static class LargeIntSequenceCalculator extends IvmSequenceCalculator {
        private LargeIntSequenceCalculator(long sequencePrefix) {
            super(sequencePrefix);
            if (sequencePrefix < 0 || sequencePrefix > (Long.MAX_VALUE >>> BIGINT_LOW_BITS)) {
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM sequence prefix exceeds the LARGEINT sequence encoding range: " + sequencePrefix);
            }
        }

        @Override
        Literal encode(int deltaIndex, BigInteger changelogSequence, boolean positive) {
            checkDeltaIndex(deltaIndex);
            checkChangelogSequence(changelogSequence);
            BigInteger sequence = BigInteger.valueOf(sequencePrefix).shiftLeft(LARGEINT_SEQUENCE_PREFIX_SHIFT)
                    .or(BigInteger.valueOf(deltaIndex).shiftLeft(LARGEINT_DELTA_INDEX_SHIFT))
                    .or(changelogSequence.shiftLeft(1));
            if (positive) {
                sequence = sequence.or(BigInteger.ONE);
            }
            return new LargeIntLiteral(sequence);
        }
    }
}
