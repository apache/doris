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
 * <p>BIGINT encodes {@code (refresh version, delta index, op)}. LARGEINT additionally encodes
 * a 64-bit binlog sequence: {@code (refresh version, delta index, binlog sequence, op)}.
 */
abstract class IvmSequenceCalculator {
    static final int DELTA_INDEX_BITS = 10;
    private static final int BIGINT_LOW_BITS = DELTA_INDEX_BITS + 1;
    private static final int LARGEINT_BINLOG_BITS = 64;
    private static final int LARGEINT_DELTA_INDEX_SHIFT = LARGEINT_BINLOG_BITS + 1;
    private static final int LARGEINT_REFRESH_VERSION_SHIFT =
            LARGEINT_DELTA_INDEX_SHIFT + DELTA_INDEX_BITS;
    private static final int MAX_DELTA_INDEX = 1 << DELTA_INDEX_BITS;
    private static final BigInteger MAX_BINLOG_SEQUENCE =
            BigInteger.ONE.shiftLeft(LARGEINT_BINLOG_BITS).subtract(BigInteger.ONE);

    final long refreshVersion;

    private IvmSequenceCalculator(long refreshVersion) {
        this.refreshVersion = refreshVersion;
    }

    static IvmSequenceCalculator create(long refreshVersion, DataType sequenceType) {
        if (sequenceType.equals(BigIntType.INSTANCE)) {
            return new BigIntSequenceCalculator(refreshVersion);
        }
        if (sequenceType.equals(LargeIntType.INSTANCE)) {
            return new LargeIntSequenceCalculator(refreshVersion);
        }
        throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                "unsupported IVM sequence type: " + sequenceType.simpleString());
    }

    final Expression encodeByDmlFactor(Expression dmlFactor, int deltaIndex) {
        return new If(new GreaterThan(dmlFactor, new TinyIntLiteral((byte) 0)),
                encode(deltaIndex, BigInteger.ZERO, true),
                encode(deltaIndex, BigInteger.ZERO, false));
    }

    abstract Literal encode(int deltaIndex, BigInteger binlogSequence, boolean positive);

    final void checkDeltaIndex(int deltaIndex) {
        if (deltaIndex < 0 || deltaIndex >= MAX_DELTA_INDEX) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM delta index exceeds the sequence encoding range: " + deltaIndex);
        }
    }

    final void checkBinlogSequence(BigInteger binlogSequence) {
        if (binlogSequence.signum() < 0 || binlogSequence.compareTo(MAX_BINLOG_SEQUENCE) > 0) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM binlog sequence exceeds the LARGEINT encoding range: " + binlogSequence);
        }
    }

    private static class BigIntSequenceCalculator extends IvmSequenceCalculator {
        private BigIntSequenceCalculator(long refreshVersion) {
            super(refreshVersion);
            if (refreshVersion < 0 || refreshVersion > (Long.MAX_VALUE >>> BIGINT_LOW_BITS)) {
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM refresh version exceeds the BIGINT sequence encoding range: " + refreshVersion);
            }
        }

        @Override
        Literal encode(int deltaIndex, BigInteger binlogSequence, boolean positive) {
            checkDeltaIndex(deltaIndex);
            if (!binlogSequence.equals(BigInteger.ZERO)) {
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "BIGINT IVM sequence does not support binlog sequence");
            }
            long sequence = (refreshVersion << BIGINT_LOW_BITS)
                    | ((long) deltaIndex << 1) | (positive ? 1 : 0);
            return new BigIntLiteral(sequence);
        }
    }

    private static class LargeIntSequenceCalculator extends IvmSequenceCalculator {
        private LargeIntSequenceCalculator(long refreshVersion) {
            super(refreshVersion);
            if (refreshVersion < 0 || refreshVersion > (Long.MAX_VALUE >>> BIGINT_LOW_BITS)) {
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM refresh version exceeds the LARGEINT sequence encoding range: " + refreshVersion);
            }
        }

        @Override
        Literal encode(int deltaIndex, BigInteger binlogSequence, boolean positive) {
            checkDeltaIndex(deltaIndex);
            checkBinlogSequence(binlogSequence);
            BigInteger sequence = BigInteger.valueOf(refreshVersion).shiftLeft(LARGEINT_REFRESH_VERSION_SHIFT)
                    .or(BigInteger.valueOf(deltaIndex).shiftLeft(LARGEINT_DELTA_INDEX_SHIFT))
                    .or(binlogSequence.shiftLeft(1));
            if (positive) {
                sequence = sequence.or(BigInteger.ONE);
            }
            return new LargeIntLiteral(sequence);
        }
    }
}
