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
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class LeadLagOffsetTest {

    private static final DecimalV3Literal NON_INTEGER_OFFSET =
            new DecimalV3Literal(new BigDecimal("922337203685477580.1"));
    private static final BigIntLiteral MAX_BIGINT_OFFSET = new BigIntLiteral(Long.MAX_VALUE);
    private static final LargeIntLiteral OVER_MAX_BIGINT_OFFSET =
            new LargeIntLiteral(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));

    @Test
    public void testLagRejectsNonIntegerOffsetBeforeTypeCoercion() {
        Lag lag = new Lag(new IntegerLiteral(1), NON_INTEGER_OFFSET);

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, lag::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(exception.getMessage().contains(
                "The offset parameter of LAG must be a constant positive integer"));
    }

    @Test
    public void testLeadRejectsNonIntegerOffsetBeforeTypeCoercion() {
        Lead lead = new Lead(new IntegerLiteral(1), NON_INTEGER_OFFSET);

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, lead::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(exception.getMessage().contains(
                "The offset parameter of LEAD must be a constant positive integer"));
    }

    @Test
    public void testLagRejectsOffsetOverMaxBigintBeforeTypeCoercion() {
        Lag lag = new Lag(new IntegerLiteral(1), OVER_MAX_BIGINT_OFFSET);

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, lag::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(exception.getMessage().contains(
                "The offset parameter of LAG must not exceed " + Long.MAX_VALUE));
    }

    @Test
    public void testLeadRejectsOffsetOverMaxBigintBeforeTypeCoercion() {
        Lead lead = new Lead(new IntegerLiteral(1), OVER_MAX_BIGINT_OFFSET);

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, lead::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(exception.getMessage().contains(
                "The offset parameter of LEAD must not exceed " + Long.MAX_VALUE));
    }

    @Test
    public void testMaxBigintOffsetIsAcceptedBeforeTypeCoercion() {
        Lag lag = new Lag(new IntegerLiteral(1), MAX_BIGINT_OFFSET);
        Lead lead = new Lead(new IntegerLiteral(1), MAX_BIGINT_OFFSET);

        Assertions.assertDoesNotThrow(lag::checkLegalityBeforeTypeCoercion);
        Assertions.assertDoesNotThrow(lead::checkLegalityBeforeTypeCoercion);
    }
}
