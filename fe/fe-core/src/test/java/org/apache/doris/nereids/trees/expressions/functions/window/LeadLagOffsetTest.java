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
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

public class LeadLagOffsetTest {

    private static final DecimalV3Literal NON_INTEGER_OFFSET =
            new DecimalV3Literal(new BigDecimal("922337203685477580.1"));

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
}
