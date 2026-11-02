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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.rules.expression.rules.SimplifyCastRule;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.TinyIntType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Round-67 review fix #1: an explicit CAST around an SPM placeholder must survive
 * parameterization.
 *
 * The placeholder reports the CAPTURED literal's type, so `CAST(_spm_const_var AS
 * TINYINT)` looked redundant for the captured value (the literal 1 IS a tinyint) and
 * SimplifyCastRule eliminated it. The frozen SQL then lost the conversion, and the
 * matching query `CAST(128 AS TINYINT)` - whose literal is a SMALLINT - substituted a
 * wider value: both the requested conversion and the result type were gone. Keeping the
 * cast is why `child.containsType(SpmConstVar.class)` opts out of the redundant-cast
 * elimination.
 */
public class SPMRound67CastTest {

    /** A cast that only LOOKS redundant because the placeholder's type is the literal's. */
    @Test
    public void testExplicitCastOverAPlaceholderSurvivesSimplification() {
        Cast overPlaceholder = new Cast(
                SpmConstVar.of(1L, new TinyIntLiteral((byte) 1)), TinyIntType.INSTANCE);
        Assertions.assertTrue(overPlaceholder.getDataType().equals(
                        overPlaceholder.child().getDataType()),
                "precondition: without the guard this cast looks redundant");
        Assertions.assertSame(overPlaceholder, SimplifyCastRule.simplifyCast(overPlaceholder),
                "the explicit cast around a substitutable placeholder must survive: the"
                        + " substituted value's type is unknown at capture time");
    }

    /** Control: an ordinary redundant cast is still eliminated. */
    @Test
    public void testOrdinaryRedundantCastIsStillEliminated() {
        Cast plain = new Cast(new TinyIntLiteral((byte) 1), TinyIntType.INSTANCE);
        Expression simplified = SimplifyCastRule.simplifyCast(plain);
        Assertions.assertNotSame(plain, simplified,
                "a genuinely redundant cast over a plain literal is still removed");
        Assertions.assertTrue(simplified instanceof TinyIntLiteral);
    }
}
