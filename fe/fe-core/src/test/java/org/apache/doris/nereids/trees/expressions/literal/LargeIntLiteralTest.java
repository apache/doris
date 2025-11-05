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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.CastException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class LargeIntLiteralTest {
    @Test
    void testUncheckedCastTo() {
        // To integer like
        LargeIntLiteral d1 = new LargeIntLiteral(new BigInteger("127"));
        Expression expression = d1.uncheckedCastTo(TinyIntType.INSTANCE);
        Assertions.assertInstanceOf(TinyIntLiteral.class, expression);
        Assertions.assertEquals(127, (int) ((TinyIntLiteral) expression).getValue());
        d1 = new LargeIntLiteral(new BigInteger("128"));
        LargeIntLiteral finalD = d1;
        Assertions.assertThrows(CastException.class, () -> finalD.checkedCastTo(TinyIntType.INSTANCE));

        d1 = new LargeIntLiteral(new BigInteger("9223372036854775807"));
        expression = d1.uncheckedCastTo(BigIntType.INSTANCE);
        Assertions.assertInstanceOf(BigIntLiteral.class, expression);
        Assertions.assertEquals(9223372036854775807L, (long) ((BigIntLiteral) expression).getValue());
        d1 = new LargeIntLiteral(new BigInteger("9223372036854775808"));
        LargeIntLiteral finalD1 = d1;
        Assertions.assertThrows(CastException.class, () -> finalD1.checkedCastTo(BigIntType.INSTANCE));

        // to string
        d1 = new LargeIntLiteral(new BigInteger("9223372036854775807"));
        expression = d1.uncheckedCastTo(StringType.INSTANCE);
        Assertions.assertInstanceOf(StringLiteral.class, expression);
        Assertions.assertEquals("9223372036854775807", ((StringLiteral) expression).value);
    }

    @Test
    void testOverflow() throws org.apache.doris.common.AnalysisException {
        LargeIntLiteral value = new LargeIntLiteral(LargeIntType.MIN_VALUE);
        Assertions.assertEquals("-170141183460469231731687303715884105728", value.getValue().toString());
        value = new LargeIntLiteral(LargeIntType.MAX_VALUE);
        Assertions.assertEquals("170141183460469231731687303715884105727", value.getValue().toString());
        Assertions.assertThrows(AnalysisException.class, () -> new LargeIntLiteral(LargeIntType.MAX_VALUE.add(new BigInteger("1"))));
        Assertions.assertThrows(AnalysisException.class, () -> new LargeIntLiteral(LargeIntType.MIN_VALUE.subtract(new BigInteger("1"))));

        Assertions.assertThrows(org.apache.doris.common.AnalysisException.class,
                () -> new org.apache.doris.analysis.LargeIntLiteral("170141183460469231731687303715884105728"));
        Assertions.assertThrows(org.apache.doris.common.AnalysisException.class,
                () -> new org.apache.doris.analysis.LargeIntLiteral("-170141183460469231731687303715884105729"));
        Assertions.assertThrows(org.apache.doris.common.AnalysisException.class,
                () -> new org.apache.doris.analysis.LargeIntLiteral(new BigDecimal("170141183460469231731687303715884105728")));
        Assertions.assertThrows(org.apache.doris.common.AnalysisException.class,
                () -> new org.apache.doris.analysis.LargeIntLiteral(new BigDecimal("-170141183460469231731687303715884105729")));
        org.apache.doris.analysis.LargeIntLiteral largeIntLiteral = new org.apache.doris.analysis.LargeIntLiteral(
                "170141183460469231731687303715884105727");
        Assertions.assertEquals("170141183460469231731687303715884105727", largeIntLiteral.toString());
        largeIntLiteral = new org.apache.doris.analysis.LargeIntLiteral(
                "-170141183460469231731687303715884105728");
        Assertions.assertEquals("-170141183460469231731687303715884105728", largeIntLiteral.toString());
    }

    @Test
    void testToLegacyLiteral() {
        LargeIntLiteral value = new LargeIntLiteral(LargeIntType.MIN_VALUE);
        LiteralExpr literalExpr = value.toLegacyLiteral();
        Assertions.assertEquals("-170141183460469231731687303715884105728", literalExpr.toString());
        value = new LargeIntLiteral(LargeIntType.MAX_VALUE);
        literalExpr = value.toLegacyLiteral();
        Assertions.assertEquals("170141183460469231731687303715884105727", literalExpr.toString());
    }
}
