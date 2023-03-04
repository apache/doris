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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExpressionTranslatorTest {

    @Test
    public void testUnaryArithmetic() {
        BitNot bitNot = new BitNot(new IntegerLiteral(1));
        ExpressionTranslator translator = ExpressionTranslator.INSTANCE;
        Expr expr = translator.visitUnaryArithmetic(bitNot, null);
        Assertions.assertEquals(expr, new ArithmeticExpr(Operator.BITNOT, new IntLiteral(1), null));
    }
}
