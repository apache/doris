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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.qe.GlobalVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InnerProductTest {

    private static final NereidsParser PARSER = new NereidsParser();

    @Test
    public void testInferNullMapKeyType() {
        Expression expression = analyze(
                "inner_product(map(null, cast(2 as float)),"
                        + " map(cast(null as int), cast(3 as float)))");

        Assertions.assertTrue(expression instanceof InnerProduct);
        for (Expression argument : ((InnerProduct) expression).getArguments()) {
            Assertions.assertTrue(argument.getDataType().isMapType());
            Assertions.assertEquals(IntegerType.INSTANCE,
                    ((MapType) argument.getDataType()).getKeyType());
        }
        Assertions.assertDoesNotThrow(expression::checkLegalityAfterRewrite);
    }

    @Test
    public void testRejectMixedMapKeyFamiliesInBothCoercionModes() {
        boolean originalBehavior = GlobalVariable.enableNewTypeCoercionBehavior;
        try {
            for (boolean enableNewBehavior : new boolean[] {true, false}) {
                GlobalVariable.enableNewTypeCoercionBehavior = enableNewBehavior;
                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                        () -> analyze("inner_product(map(1, cast(2 as float)),"
                                + " map('1', cast(3 as float)))"));
                Assertions.assertTrue(exception.getMessage().contains("same type family"),
                        exception::getMessage);
            }
        } finally {
            GlobalVariable.enableNewTypeCoercionBehavior = originalBehavior;
        }
    }

    @Test
    public void testRejectAllNullMapKeyTypesAfterCoercion() {
        Expression expression = analyze(
                "inner_product(map(null, cast(1 as float)),"
                        + " map(null, cast(2 as float)))");

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, expression::checkLegalityAfterRewrite);
        Assertions.assertTrue(exception.getMessage().contains(
                "only supports integer or string map keys"), exception::getMessage);
    }

    private Expression analyze(String sql) {
        return ExpressionRewriteTestHelper.typeCoercion(PARSER.parseExpression(sql));
    }
}
