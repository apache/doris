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

package org.apache.doris.nereids.cost;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ExpressionCostEvaluatorTest {
    protected static final NereidsParser PARSER = new NereidsParser();

    @Test
    public void test() {
        Expression expr1 = parse("a+b");
        ExpressionCostEvaluator evaluator = new ExpressionCostEvaluator();
        double cost1 = expr1.accept(evaluator, null);
        System.out.println("cost1 = " + cost1);

        Expression expr2 = parse("a + 3 + 4");
        double cost2 = expr2.accept(evaluator, null);
        System.out.println("cost2 = " + cost2);

        Expression expr3 = parse("substr(c, 3, 4)");
        double cost3 = expr3.accept(evaluator, null);
        System.out.println("cost3 = " + cost3);

        Expression expr4 = parse("a + b + 1");
        double cost4 = expr4.accept(evaluator, null);
        System.out.println("cost4 = " + cost4);

        Expression expr5 = parse("abs(a) + 1");
        double cost5 = expr5.accept(evaluator, null);
        System.out.println("cost5 = " + cost5);

        Expression expr6 = parse("a + 1");
        double cost6 = expr6.accept(evaluator, null);
        System.out.println("cost6 = " + cost6);

        Expression c = parse("c");
        Alias alias = new Alias(c);
        double cost7 = alias.accept(evaluator, null);
        System.out.println(cost7);

        Assertions.assertTrue(cost1 > cost2);
        Assertions.assertTrue(cost3 > cost2);
        Assertions.assertTrue(cost4 > cost1);
        Assertions.assertTrue(cost5 > cost6);
        Assertions.assertEquals(cost7, 0.0);
    }

    private Expression parse(String exprStr) {
        Expression expr = PARSER.parseExpression(exprStr);
        Map<String, SlotReference> bindMap = new HashMap<>();
        bindMap.put("a", new SlotReference("a", IntegerType.INSTANCE));
        bindMap.put("b", new SlotReference("b", DecimalV2Type.SYSTEM_DEFAULT));
        bindMap.put("c", new SlotReference("c", VarcharType.SYSTEM_DEFAULT));
        return expr.rewriteDownShortCircuit(e -> {
            Expression replacedExpr = bindMap.get(e.getExpressionName());
            return replacedExpr == null ? e : replacedExpr;
        });
    }
}
