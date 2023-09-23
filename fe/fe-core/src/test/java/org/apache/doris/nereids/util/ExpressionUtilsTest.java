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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * ExpressionUtils ut.
 */
public class ExpressionUtilsTest {

    private static final NereidsParser PARSER = new NereidsParser();

    @Test
    public void extractConjunctionTest() {
        List<Expression> expressions;
        Expression expr;

        expr = PARSER.parseExpression("a");
        expressions = ExpressionUtils.extractConjunction(expr);
        Assertions.assertEquals(1, expressions.size());
        Assertions.assertEquals(expr, expressions.get(0));

        expr = PARSER.parseExpression("a and b and c");
        Expression a = PARSER.parseExpression("a");
        Expression b = PARSER.parseExpression("b");
        Expression c = PARSER.parseExpression("c");

        expressions = ExpressionUtils.extractConjunction(expr);
        Assertions.assertEquals(3, expressions.size());
        Assertions.assertEquals(a, expressions.get(0));
        Assertions.assertEquals(b, expressions.get(1));
        Assertions.assertEquals(c, expressions.get(2));

        expr = PARSER.parseExpression("(a or b) and c and (e or f)");
        expressions = ExpressionUtils.extractConjunction(expr);
        Expression aOrb = PARSER.parseExpression("a or b");
        Expression eOrf = PARSER.parseExpression("e or f");
        Assertions.assertEquals(3, expressions.size());
        Assertions.assertEquals(aOrb, expressions.get(0));
        Assertions.assertEquals(c, expressions.get(1));
        Assertions.assertEquals(eOrf, expressions.get(2));
    }

    @Test
    public void extractDisjunctionTest() {
        List<Expression> expressions;
        Expression expr;

        expr = PARSER.parseExpression("a");
        expressions = ExpressionUtils.extractDisjunction(expr);
        Assertions.assertEquals(1, expressions.size());
        Assertions.assertEquals(expr, expressions.get(0));

        expr = PARSER.parseExpression("a or b or c");
        Expression a = PARSER.parseExpression("a");
        Expression b = PARSER.parseExpression("b");
        Expression c = PARSER.parseExpression("c");

        expressions = ExpressionUtils.extractDisjunction(expr);
        Assertions.assertEquals(3, expressions.size());
        Assertions.assertEquals(a, expressions.get(0));
        Assertions.assertEquals(b, expressions.get(1));
        Assertions.assertEquals(c, expressions.get(2));

        expr = PARSER.parseExpression("(a and b) or c or (e and f)");
        expressions = ExpressionUtils.extractDisjunction(expr);
        Expression aAndb = PARSER.parseExpression("a and b");
        Expression eAndf = PARSER.parseExpression("e and f");
        Assertions.assertEquals(3, expressions.size());
        Assertions.assertEquals(aAndb, expressions.get(0));
        Assertions.assertEquals(c, expressions.get(1));
        Assertions.assertEquals(eAndf, expressions.get(2));
    }
}
