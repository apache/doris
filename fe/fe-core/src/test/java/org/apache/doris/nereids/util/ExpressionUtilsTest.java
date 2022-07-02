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
    public void extractConjunctsTest() {
        List<Expression> expressions;
        Expression expr;

        expr = PARSER.parseExpression("a");
        expressions = ExpressionUtils.extractConjunct(expr);
        Assertions.assertEquals(expressions.size(), 1);
        Assertions.assertEquals(expressions.get(0), expr);

        expr = PARSER.parseExpression("a and b and c");
        Expression a = PARSER.parseExpression("a");
        Expression b = PARSER.parseExpression("b");
        Expression c = PARSER.parseExpression("c");

        expressions = ExpressionUtils.extractConjunct(expr);
        Assertions.assertEquals(expressions.size(), 3);
        Assertions.assertEquals(expressions.get(0), a);
        Assertions.assertEquals(expressions.get(1), b);
        Assertions.assertEquals(expressions.get(2), c);


        expr = PARSER.parseExpression("(a or b) and c and (e or f)");
        expressions = ExpressionUtils.extractConjunct(expr);
        Expression aOrb = PARSER.parseExpression("a or b");
        Expression eOrf = PARSER.parseExpression("e or f");
        Assertions.assertEquals(expressions.size(), 3);
        Assertions.assertEquals(expressions.get(0), aOrb);
        Assertions.assertEquals(expressions.get(1), c);
        Assertions.assertEquals(expressions.get(2), eOrf);
    }

    @Test
    public void extractDisjunctsTest() {
        List<Expression> expressions;
        Expression expr;

        expr = PARSER.parseExpression("a");
        expressions = ExpressionUtils.extractDisjunct(expr);
        Assertions.assertEquals(expressions.size(), 1);
        Assertions.assertEquals(expressions.get(0), expr);

        expr = PARSER.parseExpression("a or b or c");
        Expression a = PARSER.parseExpression("a");
        Expression b = PARSER.parseExpression("b");
        Expression c = PARSER.parseExpression("c");

        expressions = ExpressionUtils.extractDisjunct(expr);
        Assertions.assertEquals(expressions.size(), 3);
        Assertions.assertEquals(expressions.get(0), a);
        Assertions.assertEquals(expressions.get(1), b);
        Assertions.assertEquals(expressions.get(2), c);

        expr = PARSER.parseExpression("(a and b) or c or (e and f)");
        expressions = ExpressionUtils.extractDisjunct(expr);
        Expression aAndb = PARSER.parseExpression("a and b");
        Expression eAndf = PARSER.parseExpression("e and f");
        Assertions.assertEquals(expressions.size(), 3);
        Assertions.assertEquals(expressions.get(0), aAndb);
        Assertions.assertEquals(expressions.get(1), c);
        Assertions.assertEquals(expressions.get(2), eAndf);
    }
}
