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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UnboundFunctionWithUnboundStarTest {

    @Test
    public void testCount() {
        NereidsParser parser = new NereidsParser();
        Expression result = parser.parseExpression("COUNT(*)");
        Assertions.assertEquals(new Count(), result);
    }

    @Test
    public void testJsonObject() {
        NereidsParser parser = new NereidsParser();
        Expression result = parser.parseExpression("JSON_OBJECT(*)");
        Assertions.assertInstanceOf(UnboundFunction.class, result);
        UnboundFunction unboundFunction = (UnboundFunction) result;
        Assertions.assertEquals("JSON_OBJECT", unboundFunction.getName());
        Assertions.assertEquals(1, unboundFunction.arity());
        Assertions.assertEquals(new UnboundStar(ImmutableList.of()), unboundFunction.child(0));
    }

    @Test
    public void testOtherFunctionName() {
        NereidsParser parser = new NereidsParser();
        Exception exception = Assertions.assertThrowsExactly(ParseException.class,
                () -> parser.parseExpression("OTHER(*)"));
        Assertions.assertTrue(exception.getMessage()
                .contains("'*' can only be used in conjunction with COUNT or JSON_OBJECT: "),
                exception.getMessage());
    }

    @Test
    public void testFunctionNameWithDbName() {
        NereidsParser parser = new NereidsParser();
        Exception exception = Assertions.assertThrowsExactly(ParseException.class,
                () -> parser.parseExpression("db.COUNT(*)"));
        Assertions.assertTrue(exception.getMessage()
                .contains("'*' can only be used in conjunction with COUNT or JSON_OBJECT: "),
                exception.getMessage());
    }

    @Test
    public void testStarWithQualifier() {
        NereidsParser parser = new NereidsParser();
        Exception exception = Assertions.assertThrowsExactly(ParseException.class,
                () -> parser.parseExpression("COUNT(t1.*)"));
        Assertions.assertTrue(exception.getMessage()
                        .contains("'*' can not has qualifier with COUNT: "),
                exception.getMessage());

        Expression result = parser.parseExpression("JSON_OBJECT(t1.*)");
        Assertions.assertInstanceOf(UnboundFunction.class, result);
        UnboundFunction unboundFunction = (UnboundFunction) result;
        Assertions.assertEquals("JSON_OBJECT", unboundFunction.getName());
        Assertions.assertEquals(1, unboundFunction.arity());
        Assertions.assertEquals(new UnboundStar(ImmutableList.of("t1")), unboundFunction.child(0));
    }

    @Test
    public void testMoreThanOneStar() {
        NereidsParser parser = new NereidsParser();
        Exception exception = Assertions.assertThrowsExactly(ParseException.class,
                () -> parser.parseExpression("COUNT(*, *)"));
        Assertions.assertTrue(exception.getMessage()
                .contains("'*' can only be used once in conjunction with COUNT: "),
                exception.getMessage());

        Expression result = parser.parseExpression("JSON_OBJECT(t2.*, t1.*, *)");
        Assertions.assertInstanceOf(UnboundFunction.class, result);
        UnboundFunction unboundFunction = (UnboundFunction) result;
        Assertions.assertEquals("JSON_OBJECT", unboundFunction.getName());
        Assertions.assertEquals(3, unboundFunction.arity());
        Assertions.assertEquals(new UnboundStar(ImmutableList.of("t2")), unboundFunction.child(0));
        Assertions.assertEquals(new UnboundStar(ImmutableList.of("t1")), unboundFunction.child(1));
        Assertions.assertEquals(new UnboundStar(ImmutableList.of()), unboundFunction.child(2));
    }

}
