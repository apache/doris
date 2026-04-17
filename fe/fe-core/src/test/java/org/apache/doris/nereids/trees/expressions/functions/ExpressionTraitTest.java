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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.analyzer.UnboundVariable.VariableType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Variable;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for ExpressionTrait behaviors when children are `Variable`.
 */
public class ExpressionTraitTest {

    static class DummyFunction extends Expression {
        protected DummyFunction(List<Expression> children) {
            super(children);
        }

        protected DummyFunction(Expression... children) {
            super(children);
        }

        @Override
        public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
            return null;
        }

        @Override
        public Expression withChildren(List<Expression> children) {
            return new DummyFunction(children);
        }

        @Override
        protected String computeToSql() {
            return "dummy";
        }

        @Override
        public boolean nullable() {
            return false;
        }
    }

    @Test
    public void testVariableWithRealExpression() {
        IntegerLiteral lit = new IntegerLiteral(42);
        Variable var = new Variable("v", VariableType.USER, lit);

        DummyFunction func = new DummyFunction(var);

        List<Expression> args = func.getArguments();
        Assertions.assertEquals(1, args.size());
        Assertions.assertEquals(lit, args.get(0));

        Assertions.assertEquals(lit, func.getArgument(0));

        List<DataType> types = func.getArgumentsTypes();
        Assertions.assertEquals(1, types.size());
        Assertions.assertEquals(lit.getDataType(), types.get(0));

        Assertions.assertEquals(lit.getDataType(), func.getArgumentType(0));
    }

    @Test
    public void testVariableWithoutRealExpression() {
        IntegerLiteral lit = new IntegerLiteral(100);
        // create a Variable but override getRealExpression to simulate a missing real expression
        Variable var = new Variable("v", VariableType.USER, lit) {
            @Override
            public Expression getRealExpression() {
                return null;
            }
        };

        DummyFunction func = new DummyFunction(var);

        List<Expression> args = func.getArguments();
        Assertions.assertEquals(1, args.size());
        // when Variable.getRealExpression() returns null, ExpressionTrait should return the Variable itself
        Assertions.assertSame(var, args.get(0));

        Assertions.assertSame(var, func.getArgument(0));

        List<DataType> types = func.getArgumentsTypes();
        Assertions.assertEquals(1, types.size());
        // fallback to variable.getDataType()
        Assertions.assertEquals(var.getDataType(), types.get(0));

        Assertions.assertEquals(var.getDataType(), func.getArgumentType(0));
    }
}
