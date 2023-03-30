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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.BitAnd;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.BitOr;
import org.apache.doris.nereids.trees.expressions.BitXor;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * bind arithmetic function
 */
public class ArithmeticFunctionBinder {
    private static final NullLiteral dummyExpression = new NullLiteral();
    private static final Map<String, Expression> functionToExpression = ImmutableMap.<String, Expression>builder()
            .put("add", new Add(dummyExpression, dummyExpression))
            .put("subtract", new Subtract(dummyExpression, dummyExpression))
            .put("multiply", new Multiply(dummyExpression, dummyExpression))
            .put("divide", new Divide(dummyExpression, dummyExpression))
            .put("int_divide", new IntegralDivide(dummyExpression, dummyExpression))
            .put("mod", new Mod(dummyExpression, dummyExpression))
            .put("bitand", new BitAnd(dummyExpression, dummyExpression))
            .put("bitor", new BitOr(dummyExpression, dummyExpression))
            .put("bitxor", new BitXor(dummyExpression, dummyExpression))
            .put("bitnot", new BitNot(dummyExpression))
            .build();

    public boolean isBinaryArithmetic(String functionName) {
        return functionToExpression.containsKey(functionName);
    }

    public Expression bindBinaryArithmetic(String functionName, List<Expression> children) {
        return functionToExpression.get(functionName).withChildren(children);
    }
}
