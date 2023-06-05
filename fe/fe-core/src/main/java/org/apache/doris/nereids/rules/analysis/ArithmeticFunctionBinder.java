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
    private static final NullLiteral DUMMY_EXPRESSION = new NullLiteral();
    private static final Map<String, Expression> FUNCTION_TO_EXPRESSION = ImmutableMap.<String, Expression>builder()
            .put("add", new Add(DUMMY_EXPRESSION, DUMMY_EXPRESSION))
            .put("subtract", new Subtract(DUMMY_EXPRESSION, DUMMY_EXPRESSION))
            .put("multiply", new Multiply(DUMMY_EXPRESSION, DUMMY_EXPRESSION))
            .put("divide", new Divide(DUMMY_EXPRESSION, DUMMY_EXPRESSION))
            .put("int_divide", new IntegralDivide(DUMMY_EXPRESSION, DUMMY_EXPRESSION))
            .put("mod", new Mod(DUMMY_EXPRESSION, DUMMY_EXPRESSION))
            .put("bitand", new BitAnd(DUMMY_EXPRESSION, DUMMY_EXPRESSION))
            .put("bitor", new BitOr(DUMMY_EXPRESSION, DUMMY_EXPRESSION))
            .put("bitxor", new BitXor(DUMMY_EXPRESSION, DUMMY_EXPRESSION))
            .put("bitnot", new BitNot(DUMMY_EXPRESSION))
            .build();

    public boolean isBinaryArithmetic(String functionName) {
        return FUNCTION_TO_EXPRESSION.containsKey(functionName.toLowerCase());
    }

    public Expression bindBinaryArithmetic(String functionName, List<Expression> children) {
        return FUNCTION_TO_EXPRESSION.get(functionName.toLowerCase()).withChildren(children);
    }
}
