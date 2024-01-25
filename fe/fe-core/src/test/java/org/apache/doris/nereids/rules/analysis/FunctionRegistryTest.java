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

import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Year;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

// this ut will add more test case later
public class FunctionRegistryTest implements MemoPatternMatchSupported {
    private ConnectContext connectContext = MemoTestUtils.createConnectContext();

    @Test
    public void testDefaultFunctionNameIsClassName() {
        // we register Year by the code in FunctionRegistry: scalar(Year.class).
        // and default class name should be year.
        PlanChecker.from(connectContext)
                .analyze("select year('2021-01-01')")
                .matches(
                        logicalOneRowRelation().when(r -> {
                            Year year = (Year) r.getProjects().get(0).child(0);
                            Assertions.assertEquals("2021-01-01",
                                    ((Literal) year.getArguments().get(0).child(0)).getValue());
                            return true;
                        })
                );
    }

    @Test
    public void testMultiName() {
        // the substring function has 2 names:
        // 1. substring
        // 2. substr
        PlanChecker.from(connectContext)
                .analyze("select substring('abc', 1, 2), substr(substring('abcdefg', 4, 3), 1, 2)")
                .matches(
                        logicalOneRowRelation().when(r -> {
                            Substring firstSubstring = (Substring) r.getProjects().get(0).child(0);
                            Assertions.assertEquals("abc", ((Literal) firstSubstring.getSource()).getValue());
                            Assertions.assertEquals(1, ((Literal) firstSubstring.getPosition()).getValue());
                            Assertions.assertEquals(2, ((Literal) firstSubstring.getLength().get()).getValue());

                            Substring secondSubstring = (Substring) r.getProjects().get(1).child(0);
                            Assertions.assertTrue(secondSubstring.getSource() instanceof Substring);
                            Assertions.assertEquals(1, ((Literal) secondSubstring.getPosition()).getValue());
                            Assertions.assertEquals(2, ((Literal) secondSubstring.getLength().get()).getValue());
                            return true;
                        })
                );
    }

    @Test
    public void testOverrideArity() {
        // the substring function has 2 override functions:
        // 1. substring(string, position)
        // 2. substring(string, position, length)
        PlanChecker.from(connectContext)
                .analyze("select substr('abc', 1), substring('def', 2, 3)")
                .matches(
                        logicalOneRowRelation().when(r -> {
                            Substring firstSubstring = (Substring) r.getProjects().get(0).child(0);
                            Assertions.assertEquals("abc", ((Literal) firstSubstring.getSource()).getValue());
                            Assertions.assertEquals(1, ((Literal) firstSubstring.getPosition()).getValue());
                            Assertions.assertTrue(firstSubstring.getLength().isPresent());

                            Substring secondSubstring = (Substring) r.getProjects().get(1).child(0);
                            Assertions.assertEquals("def", ((Literal) secondSubstring.getSource()).getValue());
                            Assertions.assertEquals(2, ((Literal) secondSubstring.getPosition()).getValue());
                            Assertions.assertEquals(3, ((Literal) secondSubstring.getLength().get()).getValue());
                            return true;
                        })
                );
    }

    @Test
    public void testAddFunction() throws Exception {
        FunctionRegistry functionRegistry = new FunctionRegistry() {
            @Override
            protected void afterRegisterBuiltinFunctions(Map<String, List<FunctionBuilder>> name2builders) {
                name2builders.put("foo", BuiltinFunctionBuilder.resolve(ExtendFunction.class));
            }
        };

        ImmutableList<Expression> arguments = ImmutableList.of(Literal.of(1));
        FunctionBuilder functionBuilder = functionRegistry.findFunctionBuilder("foo", arguments);
        Expression function = functionBuilder.build("foo", arguments);
        Assertions.assertEquals(function.getClass(), ExtendFunction.class);
        Assertions.assertEquals(arguments, function.getArguments());
    }

    @Test
    public void testOverrideDifferenceTypes() {
        FunctionRegistry functionRegistry = new FunctionRegistry() {
            @Override
            protected void afterRegisterBuiltinFunctions(Map<String, List<FunctionBuilder>> name2builders) {
                name2builders.put("abc", BuiltinFunctionBuilder.resolve(AmbiguousFunction.class));
            }
        };

        // currently we can not support the override same arity function with difference types
        Assertions.assertThrowsExactly(AnalysisException.class, () -> {
            functionRegistry.findFunctionBuilder("abc", ImmutableList.of(Literal.of(1)));
        });
    }

    public static class ExtendFunction extends BoundFunction implements UnaryExpression, PropagateNullable,
            ExplicitlyCastableSignature {
        public ExtendFunction(Expression a1) {
            super("foo", a1);
        }

        @Override
        public List<FunctionSignature> getSignatures() {
            return ImmutableList.of(
                    FunctionSignature.ret(IntegerType.INSTANCE).args(IntegerType.INSTANCE)
            );
        }

        @Override
        public boolean hasVarArguments() {
            return false;
        }
    }

    public static class AmbiguousFunction extends ScalarFunction implements UnaryExpression, PropagateNullable,
            ExplicitlyCastableSignature {
        public AmbiguousFunction(Expression a1) {
            super("abc", a1);
        }

        public AmbiguousFunction(Literal a1) {
            super("abc", a1);
        }

        @Override
        public List<FunctionSignature> getSignatures() {
            return ImmutableList.of(
                    FunctionSignature.ret(IntegerType.INSTANCE).args(IntegerType.INSTANCE)
            );
        }

        @Override
        public boolean hasVarArguments() {
            return false;
        }
    }
}
