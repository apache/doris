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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeAcquire;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.functions.executable.ExecutableFunctions;
import org.apache.doris.nereids.trees.expressions.functions.executable.NumericArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.StringArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.TimeRoundSeries;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

/**
 * An expression evaluator that evaluates the value of an expression.
 */
public enum ExpressionEvaluator {

    INSTANCE;

    private ImmutableMultimap<String, Method> functions;

    ExpressionEvaluator() {
        registerFunctions();
    }

    /**
     * Evaluate the value of the expression.
     */
    public Expression eval(Expression expression) {

        if (!(expression.isConstant() || expression.foldable()) || expression instanceof AggregateFunction) {
            return expression;
        }

        String fnName = null;
        DataType ret = expression.getDataType();
        if (expression instanceof BinaryArithmetic) {
            BinaryArithmetic arithmetic = (BinaryArithmetic) expression;
            fnName = arithmetic.getLegacyOperator().getName();
        } else if (expression instanceof TimestampArithmetic) {
            TimestampArithmetic arithmetic = (TimestampArithmetic) expression;
            fnName = arithmetic.getFuncName();
        } else if (expression instanceof BoundFunction) {
            BoundFunction function = ((BoundFunction) expression);
            fnName = function.getName();
        }

        if ((Env.getCurrentEnv().isNullResultWithOneNullParamFunction(fnName))) {
            for (Expression e : expression.children()) {
                if (e instanceof NullLiteral) {
                    return new NullLiteral(ret);
                }
            }
        }

        return invoke(expression, fnName);
    }

    private Expression invoke(Expression expression, String fnName) {
        Method method = getFunction(fnName, expression.children());
        if (method != null) {
            try {
                int varSize = method.getParameterTypes().length;
                if (varSize == 0) {
                    return (Literal) method.invoke(null, expression.children().toArray());
                }
                boolean hasVarArgs = method.getParameterTypes()[varSize - 1].isArray();
                if (hasVarArgs) {
                    int fixedArgsSize = varSize - 1;
                    int inputSize = expression.children().size();
                    Class<?>[] parameterTypes = method.getParameterTypes();
                    Class<?> parameterType = parameterTypes[varSize - 1];
                    Class<?> componentType = parameterType.getComponentType();
                    Object varArgs = Array.newInstance(componentType, inputSize - fixedArgsSize);
                    for (int i = fixedArgsSize; i < inputSize; i++) {
                        if (!(expression.children().get(i) instanceof NullLiteral)) {
                            Array.set(varArgs, i - fixedArgsSize, expression.children().get(i));
                        }
                    }
                    Object[] objects = new Object[fixedArgsSize + 1];
                    for (int i = 0; i < fixedArgsSize; i++) {
                        objects[i] = expression.children().get(i);
                    }
                    objects[fixedArgsSize] = varArgs;

                    return (Literal) method.invoke(null, varArgs);
                }
                return (Literal) method.invoke(null, expression.children().toArray());
            } catch (InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
                return expression;
            }
        }
        return expression;
    }

    private boolean canDownCastTo(Class<?> expect, Class<?> input) {
        if (DateLiteral.class.isAssignableFrom(expect)
                || DateTimeLiteral.class.isAssignableFrom(expect)) {
            return expect.equals(input);
        }
        return expect.isAssignableFrom(input);
    }

    private Method getFunction(String fnName, List<Expression> inputs) {
        Collection<Method> expectMethods = functions.get(fnName);
        for (Method expect : expectMethods) {
            boolean match = true;
            int varSize = expect.getParameterTypes().length;
            if (varSize == 0) {
                if (inputs.size() == 0) {
                    return expect;
                } else {
                    continue;
                }
            }
            boolean hasVarArgs = expect.getParameterTypes()[varSize - 1].isArray();
            if (hasVarArgs) {
                int fixedArgsSize = varSize - 1;
                int inputSize = inputs.size();
                if (inputSize <= fixedArgsSize) {
                    continue;
                }
                Class<?>[] expectVarTypes = expect.getParameterTypes();
                for (int i = 0; i < fixedArgsSize; i++) {
                    if (!canDownCastTo(expectVarTypes[i], inputs.get(i).getClass())) {
                        match = false;
                    }
                }
                Class<?> varArgsType = expectVarTypes[varSize - 1];
                Class<?> varArgType = varArgsType.getComponentType();
                for (int i = fixedArgsSize; i < inputSize; i++) {
                    if (!canDownCastTo(varArgType, inputs.get(i).getClass())) {
                        match = false;
                    }
                }
            } else {
                int inputSize = inputs.size();
                if (inputSize != varSize) {
                    continue;
                }
                Class<?>[] expectVarTypes = expect.getParameterTypes();
                for (int i = 0; i < varSize; i++) {
                    if (!canDownCastTo(expectVarTypes[i], inputs.get(i).getClass())) {
                        match = false;
                    }
                }
            }
            if (match) {
                return expect;
            }
        }
        return null;
    }

    private void registerFunctions() {
        if (functions != null) {
            return;
        }
        ImmutableMultimap.Builder<String, Method> mapBuilder = new ImmutableMultimap.Builder<>();
        List<Class<?>> classes = ImmutableList.of(
                DateTimeAcquire.class,
                DateTimeExtractAndTransform.class,
                ExecutableFunctions.class,
                DateLiteral.class,
                DateTimeArithmetic.class,
                NumericArithmetic.class,
                StringArithmetic.class,
                TimeRoundSeries.class
        );
        for (Class<?> cls : classes) {
            for (Method method : cls.getDeclaredMethods()) {
                ExecFunctionList annotationList = method.getAnnotation(ExecFunctionList.class);
                if (annotationList != null) {
                    for (ExecFunction f : annotationList.value()) {
                        registerFEFunction(mapBuilder, method, f);
                    }
                }
                registerFEFunction(mapBuilder, method, method.getAnnotation(ExecFunction.class));
            }
        }
        this.functions = mapBuilder.build();
    }

    private void registerFEFunction(ImmutableMultimap.Builder<String, Method> mapBuilder,
            Method method, ExecFunction annotation) {
        if (annotation != null) {
            mapBuilder.put(annotation.name(), method);
        }
    }
}
