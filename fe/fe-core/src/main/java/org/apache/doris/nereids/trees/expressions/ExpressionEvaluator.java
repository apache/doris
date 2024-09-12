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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
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
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * An expression evaluator that evaluates the value of an expression.
 */
public enum ExpressionEvaluator {

    INSTANCE;

    private ImmutableMultimap<String, FunctionInvoker> functions;

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
        DataType[] args = null;
        DataType ret = expression.getDataType();
        if (expression instanceof BinaryArithmetic) {
            BinaryArithmetic arithmetic = (BinaryArithmetic) expression;
            fnName = arithmetic.getLegacyOperator().getName();
            args = new DataType[]{arithmetic.left().getDataType(), arithmetic.right().getDataType()};
        } else if (expression instanceof TimestampArithmetic) {
            TimestampArithmetic arithmetic = (TimestampArithmetic) expression;
            fnName = arithmetic.getFuncName();
            args = new DataType[]{arithmetic.left().getDataType(), arithmetic.right().getDataType()};
        } else if (expression instanceof BoundFunction) {
            BoundFunction function = ((BoundFunction) expression);
            fnName = function.getName();
            args = new DataType[function.arity()];
            for (int i = 0; i < function.children().size(); i++) {
                args[i] = function.child(i).getDataType();
            }
        }

        if ((Env.getCurrentEnv().isNullResultWithOneNullParamFunction(fnName))) {
            for (Expression e : expression.children()) {
                if (e instanceof NullLiteral) {
                    return new NullLiteral(ret);
                }
            }
        }

        return invoke(expression, fnName, args);
    }

    private Expression invoke(Expression expression, String fnName, DataType[] args) {
        FunctionSignature signature = new FunctionSignature(fnName, args, null, false);
        FunctionInvoker invoker = getFunction(signature);
        if (invoker != null) {
            try {
                if (invoker.getSignature().hasVarArgs()) {
                    int fixedArgsSize = invoker.getSignature().getArgTypes().length - 1;
                    int totalSize = expression.children().size();
                    Class<?>[] parameterTypes = invoker.getMethod().getParameterTypes();
                    Class<?> parameterType = parameterTypes[parameterTypes.length - 1];
                    Class<?> componentType = parameterType.getComponentType();
                    Object varArgs = Array.newInstance(componentType, totalSize - fixedArgsSize);
                    for (int i = fixedArgsSize; i < totalSize; i++) {
                        if (!(expression.children().get(i) instanceof NullLiteral)) {
                            Array.set(varArgs, i - fixedArgsSize, expression.children().get(i));
                        }
                    }
                    Object[] objects = new Object[fixedArgsSize + 1];
                    for (int i = 0; i < fixedArgsSize; i++) {
                        objects[i] = expression.children().get(i);
                    }
                    objects[fixedArgsSize] = varArgs;

                    return invoker.invokeVars(objects);
                }
                return invoker.invoke(expression.children());
            } catch (AnalysisException e) {
                return expression;
            }
        }
        return expression;
    }

    private FunctionInvoker getFunction(FunctionSignature signature) {
        Collection<FunctionInvoker> functionInvokers = functions.get(signature.getName());
        for (FunctionInvoker candidate : functionInvokers) {
            DataType[] candidateTypes = candidate.getSignature().getArgTypes();
            DataType[] expectedTypes = signature.getArgTypes();

            if (candidate.getSignature().hasVarArgs()) {
                if (candidateTypes.length > expectedTypes.length) {
                    continue;
                }
                boolean match = true;
                for (int i = 0; i < candidateTypes.length - 1; i++) {
                    if (!(expectedTypes[i].toCatalogDataType().matchesType(candidateTypes[i].toCatalogDataType()))) {
                        match = false;
                        break;
                    }
                }
                Type varType = candidateTypes[candidateTypes.length - 1].toCatalogDataType();
                for (int i = candidateTypes.length - 1; i < expectedTypes.length; i++) {
                    if (!(expectedTypes[i].toCatalogDataType().matchesType(varType))) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    return candidate;
                } else {
                    continue;
                }
            }
            if (candidateTypes.length != expectedTypes.length) {
                continue;
            }

            boolean match = true;
            for (int i = 0; i < candidateTypes.length; i++) {
                if (!(expectedTypes[i].toCatalogDataType().matchesType(candidateTypes[i].toCatalogDataType()))) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return candidate;
            }
        }
        return null;
    }

    private void registerFunctions() {
        if (functions != null) {
            return;
        }
        ImmutableMultimap.Builder<String, FunctionInvoker> mapBuilder = new ImmutableMultimap.Builder<>();
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

    private void registerFEFunction(ImmutableMultimap.Builder<String, FunctionInvoker> mapBuilder,
            Method method, ExecFunction annotation) {
        if (annotation != null) {
            String name = annotation.name();
            DataType returnType = DataType.convertFromString(annotation.returnType());
            List<DataType> argTypes = new ArrayList<>();
            for (String type : annotation.argTypes()) {
                argTypes.add(TypeCoercionUtils.replaceDecimalV3WithWildcard(DataType.convertFromString(type)));
            }
            DataType[] array = new DataType[argTypes.size()];
            for (int i = 0; i < argTypes.size(); i++) {
                array[i] = argTypes.get(i);
            }
            FunctionSignature signature = new FunctionSignature(name, array, returnType, annotation.varArgs());
            mapBuilder.put(name, new FunctionInvoker(method, signature));
        }
    }

    /**
     * function invoker.
     */
    public static class FunctionInvoker {
        private final Method method;
        private final FunctionSignature signature;

        public FunctionInvoker(Method method, FunctionSignature signature) {
            this.method = method;
            this.signature = signature;
        }

        public Method getMethod() {
            return method;
        }

        public FunctionSignature getSignature() {
            return signature;
        }

        public Literal invoke(List<Expression> args) throws AnalysisException {
            try {
                return (Literal) method.invoke(null, args.toArray());
            } catch (InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
                throw new AnalysisException(e.getLocalizedMessage());
            }
        }

        public Literal invokeVars(Object[] args) throws AnalysisException {
            try {
                return (Literal) method.invoke(null, args);
            } catch (InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
                throw new AnalysisException(e.getLocalizedMessage());
            }
        }
    }

    /**
     * function signature.
     */
    public static class FunctionSignature {
        private final String name;
        private final DataType[] argTypes;
        private final DataType returnType;
        private final boolean hasVarArgs;

        public FunctionSignature(String name, DataType[] argTypes, DataType returnType, boolean hasVarArgs) {
            this.name = name;
            this.argTypes = argTypes;
            this.returnType = returnType;
            this.hasVarArgs = hasVarArgs;
        }

        public DataType[] getArgTypes() {
            return argTypes;
        }

        public DataType getReturnType() {
            return returnType;
        }

        public String getName() {
            return name;
        }

        public boolean hasVarArgs() {
            return hasVarArgs;
        }
    }

}
