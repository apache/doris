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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.expressions.functions.ExecutableFunctions;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableMultimap;

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

        if (!expression.isConstant() || expression instanceof AggregateFunction) {
            return expression;
        }

        String fnName = null;
        DataType[] args = null;
        if (expression instanceof BinaryArithmetic) {
            BinaryArithmetic arithmetic = (BinaryArithmetic) expression;
            fnName = arithmetic.getLegacyOperator().getName();
            args = new DataType[]{arithmetic.left().getDataType(), arithmetic.right().getDataType()};
        } else if (expression instanceof TimestampArithmetic) {
            TimestampArithmetic arithmetic = (TimestampArithmetic) expression;
            fnName = arithmetic.getFuncName();
            args = new DataType[]{arithmetic.left().getDataType(), arithmetic.right().getDataType()};
        }

        if ((Env.getCurrentEnv().isNullResultWithOneNullParamFunction(fnName))) {
            for (Expression e : expression.children()) {
                if (e instanceof NullLiteral) {
                    return Literal.of(null);
                }
            }
        }

        return invoke(expression, fnName, args);
    }

    private Expression invoke(Expression expression, String fnName, DataType[] args) {
        FunctionSignature signature = new FunctionSignature(fnName, args, null);
        FunctionInvoker invoker = getFunction(signature);
        if (invoker != null) {
            try {
                return invoker.invoke(expression.children());
            } catch (AnalysisException e) {
                return expression;
            }
        }
        return expression;
    }

    private FunctionInvoker getFunction(FunctionSignature signature) {
        Collection<FunctionInvoker> functionInvokers = functions.get(signature.getName());
        if (functionInvokers == null) {
            return null;
        }
        for (FunctionInvoker candidate : functionInvokers) {
            DataType[] candidateTypes = candidate.getSignature().getArgTypes();
            DataType[] expectedTypes = signature.getArgTypes();

            if (candidateTypes.length != expectedTypes.length) {
                continue;
            }
            boolean match = true;
            for (int i = 0; i < candidateTypes.length; i++) {
                if (!candidateTypes[i].equals(expectedTypes[i])) {
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
        ImmutableMultimap.Builder<String, FunctionInvoker> mapBuilder =
                new ImmutableMultimap.Builder<String, FunctionInvoker>();
        Class clazz = ExecutableFunctions.class;
        for (Method method : clazz.getDeclaredMethods()) {
            ExecFunctionList annotationList = method.getAnnotation(ExecFunctionList.class);
            if (annotationList != null) {
                for (ExecFunction f : annotationList.value()) {
                    registerFEFunction(mapBuilder, method, f);
                }
            }
            registerFEFunction(mapBuilder, method, method.getAnnotation(ExecFunction.class));
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
                argTypes.add(DataType.convertFromString(type));
            }
            FunctionSignature signature = new FunctionSignature(name,
                    argTypes.toArray(new DataType[argTypes.size()]), returnType);
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
    }

    /**
     * function signature.
     */
    public static class FunctionSignature {
        private final String name;
        private final DataType[] argTypes;
        private final DataType returnType;

        public FunctionSignature(String name, DataType[] argTypes, DataType returnType) {
            this.name = name;
            this.argTypes = argTypes;
            this.returnType = returnType;
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
    }

}
