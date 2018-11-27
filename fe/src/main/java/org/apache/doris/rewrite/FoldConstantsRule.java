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

package org.apache.doris.rewrite;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * This rule replaces a constant Expr with its equivalent LiteralExpr by evaluating the
 * Expr in the BE. Exprs that are already LiteralExprs are not changed.
 *
 * TODO: Expressions fed into this rule are currently not required to be analyzed
 * in order to support constant folding in expressions that contain unresolved
 * references to select-list aliases (such expressions cannot be analyzed).
 * The cross-dependencies between rule transformations and analysis are vague at the
 * moment and make rule application overly complicated.
 *
 * Examples:
 * 1 + 1 + 1 --> 3
 * toupper('abc') --> 'ABC'
 * cast('2016-11-09' as timestamp) --> TIMESTAMP '2016-11-09 00:00:00'
 */
public class FoldConstantsRule implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new FoldConstantsRule();

    private ImmutableMultimap<String, FEFunctionInvoker> functions;
    // For most build-in functions, it will return NullLiteral when params contain NullLiteral.
    // But a few functions need to handle NullLiteral differently, such as "if". It need to add 
    // an attribute to LiteralExpr to mark null and check the attribute to decide whether to 
    // replace the result with NullLiteral when function finished. It leaves to be realized.
    // TODO chenhao16.
    private ImmutableSet<String> nonNullResultWithNullParamFunctions;

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
        if (functions == null) {
            registerFunctions();
        }
        // Avoid calling Expr.isConstant() because that would lead to repeated traversals
        // of the Expr tree. Assumes the bottom-up application of this rule. Constant
        // children should have been folded at this point.
        for (Expr child : expr.getChildren()) {
            if (!child.isLiteral() && !(child instanceof CastExpr)) {
                return expr;
            }
        }

        if (expr.isLiteral() || !expr.isConstant()) {
            return expr;
        }

        // Do not constant fold cast(null as dataType) because we cannot preserve the
        // cast-to-types and that can lead to query failures, e.g., CTAS
        if (expr instanceof CastExpr) {
            CastExpr castExpr = (CastExpr) expr;
            if (castExpr.getChild(0) instanceof NullLiteral) {
                return expr;
            }
        }
        // Analyze constant exprs, if necessary. Note that the 'expr' may become non-constant
        // after analysis (e.g., aggregate functions).
        if (!expr.isAnalyzed()) {
            expr.analyze(analyzer);
            if (!expr.isConstant()) {
                return expr;
            }
        }
        return simplify(expr);
    }

    private Expr simplify(Expr constExpr) throws AnalysisException {
        if (constExpr instanceof ArithmeticExpr
                || constExpr instanceof FunctionCallExpr
                || constExpr instanceof CastExpr) {
            Function fn = constExpr.getFn();
            // unused cast
            if (fn == null) {
               Preconditions.checkState((constExpr instanceof CastExpr)
                        && constExpr.getChildren().size() == 1);    
               return constExpr.getChild(0);
            }

            // null
            if (!nonNullResultWithNullParamFunctions.contains(fn.getFunctionName().getFunction())) {
                for (Expr e : constExpr.getChildren()) {
                    if (e instanceof NullLiteral) {
                        return new NullLiteral();
                    }
                }
            }

            List<ScalarType> argTypes = new ArrayList<>();
            for (Type type : fn.getArgs()) {
                argTypes.add((ScalarType) type);
            }
            FEFunctionSignature signature = new FEFunctionSignature(fn.functionName(),
                    argTypes.toArray(new ScalarType[argTypes.size()]), (ScalarType) fn.getReturnType());
            FEFunctionInvoker invoker = getFunction(signature);
            if (invoker != null) {
                try {
                    return invoker.invoke(constExpr.getChildrenWithoutCast());
                } catch (AnalysisException e) {
                    return constExpr;
                }
            }
        }
        return constExpr;
    }

    private FEFunctionInvoker getFunction(FEFunctionSignature signature) {
        Collection<FEFunctionInvoker> functionInvokers = functions.get(signature.getName());

        if (functionInvokers == null) {
            return null;
        }

        for (FEFunctionInvoker invoker : functionInvokers) {
            if (!invoker.getSignature().returnType.equals(signature.getReturnType())) {
                continue;
            }

            ScalarType[] argTypes1 = invoker.getSignature().getArgTypes();
            ScalarType[] argTypes2 = signature.getArgTypes();

            if (!Arrays.equals(argTypes1, argTypes2)) {
                continue;
            }
            return invoker;
        }
        return null;
    }

    private synchronized void registerFunctions() {
        // double checked locking pattern
        // functions only need to init once
        if (functions != null) {
           return;
        }
        ImmutableMultimap.Builder<String, FEFunctionInvoker> mapBuilder =
                        new ImmutableMultimap.Builder<String, FEFunctionInvoker>();
        Class clazz = FEFunctions.class;
        for (Method method : clazz.getDeclaredMethods()) {
            FEFunction annotation = method.getAnnotation(FEFunction.class);
            if (annotation != null) {
                String name = annotation.name();
                ScalarType returnType = ScalarType.createType(annotation.returnType());
                List<ScalarType> argTypes = new ArrayList<>();
                for (String type : annotation.argTypes()) {
                    argTypes.add(ScalarType.createType(type));
                }
                 FEFunctionSignature signature = new FEFunctionSignature(name,
                        argTypes.toArray(new ScalarType[argTypes.size()]), returnType);
                mapBuilder.put(name, new FEFunctionInvoker(method, signature));
            }
        }
        this.functions = mapBuilder.build();

        // Functions that need to handle null.
        ImmutableSet.Builder<String> setBuilder =
                        new ImmutableSet.Builder<String>();
        setBuilder.add("if");
        setBuilder.add("hll_hash");
        this.nonNullResultWithNullParamFunctions = setBuilder.build();
    }

    public static class FEFunctionInvoker {
        private final Method method;
        private final FEFunctionSignature signature;

        public FEFunctionInvoker(Method method, FEFunctionSignature signature) {
            this.method = method;
            this.signature = signature;
        }

        public Method getMethod() {
            return method;
        }

        public FEFunctionSignature getSignature() {
            return signature;
        }

        public LiteralExpr invoke(List<Expr> args) throws AnalysisException {
            try {
                return (LiteralExpr) method.invoke(null, args.toArray());
            } catch (InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
                throw new AnalysisException(e.getLocalizedMessage());
            }
        }

    }

    public static class FEFunctionSignature {
        private final String name;
        private final ScalarType[] argTypes;
        private final ScalarType returnType;

        public FEFunctionSignature(String name, ScalarType[] argTypes, ScalarType returnType) {
            this.name = name;
            this.argTypes = argTypes;
            this.returnType = returnType;
        }

        public ScalarType[] getArgTypes() {
            return argTypes;
        }

        public ScalarType getReturnType() {
            return returnType;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            FEFunctionSignature signature = (FEFunctionSignature) o;
            return Objects.equals(name, signature.name) && Arrays.equals(argTypes, signature.argTypes)
                    && Objects.equals(returnType, signature.returnType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, argTypes, returnType);
        }
    }
}
