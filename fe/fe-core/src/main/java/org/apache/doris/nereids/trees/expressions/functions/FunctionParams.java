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

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.MoreFieldsThread;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * This class is used to reuse the origin function's resolved signature to keep signature computation idempotent.
 * The resolved overload, coercion, and precision remain frozen even when a rewrite replaces children with the
 * already-coerced types recorded by that signature. Functions with child-derived complex metadata can refresh only
 * that metadata through {@link ComputeSignature#refreshDerivedSignature}.
 * You should provide a private/protected constructor to pass through FunctionParams to the super class(BoundFunction),
 * and override `withChildren(List&lt;Expression&gt; children)` to build FunctionParams and create a new function
 * with the FunctionParams
 */
public class FunctionParams {
    public final Optional<BoundFunction> originFunction;
    public final String functionName;
    public final List<Expression> arguments;
    public final boolean inferred;

    public FunctionParams(String functionName, List<Expression> arguments) {
        this(null, functionName, arguments, false);
    }

    /** Build function parameters and retain a safe signature-reuse context when requested. */
    public FunctionParams(
            @Nullable BoundFunction originFunction, String functionName,
            List<Expression> arguments, boolean inferred) {
        this.originFunction = MoreFieldsThread.isKeepFunctionSignature()
                ? Optional.ofNullable(originFunction) : Optional.empty();
        this.functionName = functionName;
        SignatureReuseContext reuseContext = createSignatureReuseContext(arguments);
        this.arguments = reuseContext == null
                ? arguments : retypeTypedNullArguments(this.originFunction.get(), arguments, reuseContext);
        this.inferred = inferred;
    }

    /**
     * Build one immutable context tying the lazy resolved signature to the exact immediate-origin arguments.
     * Reuse is disabled when either tree is still unbound so normal analysis can establish a fresh binding.
     */
    @Nullable
    public SignatureReuseContext getSignatureReuseContext() {
        return createSignatureReuseContext(arguments);
    }

    @Nullable
    private SignatureReuseContext createSignatureReuseContext(List<Expression> currentArguments) {
        if (!originFunction.isPresent()
                || originFunction.get().containsType(Unbound.class)
                || currentArguments.stream().anyMatch(argument -> argument.containsType(Unbound.class))) {
            return null;
        }
        BoundFunction origin = originFunction.get();
        return new SignatureReuseContext(
                origin::getSelectedSignature, origin::getSignature, origin.getArguments());
    }

    private static List<Expression> retypeTypedNullArguments(
            BoundFunction originFunction, List<Expression> arguments,
            SignatureReuseContext reuseContext) {
        boolean hasTypedNull = arguments.stream().anyMatch(argument -> argument instanceof NullLiteral
                && !argument.getDataType().isNullType());
        if (!hasTypedNull) {
            return arguments;
        }
        FunctionSignature refreshedSignature = originFunction.refreshDerivedSignature(
                reuseContext.getSelectedSignature(), reuseContext.getResolvedSignature(),
                reuseContext.getImmediateOriginArguments(), arguments);
        ImmutableList.Builder<Expression> refreshedArguments = ImmutableList.builderWithExpectedSize(
                arguments.size());
        boolean changed = false;
        for (int i = 0; i < arguments.size(); i++) {
            Expression argument = arguments.get(i);
            if (argument instanceof NullLiteral && !argument.getDataType().isNullType()) {
                DataType expectedType = refreshedSignature.getArgType(i);
                if (!ChildDerivedSignature.hasSameTypeMetadata(argument.getDataType(), expectedType)) {
                    argument = new NullLiteral(expectedType);
                    changed = true;
                }
            }
            refreshedArguments.add(argument);
        }
        return changed ? refreshedArguments.build() : arguments;
    }

    /** A coherent snapshot used for one withChildren signature-reuse step. */
    public static final class SignatureReuseContext {
        private final Supplier<FunctionSignature> selectedSignature;
        private final Supplier<FunctionSignature> resolvedSignature;
        private final ImmutableList<Expression> immediateOriginArguments;

        private SignatureReuseContext(
                Supplier<FunctionSignature> selectedSignature,
                Supplier<FunctionSignature> resolvedSignature,
                List<Expression> immediateOriginArguments) {
            this.selectedSignature = selectedSignature;
            this.resolvedSignature = resolvedSignature;
            this.immediateOriginArguments = ImmutableList.copyOf(immediateOriginArguments);
        }

        public FunctionSignature getSelectedSignature() {
            return selectedSignature.get();
        }

        public FunctionSignature getResolvedSignature() {
            return resolvedSignature.get();
        }

        public List<Expression> getImmediateOriginArguments() {
            return immediateOriginArguments;
        }
    }
}
