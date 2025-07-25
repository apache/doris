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

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * This class is used to reuse the origin function's signature to keep idempotent of compute signature.
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

    public FunctionParams(
            @Nullable BoundFunction originFunction, String functionName,
            List<Expression> arguments, boolean inferred) {
        this.originFunction = Optional.ofNullable(originFunction);
        this.functionName = functionName;
        this.arguments = arguments;
        this.inferred = inferred;
    }

    public Supplier<FunctionSignature> getOriginSignature() {
        if (originFunction.isPresent() && !hasUnboundExpression()) {
            return () -> originFunction.get().getSignature();
        } else {
            return null;
        }
    }

    private boolean hasUnboundExpression() {
        BoundFunction boundFunction = originFunction.get();
        return boundFunction.containsType(Unbound.class);
    }
}
