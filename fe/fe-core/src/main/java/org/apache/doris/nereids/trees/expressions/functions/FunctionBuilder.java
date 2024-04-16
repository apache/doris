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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * This class used to build BoundFunction(Builtin or Combinator) by a list of Expressions.
 */
public abstract class FunctionBuilder {
    public abstract Class<? extends BoundFunction> functionClass();

    /** check whether arguments can apply to the constructor */
    public abstract boolean canApply(List<? extends Object> arguments);

    public final Pair<? extends Expression, ? extends BoundFunction> build(String name, Object argument) {
        return build(name, ImmutableList.of(argument));
    }

    /**
     * build a BoundFunction by function name and arguments.
     * @param name function name which in the sql expression
     * @param arguments the function's argument expressions
     * @return the concrete bound function instance,
     *          key: the final result expression that should return, e.g. the function wrapped some cast function,
     *          value: the real BoundFunction
     */
    public abstract Pair<? extends Expression, ? extends BoundFunction> build(
            String name, List<? extends Object> arguments);
}
