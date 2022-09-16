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

import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class used to resolve a concrete BoundFunction's class and build BoundFunction by a list of Expressions.
 */
public class FunctionBuilder {
    public final int arity;

    // Concrete BoundFunction's constructor
    private final Constructor<BoundFunction> builderMethod;

    public FunctionBuilder(Constructor<BoundFunction> builderMethod) {
        this.builderMethod = Objects.requireNonNull(builderMethod, "builderMethod can not be null");
        this.arity = builderMethod.getParameterCount();
    }

    /**
     * build a BoundFunction by function name and arguments.
     * @param name function name which in the sql expression
     * @param arguments the function's argument expressions
     * @return the concrete bound function instance
     */
    public BoundFunction build(String name, List<Expression> arguments) {
        try {
            return builderMethod.newInstance(arguments.toArray(new Expression[0]));
        } catch (Throwable t) {
            String argString = arguments.stream()
                    .map(arg -> arg == null ? "null" : arg.toSql())
                    .collect(Collectors.joining(", ", "(", ")"));
            throw new IllegalStateException("Can not build function: '" + name
                    + "', expression: " + name + argString, t);
        }
    }

    @Override
    public String toString() {
        return Arrays.stream(builderMethod.getParameterTypes())
                .map(type -> type.getSimpleName())
                .collect(Collectors.joining(", ", "(", ")"));
    }

    /**
     * resolve a Concrete boundFunction's class and convert the constructors to FunctionBuilder
     * @param functionClass a class which is the child class of BoundFunction and can not be abstract class
     * @return list of FunctionBuilder which contains the constructor
     */
    public static List<FunctionBuilder> resolve(Class<? extends BoundFunction> functionClass) {
        Preconditions.checkArgument(!Modifier.isAbstract(functionClass.getModifiers()),
                "Can not resolve bind function which is abstract class: "
                        + functionClass.getSimpleName());
        return Arrays.stream(functionClass.getConstructors())
                .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
                .filter(constructor ->
                        // all arguments must be Expression
                        Arrays.stream(constructor.getParameterTypes())
                                .allMatch(Expression.class::isAssignableFrom)
                )
                .map(constructor -> new FunctionBuilder((Constructor<BoundFunction>) constructor))
                .collect(ImmutableList.toImmutableList());
    }
}
