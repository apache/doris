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

import java.lang.reflect.Array;
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

    public final boolean isVariableLength;

    // Concrete BoundFunction's constructor
    private final Constructor<BoundFunction> builderMethod;

    public FunctionBuilder(Constructor<BoundFunction> builderMethod) {
        this.builderMethod = Objects.requireNonNull(builderMethod, "builderMethod can not be null");
        this.arity = builderMethod.getParameterCount();
        this.isVariableLength = arity > 0 && builderMethod.getParameterTypes()[arity - 1].isArray();
    }

    /** check whether arguments can apply to the constructor */
    public boolean canApply(List<? extends Object> arguments) {
        if (isVariableLength && arity > arguments.size() + 1) {
            return false;
        }
        if (!isVariableLength && arguments.size() != arity) {
            return false;
        }
        for (int i = 0; i < arguments.size(); i++) {
            Class constructorArgumentType = getConstructorArgumentType(i);
            if (!constructorArgumentType.isInstance(arguments.get(i))) {
                return false;
            }
        }
        return true;
    }

    public Class getConstructorArgumentType(int index) {
        if (isVariableLength && index + 1 >= arity) {
            return builderMethod.getParameterTypes()[arity - 1].getComponentType();
        }
        return builderMethod.getParameterTypes()[index];
    }

    public BoundFunction build(String name, Object argument) {
        return build(name, ImmutableList.of(argument));
    }

    /**
     * build a BoundFunction by function name and arguments.
     * @param name function name which in the sql expression
     * @param arguments the function's argument expressions
     * @return the concrete bound function instance
     */
    public BoundFunction build(String name, List<? extends Object> arguments) {
        try {
            if (isVariableLength) {
                return builderMethod.newInstance(toVariableLengthArguments(arguments));
            } else {
                return builderMethod.newInstance(arguments.stream().toArray(Object[]::new));
            }
        } catch (Throwable t) {
            String argString = arguments.stream()
                    .map(arg -> {
                        if (arg == null) {
                            return "null";
                        } else if (arg instanceof Expression) {
                            return ((Expression) arg).toSql();
                        } else {
                            return arg.toString();
                        }
                    })
                    .collect(Collectors.joining(", ", "(", ")"));
            throw new IllegalStateException("Can not build function: '" + name
                    + "', expression: " + name + argString, t);
        }
    }

    private Object[] toVariableLengthArguments(List<? extends Object> arguments) {
        Object[] constructorArguments = new Object[arity];

        List<?> nonVarArgs = arguments.subList(0, arity - 1);
        for (int i = 0; i < nonVarArgs.size(); i++) {
            constructorArguments[i] = nonVarArgs.get(i);
        }

        List<?> varArgs = arguments.subList(arity - 1, arguments.size());
        Class constructorArgumentType = getConstructorArgumentType(arity);
        Object varArg = Array.newInstance(constructorArgumentType, varArgs.size());
        for (int i = 0; i < varArgs.size(); i++) {
            Array.set(varArg, i, varArgs.get(i));
        }
        constructorArguments[arity - 1] = varArg;

        return constructorArguments;
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
                .map(constructor -> new FunctionBuilder((Constructor<BoundFunction>) constructor))
                .collect(ImmutableList.toImmutableList());
    }
}
