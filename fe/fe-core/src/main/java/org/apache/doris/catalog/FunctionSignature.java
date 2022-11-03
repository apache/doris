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

package org.apache.doris.catalog;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;

public class FunctionSignature {
    public final DataType returnType;
    public final boolean hasVarArgs;
    public final List<DataType> argumentsTypes;
    public final int arity;

    public FunctionSignature(DataType returnType, boolean hasVarArgs, List<DataType> argumentsTypes) {
        this.returnType = Objects.requireNonNull(returnType, "returnType is not null");
        this.argumentsTypes = ImmutableList.copyOf(
                Objects.requireNonNull(argumentsTypes, "argumentsTypes is not null"));
        this.hasVarArgs = hasVarArgs;
        this.arity = argumentsTypes.size();
    }

    public Optional<DataType> getVarArgType() {
        return hasVarArgs ? Optional.of(argumentsTypes.get(arity - 1)) : Optional.empty();
    }

    public DataType getArgType(int index) {
        if (hasVarArgs && index >= arity) {
            return argumentsTypes.get(arity - 1);
        }
        return argumentsTypes.get(index);
    }

    public FunctionSignature withReturnType(DataType returnType) {
        return new FunctionSignature(returnType, hasVarArgs, argumentsTypes);
    }

    public FunctionSignature withArgumentTypes(boolean hasVarArgs, List<DataType> argumentsTypes) {
        return new FunctionSignature(returnType, hasVarArgs, argumentsTypes);
    }

    /**
     * change argument type by the signature's type and the corresponding argument's type
     * @param arguments arguments
     * @param transform param1: signature's type, param2: argument's type, return new type you want to change
     * @return
     */
    public FunctionSignature withArgumentTypes(List<Expression> arguments,
            BiFunction<DataType, Expression, DataType> transform) {
        List<DataType> newTypes = Lists.newArrayList();
        for (int i = 0; i < arguments.size(); i++) {
            newTypes.add(transform.apply(getArgType(i), arguments.get(i)));
        }
        return withArgumentTypes(hasVarArgs, newTypes);
    }

    public static FunctionSignature of(DataType returnType, List<DataType> argumentsTypes) {
        return of(returnType, false, argumentsTypes);
    }

    public static FunctionSignature of(DataType returnType, boolean hasVarArgs, List<DataType> argumentsTypes) {
        return new FunctionSignature(returnType, hasVarArgs, argumentsTypes);
    }

    public static FunctionSignature of(DataType returnType, DataType... argumentsTypes) {
        return of(returnType, false, argumentsTypes);
    }

    public static FunctionSignature of(DataType returnType, boolean hasVarArgs, DataType... argumentsTypes) {
        return new FunctionSignature(returnType, hasVarArgs, Arrays.asList(argumentsTypes));
    }

    public static FuncSigBuilder ret(DataType returnType) {
        return new FuncSigBuilder(returnType);
    }

    public static class FuncSigBuilder {
        public final DataType returnType;

        public FuncSigBuilder(DataType returnType) {
            this.returnType = returnType;
        }

        public FunctionSignature args(DataType...argTypes) {
            return FunctionSignature.of(returnType, false, argTypes);
        }

        public FunctionSignature varArgs(DataType...argTypes) {
            return FunctionSignature.of(returnType, true, argTypes);
        }
    }
}
