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

import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class FuncSig {
    public final DataType returnType;
    public final boolean hasVarArgs;
    public final List<AbstractDataType> argumentsTypes;

    public FuncSig(DataType returnType, boolean hasVarArgs, List<DataType> argumentsTypes) {
        this.returnType = Objects.requireNonNull(returnType, "returnType is not null");
        this.argumentsTypes = ImmutableList.copyOf(
                Objects.requireNonNull(argumentsTypes, "argumentsTypes is not null"));
        this.hasVarArgs = hasVarArgs;
    }

    public static FuncSig of(DataType returnType, List<DataType> argumentsTypes) {
        return of(returnType, false, argumentsTypes);
    }

    public static FuncSig of(DataType returnType, boolean hasVarArgs, List<DataType> argumentsTypes) {
        return new FuncSig(returnType, hasVarArgs, argumentsTypes);
    }

    public static FuncSig of(DataType returnType, DataType... argumentsTypes) {
        return of(returnType, false, argumentsTypes);
    }

    public static FuncSig of(DataType returnType, boolean hasVarArgs, DataType... argumentsTypes) {
        return new FuncSig(returnType, hasVarArgs, Arrays.asList(argumentsTypes));
    }

    public static FuncSigBuilder ret(DataType returnType) {
        return new FuncSigBuilder(returnType);
    }

    public static class FuncSigBuilder {
        public final DataType returnType;

        public FuncSigBuilder(DataType returnType) {
            this.returnType = returnType;
        }

        public FuncSig args(DataType...argTypes) {
            return FuncSig.of(returnType, false, argTypes);
        }

        public FuncSig varArgs(DataType...argTypes) {
            return FuncSig.of(returnType, true, argTypes);
        }
    }
}
