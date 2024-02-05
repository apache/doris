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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;

public class FunctionTypeDeducers {
    public interface TypeDeducer {
        public Type deduce(int argIdx, Type[] args);
    }

    public static final ImmutableMap<String, TypeDeducer> DEDUCERS = ImmutableMap.<String, TypeDeducer>builder()
            .put("named_struct", new NamedStructDeducer())
            .put("struct_element", new StructElementDeducer())
            .put("array_contains", new ArrayElemFuncDeducer())
            .put("array_pushback", new ArrayElemFuncDeducer())
            .put("element_at", new ArrayElemFuncDeducer())
            .build();

    public static Type deduce(String fnName, int argIdx, Type[] args) {
        if (DEDUCERS.containsKey(fnName)) {
            return DEDUCERS.get(fnName).deduce(argIdx, args);
        }
        return null;
    }

    public static class ArrayElemFuncDeducer implements TypeDeducer {
        @Override
        public Type deduce(int argIdx, Type[] args) {
            if (args.length >= 2) {
                if (argIdx == 0) {
                    // first args should only to be array or null
                    return args[0] instanceof ArrayType || args[0].isNull() ? new ArrayType(args[1]) : args[0];
                } else if (args[0].isNull()) {
                    // first arg is null, later element is not contains
                    return args[argIdx];
                } else if (args[0] instanceof ArrayType
                        && Type.isImplicitlyCastable(args[argIdx], ((ArrayType) args[0]).getItemType(), false, true)) {
                    return args[argIdx];
                } else {
                    return null;
                }
            }
            return null;
        }
    }

    public static class NamedStructDeducer implements TypeDeducer {
        @Override
        public Type deduce(int argIdx, Type[] args) {
            List<Type> evenArgs = Lists.newArrayList();
            for (int i = 0; i < args.length; i++) {
                if ((i & 1) == 1) {
                    evenArgs.add(args[i]);
                }
            }
            return new StructType(evenArgs);
        }
    }

    public static class StructElementDeducer implements TypeDeducer {
        @Override
        public Type deduce(int argIdx, Type[] args) {
            if (args[0] instanceof StructType) {
                return Type.ANY_ELEMENT_TYPE;
            }
            return null;
        }
    }
}
