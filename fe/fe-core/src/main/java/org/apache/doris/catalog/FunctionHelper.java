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

import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface FunctionHelper {
    /**
     * put functions into the target map, which the key is the function name,
     * and value is FunctionBuilder is converted from NamedFunc.
     * @param name2FuncBuilders target Map
     * @param functions the NamedFunc list to be put into the target map
     */
    static void addFunctions(Map<String, List<FunctionBuilder>> name2FuncBuilders,
            List<? extends NamedFunc<? extends BoundFunction>> functions) {
        for (NamedFunc<? extends BoundFunction> func : functions) {
            for (String name : func.names) {
                if (name2FuncBuilders.containsKey(name)) {
                    throw new IllegalStateException("Function '" + name + "' already exists in function registry");
                }

                name2FuncBuilders.put(name, func.functionBuilders);
            }
        }
    }

    default ScalarFunc scalar(Class<? extends ScalarFunction> functionClass) {
        String functionName = functionClass.getSimpleName();
        return scalar(functionClass, functionName);
    }

    /**
     * Resolve ScalaFunction class, convert to FunctionBuilder and wrap to ScalarFunc
     * @param functionClass the ScalaFunction class
     * @return ScalaFunc which contains the functionName and the FunctionBuilder
     */
    default ScalarFunc scalar(Class<? extends ScalarFunction> functionClass, String... functionNames) {
        return new ScalarFunc(functionClass, functionNames);
    }

    default AggregateFunc agg(Class<? extends AggregateFunction> functionClass) {
        String functionName = functionClass.getSimpleName();
        return new AggregateFunc(functionClass, functionName);
    }

    /**
     * Resolve AggregateFunction class, convert to FunctionBuilder and wrap to AggregateFunc
     * @param functionClass the AggregateFunction class
     * @return AggregateFunc which contains the functionName and the AggregateFunc
     */
    default AggregateFunc agg(Class<? extends AggregateFunction> functionClass, String... functionNames) {
        return new AggregateFunc(functionClass, functionNames);
    }

    /**
     * use this class to prevent the wrong type from being registered, and support multi function names
     * like substring and substr.
     */
    class NamedFunc<T extends BoundFunction> {
        public final List<String> names;
        public final Class<? extends T> functionClass;

        public final List<FunctionBuilder> functionBuilders;

        public NamedFunc(Class<? extends T> functionClass, String... names) {
            this.functionClass = functionClass;
            this.names = Arrays.stream(names)
                    .map(String::toLowerCase)
                    .collect(ImmutableList.toImmutableList());
            this.functionBuilders = FunctionBuilder.resolve(functionClass);
        }
    }

    class ScalarFunc extends NamedFunc<ScalarFunction> {
        public ScalarFunc(Class<? extends ScalarFunction> functionClass, String... names) {
            super(functionClass, names);
        }
    }

    class AggregateFunc extends NamedFunc<AggregateFunction> {
        public AggregateFunc(Class<? extends AggregateFunction> functionClass, String... names) {
            super(functionClass, names);
        }
    }
}
