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

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.Avg;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.Count;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.Max;
import org.apache.doris.nereids.trees.expressions.functions.Min;
import org.apache.doris.nereids.trees.expressions.functions.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.Substring;
import org.apache.doris.nereids.trees.expressions.functions.Sum;
import org.apache.doris.nereids.trees.expressions.functions.WeekOfYear;
import org.apache.doris.nereids.trees.expressions.functions.Year;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * New function registry for nereids.
 *
 * this class is developing for more functions.
 */
@Developing
@ThreadSafe
public class FunctionRegistry {
    public static final List<ScalarFunc> SCALAR_FUNCTIONS = ImmutableList.of(
            scalar(Substring.class, "substr", "substring"),
            scalar(WeekOfYear.class),
            scalar(Year.class)
    );

    public static final ImmutableList<AggregateFunc> AGGREGATE_FUNCTIONS = ImmutableList.of(
            agg(Avg.class),
            agg(Count.class),
            agg(Max.class),
            agg(Min.class),
            agg(Sum.class)
    );

    private final Map<String, List<FunctionBuilder>> name2Builders;

    public FunctionRegistry() {
        name2Builders = new ConcurrentHashMap<>();
        addFunctions(name2Builders, SCALAR_FUNCTIONS);
        addFunctions(name2Builders, AGGREGATE_FUNCTIONS);
        afterRegisterBuiltinFunctions(name2Builders);
    }

    // this function is used to test.
    // for example, you can create child class of FunctionRegistry and clear builtin functions or add more functions
    // in this method
    @VisibleForTesting
    protected void afterRegisterBuiltinFunctions(Map<String, List<FunctionBuilder>> name2Builders) {}

    // currently we only find function by name and arity
    public FunctionBuilder findFunctionBuilder(String name, List<Expression> arguments) {
        int arity = arguments.size();
        List<FunctionBuilder> functionBuilders = name2Builders.get(name.toLowerCase());
        if (functionBuilders.isEmpty()) {
            throw new AnalysisException("Can not found function '" + name + "'");
        }

        List<FunctionBuilder> candidateBuilders = functionBuilders.stream()
                .filter(functionBuilder -> functionBuilder.arity == arity)
                .collect(Collectors.toList());
        if (candidateBuilders.isEmpty()) {
            String candidateHints = getCandidateHint(name, candidateBuilders);
            throw new AnalysisException("Can not found function '" + name
                    + "' which has " + arity + " arity. Candidate functions are: " + candidateHints);
        }

        if (candidateBuilders.size() > 1) {
            String candidateHints = getCandidateHint(name, candidateBuilders);
            // NereidsPlanner not supported override function by the same arity, should we support it?

            throw new AnalysisException("Function '" + name + "' is ambiguous: " + candidateHints);
        }
        return candidateBuilders.get(0);
    }

    private void addFunctions(Map<String, List<FunctionBuilder>> name2FuncBuilders,
            List<? extends NamedFunc<? extends BoundFunction>> funcs) {
        for (NamedFunc<? extends BoundFunction> func : funcs) {
            for (String name : func.names) {
                if (name2FuncBuilders.containsKey(name)) {
                    throw new IllegalStateException("Function '" + name + "' already exists in function registry");
                }

                name2FuncBuilders.put(name, func.functionBuilders);
            }
        }
    }

    public String getCandidateHint(String name, List<FunctionBuilder> candidateBuilders) {
        return candidateBuilders.stream()
                .map(builder -> name + builder.toString())
                .collect(Collectors.joining(", "));
    }

    public static final ScalarFunc scalar(Class<? extends ScalarFunction> functionClass) {
        String functionName = functionClass.getSimpleName();
        return scalar(functionClass, functionName);
    }

    public static final ScalarFunc scalar(Class<? extends ScalarFunction> functionClass, String... functionNames) {
        return new ScalarFunc(functionClass, functionNames);
    }

    public static final AggregateFunc agg(Class<? extends AggregateFunction> functionClass) {
        String functionName = functionClass.getSimpleName();
        return new AggregateFunc(functionClass, functionName);
    }

    private static final AggregateFunc agg(Class<? extends AggregateFunction> functionClass, String... functionNames) {
        return new AggregateFunc(functionClass, functionNames);
    }

    /**
     * use this class to prevent the wrong type from being registered, and support multi function names
     * like substring and substr.
     */
    public static class NamedFunc<T extends BoundFunction> {
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

    public static class ScalarFunc extends NamedFunc<ScalarFunction> {
        public ScalarFunc(Class<? extends ScalarFunction> functionClass, String... names) {
            super(functionClass, names);
        }
    }

    public static class AggregateFunc extends NamedFunc<AggregateFunction> {
        public AggregateFunc(Class<? extends AggregateFunction> functionClass, String... names) {
            super(functionClass, names);
        }
    }
}
