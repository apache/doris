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
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

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
    private final Map<String, List<FunctionBuilder>> name2Builders;

    public FunctionRegistry() {
        name2Builders = new ConcurrentHashMap<>();
        registerBuiltinFunctions(name2Builders);
        afterRegisterBuiltinFunctions(name2Builders);
    }

    // this function is used to test.
    // for example, you can create child class of FunctionRegistry and clear builtin functions or add more functions
    // in this method
    @VisibleForTesting
    protected void afterRegisterBuiltinFunctions(Map<String, List<FunctionBuilder>> name2Builders) {}


    public FunctionBuilder findFunctionBuilder(String name, Object argument) {
        return findFunctionBuilder(name, ImmutableList.of(argument));
    }

    // currently we only find function by name and arity
    public FunctionBuilder findFunctionBuilder(String name, List<? extends Object> arguments) {
        int arity = arguments.size();
        List<FunctionBuilder> functionBuilders = name2Builders.get(name.toLowerCase());
        if (functionBuilders == null || functionBuilders.isEmpty()) {
            throw new AnalysisException("Can not found function '" + name + "'");
        }

        // check the arity and type
        List<FunctionBuilder> candidateBuilders = functionBuilders.stream()
                .filter(functionBuilder -> functionBuilder.canApply(arguments))
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

    private void registerBuiltinFunctions(Map<String, List<FunctionBuilder>> name2Builders) {
        FunctionHelper.addFunctions(name2Builders, BuiltinScalarFunctions.INSTANCE.scalarFunctions);
        FunctionHelper.addFunctions(name2Builders, BuiltinAggregateFunctions.INSTANCE.aggregateFunctions);
        FunctionHelper.addFunctions(name2Builders, BuiltinTableValuedFunctions.INSTANCE.tableValuedFunctions);
    }

    public String getCandidateHint(String name, List<FunctionBuilder> candidateBuilders) {
        return candidateBuilders.stream()
                .map(builder -> name + builder.toString())
                .collect(Collectors.joining(", "));
    }
}
