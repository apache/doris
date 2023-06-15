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

import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.functions.AggStateFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.UdfBuilder;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;

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
    private final Map<String, Map<String, List<FunctionBuilder>>> name2UdfBuilders;
    private static final String GLOBAL_FUNCTION = "__GLOBAL_FUNCTION__";

    public FunctionRegistry() {
        name2Builders = new ConcurrentHashMap<>();
        name2UdfBuilders = new ConcurrentHashMap<>();
        registerBuiltinFunctions(name2Builders);
        afterRegisterBuiltinFunctions(name2Builders);
    }

    // this function is used to test.
    // for example, you can create child class of FunctionRegistry and clear builtin functions or add more functions
    // in this method
    @VisibleForTesting
    protected void afterRegisterBuiltinFunctions(Map<String, List<FunctionBuilder>> name2Builders) {}


    public FunctionBuilder findFunctionBuilder(String name, Object argument) {
        return findFunctionBuilder(null, name, ImmutableList.of(argument));
    }

    // currently we only find function by name and arity and args' types.
    public FunctionBuilder findFunctionBuilder(String dbName, String name, List<?> arguments) {
        int arity = arguments.size();
        List<FunctionBuilder> functionBuilders = name2Builders.get(name.toLowerCase());
        if (CollectionUtils.isEmpty(functionBuilders) && AggStateFunctionBuilder.isAggStateCombinator(name)) {
            String nestedName = AggStateFunctionBuilder.getNestedName(name);
            String combinatorSuffix = AggStateFunctionBuilder.getCombinatorSuffix(name);

            functionBuilders = name2Builders.get(nestedName.toLowerCase());

            if (functionBuilders != null) {
                functionBuilders = functionBuilders.stream().map(builder -> {
                    return new AggStateFunctionBuilder(combinatorSuffix, builder);
                }).filter(functionBuilder -> functionBuilder.canApply(arguments)).collect(Collectors.toList());
            }
        }
        if (functionBuilders == null || functionBuilders.isEmpty()) {
            functionBuilders = findUdfBuilder(dbName, name);
            if (functionBuilders == null || functionBuilders.isEmpty()) {
                throw new AnalysisException("Can not found function '" + name + "'");
            }
        }

        // check the arity and type
        List<FunctionBuilder> candidateBuilders = functionBuilders.stream()
                .filter(functionBuilder -> functionBuilder.canApply(arguments))
                .collect(Collectors.toList());
        if (candidateBuilders.isEmpty()) {
            String candidateHints = getCandidateHint(name, functionBuilders);
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
        FunctionHelper.addFunctions(name2Builders, BuiltinTableGeneratingFunctions.INSTANCE.tableGeneratingFunctions);
        FunctionHelper.addFunctions(name2Builders, BuiltinWindowFunctions.INSTANCE.windowFunctions);
    }

    public String getCandidateHint(String name, List<FunctionBuilder> candidateBuilders) {
        return candidateBuilders.stream()
                .map(builder -> name + builder.toString())
                .collect(Collectors.joining(", ", "[", "]"));
    }


    public void addUdf(String dbName, String name, UdfBuilder builder) {
        if (dbName == null) {
            dbName = GLOBAL_FUNCTION;
        }
        synchronized (name2UdfBuilders) {
            Map<String, List<FunctionBuilder>> builders = name2UdfBuilders
                    .computeIfAbsent(dbName, k -> Maps.newHashMap());
            builders.computeIfAbsent(name, k -> Lists.newArrayList()).add(builder);
        }
    }

    public void dropUdf(String dbName, String name, List<DataType> argTypes) {
        if (dbName == null) {
            dbName = GLOBAL_FUNCTION;
        }
        synchronized (name2UdfBuilders) {
            Map<String, List<FunctionBuilder>> builders = name2UdfBuilders.getOrDefault(dbName, ImmutableMap.of());
            builders.getOrDefault(name, ImmutableList.of()).removeIf(builder -> ((UdfBuilder) builder).getArgTypes()
                    .equals(argTypes));
        }
    }

    /**
     * for test
     */
    public List<FunctionBuilder> findUdfBuilder(String dbName, String name) {
        dbName = ClusterNamespace.getFullName(ConnectContext.get().getClusterName(),
                dbName == null ? ConnectContext.get().getDatabase() : dbName);

        List<String> scopes = ImmutableList.of(dbName, GLOBAL_FUNCTION);
        synchronized (name2UdfBuilders) {
            for (String scope : scopes) {
                List<FunctionBuilder> candidate = name2UdfBuilders.getOrDefault(scope, ImmutableMap.of()).get(name);
                if (candidate != null && !candidate.isEmpty()) {
                    return candidate;
                }
            }
        }
        throw new AnalysisException(String.format("Can not find udf named %s in database %s", name, dbName));
    }
}
