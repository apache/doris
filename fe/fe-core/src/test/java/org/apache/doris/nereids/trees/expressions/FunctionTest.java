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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.analysis.LambdaFunctionCallExpr;
import org.apache.doris.catalog.BuiltinWindowFunctions;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

class FunctionTest {

    @Disabled
    @Test
    void testCatalog() {
        FunctionRegistry functionRegistry = new FunctionRegistry();
        Set<Pair<String, List<String>>> catalogFunc = getAllCatalogFunc();
        Set<String> unBounded = new HashSet<>();
        Set<String> notUnBoundedFunc = new HashSet<>();
        Set<String> unParsed = new HashSet<>();
        Set<String> unmatchedArg = new HashSet<>();
        NereidsParser parser = new NereidsParser();
        for (Pair<String, List<String>> func : catalogFunc) {
            try {
                String funcCall = construncFuncCall(func);
                Expression unboundedFunc = parser.parseExpression(funcCall);
                if (unboundedFunc instanceof UnboundFunction) {
                    Optional<List<FunctionBuilder>> builders = functionRegistry.tryGetBuiltinBuilders(func.first);
                    if (builders.isPresent() && builders.get().stream().allMatch(b -> b instanceof BuiltinFunctionBuilder)) {
                        try {
                            functionRegistry.findFunctionBuilder(((UnboundFunction) unboundedFunc).getName(), unboundedFunc.getArguments());
                        } catch (Exception e) {
                            if (BuiltinWindowFunctions.INSTANCE.windowFunctions.stream()
                                    .anyMatch(w -> w.names.contains(func.first))) {
                                // nereids' window function is different with legacy's
                                continue;
                            }
                            unmatchedArg.add(String.format("%s %s", func.first, func.second));
                        }
                        continue;
                    }
                    unBounded.add(func.first);
                } else {
                    notUnBoundedFunc.add(func.first);
                }
            } catch (Exception exception) {
                unParsed.add(func.first);
            }
        }
        System.out.println(unmatchedArg.stream().sorted().map(s -> s + "\n").collect(Collectors.toList()));
        System.out.println(unBounded.stream().sorted().map(s -> s + "\n").collect(Collectors.toList()));
        System.out.println(notUnBoundedFunc);
        System.out.println(unParsed);
    }

    private List<String> constructArg(List<String> argTypes) {
        return argTypes.stream().map(
                typeStr -> "null"
        ).collect(Collectors.toList());
    }

    private String construncFuncCall(Pair<String, List<String>> func) {
        List<String> args = constructArg(func.second);
        ImmutableSet<String> simpleFunc = ImmutableSet.<String>builder()
                .add("add")
                .add("subtract")
                .add("divide")
                .add("multiply")
                .add("eq")
                .add("le")
                .add("lt")
                .add("gt")
                .add("ne")
                .add("ge")
                .add("is_null_pred")
                .add("is_not_null_pred")
                .add("eq_for_null")
                .add("mod")
                .add("int_divide")
                .add("bitand")
                .add("bitor")
                .add("bitxor")
                .add("bitnot")
                .add("and")
                .add("or")
                .add("not")
                .add("n")
                .build();
        ImmutableSet<String> inPredicate = ImmutableSet.<String>builder()
                .add("in_set_lookup")
                .add("not_in_set_lookup")
                .add("in_iterate")
                .add("not_in_iterate")
                .build();
        if (func.first.contains("match_")) {
            return String.format("'a' %s 'abcdefg'", func.first);
        } else if (simpleFunc.contains(func.first)) {
            return "1 + 1";
        } else if (func.first.contains("like")) {
            return String.format("'a' %s '%%a%%'", func.first);
        } else if (func.first.contains("regexp")) {
            return String.format("'a' %s '^a'", func.first);
        } else if (inPredicate.contains(func.first)) {
            return String.format("continue");
        } else if (LambdaFunctionCallExpr.LAMBDA_FUNCTION_SET.contains(func.first)) {
            return String.format("%s(null)", func.first);
        }
        return String.format("%s%s", func.first,
                args.stream().collect(Collectors.joining(", ", "(", ")")));
    }

    private Set<Pair<String, List<String>>> getAllCatalogFunc() {
        FunctionSet<Function> functionSet = new FunctionSet<>();
        functionSet.init();
        return functionSet.getAllFunctions().stream()
                .filter(f -> filterUnvaliedFuncName(f.functionName()))
                .map(f -> Pair.of(f.getFunctionName().toString(),
                        Arrays.stream(f.getArgs()).map(Type::toSql).collect(Collectors.toList())))
                .collect(Collectors.toSet());
    }

    private boolean filterUnvaliedFuncName(String name) {
        if (name.contains("castto")) {
            // like casttostring
            return false;
        }
        return true;
    }
}
