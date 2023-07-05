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
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.combinator.MergeCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.UnionCombinator;
import org.apache.doris.nereids.types.AggStateType;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class used to resolve AggState's combinators
 */
public class AggStateFunctionBuilder extends FunctionBuilder {
    public static final String COMBINATOR_LINKER = "_";
    public static final String STATE = "state";
    public static final String MERGE = "merge";
    public static final String UNION = "union";

    public static final String STATE_SUFFIX = COMBINATOR_LINKER + STATE;
    public static final String MERGE_SUFFIX = COMBINATOR_LINKER + MERGE;
    public static final String UNION_SUFFIX = COMBINATOR_LINKER + UNION;

    private final FunctionBuilder nestedBuilder;

    private final String combinatorSuffix;

    public AggStateFunctionBuilder(String combinatorSuffix, FunctionBuilder nestedBuilder) {
        this.combinatorSuffix = Objects.requireNonNull(combinatorSuffix, "combinatorSuffix can not be null");
        this.nestedBuilder = Objects.requireNonNull(nestedBuilder, "nestedBuilder can not be null");
    }

    @Override
    public boolean canApply(List<? extends Object> arguments) {
        if (combinatorSuffix.equals(STATE)) {
            return nestedBuilder.canApply(arguments);
        } else {
            if (arguments.size() != 1) {
                return false;
            }
            Expression argument = (Expression) arguments.get(0);
            if (!argument.getDataType().isAggStateType()) {
                return false;
            }

            return nestedBuilder.canApply(((AggStateType) argument.getDataType()).getMockedExpressions());
        }
    }

    private AggregateFunction buildState(String nestedName, List<? extends Object> arguments) {
        return (AggregateFunction) nestedBuilder.build(nestedName, arguments);
    }

    private AggregateFunction buildMergeOrUnion(String nestedName, List<? extends Object> arguments) {
        if (arguments.size() != 1 || !(arguments.get(0) instanceof Expression)
                || !((Expression) arguments.get(0)).getDataType().isAggStateType()) {
            String argString = arguments.stream().map(arg -> {
                if (arg == null) {
                    return "null";
                } else if (arg instanceof Expression) {
                    return ((Expression) arg).toSql();
                } else {
                    return arg.toString();
                }
            }).collect(Collectors.joining(", ", "(", ")"));
            throw new IllegalStateException("Can not build AggState nested function: '" + nestedName + "', expression: "
                    + nestedName + argString);
        }

        Expression arg = (Expression) arguments.get(0);
        AggStateType type = (AggStateType) arg.getDataType();

        return (AggregateFunction) nestedBuilder.build(nestedName, type.getMockedExpressions());
    }

    @Override
    public BoundFunction build(String name, List<? extends Object> arguments) {
        String nestedName = getNestedName(name);
        if (combinatorSuffix.equals(STATE)) {
            AggregateFunction nestedFunction = buildState(nestedName, arguments);
            return new StateCombinator((List<Expression>) arguments, nestedFunction);
        } else if (combinatorSuffix.equals(MERGE)) {
            AggregateFunction nestedFunction = buildMergeOrUnion(nestedName, arguments);
            return new MergeCombinator((List<Expression>) arguments, nestedFunction);
        } else if (combinatorSuffix.equals(UNION)) {
            AggregateFunction nestedFunction = buildMergeOrUnion(nestedName, arguments);
            return new UnionCombinator((List<Expression>) arguments, nestedFunction);
        }
        return null;
    }

    public static boolean isAggStateCombinator(String name) {
        return name.toLowerCase().endsWith(STATE_SUFFIX) || name.toLowerCase().endsWith(MERGE_SUFFIX)
                || name.toLowerCase().endsWith(UNION_SUFFIX);
    }

    public static String getNestedName(String name) {
        return name.substring(0, name.length() - getCombinatorSuffix(name).length() - 1);
    }

    public static String getCombinatorSuffix(String name) {
        if (!name.contains(COMBINATOR_LINKER)) {
            throw new IllegalStateException(name + " call getCombinatorSuffix must contains " + COMBINATOR_LINKER);
        }
        return name.substring(name.lastIndexOf(COMBINATOR_LINKER) + 1);
    }
}
