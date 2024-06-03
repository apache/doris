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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.combinator.ForEachCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.MergeCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.UnionCombinator;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class used to resolve AggState's combinators
 */
public class AggCombinerFunctionBuilder extends FunctionBuilder {
    public static final String COMBINATOR_LINKER = "_";
    public static final String STATE = "state";
    public static final String MERGE = "merge";
    public static final String UNION = "union";
    public static final String FOREACH = "foreach";

    public static final String STATE_SUFFIX = COMBINATOR_LINKER + STATE;
    public static final String MERGE_SUFFIX = COMBINATOR_LINKER + MERGE;
    public static final String UNION_SUFFIX = COMBINATOR_LINKER + UNION;
    public static final String FOREACH_SUFFIX = COMBINATOR_LINKER + FOREACH;

    private final FunctionBuilder nestedBuilder;

    private final String combinatorSuffix;

    public AggCombinerFunctionBuilder(String combinatorSuffix, FunctionBuilder nestedBuilder) {
        this.combinatorSuffix = Objects.requireNonNull(combinatorSuffix, "combinatorSuffix can not be null");
        this.nestedBuilder = Objects.requireNonNull(nestedBuilder, "nestedBuilder can not be null");
    }

    @Override
    public Class<? extends BoundFunction> functionClass() {
        return nestedBuilder.functionClass();
    }

    @Override
    public boolean canApply(List<?> arguments) {
        if (combinatorSuffix.equalsIgnoreCase(STATE) || combinatorSuffix.equalsIgnoreCase(FOREACH)) {
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
        return (AggregateFunction) nestedBuilder.build(nestedName, arguments).first;
    }

    private AggregateFunction buildForEach(String nestedName, List<? extends Object> arguments) {
        List<Expression> forEachargs = arguments.stream().map(expr -> {
            if (!(expr instanceof SlotReference)) {
                throw new IllegalStateException(
                        "Can not build foreach nested function: '" + nestedName);
            }
            DataType arrayType = (((Expression) expr).getDataType());
            if (!(arrayType instanceof ArrayType)) {
                throw new IllegalStateException(
                        "foreach must be input array type: '" + nestedName);
            }
            DataType itemType = ((ArrayType) arrayType).getItemType();
            return new SlotReference("mocked", itemType, (((ArrayType) arrayType).containsNull()));
        }).collect(Collectors.toList());
        return (AggregateFunction) nestedBuilder.build(nestedName, forEachargs).first;
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

        return (AggregateFunction) nestedBuilder.build(nestedName, type.getMockedExpressions()).first;
    }

    @Override
    public Pair<BoundFunction, AggregateFunction> build(String name, List<?> arguments) {
        String nestedName = getNestedName(name);
        if (combinatorSuffix.equalsIgnoreCase(STATE)) {
            AggregateFunction nestedFunction = buildState(nestedName, arguments);
            return Pair.of(new StateCombinator((List<Expression>) arguments, nestedFunction), nestedFunction);
        } else if (combinatorSuffix.equalsIgnoreCase(MERGE)) {
            AggregateFunction nestedFunction = buildMergeOrUnion(nestedName, arguments);
            return Pair.of(new MergeCombinator((List<Expression>) arguments, nestedFunction), nestedFunction);
        } else if (combinatorSuffix.equalsIgnoreCase(UNION)) {
            AggregateFunction nestedFunction = buildMergeOrUnion(nestedName, arguments);
            return Pair.of(new UnionCombinator((List<Expression>) arguments, nestedFunction), nestedFunction);
        } else if (combinatorSuffix.equalsIgnoreCase(FOREACH)) {
            AggregateFunction nestedFunction = buildForEach(nestedName, arguments);
            return Pair.of(new ForEachCombinator((List<Expression>) arguments, nestedFunction), nestedFunction);
        }
        return null;
    }

    public static boolean isAggStateCombinator(String name) {
        return name.toLowerCase().endsWith(STATE_SUFFIX) || name.toLowerCase().endsWith(MERGE_SUFFIX)
                || name.toLowerCase().endsWith(UNION_SUFFIX) || name.toLowerCase().endsWith(FOREACH_SUFFIX);
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
