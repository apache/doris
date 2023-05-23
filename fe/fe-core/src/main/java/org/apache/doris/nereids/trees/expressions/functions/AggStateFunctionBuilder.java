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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.combinator.MergeCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.types.AggStateType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This class used to resolve AggState's combinators
 */
public class AggStateFunctionBuilder extends FunctionBuilder {
    public static final String COMBINATOR_LINKER = "_";
    public static final String STATE = "state";
    public static final String MERGE = "merge";
    public static final String UNION = "union";

    private FunctionBuilder nestedBuilder;

    private String combinatorSuffix;

    public AggStateFunctionBuilder(String combinatorSuffix, FunctionBuilder nestedBuilder) {
        this.combinatorSuffix = combinatorSuffix;
        this.nestedBuilder = nestedBuilder;
    }

    private BoundFunction buildState(String nestedName, List<? extends Object> arguments) {
        return nestedBuilder.build(nestedName, arguments);
    }

    private BoundFunction buildMergeOrUnion(String nestedName, List<? extends Object> arguments) {
        if (arguments.size() != 1 || !(arguments.get(0) instanceof Expression)
                || !((Expression) arguments.get(0)).getDataType().isAggStateType()) {
            String argString = arguments.stream()
                    .map(arg -> {
                        if (arg == null) {
                            return "null";
                        } else if (arg instanceof Expression) {
                            return ((Expression) arg).toSql();
                        } else {
                            return arg.toString();
                        }
                    })
                    .collect(Collectors.joining(", ", "(", ")"));
            throw new IllegalStateException("Can not build AggState nested function: '" + nestedName
                    + "', expression: " + nestedName + argString);
        }

        Expression arg = (Expression) arguments.get(0);
        AggStateType type = (AggStateType) arg.getDataType();

        List<Expression> nestedArgumens = type.getSubTypes().stream().map(t -> {
            return new SlotReference("mocked", t);
        }).collect(Collectors.toList());

        return nestedBuilder.build(nestedName, nestedArgumens);
    }

    @Override
    public BoundFunction build(String name, List<? extends Object> arguments) {
        String nestedName = getNestedName(name);
        if (combinatorSuffix.equals(STATE)) {
            BoundFunction nestedFunction = buildState(nestedName, arguments);
            return new StateCombinator((List<Expression>) arguments, nestedFunction);
        } else {
            BoundFunction nestedFunction = buildMergeOrUnion(nestedName, arguments);
            return new MergeCombinator((List<Expression>) arguments, nestedFunction);
        }
    }

    @Override
    public String toString() {
        return combinatorSuffix + "(" + nestedBuilder.toString() + ")";
    }

    public static boolean isAggStateCombinator(String name) {
        return name.toLowerCase().endsWith(COMBINATOR_LINKER + STATE)
                || name.toLowerCase().endsWith(COMBINATOR_LINKER + MERGE)
                || name.toLowerCase().endsWith(COMBINATOR_LINKER + UNION);
    }

    public static String getNestedName(String name) {
        return name.substring(0, name.lastIndexOf(COMBINATOR_LINKER));
    }

    public static String getCombinatorSuffix(String name) {
        return name.substring(name.lastIndexOf(AggStateFunctionBuilder.COMBINATOR_LINKER) + 1);
    }
}
