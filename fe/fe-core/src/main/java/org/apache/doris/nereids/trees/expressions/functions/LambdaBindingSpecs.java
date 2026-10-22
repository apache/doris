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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference.ArrayItemSlot;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapEntries;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.MapType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/** Standard lambda parameter binding contracts used by builtin higher-order functions. */
public final class LambdaBindingSpecs {

    public static final LambdaBindingSpec ARRAY_ZIP = LambdaBindingSpecs::bindArrayZip;
    public static final LambdaBindingSpec ARRAY_COMPARATOR = LambdaBindingSpecs::bindArrayComparator;
    public static final LambdaBindingSpec ARRAY_SORTED_MERGE = LambdaBindingSpecs::bindArraySortedMerge;
    public static final LambdaBindingSpec MAP_ENTRIES = LambdaBindingSpecs::bindMapEntries;

    private LambdaBindingSpecs() {
    }

    private static LambdaBinding bindArrayZip(
            String functionName, Lambda lambda, List<Expression> inputs) {
        if (inputs.size() != lambda.getLambdaArgumentNames().size()) {
            throw new AnalysisException(String.format(
                    "lambda %s arguments' size is not equal parameters' size", lambda.toSql()));
        }
        ImmutableList.Builder<ArrayItemReference> arguments = ImmutableList.builderWithExpectedSize(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            Expression input = inputs.get(i);
            requireArrayInput(input);
            arguments.add(new ArrayItemReference(lambda.getLambdaArgumentName(i), input));
        }
        return new ArrayLambdaBinding(lambda, arguments.build());
    }

    private static LambdaBinding bindArrayComparator(
            String functionName, Lambda lambda, List<Expression> inputs) {
        if (inputs.size() != 1 || lambda.getLambdaArgumentNames().size() != 2) {
            throw new AnalysisException("the lambda must be a binary comparator lambda");
        }
        Expression input = inputs.get(0);
        requireArrayInput(input);
        return new ArrayLambdaBinding(lambda, ImmutableList.of(
                new ArrayItemReference(lambda.getLambdaArgumentName(0), input),
                new ArrayItemReference(lambda.getLambdaArgumentName(1), input)));
    }

    /**
     * Binds the merge lambda's two parameters to the two array inputs; the trailing ascending
     * flag is not a lambda argument, so it is kept as an extra function child.
     */
    private static LambdaBinding bindArraySortedMerge(
            String functionName, Lambda lambda, List<Expression> inputs) {
        if (inputs.size() != 3 || lambda.getLambdaArgumentNames().size() != 2) {
            throw new AnalysisException(String.format(
                    "%s requires two arrays and an ascending flag: (merge_fn, arr1, arr2, is_asc)",
                    functionName));
        }
        requireArrayInput(inputs.get(0));
        requireArrayInput(inputs.get(1));
        if (!inputs.get(2).getDataType().isBooleanType()) {
            throw new AnalysisException(String.format(
                    "the ascending flag of %s must be BOOLEAN but is %s",
                    functionName, inputs.get(2).getDataType().toSql()));
        }
        return new ArrayLambdaBinding(
                lambda,
                ImmutableList.of(
                        new ArrayItemReference(lambda.getLambdaArgumentName(0), inputs.get(0)),
                        new ArrayItemReference(lambda.getLambdaArgumentName(1), inputs.get(1))),
                ImmutableList.of(inputs.get(2)));
    }

    private static LambdaBinding bindMapEntries(
            String functionName, Lambda lambda, List<Expression> inputs) {
        if (inputs.size() != 1) {
            throw new AnalysisException(String.format(
                    "%s requires exactly one map argument but has %d", functionName, inputs.size()));
        }
        if (lambda.getLambdaArgumentNames().size() != 2) {
            throw new AnalysisException(String.format(
                    "lambda of %s requires exactly two arguments but has %d",
                    functionName, lambda.getLambdaArgumentNames().size()));
        }
        Expression map = inputs.get(0);
        if (!(map.getDataType() instanceof MapType)) {
            throw new AnalysisException(String.format(
                    "the non-lambda argument of %s must be map but is %s",
                    functionName, map.getDataType().toSql()));
        }
        MapType mapType = (MapType) map.getDataType();
        ArrayItemSlot keySlot = new ArrayItemSlot(
                StatementScopeIdGenerator.newExprId(), lambda.getLambdaArgumentName(0),
                mapType.getKeyType(), true);
        ArrayItemSlot valueSlot = new ArrayItemSlot(
                StatementScopeIdGenerator.newExprId(), lambda.getLambdaArgumentName(1),
                mapType.getValueType(), true);
        return new MapLambdaBinding(lambda, map, keySlot, valueSlot);
    }

    private static void requireArrayInput(Expression input) {
        if (!(input.getDataType() instanceof ArrayType)) {
            throw new AnalysisException(String.format("lambda argument must be array but is %s", input));
        }
    }

    private static final class ArrayLambdaBinding implements LambdaBinding {
        private final Lambda lambda;
        private final List<ArrayItemReference> arguments;
        private final List<Expression> extraInputs;
        private final List<Slot> analysisSlots;

        private ArrayLambdaBinding(Lambda lambda, List<ArrayItemReference> arguments) {
            this(lambda, arguments, ImmutableList.of());
        }

        private ArrayLambdaBinding(Lambda lambda, List<ArrayItemReference> arguments,
                List<Expression> extraInputs) {
            this.lambda = lambda;
            this.arguments = arguments;
            this.extraInputs = extraInputs;
            this.analysisSlots = arguments.stream()
                    .map(ArrayItemReference::toSlot)
                    .collect(ImmutableList.toImmutableList());
        }

        @Override
        public List<Slot> getAnalysisSlots() {
            return analysisSlots;
        }

        @Override
        public List<Expression> getExtraInputs() {
            return extraInputs;
        }

        @Override
        public Lambda close(Expression analyzedBody) {
            return lambda.withLambdaFunctionArguments(analyzedBody, arguments);
        }
    }

    private static final class MapLambdaBinding implements LambdaBinding {
        private final Lambda lambda;
        private final Expression map;
        private final ArrayItemSlot keySlot;
        private final ArrayItemSlot valueSlot;

        private MapLambdaBinding(
                Lambda lambda, Expression map, ArrayItemSlot keySlot, ArrayItemSlot valueSlot) {
            this.lambda = lambda;
            this.map = map;
            this.keySlot = keySlot;
            this.valueSlot = valueSlot;
        }

        @Override
        public List<Slot> getAnalysisSlots() {
            return ImmutableList.of(keySlot, valueSlot);
        }

        @Override
        public Lambda close(Expression analyzedBody) {
            Set<String> occupiedNames = Sets.newHashSet(lambda.getLambdaArgumentNames());
            for (Slot slot : analyzedBody.<Slot>collect(expression -> expression instanceof Slot)) {
                occupiedNames.add(slot.getName());
            }
            for (Lambda nestedLambda : analyzedBody.<Lambda>collect(expression -> expression instanceof Lambda)) {
                occupiedNames.addAll(nestedLambda.getLambdaArgumentNames());
            }

            ExprId entryExprId;
            String entryName;
            do {
                entryExprId = StatementScopeIdGenerator.newExprId();
                entryName = "$_map_entry_" + entryExprId.asInt() + "_$";
            } while (occupiedNames.contains(entryName));

            ArrayItemReference entryArgument = new ArrayItemReference(
                    entryExprId, entryName, new MapEntries(map));
            Slot entrySlot = entryArgument.toSlot();
            Expression key = new ElementAt(entrySlot, new IntegerLiteral(1));
            Expression value = new ElementAt(entrySlot, new IntegerLiteral(2));
            Expression loweredBody = analyzedBody.rewriteDownShortCircuit(expression -> {
                if (expression instanceof ArrayItemSlot) {
                    ExprId exprId = ((ArrayItemSlot) expression).getExprId();
                    if (exprId.equals(keySlot.getExprId())) {
                        return key;
                    }
                    if (exprId.equals(valueSlot.getExprId())) {
                        return value;
                    }
                }
                return expression;
            });
            return new Lambda(
                    ImmutableList.of(entryName), loweredBody, ImmutableList.of(entryArgument));
        }
    }
}
