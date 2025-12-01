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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.AccessPathInfo;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.rewrite.AccessPathExpressionCollector.CollectorContext;
import org.apache.doris.nereids.rules.rewrite.NestedColumnPruning.DataTypeAccessTree;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference.ArrayItemSlot;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayExists;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayFilter;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayFirst;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayFirstIndex;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayLast;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayLastIndex;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMatchAll;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMatchAny;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayReverseSplit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraySort;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraySortBy;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraySplit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsEntry;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsKey;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsValue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapKeys;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapValues;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StructElement;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NestedColumnPrunable;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.thrift.TAccessPathType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;

/**
 * collect the access path, for example: `select struct_element(s, 'data')` has access path: ['s', 'data']
 */
public class AccessPathExpressionCollector extends DefaultExpressionVisitor<Void, CollectorContext> {
    private StatementContext statementContext;
    private boolean bottomPredicate;
    private Multimap<Integer, CollectAccessPathResult> slotToAccessPaths;
    private Stack<Map<String, Expression>> nameToLambdaArguments = new Stack<>();

    public AccessPathExpressionCollector(
            StatementContext statementContext, Multimap<Integer, CollectAccessPathResult> slotToAccessPaths,
            boolean bottomPredicate) {
        this.statementContext = statementContext;
        this.slotToAccessPaths = slotToAccessPaths;
        this.bottomPredicate = bottomPredicate;
    }

    public void collect(Expression expression) {
        expression.accept(this, new CollectorContext(statementContext, bottomPredicate));
    }

    private Void continueCollectAccessPath(Expression expr, CollectorContext context) {
        return expr.accept(this, context);
    }

    @Override
    public Void visit(Expression expr, CollectorContext context) {
        for (Expression child : expr.children()) {
            child.accept(this, new CollectorContext(context.statementContext, context.bottomFilter));
        }
        return null;
    }

    @Override
    public Void visitSlotReference(SlotReference slotReference, CollectorContext context) {
        DataType dataType = slotReference.getDataType();
        if (dataType instanceof NestedColumnPrunable) {
            context.accessPathBuilder.addPrefix(slotReference.getName().toLowerCase());
            ImmutableList<String> path = Utils.fastToImmutableList(context.accessPathBuilder.accessPath);
            int slotId = slotReference.getExprId().asInt();
            slotToAccessPaths.put(slotId, new CollectAccessPathResult(path, context.bottomFilter, context.type));
        }
        return null;
    }

    @Override
    public Void visitArrayItemSlot(ArrayItemSlot arrayItemSlot, CollectorContext context) {
        if (nameToLambdaArguments.isEmpty()) {
            return null;
        }
        context.accessPathBuilder.addPrefix(AccessPathInfo.ACCESS_ALL);
        Expression argument = nameToLambdaArguments.peek().get(arrayItemSlot.getName());
        if (argument == null) {
            return null;
        }
        return continueCollectAccessPath(argument, context);
    }

    @Override
    public Void visitAlias(Alias alias, CollectorContext context) {
        return alias.child(0).accept(this, context);
    }

    @Override
    public Void visitCast(Cast cast, CollectorContext context) {
        if (!context.accessPathBuilder.isEmpty()
                && cast.getDataType() instanceof NestedColumnPrunable
                && cast.child().getDataType() instanceof NestedColumnPrunable) {

            DataTypeAccessTree castTree = DataTypeAccessTree.of(cast.getDataType(), TAccessPathType.DATA);
            DataTypeAccessTree originTree = DataTypeAccessTree.of(cast.child().getDataType(), TAccessPathType.DATA);

            List<String> replacePath = new ArrayList<>(context.accessPathBuilder.getPathList());
            if (originTree.replacePathByAnotherTree(castTree, replacePath, 0)) {
                CollectorContext castContext = new CollectorContext(context.statementContext, context.bottomFilter);
                castContext.accessPathBuilder.accessPath.addAll(replacePath);
                return continueCollectAccessPath(cast.child(), castContext);
            }
        }
        return cast.child(0).accept(this,
                new CollectorContext(context.statementContext, context.bottomFilter)
        );
    }

    // array element at
    @Override
    public Void visitElementAt(ElementAt elementAt, CollectorContext context) {
        List<Expression> arguments = elementAt.getArguments();
        Expression first = arguments.get(0);
        if (first.getDataType().isArrayType() || first.getDataType().isMapType()) {
            context.accessPathBuilder.addPrefix(AccessPathInfo.ACCESS_ALL);
            continueCollectAccessPath(first, context);

            for (int i = 1; i < arguments.size(); i++) {
                visit(arguments.get(i), context);
            }
            return null;
        } else {
            return visit(elementAt, context);
        }
    }

    // struct element_at
    @Override
    public Void visitStructElement(StructElement structElement, CollectorContext context) {
        List<Expression> arguments = structElement.getArguments();
        Expression struct = arguments.get(0);
        Expression fieldName = arguments.get(1);
        DataType fieldType = fieldName.getDataType();

        if (fieldName.isLiteral() && (fieldType.isIntegerLikeType() || fieldType.isStringLikeType())) {
            if (fieldType.isIntegerLikeType()) {
                int fieldIndex = ((Number) ((Literal) fieldName).getValue()).intValue();
                List<StructField> fields = ((StructType) struct.getDataType()).getFields();
                if (fieldIndex >= 1 && fieldIndex <= fields.size()) {
                    String realFieldName = fields.get(fieldIndex - 1).getName();
                    context.accessPathBuilder.addPrefix(realFieldName);
                    return continueCollectAccessPath(struct, context);
                }
            }
            context.accessPathBuilder.addPrefix(((Literal) fieldName).getStringValue().toLowerCase());
            return continueCollectAccessPath(struct, context);
        }

        for (Expression argument : arguments) {
            visit(argument, context);
        }
        return null;
    }

    @Override
    public Void visitMapKeys(MapKeys mapKeys, CollectorContext context) {
        context = new CollectorContext(context.statementContext, context.bottomFilter);
        context.accessPathBuilder.addPrefix(AccessPathInfo.ACCESS_MAP_KEYS);
        return continueCollectAccessPath(mapKeys.getArgument(0), context);
    }

    @Override
    public Void visitMapValues(MapValues mapValues, CollectorContext context) {
        LinkedList<String> suffixPath = context.accessPathBuilder.accessPath;
        if (!suffixPath.isEmpty() && suffixPath.get(0).equals(AccessPathInfo.ACCESS_ALL)) {
            CollectorContext removeStarContext
                    = new CollectorContext(context.statementContext, context.bottomFilter);
            removeStarContext.accessPathBuilder.accessPath.addAll(suffixPath.subList(1, suffixPath.size()));
            removeStarContext.accessPathBuilder.addPrefix(AccessPathInfo.ACCESS_MAP_VALUES);
            return continueCollectAccessPath(mapValues.getArgument(0), removeStarContext);
        }
        context.accessPathBuilder.addPrefix(AccessPathInfo.ACCESS_MAP_VALUES);
        return continueCollectAccessPath(mapValues.getArgument(0), context);
    }

    @Override
    public Void visitMapContainsKey(MapContainsKey mapContainsKey, CollectorContext context) {
        context.accessPathBuilder.addPrefix(AccessPathInfo.ACCESS_MAP_KEYS);
        return continueCollectAccessPath(mapContainsKey.getArgument(0), context);
    }

    @Override
    public Void visitMapContainsValue(MapContainsValue mapContainsValue, CollectorContext context) {
        context.accessPathBuilder.addPrefix(AccessPathInfo.ACCESS_MAP_VALUES);
        return continueCollectAccessPath(mapContainsValue.getArgument(0), context);
    }

    @Override
    public Void visitMapContainsEntry(MapContainsEntry mapContainsEntry, CollectorContext context) {
        context.accessPathBuilder.addPrefix(AccessPathInfo.ACCESS_ALL);
        return continueCollectAccessPath(mapContainsEntry.getArgument(0), context);
    }

    @Override
    public Void visitArrayMap(ArrayMap arrayMap, CollectorContext context) {
        // ARRAY_MAP(lambda, <arr> [ , <arr> ... ] )

        Expression argument = arrayMap.getArgument(0);
        if ((argument instanceof Lambda)) {
            return collectArrayPathInLambda((Lambda) argument, context);
        }
        return visit(arrayMap, context);
    }

    @Override
    public Void visitArraySort(ArraySort arraySort, CollectorContext context) {
        // ARRAY_SORT(lambda, <arr>)

        Expression argument = arraySort.getArgument(0);
        if ((argument instanceof Lambda)) {
            return collectArrayPathInLambda((Lambda) argument, context);
        }
        return visit(arraySort, context);
    }

    @Override
    public Void visitArrayCount(ArrayCount arrayCount, CollectorContext context) {
        // ARRAY_COUNT(<lambda>, <arr>[, ... ])

        Expression argument = arrayCount.getArgument(0);
        if ((argument instanceof Lambda)) {
            return collectArrayPathInLambda((Lambda) argument, context);
        }
        return visit(arrayCount, context);
    }

    @Override
    public Void visitArrayExists(ArrayExists arrayExists, CollectorContext context) {
        // ARRAY_EXISTS([ <lambda>, ] <arr1> [, <arr2> , ...] )

        Expression argument = arrayExists.getArgument(0);
        if ((argument instanceof Lambda)) {
            return collectArrayPathInLambda((Lambda) argument, context);
        }
        return visit(arrayExists, context);
    }

    @Override
    public Void visitArrayFilter(ArrayFilter arrayFilter, CollectorContext context) {
        // ARRAY_FILTER(<lambda>, <arr>)

        Expression argument = arrayFilter.getArgument(0);
        if ((argument instanceof Lambda)) {
            collectArrayPathInLambda((Lambda) argument, context);
        }
        return visit(arrayFilter, context);
    }

    @Override
    public Void visitArrayFirst(ArrayFirst arrayFirst, CollectorContext context) {
        // ARRAY_FIRST(<lambda>, <arr>)

        Expression argument = arrayFirst.getArgument(0);
        if ((argument instanceof Lambda)) {
            collectArrayPathInLambda((Lambda) argument, context);
        }
        return visit(arrayFirst, context);
    }

    @Override
    public Void visitArrayFirstIndex(ArrayFirstIndex arrayFirstIndex, CollectorContext context) {
        // ARRAY_FIRST_INDEX(<lambda>, <arr> [, ...])

        Expression argument = arrayFirstIndex.getArgument(0);
        if ((argument instanceof Lambda)) {
            collectArrayPathInLambda((Lambda) argument, context);
        }
        return visit(arrayFirstIndex, context);
    }

    @Override
    public Void visitArrayLast(ArrayLast arrayLast, CollectorContext context) {
        // ARRAY_LAST(<lambda>, <arr>)

        Expression argument = arrayLast.getArgument(0);
        if ((argument instanceof Lambda)) {
            collectArrayPathInLambda((Lambda) argument, context);
        }
        return visit(arrayLast, context);
    }

    @Override
    public Void visitArrayLastIndex(ArrayLastIndex arrayLastIndex, CollectorContext context) {
        // ARRAY_LAST_INDEX(<lambda>, <arr> [, ...])

        Expression argument = arrayLastIndex.getArgument(0);
        if ((argument instanceof Lambda)) {
            collectArrayPathInLambda((Lambda) argument, context);
        }
        return visit(arrayLastIndex, context);
    }

    @Override
    public Void visitArrayMatchAny(ArrayMatchAny arrayMatchAny, CollectorContext context) {
        // array_match_any(lambda, <arr> [, <arr> ...])

        Expression argument = arrayMatchAny.getArgument(0);
        if ((argument instanceof Lambda)) {
            collectArrayPathInLambda((Lambda) argument, context);
        }
        return visit(arrayMatchAny, context);
    }

    @Override
    public Void visitArrayMatchAll(ArrayMatchAll arrayMatchAll, CollectorContext context) {
        // array_match_all(lambda, <arr> [, <arr> ...])

        Expression argument = arrayMatchAll.getArgument(0);
        if ((argument instanceof Lambda)) {
            collectArrayPathInLambda((Lambda) argument, context);
        }
        return visit(arrayMatchAll, context);
    }

    @Override
    public Void visitArrayReverseSplit(ArrayReverseSplit arrayReverseSplit, CollectorContext context) {
        // ARRAY_REVERSE_SPLIT(<lambda>, <arr> [, ...])

        Expression argument = arrayReverseSplit.getArgument(0);
        if ((argument instanceof Lambda)) {
            collectArrayPathInLambda((Lambda) argument, context);
        }
        return visit(arrayReverseSplit, context);
    }

    @Override
    public Void visitArraySplit(ArraySplit arraySplit, CollectorContext context) {
        // ARRAY_SPLIT(<lambda>, arr [, ...])

        Expression argument = arraySplit.getArgument(0);
        if ((argument instanceof Lambda)) {
            collectArrayPathInLambda((Lambda) argument, context);
        }
        return visit(arraySplit, context);
    }

    @Override
    public Void visitArraySortBy(ArraySortBy arraySortBy, CollectorContext context) {
        // ARRAY_SORTBY(<lambda>, <arr> [, ...])

        Expression argument = arraySortBy.getArgument(0);
        if ((argument instanceof Lambda)) {
            collectArrayPathInLambda((Lambda) argument, context);
        }
        return visit(arraySortBy, context);
    }

    // @Override
    // public Void visitIsNull(IsNull isNull, CollectorContext context) {
    //     if (context.accessPathBuilder.isEmpty()) {
    //         context.setType(TAccessPathType.META);
    //         return continueCollectAccessPath(isNull.child(), context);
    //     }
    //     return visit(isNull, context);
    // }

    private Void collectArrayPathInLambda(Lambda lambda, CollectorContext context) {
        List<Expression> arguments = lambda.getArguments();
        Map<String, Expression> nameToArray = Maps.newLinkedHashMap();
        for (Expression argument : arguments) {
            if (argument instanceof ArrayItemReference) {
                nameToArray.put(((ArrayItemReference) argument).getName(), argument.child(0));
            }
        }

        List<String> path = context.accessPathBuilder.getPathList();
        if (!path.isEmpty() && path.get(0).equals(AccessPathInfo.ACCESS_ALL)) {
            context.accessPathBuilder.removePrefix();
        }

        nameToLambdaArguments.push(nameToArray);
        try {
            continueCollectAccessPath(arguments.get(0), context);
        } finally {
            nameToLambdaArguments.pop();
        }
        return null;
    }

    /** CollectorContext */
    public static class CollectorContext {
        private StatementContext statementContext;
        private AccessPathBuilder accessPathBuilder;
        private boolean bottomFilter;
        private TAccessPathType type;

        public CollectorContext(StatementContext statementContext, boolean bottomFilter) {
            this.statementContext = statementContext;
            this.accessPathBuilder = new AccessPathBuilder();
            this.bottomFilter = bottomFilter;
            this.type = TAccessPathType.DATA;
        }

        public TAccessPathType getType() {
            return type;
        }

        public void setType(TAccessPathType type) {
            this.type = type;
        }
    }

    private static class AccessPathBuilder {
        private LinkedList<String> accessPath;

        public AccessPathBuilder() {
            accessPath = new LinkedList<>();
        }

        public AccessPathBuilder addPrefix(String prefix) {
            accessPath.addFirst(prefix);
            return this;
        }

        public AccessPathBuilder removePrefix() {
            accessPath.removeFirst();
            return this;
        }

        public List<String> getPathList() {
            return accessPath;
        }

        public boolean isEmpty() {
            return accessPath.isEmpty();
        }

        @Override
        public String toString() {
            return String.join(".", accessPath);
        }
    }

    /** AccessPathIsPredicate */
    public static class CollectAccessPathResult {
        private final List<String> path;
        private final boolean isPredicate;
        private final TAccessPathType type;

        public CollectAccessPathResult(List<String> path, boolean isPredicate, TAccessPathType type) {
            this.path = path;
            this.isPredicate = isPredicate;
            this.type = type;
        }

        public TAccessPathType getType() {
            return type;
        }

        public List<String> getPath() {
            return path;
        }

        public boolean isPredicate() {
            return isPredicate;
        }

        @Override
        public String toString() {
            return String.join(".", path) + ", " + isPredicate;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CollectAccessPathResult that = (CollectAccessPathResult) o;
            return isPredicate == that.isPredicate && Objects.equals(path, that.path);
        }

        @Override
        public int hashCode() {
            return path.hashCode();
        }
    }
}
