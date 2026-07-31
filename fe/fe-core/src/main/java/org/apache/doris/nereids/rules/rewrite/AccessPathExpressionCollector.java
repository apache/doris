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

import org.apache.doris.analysis.ColumnAccessPathType;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.rewrite.AccessPathExpressionCollector.CollectorContext;
import org.apache.doris.nereids.rules.rewrite.NestedColumnPruning.DataTypeAccessTree;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference.ArrayItemSlot;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
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
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cardinality;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Length;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsEntry;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsKey;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsValue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapKeys;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapSize;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapValues;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NestedColumnPrunable;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;

/**
 * collect the access path, for example: `select element_at(s, 'data')` has access path: ['s', 'data']
 */
public class AccessPathExpressionCollector extends DefaultExpressionVisitor<Void, CollectorContext> {
    private StatementContext statementContext;
    private boolean bottomPredicate;
    private boolean skipMetaPath;
    private Multimap<Integer, CollectAccessPathResult> slotToAccessPaths;
    private Stack<Map<String, Expression>> nameToLambdaArguments = new Stack<>();

    public AccessPathExpressionCollector(
            StatementContext statementContext, Multimap<Integer, CollectAccessPathResult> slotToAccessPaths,
            boolean bottomPredicate, boolean skipMetaPath) {
        this.statementContext = statementContext;
        this.slotToAccessPaths = slotToAccessPaths;
        this.bottomPredicate = bottomPredicate;
        this.skipMetaPath = skipMetaPath;
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
        if (dataType instanceof VariantType
                && (slotReference.hasSubColPath() || !context.accessPathBuilder.isEmpty())) {
            List<String> path = new ArrayList<>();
            path.add(slotReference.getName());
            if (slotReference.hasSubColPath()) {
                path.addAll(slotReference.getSubPath());
            }
            // Strip NULL suffix for variant sub-column access — null-flag-only optimization
            // does not apply to variant sub-column data layout.
            List<String> builderPath = context.accessPathBuilder.getPathList();
            if (builderPath.size() > 1
                    && AccessPathInfo.ACCESS_NULL.equals(builderPath.get(builderPath.size() - 1))) {
                builderPath = new ArrayList<>(builderPath.subList(0, builderPath.size() - 1));
            }
            path.addAll(builderPath);
            int slotId = slotReference.getExprId().asInt();
            slotToAccessPaths.put(slotId, new CollectAccessPathResult(
                    path, context.bottomFilter, ColumnAccessPathType.DATA));
            return null;
        }
        if (dataType instanceof NestedColumnPrunable) {
            context.accessPathBuilder.addPrefix(slotReference.getName().toLowerCase());
            ImmutableList<String> path = Utils.fastToImmutableList(context.accessPathBuilder.accessPath);
            int slotId = slotReference.getExprId().asInt();
            slotToAccessPaths.put(slotId, new CollectAccessPathResult(path, context.bottomFilter, context.type));
        }
        if (dataType.isStringLikeType()) {
            int slotId = slotReference.getExprId().asInt();
            if (!context.accessPathBuilder.isEmpty()) {
                // Accessed via an offset-only function (e.g. length()) or null-check (IS NULL).
                // Builder already has "OFFSET"/"NULL" at the tail; add the column name as prefix.
                context.accessPathBuilder.addPrefix(slotReference.getName());
                ImmutableList<String> path = ImmutableList.copyOf(context.accessPathBuilder.accessPath);
                slotToAccessPaths.put(slotId,
                        new CollectAccessPathResult(path, context.bottomFilter, ColumnAccessPathType.DATA));
            } else {
                // Direct access to the string column → record a DATA path so that any
                // concurrent offset-only path for the same slot is suppressed.
                List<String> path = ImmutableList.of(slotReference.getName());
                slotToAccessPaths.put(slotId,
                        new CollectAccessPathResult(path, context.bottomFilter, ColumnAccessPathType.DATA));
            }
            return null;
        }
        // For any other nullable column type (e.g. INT, BIGINT) accessed via IS NULL / IS NOT NULL:
        // record the [col_name, NULL] path so NestedColumnPruning can emit null-only access paths.
        // Skip NestedColumnPrunable types (already handled above) and string types (handled above).
        if (!(dataType instanceof NestedColumnPrunable) && !dataType.isStringLikeType()
                && !context.accessPathBuilder.isEmpty() && slotReference.nullable()) {
            context.accessPathBuilder.addPrefix(slotReference.getName());
            ImmutableList<String> path = ImmutableList.copyOf(context.accessPathBuilder.accessPath);
            int slotId = slotReference.getExprId().asInt();
            slotToAccessPaths.put(slotId,
                    new CollectAccessPathResult(path, context.bottomFilter, ColumnAccessPathType.DATA));
        }
        // For any other nullable column type accessed directly (not via IS NULL / length / etc.):
        // record a [col_name] full-access path so that when the column is also used via IS NULL,
        // stripNullSuffixPaths correctly suppresses the null-only optimization.
        if (!(dataType instanceof NestedColumnPrunable) && !dataType.isStringLikeType()
                && !(dataType instanceof VariantType)
                && context.accessPathBuilder.isEmpty() && slotReference.nullable()) {
            int slotId = slotReference.getExprId().asInt();
            slotToAccessPaths.put(slotId,
                    new CollectAccessPathResult(
                            ImmutableList.of(slotReference.getName()),
                            context.bottomFilter, ColumnAccessPathType.DATA));
        }
        return null;
    }

    @Override
    public Void visitLength(Length length, CollectorContext context) {
        Expression arg = length.child(0);
        // length() only needs the offset array, not the chars data.
        // Add ACCESS_STRING_OFFSET as a suffix so the path builder accumulates
        // e.g. ["str_col", "OFFSET"] or ["c_struct", "f3", "OFFSET"].
        //
        // CHAR is excluded: CHAR(N) is stored padded to N bytes per row (see BE
        // OlapColumnDataConvertorChar::clone_and_padding), so the per-row length
        // information available without reading the chars buffer is the padded
        // length (always N), not the logical post-trim length expected by
        // length(). There is no way to recover the logical length from offsets
        // alone — the chars buffer must be scanned with strnlen() (BE
        // shrink_padding_chars). Falling through to the default visit causes
        // length() to read the column normally, which is correct.
        // NOTE: arg.getDataType() is the resolved type at the leaf of any
        // chained access (struct field, map subscript, array index), so this
        // single check covers nested CHAR cases too.
        if (arg.getDataType().isStringLikeType() && !arg.getDataType().isCharType()
                && context.accessPathBuilder.isEmpty()) {
            if (skipMetaPath) {
                return arg.accept(this,
                        new CollectorContext(context.statementContext, false));
            }
            CollectorContext offsetContext =
                    new CollectorContext(context.statementContext, context.bottomFilter);
            offsetContext.accessPathBuilder.addSuffix(AccessPathInfo.ACCESS_OFFSET);
            return arg.accept(this, offsetContext);
        }
        // fall through to default (recurse into children with fresh contexts)
        return visit(length, context);
    }

    @Override
    public Void visitMapSize(MapSize mapSize, CollectorContext context) {
        Expression arg = mapSize.child();
        DataType argType = arg.getDataType();
        if (argType.isMapType() && context.accessPathBuilder.isEmpty()) {
            if (skipMetaPath) {
                return arg.accept(this,
                        new CollectorContext(context.statementContext, false));
            }
            CollectorContext offsetContext =
                    new CollectorContext(context.statementContext, context.bottomFilter);
            offsetContext.accessPathBuilder.addSuffix(AccessPathInfo.ACCESS_OFFSET);
            return arg.accept(this, offsetContext);
        }
        return visit(mapSize, context);
    }

    @Override
    public Void visitCardinality(Cardinality cardinality, CollectorContext context) {
        Expression arg = cardinality.child(0);
        // cardinality(arr) / cardinality(map) only needs the offset array, not element data.
        // Arrays and maps share the same offset-array + data storage layout as strings on the BE.
        DataType argType = arg.getDataType();
        if ((argType.isArrayType() || argType.isMapType()) && context.accessPathBuilder.isEmpty()) {
            if (skipMetaPath) {
                return arg.accept(this,
                        new CollectorContext(context.statementContext, false));
            }
            CollectorContext offsetContext =
                    new CollectorContext(context.statementContext, context.bottomFilter);
            offsetContext.accessPathBuilder.addSuffix(AccessPathInfo.ACCESS_OFFSET);
            // cardinality(map_keys(m)) == cardinality(m) == cardinality(map_values(m)):
            // all three count map entries, so emit the same [map_col, OFFSET] path.
            Expression effectiveArg = (arg instanceof MapKeys || arg instanceof MapValues)
                    ? arg.child(0) : arg;
            return effectiveArg.accept(this, offsetContext);
        }
        // fall through to default
        return visit(cardinality, context);
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
                && cast.child().getDataType() instanceof NestedColumnPrunable
                && !mapTypeIsChanged(cast.child().getDataType(), cast.getDataType(), false)) {

            DataTypeAccessTree castTree = DataTypeAccessTree.of(
                    cast.getDataType(), ColumnAccessPathType.DATA);
            DataTypeAccessTree originTree = DataTypeAccessTree.of(
                    cast.child().getDataType(), ColumnAccessPathType.DATA);

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
        } else if (first.getDataType().isVariantType() && arguments.size() >= 2
                && arguments.get(1).isLiteral()) {
            Expression keyExpr = arguments.get(1);
            DataType keyType = keyExpr.getDataType();
            if (keyType.isIntegerLikeType()) {
                String key = String.valueOf(((Number) ((Literal) keyExpr).getValue()).intValue());
                context.accessPathBuilder.addPrefix(key);
                return continueCollectAccessPath(first, context);
            } else if (keyType.isStringLikeType()) {
                context.accessPathBuilder.addPrefix(((Literal) keyExpr).getStringValue());
                return continueCollectAccessPath(first, context);
            }
            return visit(elementAt, context);
        } else if (first.getDataType().isStructType()) {
            // struct field access (formerly struct_element): collect the selected field as the path
            Expression fieldName = arguments.get(1);
            DataType fieldType = fieldName.getDataType();
            if (fieldName.isLiteral() && (fieldType.isIntegerLikeType() || fieldType.isStringLikeType())) {
                if (fieldType.isIntegerLikeType()) {
                    int fieldIndex = ((Number) ((Literal) fieldName).getValue()).intValue();
                    List<StructField> fields = ((StructType) first.getDataType()).getFields();
                    if (fieldIndex >= 1 && fieldIndex <= fields.size()) {
                        String realFieldName = fields.get(fieldIndex - 1).getName();
                        context.accessPathBuilder.addPrefix(realFieldName);
                        return continueCollectAccessPath(first, context);
                    }
                }
                context.accessPathBuilder.addPrefix(((Literal) fieldName).getStringValue().toLowerCase());
                return continueCollectAccessPath(first, context);
            }
            return visit(elementAt, context);
        } else {
            return visit(elementAt, context);
        }
    }

    @Override
    public Void visitMapKeys(MapKeys mapKeys, CollectorContext context) {
        LinkedList<String> suffixPath = context.accessPathBuilder.accessPath;
        if (isUnderIsNull(suffixPath)) {
            // map_keys(nullable_map) returns a NULL array only when the parent map is NULL.
            // The NULL suffix therefore belongs to the map itself, not to the KEYS child.
            return continueCollectAccessPath(mapKeys.getArgument(0), context);
        }
        if (!suffixPath.isEmpty() && suffixPath.get(0).equals(AccessPathInfo.ACCESS_ALL)) {
            CollectorContext removeStarContext
                    = new CollectorContext(context.statementContext, context.bottomFilter);
            removeStarContext.accessPathBuilder.accessPath.addAll(suffixPath.subList(1, suffixPath.size()));
            removeStarContext.accessPathBuilder.addPrefix(AccessPathInfo.ACCESS_MAP_KEYS);
            return continueCollectAccessPath(mapKeys.getArgument(0), removeStarContext);
        }
        context.accessPathBuilder.addPrefix(AccessPathInfo.ACCESS_MAP_KEYS);
        return continueCollectAccessPath(mapKeys.getArgument(0), context);
    }

    @Override
    public Void visitMapValues(MapValues mapValues, CollectorContext context) {
        LinkedList<String> suffixPath = context.accessPathBuilder.accessPath;
        if (isUnderIsNull(suffixPath)) {
            // map_values(nullable_map) returns a NULL array only when the parent map is NULL.
            // A map entry whose value is NULL still produces a non-NULL values array.
            return continueCollectAccessPath(mapValues.getArgument(0), context);
        }
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

    private static boolean isUnderIsNull(List<String> suffixPath) {
        return suffixPath.size() == 1 && AccessPathInfo.ACCESS_NULL.equals(suffixPath.get(0));
    }

    @Override
    public Void visitMapContainsKey(MapContainsKey mapContainsKey, CollectorContext context) {
        // MAP_CONTAINS_KEY(<map>, <key>)
        //
        // isUnderIsNull checks whether the parent of this expression is IS NULL,
        // splitting queries into two shapes:
        //
        // Shape A (parent is IS NULL):
        //   SQL: SELECT ... WHERE map_contains_key(m, k) IS NULL
        //   map_contains_key(m, k) returns NULL only when m itself is NULL — so the path
        //   should be m.NULL, not m.KEYS.NULL
        //
        // Shape B (regular predicate):
        //   SQL: SELECT ... WHERE map_contains_key(m, element_at(s, 'city'))
        //                     AND element_at(s, 'city') IS NULL
        //   We add the KEYS prefix for the map column and visit key arg: `element_at(s, 'city')` with a
        //   fresh context.
        //   s collects two paths:
        //     [s, city]        ← from key arg (fresh context → DATA path)
        //     [s, city, NULL]  ← from IS NULL
        //   NestedColumnPruning sees [s, city] and strips [s, city, NULL] in prune phase.
        if (isUnderIsNull(context.accessPathBuilder.accessPath)) {
            // Shape A: skip KEYS prefix, route NULL directly to the map column.
            return continueCollectAccessPath(mapContainsKey.getArgument(0), context);
        }
        // Shape B: map argument — only the key sub-column is needed.
        context.accessPathBuilder.addPrefix(AccessPathInfo.ACCESS_MAP_KEYS);
        continueCollectAccessPath(mapContainsKey.getArgument(0), context);
        // Shape B: key argument — visit with a fresh context to register full-data paths.
        Expression keyArg = mapContainsKey.getArgument(1);
        CollectorContext keyCtx = new CollectorContext(context.statementContext, context.bottomFilter);
        continueCollectAccessPath(keyArg, keyCtx);
        return null;
    }

    @Override
    public Void visitMapContainsValue(MapContainsValue mapContainsValue, CollectorContext context) {
        // MAP_CONTAINS_VALUE(<map>, <value>)
        // Same two-shape logic as visitMapContainsKey; see that method for the full rationale.
        //
        // Shape A (parent is IS NULL): skip VALUES prefix, route NULL to m → m.NULL.
        // Shape B (regular predicate): add VALUES prefix for m, visit value arg with fresh context.
        if (isUnderIsNull(context.accessPathBuilder.accessPath)) {
            return continueCollectAccessPath(mapContainsValue.getArgument(0), context);
        }
        context.accessPathBuilder.addPrefix(AccessPathInfo.ACCESS_MAP_VALUES);
        continueCollectAccessPath(mapContainsValue.getArgument(0), context);
        Expression valueArg = mapContainsValue.getArgument(1);
        CollectorContext valueCtx = new CollectorContext(context.statementContext, context.bottomFilter);
        continueCollectAccessPath(valueArg, valueCtx);
        return null;
    }

    @Override
    public Void visitMapContainsEntry(MapContainsEntry mapContainsEntry, CollectorContext context) {
        // MAP_CONTAINS_ENTRY(<map>, <key>, <value>)
        // Same two-shape logic as visitMapContainsKey; see that method for the full rationale.
        //
        // Shape A (parent is IS NULL): skip sub-column prefix, route NULL to m → m.NULL.
        // Shape B (regular predicate): add ACCESS_ALL for m (needs both keys and values),
        //     visit key/value args with fresh context.
        if (isUnderIsNull(context.accessPathBuilder.accessPath)) {
            return continueCollectAccessPath(mapContainsEntry.getArgument(0), context);
        }
        context.accessPathBuilder.addPrefix(AccessPathInfo.ACCESS_ALL);
        continueCollectAccessPath(mapContainsEntry.getArgument(0), context);
        for (int i = 1; i < mapContainsEntry.arity(); i++) {
            Expression entryArg = mapContainsEntry.getArgument(i);
            CollectorContext entryCtx = new CollectorContext(context.statementContext, context.bottomFilter);
            continueCollectAccessPath(entryArg, entryCtx);
        }
        return null;
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

    @Override
    public Void visitIsNull(IsNull isNull, CollectorContext context) {
        Expression arg = isNull.child();
        // Skip variant sub-column paths (v['k'] IS NULL): the sub-column path is already baked
        // into the SlotReference, so null-only access doesn't apply the same way.
        if (arg instanceof SlotReference && ((SlotReference) arg).hasSubColPath()) {
            return visit(isNull, context);
        }
        // Optimize IS NULL on nullable expressions: create a context with NULL suffix to indicate
        // only the null flag is needed. Works for top-level columns (col IS NULL → [col, NULL])
        // and nested access (element_at(s, 'city') IS NULL → [s, city, NULL]).
        // For unrecognized expressions, the default visitor resets context, safely discarding NULL.
        if (arg.nullable() && context.accessPathBuilder.isEmpty()) {
            if (skipMetaPath) {
                return arg.accept(this,
                        new CollectorContext(context.statementContext, false));
            }
            CollectorContext nullContext =
                    new CollectorContext(context.statementContext, context.bottomFilter);
            nullContext.accessPathBuilder.addSuffix(AccessPathInfo.ACCESS_NULL);
            return continueCollectAccessPath(arg, nullContext);
        }
        return visit(isNull, context);
    }

    @Override
    public Void visitIf(If ifExpr, CollectorContext context) {
        if (isUnderIsNull(context.accessPathBuilder.accessPath)) {
            ifExpr.getCondition().accept(this, new CollectorContext(context.statementContext, context.bottomFilter));
            ifExpr.getTrueValue().accept(this, copyContext(context));
            ifExpr.getFalseValue().accept(this, copyContext(context));
            return null;
        }
        return visit(ifExpr, context);
    }

    private static CollectorContext copyContext(CollectorContext context) {
        CollectorContext copy = new CollectorContext(context.statementContext, context.bottomFilter);
        copy.accessPathBuilder.addSuffix(context.accessPathBuilder.getPathList());
        copy.type = context.type;
        return copy;
    }

    @Override
    public Void visitNot(Not not, CollectorContext context) {
        // NOT(IS NULL) == IS NOT NULL: same null-only access pattern
        if (not.child() instanceof IsNull) {
            return not.child().accept(this, context);
        }
        return visit(not, context);
    }

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

        // After visiting the lambda body, for any bound array whose lambda variable
        // was NOT referenced in the body (e.g. x -> true where x never appears),
        // visitArrayItemSlot was never called and the array column's access path is
        // missing. This gap is exposed when an is-null or offset-only path has been
        // registered for the same slot — NestedColumnPruning then incorrectly prunes
        // the complex column to null-only / offset-only instead of reading full data.
        //
        // Detect usage by scanning the lambda body for ArrayItemSlots matching the
        // argument name, which is more reliable than getInputSlots() that deliberately
        // excludes ArrayItemSlot and may falsely match outer slots.
        //
        // Must use a fresh context: when the body DOES reference some variables
        // (e.g. (x,y) -> x > 0), visitArrayItemSlot mutates context.accessPathBuilder
        // in-place (addPrefix without cleanup). A fresh context isolates the fallback
        // path for unreferenced variables from pollution by referenced ones.
        for (Expression argument : arguments) {
            if (argument instanceof ArrayItemReference) {
                ExprId argExprId = ((ArrayItemReference) argument).getExprId();
                Set<ArrayItemSlot> arrayItemSlots = arguments.get(0)
                        .<ArrayItemSlot>collect(e -> e instanceof ArrayItemSlot);
                boolean isReferenced = false;
                for (ArrayItemSlot slot : arrayItemSlots) {
                    if (slot.getExprId().equals(argExprId)) {
                        isReferenced = true;
                        break;
                    }
                }
                if (!isReferenced) {
                    Expression boundArray = argument.child(0);
                    CollectorContext fullAccessCtx = new CollectorContext(
                            context.statementContext, context.bottomFilter);
                    fullAccessCtx.accessPathBuilder.addPrefix(AccessPathInfo.ACCESS_ALL);
                    continueCollectAccessPath(boundArray, fullAccessCtx);
                }
            }
        }

        return null;
    }

    /** CollectorContext */
    public static class CollectorContext {
        private StatementContext statementContext;
        private AccessPathBuilder accessPathBuilder;
        private boolean bottomFilter;
        private ColumnAccessPathType type;

        public CollectorContext(StatementContext statementContext, boolean bottomFilter) {
            this.statementContext = statementContext;
            this.accessPathBuilder = new AccessPathBuilder();
            this.bottomFilter = bottomFilter;
            this.type = ColumnAccessPathType.DATA;
        }

        public ColumnAccessPathType getType() {
            return type;
        }

        public void setType(ColumnAccessPathType type) {
            this.type = type;
        }

        public AccessPathBuilder getAccessPathBuilder() {
            return accessPathBuilder;
        }
    }

    /** AccessPathBuilder */
    public static class AccessPathBuilder {
        private LinkedList<String> accessPath;

        public AccessPathBuilder() {
            accessPath = new LinkedList<>();
        }

        public AccessPathBuilder addPrefix(String prefix) {
            accessPath.addFirst(prefix);
            return this;
        }

        public AccessPathBuilder addSuffix(String suffix) {
            accessPath.addLast(suffix);
            return this;
        }

        public AccessPathBuilder addSuffix(List<String> suffix) {
            accessPath.addAll(suffix);
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
        private final ColumnAccessPathType type;

        public CollectAccessPathResult(List<String> path, boolean isPredicate, ColumnAccessPathType type) {
            this.path = path;
            this.isPredicate = isPredicate;
            this.type = type;
        }

        public ColumnAccessPathType getType() {
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

    // if the map type is changed, we can not prune the type, because the map type need distinct the keys,
    // e.g. select map_values(cast(map(3.0, 1, 3.1, 2) as map<int, int>));
    // the result is [2] because the keys: 3.0 and 3.1 will cast to 3 and the second entry remained.
    // backend will throw exception because it can not only access the values without the cast keys,
    // so we should check whether the map type is changed, if not changed, we can prune the type.
    private static boolean mapTypeIsChanged(DataType originType, DataType castType, boolean inMap) {
        if (originType.isMapType()) {
            MapType originMapType = (MapType) originType;
            MapType castMapType = (MapType) castType;
            if (mapTypeIsChanged(originMapType.getKeyType(), castMapType.getKeyType(), true)
                    || mapTypeIsChanged(originMapType.getValueType(), castMapType.getValueType(), true)) {
                return true;
            }
            return false;
        } else if (originType.isStructType()) {
            StructType originStructType = (StructType) originType;
            StructType castStructType = (StructType) castType;
            List<Entry<String, StructField>> originFields
                    = new ArrayList<>(originStructType.getNameToFields().entrySet());
            List<Entry<String, StructField>> castFields
                    = new ArrayList<>(castStructType.getNameToFields().entrySet());

            for (int i = 0; i < originFields.size(); i++) {
                DataType originFieldType = originFields.get(i).getValue().getDataType();
                DataType castFieldType = castFields.get(i).getValue().getDataType();
                if (mapTypeIsChanged(originFieldType, castFieldType, inMap)) {
                    return true;
                }
            }
            return false;
        } else if (originType.isArrayType()) {
            ArrayType originArrayType = (ArrayType) originType;
            ArrayType castArrayType = (ArrayType) castType;
            return mapTypeIsChanged(originArrayType.getItemType(), castArrayType.getItemType(), inMap);
        } else if (inMap) {
            return !originType.equals(castType);
        } else {
            // other type changed which not in map will not affect the map
            return false;
        }
    }
}
