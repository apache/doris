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

package org.apache.doris.nereids.spm.placeholder;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.spm.SPMPlanTreeSupport;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToArgumentType;

import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Parameterizes supported literal positions with scalar or IN-list markers. One builder retains marker records and
 * allocates ids across bind and plan trees. Reuse requires the same value, parent signature, child position, query
 * block, projection item, expression-root ordinal and occurrence path. GROUP BY mirrors can deliberately reuse a
 * transformed expression to preserve analyzer equality.
 *
 * Syntax-bearing literals such as LIKE escape arguments are kept concrete. Some NULL payloads are retyped for
 * capture analysis so their markers survive Boolean consumers; replay still substitutes the matched query
 * expression.
 */
public class SPMPlaceholderBuilder extends ExpressionVisitor<Expression, Expression>
        implements SPMPlanTreeSupport.ExprTransform {

    /**
     * The ARRAY scalar functions that declare retArgType(0): each one's result IS its
     * first argument's array type, so an all-NULL input makes the whole result an all-NULL array.
     *
     * They are exactly the wrappers a projected all-NULL container can sit UNDER while the
     * enclosing call is still consumed as a Boolean operand, e.g.
     * k > 0 OR element_at(array_reverse_sort(array(NULL)), 1): the wrapper hands
     * array(NULL)'s NullType element type to element_at, so the marker has to be
     * repaired at the WRAPPER's argument position. Matching only array_sort /
     * array_distinct left every other member of the family unrecognized.
     *
     * Matched by NAME because the capture runs on the PARSE tree, where the call is still an
     * unbound Function and no signature is resolved yet; a BOUND node is classified from
     * its retArgType(0) signature instead (see followsFirstArgument), so a function added
     * to the family is recognized without touching this list. array_concat is NOT here:
     * it derives its element type from EVERY part and is handled separately
     * (isArrayConcatCall).
     */
    private static final Set<String> TYPE_PRESERVING_ARRAY_CALLS = Set.of(
            "array_apply", "array_compact", "array_difference", "array_distinct",
            "array_except", "array_except_all", "array_filter", "array_first",
            "array_intersect", "array_last", "array_popback", "array_popfront",
            "array_pushback", "array_pushfront", "array_remove", "array_reverse_sort",
            "array_shuffle", "array_slice", "array_sort", "array_sortby", "array_union",
            "reverse", "trim_array");

    /**
     * MAP functions that preserve the key and value types of their first argument.
     * DeduplicateMap follows both type groups, while MapFilter declares retArgType(0).
     */
    private static final Set<String> TYPE_PRESERVING_MAP_CALLS = Set.of("deduplicate_map", "map_filter");
    private static final int NO_MAP_KEY_DEPENDENCY = -1;

    /** Auto-increment placeholder id counter (starts at 1). */
    private long nextId = 1;

    private final Map<PlaceholderKey, Expression> placeholderIndex = new HashMap<>();
    private final Map<Expression, String> parentSignatures = new IdentityHashMap<>();

    /**
     * Optional GROUP BY mirror lookup, installed by SPMPlanTreeSupport.TreeTransformer for
     * one clause window (see setGroupByMirrorLookup): maps an expression's
     * pre-parameterization text to the ALREADY-transformed GROUP BY expression that must
     * be reused - also for NESTED subexpressions (e.g. the substr(...) inside
     * "HAVING substr(...) = 'x'"), because the analyzer resolves such a clause
     * expression-wise and demands literal expression equality with the GROUP BY under
     * ONLY_FULL_GROUP_BY. Null outside the window.
     */
    private java.util.function.Function<String, Expression> groupByMirrorLookup;

    /**
     * DFS stack of child positions: the top is the index of the expression currently
     * being visited inside its direct parent (empty = parent-less root). Two literals
     * under the same parent (e.g. the operands of "1 + 1") are different positions and
     * must never reuse one placeholder id.
     */
    private final Deque<Integer> childIndexStack = new ArrayDeque<>();

    /**
     * Query-block identity: incremented when entering a nested query block (the plan of
     * a subquery expression). Two literals in DIFFERENT blocks never share a placeholder
     * id even when value / parent / position coincide: in
     * SELECT * FROM t WHERE a=1 AND EXISTS (SELECT 1 FROM u WHERE a=1) the two a=1
     * literals used to reuse one id, and a user query with outer a=2 / inner a=3 was
     * rejected (one id must resolve to ONE value), so the baseline could never hit.
     * The counter restarts per tree (see startNewTree): bind and plan trees traverse the
     * same structure in the same order, so corresponding blocks keep corresponding
     * numbers and the cross-tree id alignment is preserved.
     */
    private long nextBlockId = 1;
    private long currentBlockId = 0;

    /**
     * Enclosing query-block ids (see enterQueryBlock): a nested block's id is
     * restored on exit, so SIBLING derived tables / CTE bodies get distinct ids while the
     * enclosing block numbering resumes - the same discipline the subquery-expression
     * blocks already followed.
     */
    private final Deque<Long> blockIdStack = new ArrayDeque<>();

    /**
     * Projection ITEM positions of the SELECT-list slots currently being visited (see
     * enterProjectionItem(int)): SELECT 1 AS x, 1 AS y must give its two
     * literals different placeholder ids although value, parent structure and child
     * position coincide (see PlaceholderKey).
     */
    private final Deque<Integer> projectItemStack = new ArrayDeque<>();

    /**
     * Ordinal of the expression ROOT currently being transformed, counted per query block in
     * walk order (see beginExpressionRoot): two equal literals in DIFFERENT roots of one
     * block (the ON clauses of two joins, two conjuncts of one filter,...) used to share one
     * placeholder id although they are independent slots - a query that varies them
     * separately (1/1 -> 2/3) was then rejected by recordPlaceholderValue and the saved
     * baseline could never be used. The counting restarts
     * per tree run AND per block, so the bind and plan trees of one baseline traverse the
     * same structure in the same order and corresponding roots keep corresponding ordinals.
     */
    private long nextRootOrdinal = 0;
    private long currentRootOrdinal = -1;

    /**
     * Depth of an all-NULL CONDITIONAL (CASE / IF / COALESCE) currently visited in a Boolean
     * context (see markerPayload): such a node has NullType and the analyzer replaces the whole
     * operand with a typed constant, so its branch NULLs are parameterized BOOLEAN.
     */
    private int booleanConditionalDepth;

    /**
     * Depth of an all-NULL container producer whose ELEMENTS / VALUES are the markers that carry
     * the container's element type (see markerPayload): an all-NULL element list is what makes
     * the constructor NullType, so its NULL elements are parameterized BOOLEAN while the
     * constructor itself sits in a container position of a NullType-producing chain.
     */
    private int containerElementDepth;

    /** Number of array items or map values before reaching the key selected by MAP_KEYS. */
    private int mapKeyDepth = NO_MAP_KEY_DEPENDENCY;

    /**
     * Whether a GROUP BY ordinal references a parameterized SELECT item. Analysis can inline
     * that expression and the physical grouping can lose its binding to the original item.
     * Keep the parameterized tree so replay binds the ordinal against the caller's values.
     */
    private boolean parameterizedGroupByOrdinalTarget;

    /** The enclosing block's root-ordinal counter, restored by exitQueryBlock. */
    private final Deque<Long> rootOrdinalStack = new ArrayDeque<>();

    /** The enclosing root's ordinal, restored by exitQueryBlock (see enterQueryBlock). */
    private final Deque<Long> currentRootOrdinalStack = new ArrayDeque<>();

    /** Installs / clears the GROUP BY mirror lookup window (null = disabled). */
    @Override
    public void setGroupByMirrorLookup(java.util.function.Function<String, Expression> lookup) {
        this.groupByMirrorLookup = lookup;
    }

    /** Reports whether capture must keep the parameterized tree for a GROUP BY ordinal. */
    public boolean hasParameterizedGroupByOrdinalTarget() {
        return parameterizedGroupByOrdinalTarget;
    }

    /** Marks a GROUP BY ordinal whose SELECT item carries placeholders. */
    @Override
    public void noteParameterizedGroupByOrdinalTarget() {
        this.parameterizedGroupByOrdinalTarget = true;
    }

    /** Starts one expression ROOT of the current block (see nextRootOrdinal). */
    private void beginExpressionRoot() {
        currentRootOrdinal = nextRootOrdinal++;
    }

    /** Restarts the per-tree query-block numbering (a tree's root block is 0). */
    public void startNewTree() {
        nextBlockId = 1;
        currentBlockId = 0;
        blockIdStack.clear();
        projectItemStack.clear();
        // one tree = one root-ordinal run: corresponding roots of the bind tree and the
        // (separately parsed) plan tree keep corresponding ordinals
        nextRootOrdinal = 0;
        currentRootOrdinal = -1;
        rootOrdinalStack.clear();
        currentRootOrdinalStack.clear();
    }

    /**
     * The whole-tree parameterization transform: the builder IS an
     * SPMPlanTreeSupport.ExprTransform, so the plan walk can hand it the query
     * BLOCK and projection ITEM scopes of the expressions it visits (see
     * SPMPlanTreeSupport.ExprTransform.enterQueryBlock). Passing a bare lambda
     * instead would lose those scopes and merge independent literals into one placeholder
     * id.
     */
    @Override
    public Expression apply(Expression expr) {
        beginExpressionRoot();
        return expr.accept(this, null);
    }

    /**
     * Transform of a SUBQUERY expression's own plan tree: every expression the walk hands
     * over is a block ROOT (the child-position sentinel -1), and the query-block /
     * projection-item scope hooks are forwarded to the enclosing builder so nested
     * derived tables keep their own block ids.
     */
    private final class SubqueryBlockTransform implements SPMPlanTreeSupport.ExprTransform {
        @Override
        public Expression apply(Expression expr) {
            beginExpressionRoot();
            childIndexStack.push(-1);
            try {
                return expr.accept(SPMPlaceholderBuilder.this, null);
            } finally {
                childIndexStack.pop();
            }
        }

        @Override
        public void setGroupByMirrorLookup(java.util.function.Function<String, Expression> lookup) {
            SPMPlaceholderBuilder.this.setGroupByMirrorLookup(lookup);
        }

        @Override
        public void noteParameterizedGroupByOrdinalTarget() {
            SPMPlaceholderBuilder.this.noteParameterizedGroupByOrdinalTarget();
        }

        @Override
        public void enterQueryBlock() {
            SPMPlaceholderBuilder.this.enterQueryBlock();
        }

        @Override
        public void exitQueryBlock() {
            SPMPlaceholderBuilder.this.exitQueryBlock();
        }

        @Override
        public void enterProjectionItem(int index) {
            SPMPlaceholderBuilder.this.enterProjectionItem(index);
        }

        @Override
        public void exitProjectionItem() {
            SPMPlaceholderBuilder.this.exitProjectionItem();
        }
    }

    /**
     * Enters a nested query block (see
     * org.apache.doris.nereids.spm.SPMPlanTreeSupport.ExprTransform.enterQueryBlock):
     * the block gets its own monotonic id, the enclosing one is restored by
     * exitQueryBlock.
     */
    @Override
    public void enterQueryBlock() {
        blockIdStack.push(currentBlockId);
        currentBlockId = nextBlockId++;
        // the root-ordinal counting is per BLOCK as well: a nested block's roots must not
        // shift the enclosing block's ordinals (and vice versa), and the enclosing counters
        // resume on exit
        rootOrdinalStack.push(nextRootOrdinal);
        nextRootOrdinal = 0;
        currentRootOrdinalStack.push(currentRootOrdinal);
        currentRootOrdinal = -1;
    }

    /** Leaves the current nested query block and restores the enclosing block id. */
    @Override
    public void exitQueryBlock() {
        if (!blockIdStack.isEmpty()) {
            currentBlockId = blockIdStack.pop();
        }
        if (!rootOrdinalStack.isEmpty()) {
            nextRootOrdinal = rootOrdinalStack.pop();
        }
        if (!currentRootOrdinalStack.isEmpty()) {
            currentRootOrdinal = currentRootOrdinalStack.pop();
        }
    }

    /**
     * Enters one projection item of a projection-like list: every literal of its subtree
     * carries this position, so two equally shaped literals in different items (or in a
     * projection and in a filter) never share one placeholder id.
     */
    @Override
    public void enterProjectionItem(int index) {
        projectItemStack.push(index);
    }

    /** Leaves the current projection item. */
    @Override
    public void exitProjectionItem() {
        if (!projectItemStack.isEmpty()) {
            projectItemStack.pop();
        }
    }

    /** Position of the projection item currently being visited; -1 outside a SELECT list. */
    private int currentProjectItem() {
        Integer top = projectItemStack.peek();
        return top == null ? -1 : top;
    }

    /** Position of the expression currently being visited inside its direct parent; -1 at a root. */
    private int currentChildIndex() {
        Integer top = childIndexStack.peek();
        return top == null ? -1 : top;
    }

    /**
     * The occurrence path of the literal currently being visited: the child positions from the
     * transformed EXPRESSION ROOT down to the literal, rendered as "0/1/..." (empty for a
     * root literal). Two equal literals can sit under structurally EQUAL parents at
     * different places of one root ("a = 1 AND a = 1": the parents are the two And
     * children), and only the path tells those independent slots apart - see
     * PlaceholderKey.occurrencePath.
     */
    private String currentOccurrencePath() {
        if (childIndexStack.isEmpty()) {
            return "";
        }
        Object[] positions = childIndexStack.toArray();
        StringBuilder sb = new StringBuilder();
        for (int i = positions.length - 1; i >= 0; i--) {
            int position = (Integer) positions[i];
            if (position == -1) {
                // A subquery's expression root starts a new path, independent of its caller.
                sb.setLength(0);
                continue;
            }
            if (sb.length() > 0) {
                sb.append('/');
            }
            sb.append(position);
        }
        return sb.toString();
    }

    // literal -> scalar placeholder.

    /**
     * Whether the expression's TYPE is NullType.
     *
     * The type is derived structurally because this builder walks the UNBOUND plan, where
     * getDataType throws. A node produces NullType when EVERY value it can yield is
     * NULL - a NULL literal, a CASE whose every result does, an IF whose two value arguments do,
     * a COALESCE / IFNULL / NVL / NULLIF / GREATEST / LEAST all of whose arguments do. Collected as
     * a Boolean operand (directly, or through a projected slot) such an expression is REPLACED by
     * the analyzer with a typed constant NULL, which deletes the placeholders inside it.
     *
     * The test is RECURSIVE on purpose: a conditional nested in the RESULT of
     * another conditional (k > 0 OR (CASE WHEN k = 0 THEN ifnull(NULL, NULL) ELSE NULL END))
     * yields NullType although its result is not a NULL LITERAL, so a literal-only test left its
     * inner markers NullType and the consumer erased them - a TRUE variant then replayed as NULL.
     * The same recursion covers the custom-signature producers: GreatestLeast.customSignature
     * returns the arguments' common type, i.e. NullType for greatest(NULL, NULL), LEAST shares the path).
     *
     * Parameter expr: the expression to classify
     * Returns: whether every value the expression can yield is NULL
     */
    private static boolean producesNullType(Expression expr) {
        if (expr == null) {
            return false;
        }
        if (expr.isNullLiteral()) {
            return true;
        }
        List<Expression> children = expr.children();
        if (hasConditionalResultInputs(expr)) {
            return allConditionalResultsMatch(expr, SPMPlaceholderBuilder::producesNullType);
        }
        if (expr instanceof WindowExpression) {
            return producesNullType(((WindowExpression) expr).getFunction());
        }
        if (isFirstArgumentTypedResult(expr)) {
            return !children.isEmpty() && producesNullType(children.get(0));
        }
        if (isElementAtCall(expr)) {
            // element_at(array(NULL), 1) takes its type from the ARRAY's element type and
            // element_at(map('x', NULL), 'x') from the MAP's VALUE type (CreateMap derives
            // MAP<STRING, NULL_TYPE>), so an all-NULL container makes the WHOLE call NullType: a
            // Boolean consumer replaced the two markers of
            // k > 0 OR element_at(array(NULL), 1) with a typed constant and the
            // element_at(array(TRUE), 1) variant had nothing to substitute, and
            // the MAP spelling lost the value marker the same way.
            return !children.isEmpty() && isAllNullContainer(children.get(0));
        }
        if (isArrayReducerCall(expr)) {
            // The SAME producer classification, for the array REDUCERS:
            // ARRAY_MIN / ARRAY_MAX return FollowToAnyDataType(0), i.e. the INPUT array's
            // element type, so an all-NULL input makes the whole call NullType - binding then
            // replaced k > 0 OR array_min(array(NULL)) with k > ph OR NULL and the
            // array_min(array(TRUE)) caller got NULL instead of TRUE. Keeping the family in
            // ONE classifier is what lets the marker be retyped before the Boolean consumer
            // drops it - the same defect the bare element_at trigger had, so it is carried here
            // instead of in a per-function inline.
            return !children.isEmpty() && isAllNullArray(children.get(0));
        }
        return false;
    }

    /** These aggregates and window functions derive the result type from their first argument. */
    private static boolean isFirstArgumentTypedResult(Expression expression) {
        if (!(expression instanceof Function)) {
            return false;
        }
        String name = ((Function) expression).getName();
        return "min".equalsIgnoreCase(name) || "max".equalsIgnoreCase(name)
                || "first_value".equalsIgnoreCase(name) || "last_value".equalsIgnoreCase(name)
                || "nth_value".equalsIgnoreCase(name) || "any_value".equalsIgnoreCase(name);
    }

    private static boolean hasConditionalResultInputs(Expression expression) {
        return expression instanceof CaseWhen || isIfConditional(expression) || isIfNullConditional(expression)
                || isNullIfConditional(expression) || isGreatestLeastConditional(expression)
                || isCoalesceConditional(expression);
    }

    /** Tests only the inputs that determine a conditional expression's result type. */
    private static boolean allConditionalResultsMatch(Expression expression, Predicate<Expression> predicate) {
        if (expression instanceof CaseWhen) {
            CaseWhen caseWhen = (CaseWhen) expression;
            return caseWhen.getWhenClauses().stream().allMatch(clause -> predicate.test(clause.getResult()))
                    && caseWhen.getDefaultValue().map(predicate::test).orElse(true);
        }
        if (isIfConditional(expression)) {
            return expression.arity() == 3
                    && predicate.test(expression.child(1)) && predicate.test(expression.child(2));
        }
        return expression.arity() > 0 && expression.children().stream().allMatch(predicate);
    }

    private static boolean allConditionalContainersMatch(Expression expression, Predicate<Expression> predicate) {
        boolean[] hasContainer = {false};
        // A bare NULL can take the type of a sibling arm, but it cannot establish a container on its own.
        return allConditionalResultsMatch(expression, input -> {
            if (input.isNullLiteral() && input.getDataType().isNullType()) {
                return true;
            }
            hasContainer[0] = true;
            return predicate.test(input);
        }) && hasContainer[0];
    }

    /**
     * Finds the scalar argument that supplies an array return type's element type. The signature's
     * FollowToAnyDataType index identifies an AnyDataType group, not an argument position.
     */
    private static int elementTypeArgumentIndex(Expression expr) {
        if (expr instanceof BoundFunction) {
            for (FunctionSignature signature : ((BoundFunction) expr).getSignatures()) {
                DataType returnType = signature.returnType;
                if (!(returnType instanceof ArrayType)) {
                    continue;
                }
                DataType itemType = ((ArrayType) returnType).getItemType();
                if (!(itemType instanceof FollowToAnyDataType)) {
                    continue;
                }
                int group = ((FollowToAnyDataType) itemType).getIndex();
                for (int i = 0; i < signature.argumentsTypes.size(); i++) {
                    DataType argumentType = signature.argumentsTypes.get(i);
                    if (argumentType instanceof AnyDataType && ((AnyDataType) argumentType).getIndex() == group) {
                        return i;
                    }
                }
            }
            return -1;
        }
        if (expr instanceof Function) {
            return switch (((Function) expr).getName().toLowerCase(Locale.ROOT)) {
                case "array_repeat", "array_agg", "collect_list", "collect_set" -> 0;
                case "array_with_constant" -> 1;
                default -> -1;
            };
        }
        return -1;
    }

    /** Tests a parsed builtin name or the corresponding bound struct constructor. */
    private static boolean isStructCall(Expression expression, boolean named) {
        if (expression instanceof UnboundFunction) {
            UnboundFunction call = (UnboundFunction) expression;
            return (call.getDbName() == null || call.getDbName().isEmpty())
                    && (named ? "named_struct" : "struct").equalsIgnoreCase(call.getName());
        }
        return named ? expression instanceof CreateNamedStruct : expression instanceof CreateStruct;
    }

    private boolean isStructFieldPosition(Expression parent) {
        return isStructCall(parent, false) || isStructCall(parent, true) && currentChildIndex() % 2 == 1;
    }

    /**
     * Whether the expression is a container whose ELEMENT type is NullType (see producesNullType):
     * an all-NULL array or an all-NULL-valued map. Both overloads of element_at return the
     * container's element / value type, so either one makes the call a NullType producer.
     *
     * Parameter expr: the expression to classify
     * Returns: whether it is an all-NULL array or map value
     */
    private static boolean isAllNullContainer(Expression expr) {
        return isAllNullArray(expr) || isAllNullMap(expr);
    }

    /** Follows a map's value type through constructors and type-preserving wrappers. */
    private static boolean isAllNullMap(Expression expr) {
        return allMapComponentsMatch(expr, false, SPMPlaceholderBuilder::producesNullType);
    }

    private static boolean allMapComponentsMatch(Expression expr, boolean key, Predicate<Expression> predicate) {
        if (expr.isNullLiteral()) {
            return true;
        }
        if (hasConditionalResultInputs(expr)) {
            return allConditionalContainersMatch(expr, input -> allMapComponentsMatch(input, key, predicate));
        }
        if (isElementAtCall(expr)) {
            return allElementAtInputsMatch(expr, input -> allMapComponentsMatch(input, key, predicate));
        }
        if (expr instanceof MapLiteral) {
            Map<Literal, Literal> entries = ((MapLiteral) expr).getValue();
            return (key ? entries.keySet() : entries.values()).stream().allMatch(predicate);
        }
        if (isTypePreservingMapCall(expr)) {
            return expr.arity() > 0 && allMapComponentsMatch(expr.child(0), key, predicate);
        }
        if (!isMapConstructor(expr)) {
            return false;
        }
        List<Expression> pairs = expr.children();
        if (pairs.size() < 2) {
            return false;
        }
        for (int i = key ? 0 : 1; i < pairs.size(); i += 2) {
            if (!predicate.test(pairs.get(i))) {
                return false;
            }
        }
        return true;
    }

    /** Tests whether the element of a recognized array producer has NullType. */
    private static boolean isAllNullArray(Expression expr) {
        return allArrayElementsMatch(expr, SPMPlaceholderBuilder::producesNullType);
    }

    private static boolean allArrayElementsMatch(Expression expr, Predicate<Expression> predicate) {
        if (expr == null) {
            return false;
        }
        if (expr.isNullLiteral()) {
            return true;
        }
        if (hasConditionalResultInputs(expr)) {
            return allConditionalContainersMatch(expr, input -> allArrayElementsMatch(input, predicate));
        }
        if (isElementAtCall(expr)) {
            return allElementAtInputsMatch(expr, input -> allArrayElementsMatch(input, predicate));
        }
        if (isArrayMapCall(expr)) {
            return expr.arity() > 0 && expr.child(0) instanceof Lambda
                    && predicate.test(((Lambda) expr.child(0)).getLambdaFunction());
        }
        if (isArrayFlattenCall(expr)) {
            return expr.arity() == 1 && allArrayElementsMatch(expr.child(0),
                    item -> allFlattenedElementsMatch(item, predicate));
        }
        int elementIndex = elementTypeArgumentIndex(expr);
        if (elementIndex >= 0) {
            return elementIndex < expr.arity() && predicate.test(expr.child(elementIndex));
        }
        if (isArrayConcatCall(expr)) {
            return !expr.children().isEmpty()
                    && expr.children().stream().allMatch(part -> allArrayElementsMatch(part, predicate));
        }
        if (isMapValuesCall(expr) || isMapKeysCall(expr)) {
            return expr.arity() == 1 && allMapComponentsMatch(expr.child(0), isMapKeysCall(expr), predicate);
        }
        if (isTypePreservingArrayCall(expr)) {
            return expr.arity() > 0 && allArrayElementsMatch(expr.child(0), predicate);
        }
        if (!isArrayConstructor(expr)) {
            return false;
        }
        List<? extends Expression> elements = expr instanceof ArrayLiteral
                ? ((ArrayLiteral) expr).getValue() : expr.children();
        return elements.stream().allMatch(predicate);
    }

    /** Follows the first argument of element_at through its array or map overload. */
    private static boolean allElementAtInputsMatch(Expression expr, Predicate<Expression> predicate) {
        return expr.arity() > 0 && (allArrayElementsMatch(expr.child(0), predicate)
                || allMapComponentsMatch(expr.child(0), false, predicate));
    }

    /** ARRAY_MAP derives its element type from the lambda body at argument zero. */
    private static boolean isArrayMapCall(Expression expr) {
        return expr instanceof Function && "array_map".equalsIgnoreCase(((Function) expr).getName());
    }

    private static boolean isArrayFlattenCall(Expression expr) {
        return expr instanceof Function && "array_flatten".equalsIgnoreCase(((Function) expr).getName());
    }

    /** Walks nested array items until the predicate accepts each flattened item. */
    private static boolean allFlattenedElementsMatch(Expression item, Predicate<Expression> predicate) {
        return predicate.test(item) || allArrayElementsMatch(item,
                nested -> allFlattenedElementsMatch(nested, predicate));
    }

    /**
     * Whether the expression is an array_concat call (see isAllNullArray).
     *
     * Matched by NAME through Function, which covers both the unbound array_concat(...)
     * call and the bound ArrayConcat node (its function name is array_concat).
     *
     * Parameter expr: the expression to classify
     * Returns: whether it is an array_concat call
     */
    private static boolean isArrayConcatCall(Expression expr) {
        return expr instanceof Function
                && "array_concat".equalsIgnoreCase(((Function) expr).getName());
    }

    /**
     * Whether the expression is a type-preserving ARRAY transformation (see isAllNullArray,
     * and TYPE_PRESERVING_ARRAY_CALLS for the family).
     *
     * Parameter expr: the expression to classify
     * Returns: whether the result's element type follows the first argument
     */
    private static boolean isTypePreservingArrayCall(Expression expr) {
        return isTypePreservingContainerCall(expr, TYPE_PRESERVING_ARRAY_CALLS, true);
    }

    /**
     * Whether the expression is a type-preserving MAP transformation (see
     * isNullTypeContainerPosition).
     *
     * Parameter expr: the expression to classify
     * Returns: whether the result's type follows the first argument's MAP
     */
    private static boolean isTypePreservingMapCall(Expression expr) {
        return isTypePreservingContainerCall(expr, TYPE_PRESERVING_MAP_CALLS, false);
    }

    /**
     * Recognizes a container transformation by a bound signature that follows its first container argument, or by
     * membership in the supplied function-name set. The name fallback also applies when no matching bound
     * signature is found.
     */
    private static boolean isTypePreservingContainerCall(Expression expr, Set<String> names,
            boolean array) {
        if (!(expr instanceof Function)) {
            return false;
        }
        if (followsFirstArgument(expr, array)) {
            return true;
        }
        return names.contains(((Function) expr).getName().toLowerCase(Locale.ROOT));
    }

    /**
     * The SIGNATURE form of isTypePreservingContainerCall, for a BOUND function: a
     * retArgType(0) declaration whose first argument is a container of the requested
     * kind. The first-argument check keeps a scalar retArgType(0) function (e.g.
     * any_value) out of the family: its result follows a SCALAR, so an all-NULL array
     * literal in that position is not a container whose type has to survive.
     *
     * Parameter expr: the expression to classify
     * Parameter array: whether the requested container kind is ARRAY (otherwise MAP)
     * Returns: whether a signature makes the result follow a container first argument
     */
    private static boolean followsFirstArgument(Expression expr, boolean array) {
        if (!(expr instanceof BoundFunction)) {
            return false;
        }
        for (FunctionSignature signature : ((BoundFunction) expr).getSignatures()) {
            if (signature == null || signature.argumentsTypes == null
                    || signature.argumentsTypes.isEmpty()
                    || !(signature.returnType instanceof FollowToArgumentType)
                    || ((FollowToArgumentType) signature.returnType).argumentIndex != 0) {
                continue;
            }
            DataType first = signature.argumentsTypes.get(0);
            if (array ? first instanceof ArrayType : first instanceof MapType) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the expression builds an ARRAY value (see producesNullType).
     *
     * Both spellings are accepted: the parsed ArrayLiteral and the unbound array(...)
     * call, which is only a Function named array before binding.
     *
     * Parameter expr: the expression to classify
     * Returns: whether it is an array constructor
     */
    private static boolean isArrayConstructor(Expression expr) {
        if (expr instanceof ArrayLiteral) {
            return true;
        }
        return expr instanceof Function
                && "array".equalsIgnoreCase(((Function) expr).getName());
    }

    /**
     * Whether the expression is an element_at call (see producesNullType).
     *
     * Matched by NAME through Function for the unbound plan, and by the analyzed
     * ElementAt node for a bound one.
     *
     * Parameter expr: the expression to classify
     * Returns: whether it is an element_at call
     */
    private static boolean isElementAtCall(Expression expr) {
        if (expr instanceof ElementAt) {
            return true;
        }
        return expr instanceof Function
                && "element_at".equalsIgnoreCase(((Function) expr).getName());
    }

    /**
     * Whether the expression is an array_min / array_max call (see
     * producesNullType).
     *
     * Both declare ret(FollowToAnyDataType(0)), i.e. the result IS the input array's
     * element type, so an all-NULL input makes the call a NullType producer exactly like
     * element_at. Matched by NAME through Function, which covers the unbound
     * calls and the bound ArrayMin / ArrayMax nodes.
     *
     * Parameter expr: the expression to classify
     * Returns: whether it is an array reducer
     */
    private static boolean isArrayReducerCall(Expression expr) {
        if (!(expr instanceof Function)) {
            return false;
        }
        String name = ((Function) expr).getName();
        return "array_min".equalsIgnoreCase(name) || "array_max".equalsIgnoreCase(name);
    }

    /**
     * Whether the expression builds a MAP value (see isAllNullMap).
     *
     * Matched by NAME through Function, which covers both the unbound map(...) call and
     * the bound CreateMap (its function name is map).
     *
     * Parameter expr: the expression to classify
     * Returns: whether it is a map constructor
     */
    private static boolean isMapConstructor(Expression expr) {
        return expr instanceof Function && "map".equalsIgnoreCase(((Function) expr).getName());
    }

    /**
     * Whether the expression is a map_values call (see isAllNullArray).
     *
     * MapValues declares ArrayType.of(FollowToAnyDataType(0)), i.e. the result is
     * an ARRAY of the input MAP's VALUE type, so an all-NULL-valued map makes the whole call an
     * all-NULL array. Matched by NAME through Function, which covers the unbound call and
     * the bound MapValues node.
     *
     * Parameter expr: the expression to classify
     * Returns: whether it is a map_values call
     */
    private static boolean isMapValuesCall(Expression expr) {
        return expr instanceof Function
                && "map_values".equalsIgnoreCase(((Function) expr).getName());
    }

    /** MAP_KEYS derives the array element type from the map's key type. */
    private static boolean isMapKeysCall(Expression expr) {
        return expr instanceof Function && "map_keys".equalsIgnoreCase(((Function) expr).getName());
    }

    /**
     * Whether the expression is a GREATEST / LEAST call (see producesNullType).
     *
     * GreatestLeast.customSignature returns the arguments' common type, so an all-NULL
     * call is NullType and a Boolean consumer replaces it outright. LEAST shares the
     * class, so both names are accepted.
     *
     * Parameter expr: the expression to classify
     * Returns: whether it is a GREATEST / LEAST call
     */
    private static boolean isGreatestLeastConditional(Expression expr) {
        if (!(expr instanceof Function)) {
            return false;
        }
        String name = ((Function) expr).getName();
        return "greatest".equalsIgnoreCase(name) || "least".equalsIgnoreCase(name);
    }

    /** Recognizes a Function named IF, case-insensitively, in bound or unbound form. */
    private static boolean isIfConditional(Expression expr) {
        return expr instanceof Function && "if".equalsIgnoreCase(((Function) expr).getName());
    }

    /**
     * Whether the expression is an IFNULL / NVL call (both parse to the same
     * Function named ifnull; nvl is accepted as the spelling as well).
     *
     * It belongs to the all-NULL conditional family even though it has no condition argument:
     * ifnull(NULL, NULL) has NullType, so a Boolean consumer replaces the WHOLE call
     * with a typed constant NULL exactly like an all-NULL CASE - k > 0 OR ifnull(NULL, NULL)
     * lost both markers and the matching ifnull(NULL, TRUE) variant had nothing to
     * substitute.
     *
     * Parameter expr: the expression to classify
     * Returns: whether it is an IFNULL / NVL call
     */
    private static boolean isIfNullConditional(Expression expr) {
        if (!(expr instanceof Function)) {
            return false;
        }
        String name = ((Function) expr).getName();
        return "ifnull".equalsIgnoreCase(name) || "nvl".equalsIgnoreCase(name);
    }

    /**
     * Whether the expression is a NULLIF call.
     *
     * NULLIF is a THIRD all-NULL producer, on its own signature path: NullIf.customSignature
     * returns the COMMON type of its two arguments, so nullif(NULL, NULL) has NullType and a
     * Boolean consumer ("k > 0 OR nullif(NULL, NULL)") replaces the whole call with a typed constant
     * NULL - the markers of both arguments disappear and the matching nullif(TRUE, NULL)
     * variant has no marker left to substitute with TRUE. It is not covered by the
     * IFNULL / NVL branch above: a different function with its own signature rule.
     *
     * Parameter expr: the expression to classify
     * Returns: whether it is a NULLIF call
     */
    private static boolean isNullIfConditional(Expression expr) {
        return expr instanceof Function
                && "nullif".equalsIgnoreCase(((Function) expr).getName());
    }

    /**
     * Whether the expression is a COALESCE call (see isIfConditional).
     *
     * Parameter expr: the expression to classify
     * Returns: whether it is a COALESCE call
     */
    private static boolean isCoalesceConditional(Expression expr) {
        return expr instanceof Function
                && "coalesce".equalsIgnoreCase(((Function) expr).getName());
    }

    /**
     * Whether the literal at the CURRENT position is a RESULT of its conditional parent (see
     * producesNullType) - a CASE's THEN / ELSE, an IF's two value arguments, or any
     * COALESCE argument.
     *
     * Only RESULTS may be typed BOOLEAN: the WHEN / condition operands compare against real
     * columns, and a BOOLEAN-typed NULL there (k = CAST(NULL AS BOOLEAN)) is a type error in
     * the replayed plan.
     *
     * Parameter parent: the literal's parent expression
     * Returns: whether the literal sits in a result position
     */
    private boolean isConditionalResultPosition(Expression parent) {
        return isConditionalResultPosition(parent, currentChildIndex());
    }

    /**
     * The position-aware form of isConditionalResultPosition(Expression).
     *
     * Parameter parent: the conditional
     * Parameter childIndex: the position of the child inside it
     * Returns: whether that position holds a result
     */
    private static boolean isConditionalResultPosition(Expression parent, int childIndex) {
        if (parent instanceof WhenClause) {
            // a WHEN clause's children are [operand, result]: only the RESULT may be typed
            return childIndex == 1;
        }
        if (parent instanceof CaseWhen) {
            // the WHEN clauses are separate WhenClause nodes, so only the ELSE is a direct child
            CaseWhen caseWhen = (CaseWhen) parent;
            return caseWhen.getDefaultValue().isPresent()
                    && childIndex == caseWhen.children().size() - 1;
        }
        if (isIfNullConditional(parent) || isNullIfConditional(parent)
                || isGreatestLeastConditional(parent)) {
            // every IFNULL / NVL / NULLIF / GREATEST / LEAST argument is a value
            return true;
        }
        if (isIfConditional(parent)) {
            return childIndex == 1 || childIndex == 2;
        }
        return isCoalesceConditional(parent) || isFirstArgumentTypedResult(parent) && childIndex == 0;
    }

    @Override
    public Expression visitLiteral(Literal literal, Expression parent) {
        // The ESCAPE operand of LIKE must stay a STRING LITERAL: parameterizing it turned
        // the escape into a scalar placeholder call, and the analyzer rejected the whole
        // statement ("like escape character must be a string literal" on
        // "k like '%a%' escape '!'"). An escape character is part of the LIKE operator's
        // syntax, not a search value: keeping it literal makes a user query with a
        // DIFFERENT escape character a structural mismatch (the safe direction - the
        // query simply runs without the baseline) instead of an unplannable baseline.
        // A plain LIKE (no ESCAPE) carries only the left / pattern children; child(2)
        // must not be touched then (the pattern literal is visited with the 2-child
        // LIKE as its parent, and an unguarded child(2) read threw
        // ArrayIndexOutOfBoundsException on every such CREATE).
        if (parent instanceof Like && parent.children().size() > 2
                && literal == parent.child(2)) {
            return literal;
        }
        // The named struct's labels are literal selectors. Only its field values can vary.
        if (isStructCall(parent, true) && currentChildIndex() % 2 == 0) {
            return literal;
        }
        // Deduplicate by value + parent structure + child position: a literal reuses an
        // existing id only when the value, the parent expression structure AND the
        // position inside that parent all match. The position is required - the two
        // operands of "1 + 1" share one parent structure but are different positions,
        // and a similar query may carry different values there (one shared id would make
        // the two occurrences resolve to the same user value and never match).
        int childIndex = currentChildIndex();
        long blockId = currentBlockId;
        int projectItem = currentProjectItem();
        long rootOrdinal = currentRootOrdinal;
        String occurrencePath = currentOccurrencePath();
        PlaceholderKey key = PlaceholderKey.of(literal, parent, childIndex,
                blockId, projectItem, rootOrdinal, occurrencePath, parentSignatures);
        Expression existing = placeholderIndex.get(key);
        if (existing != null) {
            return existing;
        }
        long id = nextId++;
        boolean structField = isStructFieldPosition(parent);
        boolean containerPosition = isNullTypeContainerPosition(parent);
        boolean conditionalContainer = containerElementDepth > 0
                && (literal instanceof ArrayLiteral || literal instanceof MapLiteral)
                && isConditionalResultPosition(parent);
        SpmConstVar placeholder = SpmConstVar.of(id,
                markerPayload(literal, parent, currentProjectItem() >= 0 || structField,
                        structField || booleanConditionalDepth > 0 && isConditionalResultPosition(parent),
                        booleanConditionalDepth > 0 && (containerPosition || conditionalContainer),
                        containerElementDepth > 0 && !containerPosition, mapKeyDepth));
        placeholderIndex.put(key, placeholder);
        return placeholder;
    }

    /**
     * Whether the literal sits in the CONTAINER position of a call that takes its RESULT type from
     * it (see markerPayload): element_at's first argument, or the array argument of the
     * array_min / array_max reducers (their signature is
     * FollowToAnyDataType(0)).
     *
     * The container itself is not a value position: its marker has to stay an ARRAY / MAP, so the
     * Boolean retyping of a bare NULL cannot apply to it - the container needs its own repair.
     *
     * Parameter parent: the literal's parent expression
     * Returns: whether the literal is such a container
     */
    private boolean isNullTypeContainerPosition(Expression parent) {
        if ((isElementAtCall(parent) || isArrayReducerCall(parent)) && currentChildIndex() == 0) {
            return true;
        }
        // A TYPE-PRESERVING WRAPPER forwards its INPUT's element type to the enclosing call
        // : element_at(array_sort([NULL]), 1) reads the result THROUGH array_sort's
        // retArgType(0), so the wrapper's array argument sits in the SAME container position.
        // The unrecognized chain left the ARRAY<NULL_TYPE> literal untouched, element_at stayed
        // NullType, and the Boolean consumer replaced the whole operand together with its
        // markers - the element_at(array_sort([TRUE]), 1) caller then had no id to substitute.
        // The SAME chain exists for every other retArgType(0) member
        // (array_reverse_sort, array_slice, array_union,...: see
        // TYPE_PRESERVING_ARRAY_CALLS), so the classifier covers the family instead of two names.
        // ARRAY_CONCAT derives its element type from EVERY part, so each part is such a position,
        // and MAP_VALUES turns its map's VALUE type into the result's element type (position 0).
        if (isArrayFlattenCall(parent) || isTypePreservingArrayCall(parent) || isTypePreservingMapCall(parent)) {
            return currentChildIndex() == 0;
        }
        if (isArrayConcatCall(parent)) {
            return true;
        }
        return (isMapValuesCall(parent) || isMapKeysCall(parent)) && currentChildIndex() == 0;
    }

    /**
     * Chooses the literal payload used while analyzing a scalar placeholder. The payload controls the marker's
     * capture-time type even though replay replaces the marker by id. A NULL used as a Boolean operand,
     * conditional result or selected output can otherwise be coerced into a new Boolean NULL expression,
     * discarding the marker. Relevant scalar NULLs are therefore typed Boolean. All-NULL container payloads
     * receive corresponding element or value typing so the marker survives the same coercion. The replacement
     * still uses the caller's actual value and type.
     */
    private static Literal markerPayload(Literal literal, Expression parent,
            boolean projectionItem, boolean booleanConditionalResult,
            boolean booleanElementContainer, boolean containerElement, int mapKeyDepth) {
        if (containerElement && literal.isNullLiteral()) {
            // array(NULL) builds its container from the ELEMENTS, so an all-NULL element list is
            // exactly what makes the constructor NullType. The NULL marker is a CHILD of the
            // constructor (not the container the container-position repair retypes above), so
            // typing IT BOOLEAN makes the constructor ARRAY<BOOLEAN> and removes the NullType from
            // the chain - k > 0 OR element_at(array_reverse_sort(array(NULL)), 1) kept the whole
            // operand as a typed constant NULL otherwise, together with every marker inside it
            // . A NULL value stays NULL through any coercion, so only the marker's
            // TYPE changes.
            return NullLiteral.BOOLEAN_INSTANCE;
        }
        if (booleanElementContainer) {
            Literal payload = booleanContainerPayload(literal, parent, mapKeyDepth);
            if (payload != null) {
                return payload;
            }
        }
        if (projectionItem && (literal instanceof ArrayLiteral || literal instanceof MapLiteral)
                && isAllNullContainer(literal)) {
            // A consumer can read the NULL-typed element or value through a derived-table slot.
            Literal payload = booleanContainerPayload(literal, parent, NO_MAP_KEY_DEPENDENCY);
            if (payload != null) {
                return payload;
            }
        }
        boolean booleanOperand = parent instanceof And || parent instanceof Or
                || parent instanceof Not;
        if (literal.isNullLiteral() && (booleanOperand || booleanConditionalResult)) {
            return NullLiteral.BOOLEAN_INSTANCE;
        }
        // A projected NULL (SELECT NULL AS n / a bare SELECT NULL) exposes a NullType
        // SLOT, and a consumer that binds it as a BOOLEAN operand (an enclosing OR / AND /
        // WHERE, even in another query block) has the analyzer replace that operand with a
        // typed constant NULL - dropping the slot, and with it this marker. A TRUE variant of
        // the derived column then has nothing to substitute and the replay keeps NULL.
        // Typing the projected NULL BOOLEAN removes the NullType from the tree; a NULL value
        // stays NULL through any later coercion, so the only thing that changes is that the
        // marker (and the slot that carries it) survives.
        if (literal.isNullLiteral() && projectionItem
                && (parent == null || parent instanceof org.apache.doris.nereids.trees.expressions.Alias)) {
            return NullLiteral.BOOLEAN_INSTANCE;
        }
        return literal;
    }

    /**
     * Whether the literal currently being captured sits at an ELEMENT position whose marker
     * decides the enclosing container's type (see markerPayload): an ARRAY constructor's element,
     * or the VALUE half of a MAP constructor's key/value pairs - a map's keys feed its KEY type
     * and keep their own typing.
     *
     * Parameter parent: the literal's parent expression
     * Returns: whether the literal is such an element
     */
    private boolean isContainerElementPosition(Expression parent) {
        if (containerElementDepth == 0) {
            return false;
        }
        if (isMapConstructor(parent)) {
            return (currentChildIndex() % 2 == 0) == (mapKeyDepth == 0);
        }
        if ((parent instanceof WhenClause || isIfConditional(parent) || isFirstArgumentTypedResult(parent))
                && !isConditionalResultPosition(parent)) {
            return false;
        }
        if ((isArrayMapCall(parent) || parent instanceof Lambda || isElementAtCall(parent))
                && currentChildIndex() != 0) {
            return false;
        }
        // Counts and other non-element arguments do not determine the array's element type.
        int elementIndex = elementTypeArgumentIndex(parent);
        return elementIndex < 0 || elementIndex == currentChildIndex();
    }

    /**
     * Gives a whole-container marker a Boolean leaf type along the dependency followed by the consumer.
     * Retyping preserves the container's shape and all values, including the unrelated map component.
     */
    private static Literal booleanContainerPayload(Literal literal, Expression parent, int mapKeyDepth) {
        if (literal.isNullLiteral()) {
            if (containerIsArray(parent)) {
                return new ArrayLiteral(List.of(NullLiteral.BOOLEAN_INSTANCE),
                        ArrayType.of(BooleanType.INSTANCE));
            }
            if (containerIsMap(parent)) {
                DataType keyType = mapKeyDepth == 0 ? BooleanType.INSTANCE : mapKeyTypeOf(parent);
                return new NullLiteral(MapType.of(keyType, BooleanType.INSTANCE));
            }
            return null;
        }
        if (!(literal instanceof ArrayLiteral) && !(literal instanceof MapLiteral)) {
            return null;
        }
        DataType type = literal.getDataType();
        DataType targetType = booleanContainerLeafType(type, mapKeyDepth);
        // The parser has already coerced the literal's elements to their common types.
        return targetType.equals(type) ? null : (Literal) literal.checkedCastWithStrictChecking(targetType, true);
    }

    /** Types a NullType leaf through the selected array items, map values or map key. */
    private static DataType booleanContainerLeafType(DataType type, int mapKeyDepth) {
        if (type.isNullType()) {
            return BooleanType.INSTANCE;
        }
        int itemKeyDepth = Math.max(mapKeyDepth - 1, NO_MAP_KEY_DEPENDENCY);
        if (type instanceof ArrayType) {
            DataType item = ((ArrayType) type).getItemType();
            DataType booleanItem = booleanContainerLeafType(item, itemKeyDepth);
            return item.equals(booleanItem) ? type : ArrayType.of(booleanItem);
        }
        if (type instanceof MapType) {
            MapType map = (MapType) type;
            DataType key = map.getKeyType();
            DataType value = map.getValueType();
            if (mapKeyDepth == 0) {
                DataType booleanKey = booleanContainerLeafType(key, NO_MAP_KEY_DEPENDENCY);
                return key.equals(booleanKey) ? type : MapType.of(booleanKey, value);
            }
            DataType booleanValue = booleanContainerLeafType(value, itemKeyDepth);
            // Keep a usable key type when the selected value is retyped and the capture has no typed key.
            return value.equals(booleanValue) ? type
                    : MapType.of(key.isNullType() ? StringType.INSTANCE : key, booleanValue);
        }
        return type;
    }

    /** Translates the selected map key from the current result type to the selected input type. */
    private int mapKeyDepthForInput(Expression expression) {
        if (isMapKeysCall(expression)) {
            return 0;
        }
        if (mapKeyDepth < 0) {
            return NO_MAP_KEY_DEPENDENCY;
        }
        if (isElementAtCall(expression)) {
            return mapKeyDepth + 1;
        }
        if (isMapConstructor(expression) || isArrayConstructor(expression) || isArrayMapCall(expression)
                || elementTypeArgumentIndex(expression) >= 0) {
            return Math.max(mapKeyDepth - 1, NO_MAP_KEY_DEPENDENCY);
        }
        return mapKeyDepth;
    }

    /**
     * Whether one container-taking call resolves to the ARRAY signature (see
     * booleanContainerPayload).
     *
     * The index of the ARRAY overload is NUMERIC, while a STRING index selects the MAP overload;
     * an index that is not a literal at all cannot be classified and keeps the historical
     * behaviour (the container marker stays a bare NULL).
     *
     * Parameter parent: the call
     * Returns: whether the call can only be the ARRAY overload
     */
    private static boolean containerIsArray(Expression parent) {
        if (isArrayReducerCall(parent) || isArrayFlattenCall(parent) || isTypePreservingArrayCall(parent)
                || isArrayConcatCall(parent)) {
            // array_min / array_max have no map overload at all, and the type-preserving
            // transformations only accept an array
            return true;
        }
        if (isMapValuesCall(parent) || isMapKeysCall(parent)) {
            return false; // map_values takes a MAP
        }
        if (!(parent instanceof Function) || parent.children().size() < 2) {
            return false;
        }
        Expression index = parent.child(1);
        return index instanceof Literal
                && !((Literal) index).getDataType().isStringLikeType();
    }

    /**
     * Whether one container-taking call resolves to the MAP signature (see
     * booleanContainerPayload): map_values always reads a map, and an element_at
     * whose INDEX is a STRING selects the MAP overload. A non-literal index cannot be
     * classified and is not repaired.
     *
     * Parameter parent: the call
     * Returns: whether the call can only be the MAP overload
     */
    private static boolean containerIsMap(Expression parent) {
        if (isMapValuesCall(parent) || isMapKeysCall(parent) || isTypePreservingMapCall(parent)) {
            return true;
        }
        if (!(parent instanceof Function) || parent.children().size() < 2) {
            return false;
        }
        Expression index = parent.child(1);
        return index instanceof Literal && ((Literal) index).getDataType().isStringLikeType();
    }

    /** The MAP KEY type a repaired bare-NULL container has to carry: the written index's type. */
    private static DataType mapKeyTypeOf(Expression parent) {
        if (parent instanceof Function && parent.children().size() >= 2) {
            Expression index = parent.child(1);
            if (index instanceof Literal) {
                return ((Literal) index).getDataType();
            }
        }
        return StringType.INSTANCE;
    }

    // IN (constant list) -> list placeholder.

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, Expression parent) {
        // Rewrite the compareExpr first (its parent is the InPredicate, position 0 in
        // InPredicate.children; the options take positions 1..n below)
        Expression newCompare;
        childIndexStack.push(0);
        try {
            newCompare = inPredicate.getCompareExpr().accept(this, inPredicate);
        } finally {
            childIndexStack.pop();
        }

        if (inPredicate.optionsAreLiterals()) {
            // The whole constant list -> SpmConstList. Deduplicate like a scalar
            // literal: an identical IN predicate (same value list, same parent
            // structure, same child position) reuses the existing placeholder id. This
            // keeps the ids shared between the bind tree and the (separately parsed)
            // plan tree, so the values extracted against the bind tree substitute into
            // the plan tree.
            int childIndex = currentChildIndex();
            long blockId = currentBlockId;
            int projectItem = currentProjectItem();
            long rootOrdinal = currentRootOrdinal;
            String occurrencePath = currentOccurrencePath();
            PlaceholderKey key = PlaceholderKey.of(inPredicate, parent, childIndex,
                    blockId, projectItem, rootOrdinal, occurrencePath, parentSignatures);
            Expression existing = placeholderIndex.get(key);
            if (existing != null) {
                return existing;
            }
            long id = nextId++;
            SpmConstList list = SpmConstList.of(id, inPredicate.getOptions());
            InPredicate placeholder = new InPredicate(newCompare, ImmutableList.of(list));
            placeholderIndex.put(key, placeholder);
            return placeholder;
        }

        // Non-constant list (contains subqueries/expressions): rewrite each child under
        // its OWN position. Visiting every option with the same (stale outer) child index
        // merged repeated values - "a IN (1, b, 1)" gave the two 1 positions one
        // placeholder id, and a structurally matching "a IN (2, b, 3)" was then rejected
        // by the one-id-one-value rule. Positions mirror InPredicate.children.
        List<Expression> newOptions = new ArrayList<>(inPredicate.getOptions().size());
        for (int i = 0; i < inPredicate.getOptions().size(); i++) {
            Expression option = inPredicate.getOptions().get(i);
            childIndexStack.push(i + 1);
            Expression newOption;
            try {
                newOption = option.accept(this, inPredicate);
            } finally {
                childIndexStack.pop();
            }
            newOptions.add(newOption);
        }
        return new InPredicate(newCompare, newOptions);
    }

    // subquery (constants inside subqueries are parameterized too).

    @Override
    public Expression visitSubqueryExpr(SubqueryExpr subqueryExpr, Expression parent) {
        // parameterize the InSubquery compare expression (its placeholder id must stay in
        // sync with the user side during matching)
        Expression newCompare = subqueryExpr instanceof InSubquery
                ? ((InSubquery) subqueryExpr).getCompareExpr().accept(this, subqueryExpr) : null;
        // Parameterize supported literal positions in the subquery plan (filters, having,
        // projections, aggregate... and nested subqueries recursively). The
        // expressions of the subquery plan are NEW roots: push the root sentinel so a
        // bare-literal root records -1 instead of a stale outer child index. The plan
        // is also a NEW query block (see enterQueryBlock), restored afterwards so
        // sibling subqueries get distinct ids while the outer block numbering resumes.
        enterQueryBlock();
        LogicalPlan newPlan;
        // A subquery plan is a NEW query block: the GROUP BY mirror window of the clause
        // being transformed (see setGroupByMirrorLookup) must NOT leak into it - its
        // expressions are not copies of the outer aggregate's group-by keys, and reusing
        // the outer placeholder ids across blocks would break the per-block value model.
        java.util.function.Function<String, Expression> savedMirror = groupByMirrorLookup;
        groupByMirrorLookup = null;
        try {
            newPlan = SPMPlanTreeSupport.transform(
                    subqueryExpr.getQueryPlan(), new SubqueryBlockTransform());
        } finally {
            groupByMirrorLookup = savedMirror;
            exitQueryBlock();
        }
        if (newPlan == subqueryExpr.getQueryPlan() && newCompare == null) {
            return subqueryExpr;
        }
        Expression rebuilt = SPMSubquerySupport.rebuildSubquery(subqueryExpr, newPlan);
        if (newCompare != null && rebuilt instanceof InSubquery) {
            InSubquery in = (InSubquery) rebuilt;
            return new InSubquery(newCompare, in.getQueryPlan(), in.getCorrelateSlots(),
                    in.getTypeCoercionExpr(), in.isNot());
        }
        return rebuilt;
    }

    // default: rebuild children bottom-up (parent-aware).

    @Override
    public Expression visit(Expression expr, Expression parent) {
        // A GROUP BY mirror window (see setGroupByMirrorLookup) makes a nested copy of an
        // already parameterized group-by expression reuse THAT expression (its placeholder
        // ids): the analyzer only accepts
        // "GROUP BY substr(x, 1, 20)... ORDER BY / HAVING substr(x, 1, 20)" when the two
        // copies are literally equal, and two independently parameterized copies never are
        // (each occurrence is its own expression root, see visitLiteral).
        if (groupByMirrorLookup != null) {
            // the KEY is the SCOPE-AWARE identity, not the rendered text (see
            // SPMPlanTreeSupport.expressionIdentity): db1.f(x) and db2.f(x) render identically
            // and would otherwise share one mirror entry
            Expression mirror = groupByMirrorLookup.apply(
                    SPMPlanTreeSupport.expressionIdentity(expr));
            if (mirror != null) {
                return mirror;
            }
        }
        List<Expression> children = expr.children();
        if (children.isEmpty()) {
            return expr;
        }
        boolean changed = false;
        List<Expression> newChildren = new ArrayList<>(children.size());
        // A projected value can reach a Boolean consumer through a derived-table or CTE slot.
        // Retype NULL-producing expressions and containers before binding can erase their markers.
        boolean projectedValue = currentProjectItem() >= 0
                && (parent == null || parent instanceof org.apache.doris.nereids.trees.expressions.Alias
                        || parent instanceof org.apache.doris.nereids.analyzer.UnboundAlias);
        // Each STRUCT field has its own type. A derived-table consumer can bind that
        // type as a Boolean operand and discard a field whose marker still has NullType.
        boolean typedOutput = projectedValue || isStructFieldPosition(parent);
        boolean booleanConditionalOperand = (typedOutput
                || parent instanceof And || parent instanceof Or || parent instanceof Not)
                && producesNullType(expr);
        boolean containerElementOperand = (typedOutput
                || booleanConditionalDepth > 0 && isNullTypeContainerPosition(parent))
                && isAllNullContainer(expr);

        if (booleanConditionalOperand) {
            booleanConditionalDepth++;
        }
        if (containerElementOperand) {
            containerElementDepth++;
        }
        int savedMapKeyDepth = mapKeyDepth;
        int inputMapKeyDepth = containerElementDepth > 0 ? mapKeyDepthForInput(expr) : NO_MAP_KEY_DEPENDENCY;
        try {
            for (int i = 0; i < children.size(); i++) {
                Expression child = children.get(i);
                // each child's parent is this expression; remember its position so sibling
                // literals ("1 + 1") never merge into one placeholder id
                childIndexStack.push(i);
                Expression newChild;
                int savedContainerDepth = containerElementDepth;
                int savedBooleanDepth = booleanConditionalDepth;
                if (expr instanceof WindowExpression && i != 0
                        || containerElementDepth > 0 && !isContainerElementPosition(expr)) {
                    // Excluded positions also exclude their descendants (map keys, counts,
                    // and conditional predicates), rather than only a direct literal.
                    containerElementDepth = 0;
                    booleanConditionalDepth = 0;
                    mapKeyDepth = NO_MAP_KEY_DEPENDENCY;
                } else {
                    mapKeyDepth = inputMapKeyDepth;
                }
                try {
                    newChild = child.accept(this, expr);
                } finally {
                    containerElementDepth = savedContainerDepth;
                    booleanConditionalDepth = savedBooleanDepth;
                    mapKeyDepth = savedMapKeyDepth;
                    childIndexStack.pop();
                }
                newChildren.add(newChild);
                if (newChild != child) {
                    changed = true;
                }
            }
        } finally {
            if (containerElementOperand) {
                containerElementDepth--;
            }
            if (booleanConditionalOperand) {
                booleanConditionalDepth--;
            }
        }
        if (!changed) {
            return expr;
        }
        try {
            return expr.withChildren(newChildren);
        } catch (RuntimeException e) {
            // This expression type does not implement withChildren -> keep as-is (safe fallback)
            return expr;
        }
    }
}
