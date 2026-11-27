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

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.spm.SPMContextFunctions;
import org.apache.doris.nereids.spm.SPMPlanTreeSupport;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateStruct;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parameterizes supported literal positions with scalar or IN-list markers. One builder retains marker records and
 * allocates ids across bind and plan trees. Reuse requires the same value, parent signature, child position, query
 * block, projection item, expression-root ordinal and occurrence path. GROUP BY mirrors can deliberately reuse a
 * transformed expression to preserve analyzer equality.
 *
 * Syntax-bearing literals such as LIKE escape arguments are kept concrete. Every scalar marker retains its
 * original literal, including untyped NULL and containers with NULL elements; analysis handles type coercion.
 */
public class SPMPlaceholderBuilder extends ExpressionVisitor<Expression, Expression>
        implements SPMPlanTreeSupport.ExprTransform {

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

    /** Marker-id sets of grouping expressions that need the capture-time column proof. */
    private final Set<Set<Long>> groupingColumnTargets = new HashSet<>();

    /** The enclosing block's root-ordinal counter, restored by exitQueryBlock. */
    private final Deque<Long> rootOrdinalStack = new ArrayDeque<>();

    /** The enclosing root's ordinal, restored by exitQueryBlock (see enterQueryBlock). */
    private final Deque<Long> currentRootOrdinalStack = new ArrayDeque<>();

    /** Installs / clears the GROUP BY mirror lookup window (null = disabled). */
    @Override
    public void setGroupByMirrorLookup(java.util.function.Function<String, Expression> lookup) {
        this.groupByMirrorLookup = lookup;
    }

    /** Returns the marker sets that the grouping-column proof must preserve. */
    public Set<Set<Long>> getGroupingColumnTargets() {
        return Set.copyOf(groupingColumnTargets);
    }

    /** Records targets across both bind and plan trees before optimization can erase their provenance. */
    @Override
    public void noteParameterizedGroupingTarget(Expression target) {
        groupingColumnTargets.add(Set.copyOf(SPMPlanTreeSupport.collectPlaceholderIds(target)));
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
        public void noteParameterizedGroupingTarget(Expression target) {
            SPMPlaceholderBuilder.this.noteParameterizedGroupingTarget(target);
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

    /** Tests a parsed builtin name or the corresponding bound struct constructor. */
    private static boolean isStructCall(Expression expression, boolean named) {
        if (expression instanceof UnboundFunction) {
            UnboundFunction call = (UnboundFunction) expression;
            return (call.getDbName() == null || call.getDbName().isEmpty())
                    && (named ? "named_struct" : "struct").equalsIgnoreCase(call.getName());
        }
        return named ? expression instanceof CreateNamedStruct : expression instanceof CreateStruct;
    }

    @Override
    public Expression visitLiteral(Literal literal, Expression parent) {
        // A constant SELECT item can itself be a grouping key. GROUPING(1) is
        // a reference to that expression, not a fresh independent value.
        if (groupByMirrorLookup != null && SPMPlanTreeSupport.groupingFunctionName(parent) != null) {
            Expression mirror = groupByMirrorLookup.apply(SPMPlanTreeSupport.expressionIdentity(literal));
            if (mirror != null) {
                return mirror;
            }
        }
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
        SpmConstVar placeholder = SpmConstVar.of(id, literal);
        placeholderIndex.put(key, placeholder);
        return placeholder;
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
        if (SPMContextFunctions.isPrecisionCall(expr)) {
            return expr;
        }
        List<Expression> children = expr.children();
        if (children.isEmpty()) {
            return expr;
        }
        boolean changed = false;
        List<Expression> newChildren = new ArrayList<>(children.size());
        for (int i = 0; i < children.size(); i++) {
            Expression child = children.get(i);
            // Each child's parent is this expression; remember its position so sibling
            // literals ("1 + 1") never merge into one placeholder id.
            childIndexStack.push(i);
            Expression newChild;
            try {
                newChild = child.accept(this, expr);
            } finally {
                childIndexStack.pop();
            }
            newChildren.add(newChild);
            if (newChild != child) {
                changed = true;
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
