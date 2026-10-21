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
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * SPMPlaceholderBuilder
 *
 * Replaces the literal constants in a query with
 * SPM placeholder functions so that one baseline can match structurally identical
 * queries. Core replacement rules:
 *
 * - Literal -> SpmConstVar (scalar placeholder with an auto-increment id)
 * - "IN (constant list)" -> InPredicate(expr, [SpmConstList(id, ...)]) (list placeholder)
 *
 * The auto-increment id guarantees that placeholders in the same position of bindSql and
 * planSql share the same id (in path A auto-capture both come from the same already
 * parameterized tree, so the ids naturally match).
 *
 * Placeholder deduplication follows: a literal
 * reuses an existing placeholder id only when the literal value (Step 1), its parent
 * expression structure (Step 2, PlaceholderExpr.matches) AND its position inside that
 * parent (child index) are equal. This is important for matching correctness: with
 * value-only dedup, "a = 1 AND b = 1" would assign one id to both literals and a user
 * query "a = 5 AND b = 7" could never match; without the child index, the two operands
 * of "1 + 1" would share one id and a similar query "2 + 1" could never match either.
 */
public class SPMPlaceholderBuilder extends ExpressionVisitor<Expression, Expression>
        implements SPMPlanTreeSupport.ExprTransform {

    /** Auto-increment placeholder id counter (starts at 1). */
    private long nextId = 1;

    /** Placeholder record list. */
    private final List<PlaceholderExpr> placeholderExprs = new ArrayList<>();

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
     * Enclosing query-block ids (see enterQueryBlock()): a nested block's id is
     * restored on exit, so SIBLING derived tables / CTE bodies get distinct ids while the
     * enclosing block numbering resumes - the same discipline the subquery-expression
     * blocks already followed.
     */
    private final Deque<Long> blockIdStack = new ArrayDeque<>();

    /**
     * Projection ITEM positions of the SELECT-list slots currently being visited (see
     * enterProjectionItem(int)): SELECT 1 AS x, 1 AS y must give its two
     * literals different placeholder ids although value, parent structure and child
     * position coincide (see PlaceholderExpr#matches).
     */
    private final Deque<Integer> projectItemStack = new ArrayDeque<>();

    /**
     * Ordinal of the expression ROOT currently being transformed, counted per query block in
     * walk order (see beginExpressionRoot): two equal literals in DIFFERENT roots of one
     * block (the ON clauses of two joins, two conjuncts of one filter, ...) used to share one
     * placeholder id although they are independent slots - a query that varies them
     * separately (1/1 -> 2/3) was then rejected by recordPlaceholderValue and the saved
     * baseline could never be used (the reviewer's round-58 finding). The counting restarts
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
     * Whether a bare integer GROUP BY ordinal references a SELECT item that carries a
     * placeholder (see SPMPlanTreeSupport#visitLogicalAggregate). The ordinal itself stays
     * CONCRETE, but the analyzer resolves it to that item's (parameterized) expression, and
     * the frozen rendering of a placeholder group key is NOT faithful: the capture optimizer
     * rewrote `GROUP BY &lt;placeholder&gt;` into a child column, so a frozen replay of
     * `SELECT 1 AS c, count(*) ... GROUP BY 1` grouped by `k` while the user's own query
     * grouped by the constant. The CREATE keeps the user planSql then and replays the
     * parameterized tree, whose analysis resolves the ordinal in the caller's own query.
     */
    private boolean parameterizedGroupByOrdinalTarget;

    /** The enclosing block's root-ordinal counter, restored by exitQueryBlock(). */
    private final Deque<Long> rootOrdinalStack = new ArrayDeque<>();

    /** The enclosing root's ordinal, restored by exitQueryBlock() (see enterQueryBlock). */
    private final Deque<Long> currentRootOrdinalStack = new ArrayDeque<>();

    /** Installs / clears the GROUP BY mirror lookup window (null = disabled). */
    public void setGroupByMirrorLookup(java.util.function.Function<String, Expression> lookup) {
        this.groupByMirrorLookup = lookup;
    }

    /** See parameterizedGroupByOrdinalTarget. */
    public boolean hasParameterizedGroupByOrdinalTarget() {
        return parameterizedGroupByOrdinalTarget;
    }

    /** Marks one bare integer GROUP BY ordinal whose SELECT item carries placeholders. */
    public void noteParameterizedGroupByOrdinalTarget() {
        this.parameterizedGroupByOrdinalTarget = true;
    }

    /**
     * Parameterizes a list of expressions with this single shared builder (entry point).
     *
     * Every expression is parameterized in order with the same builder, so placeholder
     * ids stay globally unique and position-aligned across all query blocks (the topmost
     * block's literals get the lowest ids, then derived tables / CTE bodies / nested
     * blocks, in DFS order). This is what keeps the bind expressions of a multi-block
     * baseline matched against the user query block by block.
     *
     * @param exprs the raw expressions to parameterize (one per query block)
     * @return the parameterized expressions (containing SpmConstVar / SpmConstList)
     */
    public List<Expression> parameterizeExpressions(List<Expression> exprs) {
        List<Expression> parameterized = new ArrayList<>(exprs.size());
        for (Expression expr : exprs) {
            // each element is one query block of the same tree: its own block scope
            startNewTree();
            beginExpressionRoot();
            parameterized.add(expr.accept(this, null));
        }
        return parameterized;
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
     * SPMPlanTreeSupport.ExprTransform#enterQueryBlock()). Passing a bare lambda
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
     * org.apache.doris.nereids.spm.SPMPlanTreeSupport.ExprTransform#enterQueryBlock()):
     * the block gets its own monotonic id, the enclosing one is restored by
     * exitQueryBlock().
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

    public List<PlaceholderExpr> getPlaceholderExprs() {
        return placeholderExprs;
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
     * PlaceholderExpr#occurrencePath.
     */
    private String currentOccurrencePath() {
        if (childIndexStack.isEmpty()) {
            return "";
        }
        Object[] positions = childIndexStack.toArray();
        StringBuilder sb = new StringBuilder();
        for (int i = positions.length - 1; i >= 0; i--) {
            if (sb.length() > 0) {
                sb.append('/');
            }
            sb.append(positions[i]);
        }
        return sb.toString();
    }

    // ==================== literal -> scalar placeholder ====================

    /**
     * Whether the expression's TYPE is NullType.
     *
     * The type is derived structurally because this builder walks the UNBOUND plan, where
     * {@code getDataType()} throws. A node produces NullType when EVERY value it can yield is
     * NULL - a NULL literal, a CASE whose every result does, an IF whose two value arguments do,
     * a COALESCE / IFNULL / NVL / NULLIF / GREATEST / LEAST all of whose arguments do. Collected as
     * a Boolean operand (directly, or through a projected slot) such an expression is REPLACED by
     * the analyzer with a typed constant NULL, which deletes the placeholders inside it.
     *
     * The test is RECURSIVE on purpose (round-78 #5): a conditional nested in the RESULT of
     * another conditional (`k &gt; 0 OR (CASE WHEN k = 0 THEN ifnull(NULL, NULL) ELSE NULL END)`)
     * yields NullType although its result is not a NULL LITERAL, so a literal-only test left its
     * inner markers NullType and the consumer erased them - a TRUE variant then replayed as NULL.
     * The same recursion covers the custom-signature producers: {@code GreatestLeast#customSignature}
     * returns the arguments' common type, i.e. NullType for {@code greatest(NULL, NULL)} (round-78
     * #1, LEAST shares the path).
     *
     * @param expr the expression to classify
     * @return whether every value the expression can yield is NULL
     */
    private static boolean producesNullType(Expression expr) {
        if (expr == null) {
            return false;
        }
        if (expr.isNullLiteral()) {
            return true;
        }
        List<Expression> children = expr.children();
        if (expr instanceof CaseWhen) {
            CaseWhen caseWhen = (CaseWhen) expr;
            for (WhenClause clause : caseWhen.getWhenClauses()) {
                if (!producesNullType(clause.getResult())) {
                    return false;
                }
            }
            return !caseWhen.getDefaultValue().isPresent()
                    || producesNullType(caseWhen.getDefaultValue().get());
        }
        if (isIfConditional(expr)) {
            return children.size() == 3 && producesNullType(children.get(1))
                    && producesNullType(children.get(2));
        }
        if (isIfNullConditional(expr) || isNullIfConditional(expr)
                || isGreatestLeastConditional(expr) || isCoalesceConditional(expr)) {
            // every argument contributes a value the result may equal: all-NULL arguments make
            // the whole call NullType (IFNULL / NVL / NULLIF / GREATEST / LEAST / COALESCE)
            if (children.isEmpty()) {
                return false;
            }
            for (Expression child : children) {
                if (!producesNullType(child)) {
                    return false;
                }
            }
            return true;
        }
        if (isElementAtCall(expr)) {
            // `element_at(array(NULL), 1)` takes its type from the ARRAY's element type, so an
            // all-NULL array literal makes the WHOLE call NullType: a Boolean consumer replaced
            // the two markers of `k > 0 OR element_at(array(NULL), 1)` with a typed constant and
            // the `element_at(array(NULL), 1)` variant had nothing to substitute (round-79 #3).
            return !children.isEmpty() && isAllNullArray(children.get(0));
        }
        return false;
    }

    /**
     * Whether the expression is an all-NULL array (see producesNullType).
     *
     * An array LITERAL whose every element produces NullType has type ARRAY&lt;NULL_TYPE&gt;, so a
     * consumer that reads an ELEMENT (`element_at`) is NullType as well. Both spellings are
     * accepted: the parsed {@link ArrayLiteral} and the unbound `array(...)` call, which is only
     * a {@link Function} named {@code array} before binding.
     *
     * @param expr the expression to classify
     * @return whether it is an all-NULL array value
     */
    private static boolean isAllNullArray(Expression expr) {
        if (expr == null) {
            return false;
        }
        if (expr.isNullLiteral()) {
            // a NULL literal is untyped ARRAY<NULL_TYPE> in an array position
            return true;
        }
        if (!isArrayConstructor(expr)) {
            return false;
        }
        List<Expression> elements = expr.children();
        if (elements.isEmpty()) {
            return false;
        }
        for (Expression element : elements) {
            if (!producesNullType(element)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether the expression builds an ARRAY value (see producesNullType).
     *
     * Both spellings are accepted: the parsed {@link ArrayLiteral} and the unbound `array(...)`
     * call, which is only a {@link Function} named {@code array} before binding.
     *
     * @param expr the expression to classify
     * @return whether it is an array constructor
     */
    private static boolean isArrayConstructor(Expression expr) {
        if (expr instanceof ArrayLiteral) {
            return true;
        }
        return expr instanceof Function
                && "array".equalsIgnoreCase(((Function) expr).getName());
    }

    /**
     * Whether the expression is an {@code element_at} call (see producesNullType).
     *
     * Matched by NAME through {@link Function} for the unbound plan, and by the analyzed
     * {@link ElementAt} node for a bound one.
     *
     * @param expr the expression to classify
     * @return whether it is an element_at call
     */
    private static boolean isElementAtCall(Expression expr) {
        if (expr instanceof ElementAt) {
            return true;
        }
        return expr instanceof Function
                && "element_at".equalsIgnoreCase(((Function) expr).getName());
    }

    /**
     * Whether the expression is a {@code GREATEST} / {@code LEAST} call (see producesNullType).
     *
     * {@code GreatestLeast.customSignature} returns the arguments' common type, so an all-NULL
     * call is NullType and a Boolean consumer replaces it outright (round-78 #1). LEAST shares the
     * class, so both names are accepted.
     *
     * @param expr the expression to classify
     * @return whether it is a GREATEST / LEAST call
     */
    private static boolean isGreatestLeastConditional(Expression expr) {
        if (!(expr instanceof Function)) {
            return false;
        }
        String name = ((Function) expr).getName();
        return "greatest".equalsIgnoreCase(name) || "least".equalsIgnoreCase(name);
    }

    /**
     * Whether the expression is an {@code IF} / {@code COALESCE} CALL. Matched by NAME through
     * the {@link Function} interface because the parameterized plan is the UNBOUND one, where
     * `IF(...)` is still an {@link UnboundFunction} (only the analyzed {@link If} node exists
     * after binding).
     *
     * @param expr the expression to classify
     * @return whether it is an IF call
     */
    private static boolean isIfConditional(Expression expr) {
        return expr instanceof Function && "if".equalsIgnoreCase(((Function) expr).getName());
    }

    /**
     * Whether the expression is an {@code IFNULL} / {@code NVL} call (both parse to the same
     * {@link Function} named {@code ifnull}; {@code nvl} is accepted as the spelling as well).
     *
     * It belongs to the all-NULL conditional family even though it has no condition argument:
     * {@code ifnull(NULL, NULL)} has NullType, so a Boolean consumer replaces the WHOLE call
     * with a typed constant NULL exactly like an all-NULL CASE - `k > 0 OR ifnull(NULL, NULL)`
     * lost both markers and the matching `ifnull(NULL, TRUE)` variant had nothing to
     * substitute (round-76 #1).
     *
     * @param expr the expression to classify
     * @return whether it is an IFNULL / NVL call
     */
    private static boolean isIfNullConditional(Expression expr) {
        if (!(expr instanceof Function)) {
            return false;
        }
        String name = ((Function) expr).getName();
        return "ifnull".equalsIgnoreCase(name) || "nvl".equalsIgnoreCase(name);
    }

    /**
     * Whether the expression is a {@code NULLIF} call.
     *
     * NULLIF is a THIRD all-NULL producer, on its own signature path: {@code NullIf.customSignature}
     * returns the COMMON type of its two arguments, so {@code nullif(NULL, NULL)} has NullType and a
     * Boolean consumer ("k > 0 OR nullif(NULL, NULL)") replaces the whole call with a typed constant
     * NULL - the markers of both arguments disappear and the matching {@code nullif(TRUE, NULL)}
     * variant has no marker left to substitute with TRUE (round-77 #3). It is not covered by the
     * IFNULL / NVL branch above: a different function with its own signature rule.
     *
     * @param expr the expression to classify
     * @return whether it is a NULLIF call
     */
    private static boolean isNullIfConditional(Expression expr) {
        return expr instanceof Function
                && "nullif".equalsIgnoreCase(((Function) expr).getName());
    }

    /**
     * Whether the expression is a {@code COALESCE} call (see isIfConditional).
     *
     * @param expr the expression to classify
     * @return whether it is a COALESCE call
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
     * columns, and a BOOLEAN-typed NULL there (`k = CAST(NULL AS BOOLEAN)`) is a type error in
     * the replayed plan.
     *
     * @param parent the literal's parent expression
     * @return whether the literal sits in a result position
     */
    private boolean isConditionalResultPosition(Expression parent) {
        return isConditionalResultPosition(parent, currentChildIndex());
    }

    /**
     * The position-aware form of {@link #isConditionalResultPosition(Expression)}.
     *
     * @param parent     the conditional
     * @param childIndex the position of the child inside it
     * @return whether that position holds a result
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
        if (isArrayConstructor(parent)) {
            // an ARRAY's elements are the array's VALUES: under an all-NULL element type the
            // consuming call is NullType and gets replaced as a whole, so every element marker
            // has to be BOOLEAN already (round-79 #3)
            return true;
        }
        return isCoalesceConditional(parent);
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
        for (PlaceholderExpr record : placeholderExprs) {
            if (record.matches(literal, parent, childIndex, blockId, projectItem, rootOrdinal,
                    occurrencePath)) {
                return record.getPlaceholderExpr();
            }
        }
        long id = nextId++;
        SpmConstVar placeholder = SpmConstVar.of(id,
                markerPayload(literal, parent, currentProjectItem() >= 0,
                        booleanConditionalDepth > 0 && isConditionalResultPosition(parent)));
        placeholderExprs.add(new PlaceholderExpr(literal, placeholder, parent, childIndex,
                blockId, projectItem, rootOrdinal, occurrencePath));
        return placeholder;
    }

    /**
     * The literal the placeholder keeps as its PAYLOAD (readability only - the replay
     * substitutes the whole placeholder by id).
     *
     * A captured NULL is special: a NULL literal's type is NullType, and the analyzer
     * COERCES a NullType operand of AND / OR / NOT to a typed BOOLEAN null literal
     * (ExpressionAnalyzer.visitOr / visitAnd / visitNot). That rewrite REPLACES the whole
     * operand, i.e. it deleted the placeholder: the frozen SQL of
     * `k > 0 OR NULL` contained `k > _spm_const_var(1, 0) OR NULL` and the second id was
     * gone, so a matching `k > 0 OR TRUE` (same literal-masked digest, id 2 extracted as
     * TRUE) had no marker left to substitute and the replay returned NULL instead of TRUE.
     * Typing the payload BOOLEAN keeps the marker where the analyzer expects a boolean
     * operand: it is not a NullType operand any more, the value substituted at replay
     * still lands in the same slot, and matching is unaffected (a placeholder accepts any
     * user literal, see SPMAstCheckVisitor).
     *
     * @param literal the captured literal
     * @param parent  the literal's parent expression (null at an expression root)
     * @return the payload the placeholder keeps
     */
    private static Literal markerPayload(Literal literal, Expression parent,
            boolean projectionItem, boolean booleanConditionalResult) {
        boolean booleanOperand = parent instanceof And || parent instanceof Or
                || parent instanceof Not;
        if (literal.isNullLiteral() && (booleanOperand || booleanConditionalResult)) {
            return NullLiteral.BOOLEAN_INSTANCE;
        }
        // A projected NULL (`SELECT NULL AS n` / a bare `SELECT NULL`) exposes a NullType
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

    // ==================== IN (constant list) -> list placeholder ====================

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, Expression parent) {
        // Rewrite the compareExpr first (its parent is the InPredicate, position 0 in
        // InPredicate.children(); the options take positions 1..n below)
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
            for (PlaceholderExpr record : placeholderExprs) {
                if (record.matches(inPredicate, parent, childIndex, blockId, projectItem,
                        rootOrdinal, occurrencePath)) {
                    return record.getPlaceholderExpr();
                }
            }
            long id = nextId++;
            SpmConstList list = SpmConstList.of(id, inPredicate.getOptions());
            InPredicate placeholder = new InPredicate(newCompare, ImmutableList.of(list));
            placeholderExprs.add(new PlaceholderExpr(inPredicate, placeholder, parent, childIndex,
                    blockId, projectItem, rootOrdinal, occurrencePath));
            return placeholder;
        }

        // Non-constant list (contains subqueries/expressions): rewrite each child under
        // its OWN position. Visiting every option with the same (stale outer) child index
        // merged repeated values - "a IN (1, b, 1)" gave the two 1 positions one
        // placeholder id, and a structurally matching "a IN (2, b, 3)" was then rejected
        // by the one-id-one-value rule. Positions mirror InPredicate.children().
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

    // ==================== subquery (constants inside subqueries are parameterized too) ====================

    @Override
    public Expression visitSubqueryExpr(SubqueryExpr subqueryExpr, Expression parent) {
        // parameterize the InSubquery compare expression (its placeholder id must stay in
        // sync with the user side during matching)
        Expression newCompare = subqueryExpr instanceof InSubquery
                ? ((InSubquery) subqueryExpr).getCompareExpr().accept(this, subqueryExpr) : null;
        // parameterize EVERY literal of the subquery's own plan tree (filters, having,
        // projections, aggregate ... and nested subqueries recursively). The
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

    // ==================== default: rebuild children bottom-up (parent-aware) ====================

    @Override
    public Expression visit(Expression expr, Expression parent) {
        // A GROUP BY mirror window (see setGroupByMirrorLookup) makes a nested copy of an
        // already parameterized group-by expression reuse THAT expression (its placeholder
        // ids): the analyzer only accepts
        // "GROUP BY substr(x, 1, 20) ... ORDER BY / HAVING substr(x, 1, 20)" when the two
        // copies are literally equal, and two independently parameterized copies never are
        // (each occurrence is its own expression root, see visitLiteral).
        if (groupByMirrorLookup != null) {
            Expression mirror = groupByMirrorLookup.apply(expr.toString());
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
        // A CONDITIONAL (CASE / IF / COALESCE) that IS a Boolean operand (OR / AND / NOT)
        // contributes BOOLEAN-typed markers: an all-NULL conditional has NullType, and the
        // analyzer then replaces the WHOLE operand with a typed constant NULL - deleting every
        // placeholder inside it (`k > 0 OR CASE WHEN k = 0 THEN NULL ELSE NULL END` lost both
        // ids and a TRUE variant had nothing to substitute).
        //
        // A PROJECTED conditional (the expression of a SELECT item, `... AS n`) reaches its
        // Boolean consumer through a DERIVED / CTE OUTPUT SLOT, i.e. across the
        // producer/consumer query-block boundary: `SELECT k > 0 OR n FROM
        // (SELECT k, CASE WHEN k = 0 THEN NULL ELSE NULL END AS n FROM t) s`. The conditional
        // has no direct Boolean parent here, so the guard above left its markers NullType,
        // the derived slot n stayed NullType, and the CONSUMER's ExpressionAnalyzer.visitOr
        // replaced that entire slot with a typed constant NULL while the frozen plan was
        // optimized - the frozen projection became `k > _spm_const_var(A) OR NULL` and the
        // matching ELSE TRUE variant returned NULL instead of TRUE. Typing the conditional's
        // NULL markers BOOLEAN makes the projected column BOOLEAN, so the slot (and the
        // markers inside it) survive any later binding; a NULL value stays NULL through any
        // coercion. Same rule the projected NULL literal already follows (see markerPayload).
        //
        // The CASE-only repair left the SAME defect for every other all-NULL conditional: a
        // projected `IF(k = 0, NULL, NULL)` consumed by OR is replaced the same way, so the
        // family is recognised structurally (see producesNullType) instead of by
        // node class.
        boolean projectedConditional = producesNullType(expr)
                && currentProjectItem() >= 0
                && (parent == null
                        || parent instanceof org.apache.doris.nereids.trees.expressions.Alias
                        || parent instanceof org.apache.doris.nereids.analyzer.UnboundAlias);
        boolean booleanConditionalOperand = producesNullType(expr)
                && (parent instanceof And || parent instanceof Or || parent instanceof Not
                        || projectedConditional);

        if (booleanConditionalOperand) {
            booleanConditionalDepth++;
        }
        try {
            for (int i = 0; i < children.size(); i++) {
                Expression child = children.get(i);
                // each child's parent is this expression; remember its position so sibling
                // literals ("1 + 1") never merge into one placeholder id
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
        } finally {
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
