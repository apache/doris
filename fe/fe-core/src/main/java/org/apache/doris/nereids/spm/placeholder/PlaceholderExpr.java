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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;

import java.util.List;
import java.util.Objects;

/**
 * PlaceholderExpr - a placeholder record.
 *
 * A placeholder record kept by SPMPlaceholderBuilder. Each time
 * SPMPlaceholderBuilder creates a placeholder during parameterization it records one
 * entry containing:
 *
 * - originalExpr: the original literal (or IN predicate)
 * - placeholderExpr: the placeholder expression it was replaced with (a SpmConstVar, or
 *   an InPredicate containing a SpmConstList)
 * - parentExpr: the parent expression structure (used for the "value + parent
 *   structure + child position" double check during cross-AST matching)
 * - childIndex: the position of the literal inside its direct parent (siblings with the
 *   same value must never share one id, see matches())
 */
public class PlaceholderExpr {

    /** The original literal expression (e.g. IntegerLiteral(1)). */
    private final Expression originalExpr;

    /** The placeholder expression it was replaced with (e.g. SpmConstVar(1, 1)). */
    private final Expression placeholderExpr;

    /** The parent expression structure (used for parent-structure matching). */
    private final Expression parentExpr;

    /** Position of the literal inside its direct parent; -1 for a parent-less root. */
    private final int childIndex;

    /** Query-block identity of the literal (see SPMPlaceholderBuilder#startNewTree):
     *  two literals in different query blocks never share one placeholder id. */
    private final long blockId;

    /**
     * POSITION of the SELECT-list item the literal was found in, or -1 when it is not
     * below a projection item (filters, join conditions, ...). Value + parent structure
     * + child position do not distinguish {@code SELECT 1 AS x, 1 AS y}: both literals
     * sit at child 0 of an Alias whose parent signature omits the alias name, so they
     * used to share one placeholder id and a variant with different values per column
     * could never match (reviewer round 32 #9).
     */
    private final int projectItem;

    /**
     * Constructs a PlaceholderExpr.
     *
     * @param originalExpr    the original literal
     * @param placeholderExpr the placeholder expression
     * @param parentExpr      the parent expression structure (may be null when there is
     *                        no parent node)
     * @param childIndex      the position of the literal inside its direct parent
     *                        (the child index); -1 when there is no parent node
     * @param blockId         the query block of the literal
     * @param projectItem     the SELECT-list item position of the literal; -1 outside a
     *                        projection
     */
    public PlaceholderExpr(Expression originalExpr, Expression placeholderExpr, Expression parentExpr,
            int childIndex, long blockId, int projectItem) {
        this.originalExpr = Objects.requireNonNull(originalExpr, "originalExpr can not be null");
        this.placeholderExpr = Objects.requireNonNull(placeholderExpr, "placeholderExpr can not be null");
        this.parentExpr = parentExpr;
        this.childIndex = childIndex;
        this.blockId = blockId;
        this.projectItem = projectItem;
    }

    /**
     * Constructs a PlaceholderExpr outside a projection item (see the primary
     * constructor); kept for callers that carry no projection context.
     */
    public PlaceholderExpr(Expression originalExpr, Expression placeholderExpr, Expression parentExpr,
            int childIndex, long blockId) {
        this(originalExpr, placeholderExpr, parentExpr, childIndex, blockId, -1);
    }

    public Expression getOriginalExpr() {
        return originalExpr;
    }

    public Expression getPlaceholderExpr() {
        return placeholderExpr;
    }

    public Expression getParentExpr() {
        return parentExpr;
    }

    public int getChildIndex() {
        return childIndex;
    }

    public long getBlockId() {
        return blockId;
    }

    public int getProjectItem() {
        return projectItem;
    }

    /**
     * Returns whether this record matches the given "value + parent structure + child
     * position" (corresponds to PlaceholderExpr.equals(expr, parent, index) in a
     * position aware dedup).
     *
     * The parent structure is compared by canonical SQL text (not by object equals):
     * the parent may contain a SubqueryExpr whose structural equality depends on its
     * inner plan (relation ids etc.), which is not stable across independently parsed
     * trees. Two semantically identical parents of the bind tree and the (separately
     * parsed) plan tree therefore normalize to the same text and reuse the same
     * placeholder id, keeping the ids of the two trees aligned.
     *
     * The child position must match as well: two literals under the SAME parent (e.g.
     * the two operands of "1 + 1", or "WHEN 0 THEN 0") have equal parent structures but
     * are different positions, and a similar query may carry different values there.
     * Sharing one id would make every occurrence of that id resolve to the same user
     * value, so such a similar query could never match (over-conservative dedup).
     *
     * @param expr       the original literal to match
     * @param parent     the parent expression to match
     * @param childIndex the position of the literal inside its direct parent
     * @return whether it matches
     */
    public boolean matches(Expression expr, Expression parent, int childIndex, long blockId) {
        return matches(expr, parent, childIndex, blockId, -1);
    }

    /**
     * Same as the four-argument overload with an explicit projection item position: a
     * literal below a DIFFERENT SELECT-list item of the same block never reuses this id
     * even when value / parent / child position coincide (see {@link #projectItem}).
     */
    public boolean matches(Expression expr, Expression parent, int childIndex, long blockId,
            int projectItem) {
        if (this.blockId != blockId || this.projectItem != projectItem) {
            // different query blocks (outer query vs subquery / derived table) or different
            // SELECT-list items: never share one id, or a user query with different values
            // in the two positions could never match
            return false;
        }
        if (!originalExpr.equals(expr)) {
            return false;
        }
        // A literal with no parent (top-level expression, e.g. an ORDER BY key that is a
        // bare literal) only reuses the id of another parent-less literal. Comparing a
        // non-null parentExpr against a null parent (or vice versa) must not crash on
        // parentSignature(null) and must not share an id across different nesting levels.
        if (parentExpr == null || parent == null) {
            return parentExpr == null && parent == null;
        }
        return this.childIndex == childIndex
                && parentSignature(parentExpr).equals(parentSignature(parent));
    }

    /**
     * Structural signature of a parent expression, used for the parent-structure check
     * of the dedup. Deliberately NOT based on toSql() of the whole parent: a
     * subquery's SQL text embeds its plan node ids (e.g. "LogicalProject[90]"), which
     * differ between independently parsed trees, and object equality of a subquery
     * depends on inner plan ids as well. The signature therefore walks the expression
     * tree class by class and normalizes any SubqueryExpr to a fixed marker, so
     * semantically identical parents of the bind tree and the (separately parsed) plan
     * tree produce the same signature and reuse the same placeholder id.
     *
     * Only a leaf (a slot, a literal, ...) is rendered by toSql() - a slot's
     * SQL is its (stable) column name, not an id - and it is prefixed by its class name
     * so two different leaf kinds that print the same text (e.g. an IntegerLiteral
     * "1" vs a DoubleLiteral "1.0" would not, but two leaf classes with equal SQL could)
     * are never treated as the same parent. A parent is in practice never a leaf (a
     * literal's parent always has children), so this branch is defensive.
     *
     * Residual signature collisions only ever cause an OVER-conservative dedup (two
     * distinct literal slots with the same child position sharing one placeholder id),
     * never a wrong rewrite: the Level 3 check requires every occurrence of one id to
     * resolve to the same user value (SPMAstCheckVisitor#recordPlaceholderValue), so a
     * user query with differing values is simply not matched.
     */
    private static String parentSignature(Expression expr) {
        if (expr instanceof SubqueryExpr) {
            return "<subquery>";
        }
        List<Expression> children = expr.children();
        if (children.isEmpty()) {
            return expr.getClass().getSimpleName() + ":" + expr.toSql();
        }
        StringBuilder sb = new StringBuilder(expr.getClass().getSimpleName());
        for (Expression child : children) {
            sb.append('(').append(parentSignature(child)).append(')');
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "PlaceholderExpr{original=" + originalExpr
                + ", placeholder=" + placeholderExpr
                + ", parent=" + parentExpr + "}";
    }
}
