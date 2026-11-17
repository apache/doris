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
import java.util.Map;
import java.util.Objects;

/**
 * PlaceholderExpr - a placeholder record.
 *
 * A placeholder record kept by SPMPlaceholderBuilder. Each time
 * SPMPlaceholderBuilder creates a placeholder during parameterization it records one
 * entry containing:
 *
 * originalExpr: the original literal (or IN predicate)
 * placeholderExpr: the placeholder expression it was replaced with (a SpmConstVar, or
 *   an InPredicate containing a SpmConstList)
 * parentExpr: the parent expression structure (used for the "value + parent
 *   structure + child position" double check during cross-AST matching)
 * childIndex: the position of the literal inside its direct parent (siblings with the
 *   same value must never share one id, see matches)
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

    /**
     * Query-block identity of the literal (see SPMPlaceholderBuilder.startNewTree):
     *  two literals in different query blocks never share one placeholder id.
     */
    private final long blockId;

    /**
     * POSITION of the SELECT-list item the literal was found in, or -1 when it is not
     * below a projection item (filters, join conditions,...). Value + parent structure
     * + child position do not distinguish SELECT 1 AS x, 1 AS y: both literals
     * sit at child 0 of an Alias whose parent signature omits the alias name, so they
     * used to share one placeholder id and a variant with different values per column
     * could never match.
     */
    private final int projectItem;

    /**
     * The ordinal of the expression ROOT the literal belongs to, counted per query block in
     * the walk order of one tree (see SPMPlaceholderBuilder.beginExpressionRoot): two equal
     * literals in DIFFERENT expression roots of the same block (e.g. the ON clauses of two
     * joins, or two conjuncts of one filter) are independent slots even when value, parent
     * structure and child position coincide - sharing one id would make a query that varies
     * them separately (1/1 -> 2/3) fail recordPlaceholderValue and never match the saved
     * baseline.
     */
    private final long rootOrdinal;

    /**
     * The child-position path from the transformed expression ROOT down to the literal
     * (e.g. "0/1"), empty for a root literal: two equal literals under STRUCTURALLY EQUAL
     * parents that sit at different places of ONE root ("a = 1 AND a = 1" - the parents
     * are the two And children) are independent slots as well, and only the path tells them
     * apart.
     */
    private final String occurrencePath;

    /**
     * Constructs a PlaceholderExpr.
     *
     * Parameter originalExpr: the original literal
     * Parameter placeholderExpr: the placeholder expression
     * Parameter parentExpr: the parent expression structure (may be null when there is
     *                        no parent node)
     * Parameter childIndex: the position of the literal inside its direct parent
     *                        (the child index); -1 when there is no parent node
     * Parameter blockId: the query block of the literal
     * Parameter projectItem: the SELECT-list item position of the literal; -1 outside a
     *                        projection
     * Parameter rootOrdinal: the ordinal of the expression root the literal belongs to
     *                        (-1 when the caller carries no root scope)
     * Parameter occurrencePath: the child-position path from that root down to the literal
     *                        ("" when the caller carries no path)
     */
    public PlaceholderExpr(Expression originalExpr, Expression placeholderExpr, Expression parentExpr,
            int childIndex, long blockId, int projectItem, long rootOrdinal,
            String occurrencePath) {
        this.originalExpr = Objects.requireNonNull(originalExpr, "originalExpr can not be null");
        this.placeholderExpr = Objects.requireNonNull(placeholderExpr, "placeholderExpr can not be null");
        this.parentExpr = parentExpr;
        this.childIndex = childIndex;
        this.blockId = blockId;
        this.projectItem = projectItem;
        this.rootOrdinal = rootOrdinal;
        this.occurrencePath = occurrencePath == null ? "" : occurrencePath;
    }

    /**
     * Constructs a PlaceholderExpr outside a projection item and without root scope (see the
     * primary constructor); kept for callers that carry neither.
     */
    public PlaceholderExpr(Expression originalExpr, Expression placeholderExpr, Expression parentExpr,
            int childIndex, long blockId) {
        this(originalExpr, placeholderExpr, parentExpr, childIndex, blockId, -1, -1, "");
    }

    /**
     * Constructs a PlaceholderExpr with a projection item but without root scope (see the
     * primary constructor).
     */
    public PlaceholderExpr(Expression originalExpr, Expression placeholderExpr, Expression parentExpr,
            int childIndex, long blockId, int projectItem) {
        this(originalExpr, placeholderExpr, parentExpr, childIndex, blockId, projectItem, -1, "");
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

    public long getRootOrdinal() {
        return rootOrdinal;
    }

    public String getOccurrencePath() {
        return occurrencePath;
    }

    /**
     * Matches the original expression, query block, default projection and occurrence scopes, child position and
     * parent signature. Parent signatures compare expression classes and recursively rendered leaves, with
     * subquery plans represented by a fixed marker. They do not use canonical SQL for the whole parent. Two
     * parentless expressions match after the expression and scope checks, without a child-index comparison.
     */
    public boolean matches(Expression expr, Expression parent, int childIndex, long blockId) {
        return matches(expr, parent, childIndex, blockId, -1, -1, "");
    }

    /**
     * Same as the four-argument overload with an explicit projection item position: a
     * literal below a DIFFERENT SELECT-list item of the same block never reuses this id
     * even when value / parent / child position coincide (see projectItem).
     */
    public boolean matches(Expression expr, Expression parent, int childIndex, long blockId,
            int projectItem) {
        return matches(expr, parent, childIndex, blockId, projectItem, -1, "");
    }

    /**
     * The full identity: value + parent structure + child position + block + projection item
     * + the OCCURRENCE of the literal (the expression root it belongs to and its path inside
     * that root, see rootOrdinal / occurrencePath). The occurrence is what keeps two
     * independent slots of ONE block apart: two equal literals in the ON clauses of two
     * joins, or at different places of one conjunct, have equal value / parent / position /
     * block / item, and sharing one id made a query that varies them separately (1/1 ->
     * 2/3) unmatchable although SPM's matcher compares exactly these two literals.
     */
    public boolean matches(Expression expr, Expression parent, int childIndex, long blockId,
            int projectItem, long rootOrdinal, String occurrencePath) {
        if (this.blockId != blockId || this.projectItem != projectItem) {
            // different query blocks (outer query vs subquery / derived table) or different
            // SELECT-list items: never share one id, or a user query with different values
            // in the two positions could never match
            return false;
        }
        if (this.rootOrdinal != rootOrdinal
                || !Objects.equals(this.occurrencePath, occurrencePath == null ? ""
                        : occurrencePath)) {
            // different expression OCCURRENCES (see rootOrdinal / occurrencePath): the two
            // literals are independent slots, and a query varying them separately must still
            // match
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

    /** Occurrence index key; compare parent signatures only after the cheaper identity fields match. */
    record MatchKey(Expression expression, Expression parent, int childIndex,
            long blockId, int projectItem, long rootOrdinal, String occurrencePath,
            Map<Expression, String> parentSignatures) {
        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof MatchKey)) {
                return false;
            }
            MatchKey key = (MatchKey) other;
            if (blockId != key.blockId || projectItem != key.projectItem || rootOrdinal != key.rootOrdinal
                    || childIndex != key.childIndex || !occurrencePath.equals(key.occurrencePath)
                    || !expression.equals(key.expression)) {
                return false;
            }
            if (parent == key.parent) {
                return true;
            }
            return parent != null && key.parent != null
                    && signature().equals(key.signature());
        }

        private String signature() {
            return parentSignatures.computeIfAbsent(parent, PlaceholderExpr::parentSignature);
        }

        @Override
        public int hashCode() {
            int hash = expression.hashCode();
            hash = 31 * hash + childIndex;
            hash = 31 * hash + Long.hashCode(blockId);
            hash = 31 * hash + projectItem;
            hash = 31 * hash + Long.hashCode(rootOrdinal);
            hash = 31 * hash + occurrencePath.hashCode();
            // Parent shape participates in equality but not hashing. Different occurrences
            // already separate independent positions without constructing signature strings.
            return 31 * hash + (parent == null ? 0 : 1);
        }
    }

    static MatchKey matchKey(Expression expression, Expression parent, int childIndex,
            long blockId, int projectItem, long rootOrdinal, String occurrencePath,
            Map<Expression, String> parentSignatures) {
        return new MatchKey(expression, parent, parent == null ? 0 : childIndex,
                blockId, projectItem, rootOrdinal, occurrencePath == null ? "" : occurrencePath,
                parentSignatures);
    }

    /**
     * Builds a parent signature from expression class names and child signatures. Leaves include their SQL text; a
     * subquery expression uses a fixed marker so separately parsed plan ids do not affect correspondence. Alias
     * names and other non-child metadata are not encoded here.
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
