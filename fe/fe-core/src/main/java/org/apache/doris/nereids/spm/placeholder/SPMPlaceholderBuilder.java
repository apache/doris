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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
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
public class SPMPlaceholderBuilder extends ExpressionVisitor<Expression, Expression> {

    /** Auto-increment placeholder id counter (starts at 1). */
    private long nextId = 1;

    /** Placeholder record list. */
    private final List<PlaceholderExpr> placeholderExprs = new ArrayList<>();

    /**
     * DFS stack of child positions: the top is the index of the expression currently
     * being visited inside its direct parent (empty = parent-less root). Two literals
     * under the same parent (e.g. the operands of "1 + 1") are different positions and
     * must never reuse one placeholder id.
     */
    private final Deque<Integer> childIndexStack = new ArrayDeque<>();

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
            parameterized.add(expr.accept(this, null));
        }
        return parameterized;
    }

    public List<PlaceholderExpr> getPlaceholderExprs() {
        return placeholderExprs;
    }

    /** Position of the expression currently being visited inside its direct parent; -1 at a root. */
    private int currentChildIndex() {
        Integer top = childIndexStack.peek();
        return top == null ? -1 : top;
    }

    // ==================== literal -> scalar placeholder ====================

    @Override
    public Expression visitLiteral(Literal literal, Expression parent) {
        // Deduplicate by value + parent structure + child position: a literal reuses an
        // existing id only when the value, the parent expression structure AND the
        // position inside that parent all match. The position is required - the two
        // operands of "1 + 1" share one parent structure but are different positions,
        // and a similar query may carry different values there (one shared id would make
        // the two occurrences resolve to the same user value and never match).
        int childIndex = currentChildIndex();
        for (PlaceholderExpr record : placeholderExprs) {
            if (record.matches(literal, parent, childIndex)) {
                return record.getPlaceholderExpr();
            }
        }
        long id = nextId++;
        SpmConstVar placeholder = SpmConstVar.of(id, literal);
        placeholderExprs.add(new PlaceholderExpr(literal, placeholder, parent, childIndex));
        return placeholder;
    }

    // ==================== IN (constant list) -> list placeholder ====================

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, Expression parent) {
        // Rewrite the compareExpr first (its parent is the InPredicate)
        Expression newCompare = inPredicate.getCompareExpr().accept(this, inPredicate);

        if (inPredicate.optionsAreLiterals()) {
            // The whole constant list -> SpmConstList. Deduplicate like a scalar
            // literal: an identical IN predicate (same value list, same parent
            // structure, same child position) reuses the existing placeholder id. This
            // keeps the ids shared between the bind tree and the (separately parsed)
            // plan tree, so the values extracted against the bind tree substitute into
            // the plan tree.
            int childIndex = currentChildIndex();
            for (PlaceholderExpr record : placeholderExprs) {
                if (record.matches(inPredicate, parent, childIndex)) {
                    return record.getPlaceholderExpr();
                }
            }
            long id = nextId++;
            SpmConstList list = SpmConstList.of(id, inPredicate.getOptions());
            InPredicate placeholder = new InPredicate(newCompare, ImmutableList.of(list));
            placeholderExprs.add(new PlaceholderExpr(inPredicate, placeholder, parent, childIndex));
            return placeholder;
        }

        // Non-constant list (contains subqueries/expressions): rewrite each child
        List<Expression> newOptions = new ArrayList<>(inPredicate.getOptions().size());
        for (Expression option : inPredicate.getOptions()) {
            newOptions.add(option.accept(this, inPredicate));
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
        // bare-literal root records -1 instead of a stale outer child index.
        LogicalPlan newPlan = SPMPlanTreeSupport.transform(
                subqueryExpr.getQueryPlan(), expr -> {
                    childIndexStack.push(-1);
                    try {
                        return expr.accept(this, null);
                    } finally {
                        childIndexStack.pop();
                    }
                });
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
        List<Expression> children = expr.children();
        if (children.isEmpty()) {
            return expr;
        }
        boolean changed = false;
        List<Expression> newChildren = new ArrayList<>(children.size());
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
