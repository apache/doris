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

package org.apache.doris.optimizer.operator;

import com.google.common.collect.Lists;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.OptUtils;
import org.apache.doris.optimizer.base.OptColumnRefSet;
import org.apache.doris.optimizer.base.OptLogicalProperty;

import java.util.List;

public class ExpressionPreprocessor {
    public static OptExpression preprocess(OptExpression expr, OptColumnRefSet columns) {
        // (1) remove unused CTE anchors
        expr = removeUnusedCTEs(expr);
        // (2.a) remove intermediate superfluous limit
        expr = removeSuperfluousLimit(expr);
        // (2.b) remove intermediate superfluous distinct
        expr = removeSuperfluousDistinctInDQA(expr);
        // (3) trim unnecessary existential subqueries
        expr = trimExistentialSubqueries(expr);
        // (4) collapse cascaded union / union all
        expr = collapseUnionUnionAll(expr);
        // (5) remove superfluous outer references from the order spec in limits, grouping columns in GbAgg, and
        // Partition/Order columns in window operators
        expr = removeSuperfluousOuterRef(expr);
        // (6) remove superfluous equality
        expr = pruneSuperfluousEquality(expr);
        // (7) simplify quantified subqueries
        expr = simplifyQuantifiedSubqueries(expr);
        // (8) do preliminary unnesting of scalar subqueries
        expr = unnestScalarSubqueries(expr);
        // (9) unnest AND/OR/NOT predicates
        expr = OptExpressionUtils.unnestExpr(expr);
        // (10) infer predicates from constraints
        expr = inferPredicates(expr);
        // (11) eliminate self comparisons
        expr = eliminateSelfComparison(expr);
        // (12) remove duplicate AND/OR children
        // (13) factorize common expressions
        // (14) infer filters out of components of disjunctive filters
        // (15) pre-process window functions
        // (16) eliminate unused computed columns
        expr = pruneUnusedComputedCols(expr);
        // (17) normalize expression

        // (18) transform outer join into inner join whenever possible
        expr = outerJoinToInnerJoin(expr);
        // (19) collapse cascaded inner joins
        expr = collapseInnerJoins(expr);
        // (20) after transforming outer joins to inner joins, we may be able to generate more predicates from constraints
        // (21) eliminate empty subtrees
        expr = pruneEmptySubtrees(expr);
        // (22) collapse cascade of projects
        expr = collapseProjects(expr);
        // (23) insert dummy project when the scalar subquery is under a project and returns an outer reference
        // (24) reorder the children of scalar cmp operator to ensure that left child is scalar ident and right child is scalar const
        // (25) rewrite IN subquery to EXIST subquery with a predicate
        // (26) normalize expression again
        return expr;
    }

    // return unused CTE
    private static OptExpression removeUnusedCTEs(OptExpression expr) {
        // TODO(zc): currently, we transfer CTE to inline-view. We will support CTE later
        return expr;
    }
    // an intermediate limit is removed if it has neither row count nor offset
    private static OptExpression removeSuperfluousLimit(OptExpression expr) {
        return expr;
    }
    // distinct is removed from a DQA(Distributed Qualified Aggregates) if it has a max or min agg
    // e.g. select max(distinct(a)) from tbl -> select max(a) from tbl
    private static OptExpression removeSuperfluousDistinctInDQA(OptExpression expr) {
        OptOperator op = expr.getOp();
        if (op instanceof OptItemAggFunc) {
            OptExpression prjList = expr.getInput(1);
            for (OptExpression element : prjList.getInputs()) {
                if (!(element.getInput(0).getOp() instanceof OptItemAggFunc)) {
                    continue;
                }
                OptItemAggFunc aggFunc = (OptItemAggFunc) element.getInput(0).getOp();
                if (aggFunc.isDistinct() && aggFunc.isMinMax()) {
                    aggFunc.setDistinct(false);
                }
            }
        }
        List<OptExpression> inputs = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            inputs.add(removeSuperfluousDistinctInDQA(input));
        }
        return OptExpression.create(op, inputs);
    }
    // an existential subquery whose inner expression is a GbAgg
    // with no grouping columns is replaced with a Boolean constant
    //
    //      Example:
    //
    //          exists(select sum(i) from X) --> True
    //          not exists(select sum(i) from X) --> False
    private static OptExpression trimExistentialSubqueries(OptExpression expr) {
        OptOperator op = expr.getOp();
        if (OptUtils.isExistentialSubquery(op)) {
            OptExpression inner = expr.getInput(0);
            if (inner.getOp() instanceof OptLogicalAggregate) {
                OptLogicalAggregate gbAgg = (OptLogicalAggregate) inner.getOp();
                if (gbAgg.getGroupBy().size() == 0) {
                    boolean value = true;
                    if (op instanceof OptItemSubqueryNotExists) {
                        value = false;
                    }
                    return OptUtils.createConstBoolExpression(value);
                }
            }
        }
        List<OptExpression> inputs = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            inputs.add(trimExistentialSubqueries(input));
        }
        // TODO(zc)
        if (OptUtils.isItemAnd(expr)) {

        }
        if (OptUtils.isItemOr(expr)) {

        }
        return OptExpression.create(op, inputs);
    }
    // collapse cascaded union/union all into an NAry union/union all operator
    private static OptExpression collapseUnionUnionAll(OptExpression expr) {
        return expr;
    }
    // Remove outer references from order spec inside limit, grouping columns
    // in GbAgg, and Partition/Order columns in window operators
    //
    // Example, for the schema: t(a, b), s(i, j)
    // The query:
    //          select * from t where a < all (select i from s order by j, b limit 1);
    //      should be equivalent to:
    //          select * from t where a < all (select i from s order by j limit 1);
    //      after removing the outer reference (b) from the order by clause of the
    //      subquery (all tuples in the subquery have the same value for the outer ref)
    //
    //      Similarly,
    //          select * from t where a in (select count(i) from s group by j, b);
    //      is equivalent to:
    //          select * from t where a in (select count(i) from s group by j);
    //
    //      Similarly,
    //          select * from t where a in (select row_number() over (partition by t.a order by t.b) from s);
    //      is equivalent to:
    //          select * from t where a in (select row_number() over () from s);
    private static OptExpression removeSuperfluousOuterRef(OptExpression expr) {
        return expr;
    }
    // remove superfluous equality operations
    private static OptExpression pruneSuperfluousEquality(OptExpression expr) {
        return expr;
    }
    // a quantified subquery with maxcard 1 is simplified as a scalar subquery
    //
    // Example:
    //      a = ANY (select sum(i) from X) --> a = (select sum(i) from X)
    //      a <> ALL (select sum(i) from X) --> a <> (select sum(i) from X)
    private static OptExpression simplifyQuantifiedSubqueries(OptExpression expr) {
        OptOperator op = expr.getOp();
        if (OptUtils.isQuantifiedSubquery(op) && expr.getInput(0).getLogicalProperty().getMaxcard().getValue() == 1) {
            OptExpression inner = expr.getInput(0);

            OptExpression child = inner;
            OptOperator childOp = child.getOp();
            while (childOp != null && OptUtils.isLogicalUnary(childOp)) {
                child = child.getInput(0);
                childOp = child.getOp();
            }

            boolean isGbAggWithoutGrpCols = (childOp instanceof OptLogicalAggregate)
                    && ((OptLogicalAggregate) childOp).getGroupBy().size() == 0;
            // TODO(zc): support const table get
            boolean isOneRowConstTable = false;
            if (isGbAggWithoutGrpCols || isOneRowConstTable) {
                // TODO(zc):
                // OptExpression itemExpr = expr.getInput(1);
                // OptExpression itemSubquery = OptExpression.create(new OptItemSubquery, inner);

                // return OptUtils.createBinaryPredicate(itemExpr, itemSubquery);
            }
        }
        List<OptExpression> newInputs = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            newInputs.add(simplifyQuantifiedSubqueries(input));
        }
        return OptExpression.create(op, newInputs);
    }
    // preliminary unnesting of scalar subqueries
    // Example:
    //      Input:   SELECT k, (SELECT (SELECT Y.i FROM Y WHERE Y.j=X.j)) from X
    //      Output:  SELECT k, (SELECT Y.i FROM Y WHERE Y.j=X.j) from X
    private static OptExpression unnestScalarSubqueries(OptExpression expr) {
        return expr;
    }
    // driver for inferring predicates from constraints
    private static OptExpression inferPredicates(OptExpression expr) {
        return expr;
    }
    // eliminate self comparisons in the given expression
    private static OptExpression eliminateSelfComparison(OptExpression expr) {
        return expr;
    }
    //  Workhorse for pruning unused computed columns
    //
    //  The required columns passed by the query is passed to this pre-processing
    //  stage and the list of columns are copied to a new list. This driver function
    //  calls the PexprPruneUnusedComputedColsRecursive function with the copied
    //  required column set. The original required columns set is not modified by
    //  this preprocessor.
    //
    //  Extra copy of the required columns set is avoided in each recursive call by
    //  creating a one-time copy and passing it by reference for all the recursive
    //  calls.
    //
    //  The functional behavior of the pruneUnusedComputedCols changed slightly
    //  because we do not delete the required column set at the end of every
    //  call but pass it to the next and consecutive recursive calls. However,
    //  it is safe to add required columns by each operator we traverse, because non
    //  of the required columns from other child of a tree will appear on the project
    //  list of the other children.
    //
    // Therefore, the added columns to the required columns which is caused by
    // the recursive call and passing by reference will not have a bad affect
    // on the overall result.
    private static OptExpression pruneUnusedComputedCols(OptExpression expr) {
        return expr;
    }
    // transform outer joins into inner joins
    private static OptExpression outerJoinToInnerJoin(OptExpression expr) {
        return expr;
    }
    // collapse cascaded inner joins into NAry-joins
    private static OptExpression collapseInnerJoins(OptExpression expr) {
        if (OptUtils.isInnerJoin(expr)) {
            List<OptExpression> logicalChildren = Lists.newArrayList();
            List<OptExpression> itemChildren = Lists.newArrayList();
            boolean isCollapsed = false;
            for (int i = 0; i < expr.arity() - 1; ++i) {
                OptExpression childExpr = expr.getInput(i);
                if (OptUtils.isInnerJoin(childExpr)) {
                    isCollapsed = true;
                    OptUtils.collectChildren(childExpr, logicalChildren, itemChildren);
                } else {
                    logicalChildren.add(collapseInnerJoins(childExpr));
                }
            }
            itemChildren.add(expr.getInput(expr.arity() - 1));
            logicalChildren.add(OptPredicateUtils.createConjunction(itemChildren));

            OptExpression nAryJoin = OptExpression.create(new OptLogicalNAryJoin(), logicalChildren);
            if (isCollapsed) {
                // a join was collapsed with its children into NAry-Join, we need to recursively
                // process the created NAry join
                nAryJoin = collapseProjects(nAryJoin);
            }
            return nAryJoin;
        }

        List<OptExpression> newInput = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            newInput.add(collapseInnerJoins(input));
        }
        return OptExpression.create(expr.getOp(), newInput);
    }
    // eliminate subtrees that have a zero output cardinality, replacing them
    // with a const table get with the same output schema and zero tuples
    private static OptExpression pruneEmptySubtrees(OptExpression expr) {
        OptOperator op = expr.getOp();
        if (op.isLogical()) {
            OptLogicalProperty prop = expr.getLogicalProperty();
            if (prop.getMaxcard().getValue() == 0) {
                // TODO(zc): return empty const get
                //
            }
        }
        List<OptExpression> newInputs = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            newInputs.add(pruneEmptySubtrees(input));
        }
        return OptExpression.create(op, newInputs);
    }
    // collapse cascaded logical project operators
    private static OptExpression collapseProjects(OptExpression expr) {
        List<OptExpression> newInputs = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            newInputs.add(collapseProjects(input));
        }
        OptExpression newExpr = OptExpression.create(expr.getOp(), newInputs);
        OptExpression collapsedExpr = OptUtils.collapseProjects(newExpr);
        if (collapsedExpr == null) {
            return newExpr;
        }
        return collapsedExpr;
    }

    private static boolean isConvert2InIsConvertable(OptExpression expr) {
        return false;
    }

    private static OptExpression convertToIn(OptExpression expr) {
        if (OptUtils.isItemOr(expr) || OptUtils.isItemAnd(expr)) {
            CompoundPredicate.Operator op = ((OptItemCompoundPredicate) expr.getOp()).getOp();
            // derive constraints on all of the simple scalar children
            // and add them to a new AND or OR expression
            List<OptExpression> collapseExprs = Lists.newArrayList();
            List<OptExpression> remaindingExprs = Lists.newArrayList();
            for (OptExpression input : expr.getInputs()) {
                if (isConvert2InIsConvertable(input)) {
                    collapseExprs.add(input);
                } else {
                    // recursively convert the remainder and add to the array
                    remaindingExprs.add(convertToIn(input));
                }
                if (collapseExprs.size() != 0) {

                }
            }
        }

        List<OptExpression>  newInputs = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            newInputs.add(convertToIn(input));
        }
        return OptExpression.create(expr.getOp(), newInputs);
    }
}
