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
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.OptUtils;
import org.apache.doris.optimizer.base.OptColumnRefSet;

import java.util.List;

public class OptNormalizer {
    private static boolean isPushable(OptExpression expr, OptExpression conjunct) {
        OptColumnRefSet usedColumns = conjunct.getItemProperty().getUsedColumns();
        OptColumnRefSet outputColumns = expr.getLogicalProperty().getOutputColumns();
        return outputColumns.contains(usedColumns);
    }
    // split one conjunction into many conjuncts.
    private static void splitConjunction(OptExpression expr, OptExpression conj,
                                         List<OptExpression> pushableExprs,
                                         List<OptExpression> unpushableExprs) {
        List<OptExpression> conjuncts = OptPredicateUtils.extractConjuncts(conj);
        for (OptExpression conjunct : conjuncts) {
            if (isPushable(expr, conjunct)) {
                pushableExprs.add(conjunct);
            } else {
                unpushableExprs.add(conjunct);
            }
        }
    }
    // A SELECT on top of LOJ, where SELECT's predicate is NULL-filtering and
    // uses columns from LOJ's inner child, is simplified as Inner-Join
    // Example:
    //      select * from (select * from R left join S on r1=s1) as foo where foo.s1>0;
    //      is converted to:
    //      select * from R inner join S on r1=s1 and s1>0;
    private static OptExpression isSimplifySelectOnOuterJoin(OptExpression outerJoin, OptExpression pred) {
        if (outerJoin.arity() == 0) {
            return null;
        }
        OptExpression childOuter = outerJoin.getInput(0);
        OptExpression childInner = outerJoin.getInput(1);
        OptExpression childPred = outerJoin.getInput(2);

        OptColumnRefSet outputColumns = childInner.getLogicalProperty().getOutputColumns();
        if (OptPredicateUtils.isNullRejecting(pred, outputColumns)) {
            // we have a predicate on top of LOJ that uses LOJ's inner child,
            // if the predicate filters-out nulls, we can add it to the join
            // predicate and turn LOJ into Inner-Join
            return OptExpression.create(new OptLogicalInnerJoin(), childOuter, childOuter,
                    OptPredicateUtils.createConjunction(pred, childPred));
        }
        // TODO(zc)
        return null;
    }

    // Return a Select expression, if needed, with a scalar condition made of
    // given array of conjuncts
    private static OptExpression createSelect(OptExpression expr, List<OptExpression> conjuncts) {
        if (conjuncts.isEmpty()) {
            return expr;
        }
        OptExpression conjunction = OptPredicateUtils.createConjunction(conjuncts);
        OptExpression select = OptUtils.createLogicalSelect(expr, conjunction);
        if (select.getOp() instanceof OptLogicalSelect) {
            // Select node was pruned, return created expression
            return select;
        }

        OptExpression selectChild = select.getInput(0);
        if (!(selectChild.getOp() instanceof OptLogicalLeftOuterJoin)) {
            // child of Select is not an outer join, return created Select expression
            return select;
        }

        // we have a Select on top of Outer Join expression, attempt simplifying expression into InnerJoin
        OptExpression simplifiedExpr = isSimplifySelectOnOuterJoin(selectChild, select.getInput(1));
        if (simplifiedExpr != null) {
            return normalize(simplifiedExpr);
        }
        return select;
    }

    // Push scalar expression through left outer join children;
    // this only handles the case of a SELECT on top of LEFT OUTER JOIN;
    // pushing down join predicates is handled in PushThruJoin();
    // here, we push predicates of the top SELECT node through LEFT OUTER JOIN's
    // outer child
    public static OptExpression pushThroughOuterChild(OptExpression join, OptExpression conj) {
        if (join.arity() == 0) {
            return OptUtils.createLogicalSelect(join, conj);
        }
        OptExpression outer = join.getInput(0);
        OptExpression inner = join.getInput(1);
        OptExpression pred = join.getInput(2);

        // split conjunction into pushable and unpushable
        List<OptExpression> pushableConjuncts = Lists.newArrayList();
        List<OptExpression> unpushableConjuncts = Lists.newArrayList();
        splitConjunction(join, conj, pushableConjuncts, unpushableConjuncts);

        OptExpression resultExpr = null;
        if (pushableConjuncts.size() > 0) {
            OptExpression newConjunction = OptPredicateUtils.createConjunction(pushableConjuncts);
            // create a new select node on top of the outer child
            OptExpression newSelect = OptExpression.create(new OptLogicalSelect(), outer, newConjunction);

            // push predicate through the new select to create a new outer child
            OptExpression newOuter = pushThrough(newSelect, newConjunction);
            // create a new outer join using the new outer child and the new inner child
            OptExpression newExpr = OptExpression.create(join.getOp(), newOuter, inner, pred);

            // call push down predicates on the new outer join
            OptExpression constTrue = OptUtils.createConstBoolExpression(true);
            resultExpr = pushThrough(newExpr, constTrue);
        }

        if (unpushableConjuncts.size() > 0) {
            OptExpression outerjoin = join;
            if (pushableConjuncts.size() > 0) {
                outerjoin = resultExpr;
            }
            // call push down on the outer join predicates
            OptExpression constTrue = OptUtils.createConstBoolExpression(true);
            OptExpression newExpr = pushThrough(outer, constTrue);
            resultExpr = createSelect(newExpr, unpushableConjuncts);
        }
        return resultExpr;
    }
    public static OptExpression pushThroughJoin(OptExpression join, OptExpression conj) {
        OptOperator op = join.getOp();
        boolean isOuterJoin = false;
        if (isOuterJoin && !OptUtils.isItemConstTrue(conj)) {
            // whenever possible, push incoming predicate through outer join's outer child,
            // recursion will eventually reach the rest of PushThruJoin() to process join predicates
            return pushThroughOuterChild(join, conj);
        }
        // combine conjunct with join predicate
        OptExpression itemExpr = join.getInput(join.arity() - 1);
        OptExpression pred = OptPredicateUtils.createConjunction(itemExpr, conj);
        // break predicate to conjuncts
        List<OptExpression> conjuncts = OptPredicateUtils.extractConjuncts(pred);
        List<OptExpression> newInputs = Lists.newArrayList();
        for (int i = 0; i < join.arity() - 1; ++i) {
            OptExpression input = join.getInput(i);
            if (i == 0 && isOuterJoin) {
                newInputs.add(normalize(input));
                continue;
            }
            // TOOD(zc):
            // pushThrough(input, conjuncts);
        }
        return null;
    }
    // Check if we should push predicates through expression's outer child
    public static boolean isPushThruOuterChild(OptExpression expr) {
        return expr.getOp() instanceof OptLogicalLeftOuterJoin;
    }

    private static boolean isChild(OptExpression expr, OptExpression childExpr) {
        for (OptExpression input : expr.getInputs()) {
            if (childExpr == input) {
                return true;
            }
        }
        return false;
    }

    // Push an array of conjuncts through a logical expression;
    // compute an array of unpushable conjuncts
    private static OptExpression pushThrough(OptExpression logical, List<OptExpression> conjuncts,
                                    List<OptExpression> remainingConjuncts) {
        List<OptExpression> pushableConjuncts = Lists.newArrayList();
        for (OptExpression conjunct : conjuncts) {
            if (isPushable(logical, conjunct)) {
                pushableConjuncts.add(conjunct);
            } else {
                remainingConjuncts.add(conjunct);
            }
        }
        OptExpression conjunction = OptPredicateUtils.createConjunction(pushableConjuncts);
        if (isPushThruOuterChild(logical)) {
            return pushThroughOuterChild(logical, conjunction);
        } else {
            return pushThrough(logical, conjunction);
        }
    }
    public static OptExpression pushThrough(OptExpression logical, OptExpression conj) {
        if (logical.arity() == 0) {
            return OptUtils.createLogicalSelect(logical, conj);
        }
        OptExpression newExpr = ((OptLogical) logical.getOp()).pushThrough(logical, conj);
        if (newExpr != null) {
            return newExpr;
        }
        // can't push predicates through, start a new normalization path
        OptExpression normalizedExpr = recursiveNormalize(logical);
        if (!isChild(logical, conj)) {
            normalizedExpr = OptUtils.createLogicalSelect(normalizedExpr, conj);
        }
        return normalizedExpr;
    }

    public static OptExpression recursiveNormalize(OptExpression expr) {
        List<OptExpression> newInputs = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            // TODO(zc)
            //  newInputs.add()
        }
        return null;
    }

    public static OptExpression normalize(OptExpression expr) {
        if (expr.arity() == 0) {
            return expr;
        }
        OptOperator op = expr.getOp();
        if (op.isLogical() && ((OptLogical) op).isSelectOp()) {
            if (isPushThruOuterChild(expr)) {
                OptExpression constTrue = OptUtils.createConstBoolExpression(true);
                return pushThrough(expr, constTrue);
            } else {
                List<OptExpression> newInputs = Lists.newArrayList();
                for (int i = 0; i < expr.arity() - 1; ++i) {
                    newInputs.add(expr.getInput(i));
                }
                OptExpression pred = expr.getInput(expr.arity() - 1);
                OptExpression normalizedPred = recursiveNormalize(pred);
                newInputs.add(normalizedPred);
                OptExpression newExpr = OptExpression.create(expr.getOp(), newInputs);
                return pushThrough(newExpr, normalizedPred);
            }
        } else {
            return recursiveNormalize(expr);
        }
    }

    // Push a conjunct through a select
    public static OptExpression pushThroughSelect(OptExpression select, OptExpression pred) {
        OptExpression logicalChild = select.getInput(0);
        OptExpression itemChild = select.getInput(1);
        OptExpression newPred = OptPredicateUtils.createConjunction(itemChild, pred);

        if (OptUtils.isItemConstTrue(newPred)) {
            return normalize(logicalChild);
        }

        if (logicalChild.getOp() instanceof OptLogicalLeftOuterJoin) {
            OptExpression simplifiedExpr = isSimplifySelectOnOuterJoin(logicalChild, newPred);
            if (simplifiedExpr != null) {
                return normalize(simplifiedExpr);
            }
        }

        if (isPushThruOuterChild(logicalChild)) {
            return pushThroughOuterChild(logicalChild, newPred);
        } else {
            // logical child may not pass all predicates through, we need to collect
            // unpushable predicates, if any, into a top Select node
            List<OptExpression> conjuncts = OptPredicateUtils.extractConjuncts(newPred);
            List<OptExpression> remainingConjuncts = Lists.newArrayList();
            OptExpression resultExpr = pushThrough(logicalChild, conjuncts, remainingConjuncts);
            return createSelect(resultExpr, remainingConjuncts);
        }
    }

    // Push a conjunct through a unary operator with scalar child
    public static OptExpression pushThruUnaryWithScalarChild(OptExpression expr, OptExpression pred) {
        OptExpression logicalChild = expr.getInput(0);
        OptExpression itemChild = expr.getInput(1);

        List<OptExpression> conjuncts = OptPredicateUtils.extractConjuncts(pred);
        List<OptExpression> remainingConjuncts = Lists.newArrayList();
        OptExpression newLogicalChild = pushThrough(logicalChild, conjuncts, remainingConjuncts);

        OptExpression newExpr = OptExpression.create(expr.getOp(), newLogicalChild, itemChild);
        return createSelect(newExpr, remainingConjuncts);
    }

    public static OptExpression pushThruUnaryWithoutScalarChild(OptExpression expr, OptExpression pred) {
        OptExpression logicalChild = expr.getInput(0);

        List<OptExpression> conjuncts = OptPredicateUtils.extractConjuncts(pred);
        List<OptExpression> remainingConjuncts = Lists.newArrayList();
        OptExpression newLogicalChild = pushThrough(logicalChild, conjuncts, remainingConjuncts);

        OptExpression newExpr = OptExpression.create(expr.getOp(), newLogicalChild);
        return createSelect(newExpr, remainingConjuncts);
    }
}
