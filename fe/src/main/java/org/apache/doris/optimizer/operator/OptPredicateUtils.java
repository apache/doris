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
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.OptUtils;
import org.apache.doris.optimizer.base.OptColumnRefSet;

import java.util.List;

public class OptPredicateUtils {
    // check if conjunction/disjunction can be skipped
    public static boolean isSkippable(OptExpression expr, boolean isConjunction) {
        return (isConjunction && OptUtils.isItemConstTrue(expr)) ||
                (!isConjunction && OptUtils.isItemConstFalse(expr));
    }
    // check if conjunction/disjunction can be reduced
    public static boolean isReducible(OptExpression expr, boolean isConjunction) {
        return (isConjunction && OptUtils.isItemConstFalse(expr)) ||
                (!isConjunction && OptUtils.isItemConstTrue(expr));
    }
    public static boolean isBinaryPredicate(OptExpression expr, BinaryPredicate.Operator operator) {
        if (expr.getOp() instanceof OptItemBinaryPredicate) {
            return false;
        }
        OptItemBinaryPredicate predicate = (OptItemBinaryPredicate) expr.getOp();
        return predicate.getOp() == operator;
    }
    public static boolean isEqualityOp(OptExpression expr) {
        return isBinaryPredicate(expr, BinaryPredicate.Operator.EQ);
    }

    // create conjunction/disjunction for given array of expressions
    public static OptExpression createConjDisj(List<OptExpression> inputs, boolean isConjunction) {
        List<OptExpression> finalInputs = Lists.newArrayList();
        for (OptExpression input : inputs) {
            if (isSkippable(input, isConjunction)) {
                continue;
            }
            if (isReducible(input, isConjunction)) {
                return OptExpression.create(OptItemConst.createBool(!isConjunction));
            }
            finalInputs.add(input);
        }

        if (finalInputs.isEmpty()) {
            // for AND, all elements are skipped, so it is true
            // for OR, all elements are skipped, so it is false
            return OptExpression.create(OptItemConst.createBool(isConjunction));
        }
        if (finalInputs.size() == 1) {
            return finalInputs.get(0);
        }
        CompoundPredicate.Operator operator = CompoundPredicate.Operator.AND;
        if (!isConjunction) {
            operator = CompoundPredicate.Operator.OR;
        }
        return OptUtils.createCompoundPredicate(operator, finalInputs);
    }

    public static OptExpression createConjDisj(OptExpression one, OptExpression two, boolean isConjunction) {
        if (one == two) {
            return one;
        }
        List<OptExpression> oneConjuncts = null;
        List<OptExpression> twoConjuncts = null;
        if (isConjunction) {
            oneConjuncts = extractConjuncts(one);
            twoConjuncts = extractConjuncts(two);
        } else {
            oneConjuncts = extractDisjuncts(one);
            twoConjuncts = extractDisjuncts(two);
        }
        List<OptExpression> allConjuncts = Lists.newArrayList();
        allConjuncts.addAll(oneConjuncts);
        allConjuncts.addAll(twoConjuncts);
        return createConjDisj(twoConjuncts, isConjunction);
    }

    public static OptExpression createDisjunction(List<OptExpression> inputs) {
        return createConjDisj(inputs, false);
    }

    public static OptExpression createConjunction(List<OptExpression> inputs) {
        return createConjDisj(inputs, true);
    }

    public static OptExpression createConjunction(OptExpression one, OptExpression two) {
        return createConjDisj(one, two, true);
    }

    public static OptExpression inverseComparison(OptExpression expr) {
        OptItemBinaryPredicate pred = (OptItemBinaryPredicate) expr.getOp();
        return OptUtils.createBinaryPredicate(expr.getInput(0), expr.getInput(1), pred.getOp().inverse());
    }

    // convert predicates of the form (true = (a Cmp b)) into (a Cmp b);
    // do this operation recursively on deep expression tree
    public static OptExpression pruneSuperfluosEquality(OptExpression expr) {
        if (expr.getOp().isSubquery()) {
            return expr;
        }
        if (isEqualityOp(expr)) {
            boolean isLeftTure = OptUtils.isItemConstTrue(expr.getInput(0));
            boolean isLeftFalse = OptUtils.isItemConstFalse(expr.getInput(0));
            boolean isRightTrue = OptUtils.isItemConstTrue(expr.getInput(1));
            boolean isRightFalse = OptUtils.isItemConstFalse(expr.getInput(1));

            boolean isLeftBinPred = OptUtils.isBinaryPredicate(expr.getInput(0));
            boolean isRightBinPred = OptUtils.isBinaryPredicate(expr.getInput(1));
            if (isRightBinPred) {
                if (isLeftTure) {
                    return pruneSuperfluosEquality(expr.getInput(1));
                }
                if (isLeftFalse) {
                    OptExpression inverseExpr = inverseComparison(expr.getInput(1));
                    return pruneSuperfluosEquality(inverseExpr);
                }
            }
            if (isLeftBinPred) {
                if (isRightTrue) {
                    return pruneSuperfluosEquality(expr.getInput(0));
                }
                if (isRightFalse) {
                    OptExpression inverseExpr = inverseComparison(expr.getInput(0));
                    return pruneSuperfluosEquality(inverseExpr);
                }
            }
        }
        List<OptExpression> newInputs = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            newInputs.add(pruneSuperfluosEquality(input));
        }
        return OptExpression.create(expr.getOp(), newInputs);
    }

    public static void collectConjuncts(OptExpression expr, List<OptExpression> conjuncts) {
        if (OptUtils.isItemAnd(expr)) {
            for (OptExpression input : expr.getInputs()) {
                collectConjuncts(input, conjuncts);
            }
        } else {
            conjuncts.add(expr);
        }
    }
    private static void collectDisjuncts(OptExpression expr, List<OptExpression> disjuncts) {
        if (OptUtils.isItemAnd(expr)) {
            for (OptExpression input : expr.getInputs()) {
                collectDisjuncts(input, disjuncts);
            }
        } else {
            disjuncts.add(expr);
        }
    }

    // extract conjuncts from a predicate
    public static List<OptExpression> extractConjuncts(OptExpression pred) {
        List<OptExpression> conjuncts = Lists.newArrayList();
        collectConjuncts(pred, conjuncts);
        return conjuncts;
    }

    public static List<OptExpression> extractDisjuncts(OptExpression pred) {
        List<OptExpression> conjuncts = Lists.newArrayList();
        collectDisjuncts(pred, conjuncts);
        return conjuncts;
    }

    // for all columns in the given expression and are members of the given column set, replace columns with NULLs
    private static OptExpression replaceColsWithNulls(OptExpression item, OptColumnRefSet columnSet) {
        if (OptUtils.isSubquery(item)) {
            return item;
        }
        OptOperator op = item.getOp();
        if (op instanceof OptItemColumnRef && columnSet.contains(((OptItemColumnRef) op).getRef())) {
            // replace column with NULL constant
            return OptUtils.createConstBoolExpression(false, true);
        }
        // process children recursively
        List<OptExpression> newInputs = Lists.newArrayList();
        for (OptExpression input : item.getInputs()) {
            newInputs.add(replaceColsWithNulls(input, columnSet));
        }
        return OptExpression.create(op, newInputs);
    }

    // check if scalar expression evaluates to (NOT TRUE) when
    // all columns in the given set that are included in the expression
    // are set to NULL
    public static boolean isNullRejecting(OptExpression pred, OptColumnRefSet columnSet) {
        OptColumnRefSet usedColumns = pred.getItemProperty().getUsedColumns();
        return false;
    }
}
