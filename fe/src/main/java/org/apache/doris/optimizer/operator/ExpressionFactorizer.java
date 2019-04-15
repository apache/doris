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
import com.google.common.collect.Maps;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.OptUtils;

import java.util.List;
import java.util.Map;

public class ExpressionFactorizer {
    interface ProcessDisjFunction {
        OptExpression apply(OptExpression expr, OptExpression lowestLogicalAncestor);
    }

    // Factorize common expressions in an OR tree;
    // the result is a conjunction of factors and a residual Or tree
    // Example:
    // input:  [(A=B AND C>0) OR (A=B AND A>0) OR (A=B AND B>0)]
    // output: [(A=B) AND (C>0 OR A>0 OR B>0)]
    private static class FactorizeDisjFunction implements ProcessDisjFunction {
        @Override
        public OptExpression apply(OptExpression expr, OptExpression lowestLogicalAncestor) {
            Map<OptExpression, Integer> factorMap = buildFactorMap(expr);
            List<OptExpression> residualExprs = Lists.newArrayList();
            List<OptExpression> factorExprs = Lists.newArrayList();

            for (OptExpression input : expr.getInputs()) {
                if (OptUtils.isItemAnd(input)) {
                    List<OptExpression> conjuncts = Lists.newArrayList();
                    for (OptExpression conjunct : input.getInputs()) {
                        addFactors(conjunct, factorExprs, conjuncts, factorMap);
                    }
                    if (conjuncts.size() > 0) {
                        residualExprs.add(OptPredicateUtils.createConjunction(conjuncts));
                    }
                } else {
                    addFactors(input, factorExprs, residualExprs, factorMap);
                }
            }
            if (residualExprs.size() > 0) {
                factorExprs.add(OptPredicateUtils.createDisjunction(residualExprs));
            }
            return OptPredicateUtils.createConjunction(factorExprs);
        }
    }

    public static OptExpression factorizeExpr(OptExpression expr) {
        expr = processDisjDescendents(expr, null, new FactorizeDisjFunction());
        // factorization might reveal unnested AND/OR
        expr = OptExpressionUtils.unnestExpr(expr);
        // eliminate duplicate AND/OR children
        expr = OptExpressionUtils.dedupChildern(expr);
        return expr;
    }

    private static OptExpression processDisjDescendents(
            OptExpression expr, OptExpression lowestLogicalAncestor, ProcessDisjFunction function) {
        OptExpression logicalAncestor = lowestLogicalAncestor;
        if (expr.getOp().isLogical()) {
            logicalAncestor = expr;
        }
        if (OptUtils.isItemOr(expr)) {
            return function.apply(expr, logicalAncestor);
        }
        List<OptExpression> newInputs = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            newInputs.add(processDisjDescendents(input, logicalAncestor, function));
        }
        return OptExpression.create(expr.getOp(), newInputs);
    }

    // Helper for building a factors map
    // Example:
    // input:  (A=B AND B=C AND B>0) OR (A=B AND B=C AND A>0)
    // output: [(A=B, 2), (B=C, 2)]
    private static Map<OptExpression, Integer> buildFactorMap(OptExpression expr) {
        Map<OptExpression, Integer> globalMap = Maps.newHashMap();
        // iterate over child disjuncts;
        // if a disjunct is an AND tree, iterate over its children
        for (OptExpression input : expr.getInputs()) {
            List<OptExpression> inputChildren;
            if (OptUtils.isItemAnd(input)) {
                inputChildren = input.getInputs();
            } else {
                inputChildren = Lists.newArrayList(input);
            }
            for (OptExpression inputChild : inputChildren) {
                Integer count = globalMap.get(inputChild);
                if (count != null) {
                    count++;
                } else {
                    globalMap.put(inputChild, new Integer(1));
                }
            }
        }
        Map<OptExpression, Integer> factorMap = Maps.newHashMap();
        for (Map.Entry<OptExpression, Integer> entry : globalMap.entrySet()) {
            if (entry.getValue() == expr.arity()) {
                factorMap.put(entry.getKey(), entry.getValue());
            }
        }
        return factorMap;
    }

    // Helper for adding a given expression either to the given factors
    // array or to a residuals array
    private static void addFactors(OptExpression expr, List<OptExpression> factors,
                                   List<OptExpression> residuals,
                                   Map<OptExpression, Integer> factorMap) {
        Integer count = factorMap.get(expr);
        if (count != null) {
            boolean found = false;
            for (OptExpression factor : factors) {
                if (expr.equals(factor)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                factors.add(expr);
            }
            // replace factor with constant True in the residuals array
            residuals.add(OptPredicateUtils.createConjunction(null));
        } else {
            residuals.add(expr);
        }
    }

    // Compute disjunctive pre-filters that can be pushed to the column creators.
    // These inferred filters need to "cover" all the disjuncts with expressions
    // coming from the same source.
    // For instance, out of the predicate
    // ((sale_type = 's'::text AND dyear = 2001 AND year_total > 0::numeric) OR
    // (sale_type = 's'::text AND dyear = 2002) OR
    // (sale_type = 'w'::text AND dmoy = 7 AND year_total > 0::numeric) OR
    // (sale_type = 'w'::text AND dyear = 2002 AND dmoy = 7))
    //
    // we can infer the filter
    // dyear=2001 OR dyear=2002 OR dmoy=7 OR (dyear=2002 AND dmoy=7)
    //
    // which can later be pushed down by the normalizer
    // TODO(zc): support this later
    public static OptExpression extractInferredFilters(OptExpression expr) {
        return expr;
    }
}
