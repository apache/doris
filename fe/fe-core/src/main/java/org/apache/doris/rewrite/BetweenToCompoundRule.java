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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/BetweenToCompoundRule.java
// and modified by Doris

package org.apache.doris.rewrite;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BetweenPredicate;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.common.AnalysisException;

/**
 * Rewrites BetweenPredicates into an equivalent conjunctive/disjunctive
 * CompoundPredicate.
 * It can be applied to pre-analysis expr trees and therefore does not reanalyze
 * the transformation output itself.
 * Examples:
 * A BETWEEN X AND Y ==> A >= X AND A <= Y
 * A NOT BETWEEN X AND Y ==> A < X OR A > Y
 */
public final class BetweenToCompoundRule implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new BetweenToCompoundRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (!(expr instanceof BetweenPredicate)) return expr;
        BetweenPredicate bp = (BetweenPredicate) expr;
        Expr result = null;
        if (bp.isNotBetween()) {
            // Rewrite into disjunction.
            Predicate lower = new BinaryPredicate(BinaryPredicate.Operator.LT,
                    bp.getChild(0), bp.getChild(1));
            Predicate upper = new BinaryPredicate(BinaryPredicate.Operator.GT,
                    bp.getChild(0), bp.getChild(2));
            result = new CompoundPredicate(CompoundPredicate.Operator.OR, lower, upper);
        } else {
            // Rewrite into conjunction.
            Predicate lower = new BinaryPredicate(BinaryPredicate.Operator.GE,
                    bp.getChild(0), bp.getChild(1));
            Predicate upper = new BinaryPredicate(BinaryPredicate.Operator.LE,
                    bp.getChild(0), bp.getChild(2));
            result = new CompoundPredicate(CompoundPredicate.Operator.AND, lower, upper);
        }
        return result;
    }

    private BetweenToCompoundRule() {}
}
