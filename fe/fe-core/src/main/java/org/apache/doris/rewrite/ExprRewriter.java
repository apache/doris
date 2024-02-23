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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/ExprRewriter.java
// and modified by Doris

package org.apache.doris.rewrite;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.rewrite.mvrewrite.ExprToSlotRefRule;
import org.apache.doris.thrift.TQueryOptions;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helper class that drives the transformation of Exprs according to a given
 * list of
 * ExprRewriteRules. The rules are applied as follows:
 * - a single rule is applied repeatedly to the Expr and all its children in a
 * bottom-up
 * fashion until there are no more changes
 * - the rule list is applied repeatedly until no rule has made any changes
 * - the rules are applied in the order they appear in the rule list
 * Keeps track of how many transformations were applied.
 *
 * There are two types of Rewriter, the first is Repeat Rewriter,
 * and the other is Once Rewriter.
 * The Repeat Rewriter framework will call Rule repeatedly
 * until the entire expression does not change.
 * The Once Rewriter framework will only call Rule once.
 * According to different Rule strategies,
 * Doris match different Rewriter framework execution.
 */
public class ExprRewriter {
    private boolean useUpBottom = false;
    private int numChanges = 0;
    private final List<ExprRewriteRule> rules;

    // The type of clause that executes the rule.
    // This type is only used in InferFiltersRule, RewriteDateLiteralRule, other
    // rules are not used
    public enum ClauseType {
        INNER_JOIN_CLAUSE,
        LEFT_OUTER_JOIN_CLAUSE,
        RIGHT_OUTER_JOIN_CLAUSE,
        FULL_OUTER_JOIN_CLAUSE,
        LEFT_SEMI_JOIN_CLAUSE,
        RIGHT_SEMI_JOIN_CLAUSE,
        LEFT_ANTI_JOIN_CLAUSE,
        RIGHT_ANTI_JOIN_CLAUSE,
        CROSS_JOIN_CLAUSE,
        WHERE_CLAUSE,
        OTHER_CLAUSE; // All other clauses that are not on and not where

        public static ClauseType fromJoinType(JoinOperator joinOp) {
            switch (joinOp) {
                case INNER_JOIN:
                    return INNER_JOIN_CLAUSE;
                case LEFT_OUTER_JOIN:
                    return LEFT_OUTER_JOIN_CLAUSE;
                case RIGHT_OUTER_JOIN:
                    return RIGHT_OUTER_JOIN_CLAUSE;
                case FULL_OUTER_JOIN:
                    return FULL_OUTER_JOIN_CLAUSE;
                case LEFT_SEMI_JOIN:
                    return LEFT_SEMI_JOIN_CLAUSE;
                case RIGHT_SEMI_JOIN:
                    return RIGHT_SEMI_JOIN_CLAUSE;
                case NULL_AWARE_LEFT_ANTI_JOIN:
                case LEFT_ANTI_JOIN:
                    return LEFT_ANTI_JOIN_CLAUSE;
                case RIGHT_ANTI_JOIN:
                    return RIGHT_ANTI_JOIN_CLAUSE;
                case CROSS_JOIN:
                    return CROSS_JOIN_CLAUSE;
                default:
                    return OTHER_CLAUSE;
            }
        }

        public boolean isInferable() {
            if (this == INNER_JOIN_CLAUSE
                    || this == LEFT_SEMI_JOIN_CLAUSE
                    || this == RIGHT_SEMI_JOIN_CLAUSE
                    || this == WHERE_CLAUSE
                    || this == OTHER_CLAUSE) {
                return true;
            }
            return false;
        }

        public boolean isOnClause() {
            return this.compareTo(WHERE_CLAUSE) < 0;
        }
    }

    // Once-only Rules
    private List<ExprRewriteRule> onceRules = Lists.newArrayList();

    public ExprRewriter(List<ExprRewriteRule> rules) {
        this.rules = rules;
    }

    public ExprRewriter(List<ExprRewriteRule> rules, List<ExprRewriteRule> onceRules) {
        this.rules = rules;
        this.onceRules = onceRules;
    }

    public ExprRewriter(ExprRewriteRule rule) {
        rules = Lists.newArrayList(rule);
    }

    public void setInfoMVRewriter(Set<TupleId> disableTuplesMVRewriter, ExprSubstitutionMap mvSMap,
            ExprSubstitutionMap aliasSMap) {
        for (ExprRewriteRule rule : rules) {
            if (rule instanceof ExprToSlotRefRule) {
                ((ExprToSlotRefRule) rule).setInfoMVRewriter(disableTuplesMVRewriter, mvSMap, aliasSMap);
            }
        }
    }

    public void setUpBottom() {
        useUpBottom = true;
    }

    public Expr rewrite(Expr expr, Analyzer analyzer) throws AnalysisException {
        ClauseType clauseType = ClauseType.OTHER_CLAUSE;
        return rewrite(expr, analyzer, clauseType);
    }

    public Expr rewrite(Expr expr, Analyzer analyzer, ClauseType clauseType) throws AnalysisException {
        // Keep applying the rule list until no rule has made any changes.
        int oldNumChanges;
        Expr rewrittenExpr = expr;
        do {
            oldNumChanges = numChanges;
            for (ExprRewriteRule rule : rules) {
                // when foldConstantByBe is on, fold all constant expr by BE instead of applying
                // FoldConstantsRule in FE
                if (rule instanceof FoldConstantsRule && analyzer.safeIsEnableFoldConstantByBe()) {
                    continue;
                }
                rewrittenExpr = applyRuleRepeatedly(rewrittenExpr, rule, analyzer, clauseType);
            }
        } while (oldNumChanges != numChanges);

        for (ExprRewriteRule rule : onceRules) {
            rewrittenExpr = applyRuleOnce(rewrittenExpr, rule, analyzer, clauseType);
        }
        return rewrittenExpr;
    }

    private Expr applyRuleOnce(Expr expr, ExprRewriteRule rule, Analyzer analyzer, ClauseType clauseType)
            throws AnalysisException {
        Expr rewrittenExpr = rule.apply(expr, analyzer, clauseType);
        if (rewrittenExpr != expr) {
            numChanges++;
        }
        return rewrittenExpr;
    }

    /**
     * FoldConstantsRule rewrite
     */
    public void rewriteConstant(Map<String, Expr> exprMap, Analyzer analyzer, TQueryOptions tQueryOptions)
            throws AnalysisException {
        if (exprMap.isEmpty()) {
            return;
        }
        boolean changed = false;
        // rewrite constant expr
        for (ExprRewriteRule rule : rules) {
            if (rule instanceof FoldConstantsRule) {
                changed = ((FoldConstantsRule) rule).apply(exprMap, analyzer, changed, tQueryOptions);
            }
        }
        if (changed) {
            ++numChanges;
        }
    }

    public Expr rewriteElementAtToSlot(Expr inputExpr, Analyzer analyzer)
            throws AnalysisException {
        boolean changed = false;
        for (ExprRewriteRule rule : rules) {
            if (rule instanceof ElementAtToSlotRefRule) {
                Expr newExpr = ((ElementAtToSlotRefRule) rule).rewrite(inputExpr, analyzer);
                if (!newExpr.equals(inputExpr)) {
                    inputExpr = newExpr;
                    changed = true;
                }
            }
        }
        if (changed) {
            ++numChanges;
        }
        return inputExpr;
    }

    /**
     * Applies 'rule' on the Expr tree rooted at 'expr' until there are no more
     * changes.
     * Returns the transformed Expr or 'expr' if there were no changes.
     */
    private Expr applyRuleRepeatedly(Expr expr, ExprRewriteRule rule, Analyzer analyzer, ClauseType clauseType)
            throws AnalysisException {
        int oldNumChanges;
        Expr rewrittenExpr = expr;
        do {
            oldNumChanges = numChanges;
            rewrittenExpr = applyRule(rewrittenExpr, rule, analyzer, clauseType);
        } while (oldNumChanges != numChanges);
        return rewrittenExpr;
    }

    private Expr applyRule(Expr expr, ExprRewriteRule rule, Analyzer analyzer, ClauseType clauseType)
            throws AnalysisException {
        if (useUpBottom) {
            return applyRuleUpBottom(expr, rule, analyzer, clauseType);
        } else {
            return applyRuleBottomUp(expr, rule, analyzer, clauseType);
        }
    }

    /**
     * Applies 'rule' on 'expr' and all its children in a bottom-up fashion.
     * Returns the transformed Expr or 'expr' if there were no changes.
     */
    private Expr applyRuleBottomUp(Expr expr, ExprRewriteRule rule, Analyzer analyzer, ClauseType clauseType)
            throws AnalysisException {
        for (int i = 0; i < expr.getChildren().size(); ++i) {
            expr.setChild(i, applyRuleBottomUp(expr.getChild(i), rule, analyzer, clauseType));
        }
        Expr rewrittenExpr = rule.apply(expr, analyzer, clauseType);
        if (rewrittenExpr != expr) {
            ++numChanges;
        }
        return rewrittenExpr;
    }

    private Expr applyRuleUpBottom(Expr expr, ExprRewriteRule rule, Analyzer analyzer, ClauseType clauseType)
            throws AnalysisException {
        Expr rewrittenExpr = rule.apply(expr, analyzer, clauseType);
        if (rewrittenExpr != expr) {
            ++numChanges;
        }
        for (int i = 0; i < expr.getChildren().size(); ++i) {
            expr.setChild(i, applyRuleUpBottom(expr.getChild(i), rule, analyzer, clauseType));
        }
        return rewrittenExpr;
    }

    public void rewriteList(List<Expr> exprs, Analyzer analyzer) throws AnalysisException {
        for (int i = 0; i < exprs.size(); ++i) {
            exprs.set(i, rewrite(exprs.get(i), analyzer));
        }
    }

    public void reset() {
        numChanges = 0;
    }

    public boolean changed() {
        return numChanges > 0;
    }

    public int getNumChanges() {
        return numChanges;
    }
}
