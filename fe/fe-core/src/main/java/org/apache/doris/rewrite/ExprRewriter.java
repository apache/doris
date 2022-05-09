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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * Helper class that drives the transformation of Exprs according to a given list of
 * ExprRewriteRules. The rules are applied as follows:
 * - a single rule is applied repeatedly to the Expr and all its children in a bottom-up
 *   fashion until there are no more changes
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
    private int numChanges_ = 0;
    private final List<ExprRewriteRule> rules_;

    // The type of clause that executes the rule.
    // This type is only used in InferFiltersRule, RewriteDateLiteralRule, other rules are not used
    public enum ClauseType {
        ON_CLAUSE,
        WHERE_CLAUSE,
        OTHER_CLAUSE,    // All other clauses that are not on and not where
    }

    // Once-only Rules
    private List<ExprRewriteRule> onceRules_ = Lists.newArrayList();

    public ExprRewriter(List<ExprRewriteRule> rules) {
        rules_ = rules;
    }

    public ExprRewriter(List<ExprRewriteRule> rules, List<ExprRewriteRule> onceRules) {
        rules_ = rules;
        onceRules_ = onceRules;
    }

    public ExprRewriter(ExprRewriteRule rule) {
        rules_ = Lists.newArrayList(rule);
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
            oldNumChanges = numChanges_;
            for (ExprRewriteRule rule: rules_) {
                // when foldConstantByBe is on, fold all constant expr by BE instead of applying FoldConstantsRule in FE.
                if (rule instanceof FoldConstantsRule && analyzer.safeIsEnableFoldConstantByBe()) {
                    continue;
                }
                rewrittenExpr = applyRuleRepeatedly(rewrittenExpr, rule, analyzer, clauseType);
            }
        } while (oldNumChanges != numChanges_);

        for (ExprRewriteRule rule: onceRules_) {
            rewrittenExpr = applyRuleOnce(rewrittenExpr, rule, analyzer, clauseType);
        }
        return rewrittenExpr;
    }

    private Expr applyRuleOnce(Expr expr, ExprRewriteRule rule, Analyzer analyzer, ClauseType clauseType) throws AnalysisException {
        Expr rewrittenExpr = rule.apply(expr, analyzer, clauseType);
        if (rewrittenExpr != expr) {
            numChanges_++;
        }
        return rewrittenExpr;
    }

    /**
     * FoldConstantsRule rewrite
     */
    public void rewriteConstant(Map<String, Expr> exprMap, Analyzer analyzer) throws AnalysisException {
        if (exprMap.isEmpty()) {
            return;
        }
        boolean changed = false;
        // rewrite constant expr
        for (ExprRewriteRule rule : rules_) {
            if (rule instanceof FoldConstantsRule) {
                changed = ((FoldConstantsRule) rule).apply(exprMap, analyzer, changed);
            }
        }
        if (changed) {
            ++numChanges_;
        }
    }

    /**
     * Applies 'rule' on the Expr tree rooted at 'expr' until there are no more changes.
     * Returns the transformed Expr or 'expr' if there were no changes.
     */
    private Expr applyRuleRepeatedly(Expr expr, ExprRewriteRule rule, Analyzer analyzer, ClauseType clauseType)
            throws AnalysisException {
        int oldNumChanges;
        Expr rewrittenExpr = expr;
        do {
            oldNumChanges = numChanges_;
            rewrittenExpr = applyRuleBottomUp(rewrittenExpr, rule, analyzer, clauseType);
        } while (oldNumChanges != numChanges_);
        return rewrittenExpr;
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
        if (rewrittenExpr != expr) ++numChanges_;
        return rewrittenExpr;
    }

    public void rewriteList(List<Expr> exprs, Analyzer analyzer) throws AnalysisException {
        for (int i = 0; i < exprs.size(); ++i) exprs.set(i, rewrite(exprs.get(i), analyzer));
    }

    public void reset() { numChanges_ = 0; }
    public boolean changed() { return numChanges_ > 0; }
    public int getNumChanges() { return numChanges_; }
}
