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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/** replace SlotReference ExprId in logical plans */
public class ExprIdRewriter extends ExpressionRewrite {
    private final List<Rule> rules;
    private final JobContext jobContext;

    public ExprIdRewriter(ReplaceRule replaceRule, JobContext jobContext) {
        super(new ExpressionRuleExecutor(ImmutableList.of(new ReplaceExprIds(replaceRule))));
        rules = buildRules();
        this.jobContext = jobContext;
    }

    /** Replace the exprIds of one expression structurally, see ReplaceRule#replaceExprIds. */
    private static class ReplaceExprIds implements ExpressionRewriteRule<ExpressionRewriteContext> {
        private final ReplaceRule replaceRule;

        ReplaceExprIds(ReplaceRule replaceRule) {
            this.replaceRule = replaceRule;
        }

        @Override
        public Expression rewrite(Expression expression, ExpressionRewriteContext context) {
            return replaceRule.replaceExprIds(expression);
        }
    }

    /**
     * Follow the replacement chain of an exprId to its end: an exprId may be renamed more than once, e.g. by
     * several rules or by several passes of the same rule, and only the last exprId exists in the plan then.
     */
    public static ExprId lastExprId(Map<ExprId, ExprId> replaceMap, ExprId exprId) {
        ExprId lastExprId = replaceMap.get(exprId);
        if (lastExprId == null) {
            return exprId;
        }
        while (true) {
            ExprId nextExprId = replaceMap.get(lastExprId);
            if (nextExprId == null) {
                return lastExprId;
            }
            lastExprId = nextExprId;
        }
    }

    /**rewriteExpr*/
    public Plan rewriteExpr(Plan plan, Map<ExprId, ExprId> replaceMap) {
        if (replaceMap.isEmpty()) {
            return plan;
        }
        for (Rule rule : rules) {
            Pattern<Plan> pattern = (Pattern<Plan>) rule.getPattern();
            if (pattern.matchPlanTree(plan)) {
                List<Plan> newPlans = rule.transform(plan, jobContext.getCascadesContext());
                Plan newPlan = newPlans.get(0);
                if (!newPlan.deepEquals(plan)) {
                    return newPlan;
                }
            }
        }
        // ExpressionRewrite has a rule for the common sink types only, e.g. there is none for the blackhole sink. A
        // sink takes its output expressions from its child, so it must follow the fresh exprIds of the child as
        // well, otherwise it would keep expecting a slot which the rewritten child does not output any more. So
        // rewrite the output expressions of any sink type here.
        if (plan instanceof LogicalSink) {
            LogicalSink<? extends Plan> sink = (LogicalSink<? extends Plan>) plan;
            ExpressionRewriteContext context = new ExpressionRewriteContext(sink, jobContext.getCascadesContext());
            ImmutableList.Builder<NamedExpression> newOutputExprs
                    = ImmutableList.builderWithExpectedSize(sink.getOutputExprs().size());
            boolean changed = false;
            for (NamedExpression outputExpr : sink.getOutputExprs()) {
                NamedExpression newOutputExpr = (NamedExpression) rewriter.rewrite(outputExpr, context);
                changed |= !newOutputExpr.equals(outputExpr);
                newOutputExprs.add(newOutputExpr);
            }
            if (changed) {
                return sink.withOutputExprs(newOutputExprs.build());
            }
        }
        return plan;
    }

    /**
     * Iteratively rewrites IDs using the replaceMap:
     * 1. For a given SlotReference with initial ID, retrieve the corresponding value ID from the replaceMap.
     * 2. If the value ID exists within the replaceMap, continue the lookup process using the value ID
     * until it no longer appears in the replaceMap.
     * 3. return SlotReference final value ID as the result of the rewrite.
     * e.g. replaceMap:{0:3, 1:6, 6:7}
     * SlotReference:a#0 -> a#3, a#1 -> a#7
     * */
    public static class ReplaceRule {
        private static final DefaultExpressionRewriter<Map<ExprId, ExprId>> SLOT_REPLACER =
                new DefaultExpressionRewriter<Map<ExprId, ExprId>>() {
                    @Override
                    public Expression visitSlotReference(SlotReference slot, Map<ExprId, ExprId> replaceMap) {
                        if (replaceMap.get(slot.getExprId()) == null) {
                            return slot;
                        }
                        return slot.withExprId(lastExprId(replaceMap, slot.getExprId()));
                    }
                };
        private static final DefaultExpressionRewriter<Map<ExprId, ExprId>> SLOT_ALIAS_REPLACER =
                new DefaultExpressionRewriter<Map<ExprId, ExprId>>() {
                    @Override
                    public Expression visitSlotReference(SlotReference slot, Map<ExprId, ExprId> replaceMap) {
                        if (replaceMap.get(slot.getExprId()) == null) {
                            return slot;
                        }
                        return slot.withExprId(lastExprId(replaceMap, slot.getExprId()));
                    }

                    @Override
                    public Expression visitAlias(Alias alias, Map<ExprId, ExprId> replaceMap) {
                        if (replaceMap.get(alias.getExprId()) == null) {
                            return alias;
                        }
                        return alias.withExprId(lastExprId(replaceMap, alias.getExprId()));
                    }
                };

        private final Map<ExprId, ExprId> replaceMap;
        private final DefaultExpressionRewriter<Map<ExprId, ExprId>> structuralReplacer;
        private DefaultExpressionRewriter<Map<ExprId, ExprId>> replacer;

        /**
         * constructor
         */
        public ReplaceRule(Map<ExprId, ExprId> replaceMap, boolean rewriteAlias) {
            this.replaceMap = replaceMap;
            if (rewriteAlias) {
                replacer = SLOT_ALIAS_REPLACER;
            } else {
                replacer = SLOT_REPLACER;
            }
            // children first: the id replacement of a node must not stop the traversal, e.g. an Alias keeps its own
            // exprId while a slot of the expression below it is renamed. The replacer is applied to the node itself
            // only, its children are already rewritten here, so the whole expression is visited once.
            this.structuralReplacer = new DefaultExpressionRewriter<Map<ExprId, ExprId>>() {
                @Override
                public Expression visit(Expression expr, Map<ExprId, ExprId> replaceMap) {
                    Expression withRewrittenChildren = rewriteChildren(this, expr, replaceMap);
                    if (withRewrittenChildren instanceof SlotReference
                            || (rewriteAlias && withRewrittenChildren instanceof Alias)) {
                        return withRewrittenChildren.accept(replacer, replaceMap);
                    }
                    return withRewrittenChildren;
                }
            };
        }

        public ReplaceRule(Map<ExprId, ExprId> replaceMap) {
            this(replaceMap, false);
        }

        /**
         * Replace the exprIds of the slots (and of the aliases, when this rule was created for them) in one
         * expression. Unlike the bottom up expression rewriter this enters every branch, including the branches
         * which require short circuit evaluation, e.g. the assignments of MERGE INTO: replacing an exprId only
         * rebuilds the expression and never evaluates it, and a reference which keeps an old exprId would point to
         * a slot which the rewritten child does not output any more.
         */
        public Expression replaceExprIds(Expression expression) {
            return expression.accept(structuralReplacer, replaceMap);
        }

    }
}
