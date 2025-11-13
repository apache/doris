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
//
// package org.apache.doris.nereids.rules.analysis;
//
// import org.apache.doris.nereids.CascadesContext;
// import org.apache.doris.nereids.pattern.Pattern;
// import org.apache.doris.nereids.rules.Rule;
// import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
// import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
// import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
// import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
// import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
// import org.apache.doris.nereids.trees.expressions.Alias;
// import org.apache.doris.nereids.trees.expressions.Expression;
// import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
// import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
// import org.apache.doris.nereids.trees.plans.Plan;
//
// import com.google.common.collect.ImmutableList;
//
// import java.util.List;
//
// /**CacheSignature*/
// public class CacheSignature extends ExpressionRewrite {
//     private static final ReplaceRule INSTANCE = new ReplaceRule();
//     private final List<Rule> rules;
//     private final CascadesContext cascadesContext;
//
//     public CacheSignature(CascadesContext ctx) {
//         super(new ExpressionRuleExecutor(ImmutableList.of(bottomUp(INSTANCE))));
//         rules = buildRules();
//         cascadesContext = ctx;
//     }
//
//     /**rewriteExpr*/
//     public Plan rewriteExpr(Plan plan) {
//         for (Rule rule : rules) {
//             Pattern<Plan> pattern = (Pattern<Plan>) rule.getPattern();
//             if (pattern.matchPlanTree(plan)) {
//                 List<Plan> newPlans = rule.transform(plan, cascadesContext);
//                 Plan newPlan = newPlans.get(0);
//                 if (!newPlan.deepEquals(plan)) {
//                     return newPlan;
//                 }
//             }
//         }
//         return plan;
//     }
//
//     private static class ReplaceRule implements ExpressionPatternRuleFactory {
//         @Override
//         public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
//             return ImmutableList.of(
//                     matchesType(Alias.class).thenApply(ctx -> {
//                         Alias alias = ctx.expr;
//                         // 对alias的child进行改写，对实现了NeedSessionVarGuard接口的表达式，添加SessionVarGuardExpr
//                         // 自顶向下遍历，发现第一个需要添加SessionVarGuardExpr的表达式就添加，避免重复添加
//                         alias.child().accept(new DefaultExpressionVisitor<Void, Void>() {
//                             @Override
//                             public Void visitBoundFunction(BoundFunction expr, Void context) {
//                                 expr.getSignature();
//                                 return null;
//                             }
//                         }, null);
//                         return alias;
//                     }).toRule(ExpressionRuleType.ADD_SESSION_VAR_GUARD)
//             );
//         }
//     }
// }
