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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.rewrite.BottomUpVisitorRewriteJob;
import org.apache.doris.nereids.rules.FilteredRules;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.qe.cache.CacheAnalyzer;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** FoldConstantForSqlCache */
public class FoldConstantForSqlCache implements CustomRewriter {
    private static final BottomUpVisitorRewriteJob rewriteJob;

    static {
        List<Rule> rules = new ExpressionRewrite(
                new ExpressionRuleExecutor(ImmutableList.of()) {
                    @Override
                    public Expression rewrite(Expression root, ExpressionRewriteContext ctx) {
                        StatementContext statementContext = ctx.cascadesContext.getStatementContext();
                        SqlCacheContext sqlCacheContext = statementContext.getSqlCacheContext().get();
                        Expression foldNondeterministic = new FoldConstantRuleOnFE(true) {
                            @Override
                            public Expression visitBoundFunction(
                                    BoundFunction boundFunction, ExpressionRewriteContext context) {
                                Expression fold = super.visitBoundFunction(boundFunction, context);
                                boolean unfold = !fold.isDeterministic();
                                if (unfold) {
                                    sqlCacheContext.setCannotProcessExpression(true);
                                }
                                if (!boundFunction.isDeterministic() && !unfold) {
                                    sqlCacheContext.addFoldNondeterministicPair(boundFunction, fold);
                                }
                                return fold;
                            }
                        }.rewrite(root, ctx);

                        if (foldNondeterministic != root) {
                            sqlCacheContext.addFoldFullNondeterministicPair(root, foldNondeterministic);
                            return foldNondeterministic;
                        }
                        return root;
                    }
                }
        ).buildRules();

        rewriteJob = new BottomUpVisitorRewriteJob(
                new FilteredRules(rules), Predicates.alwaysTrue());
    }

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        boolean wantToUseSqlCache = CacheAnalyzer.canUseSqlCache(
                cascadesContext.getConnectContext().getSessionVariable());
        StatementContext statementContext = cascadesContext.getStatementContext();
        SqlCacheContext sqlCacheContext = statementContext.getSqlCacheContext().orElse(null);
        if (!wantToUseSqlCache || !statementContext.hasNondeterministic()
                || sqlCacheContext == null
                || !sqlCacheContext.supportSqlCache()) {
            return plan;
        }

        rewriteJob.execute(jobContext);
        return cascadesContext.getRewritePlan();
    }
}
