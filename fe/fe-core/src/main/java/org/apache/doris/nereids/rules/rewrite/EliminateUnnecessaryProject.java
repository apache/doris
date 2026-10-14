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

import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

import com.google.common.collect.ImmutableList;

/**
 * remove the project that output same with its child to avoid we get two consecutive projects in best plan.
 * for more information, please see <a href="https://github.com/apache/doris/pull/13886">this PR</a>
 */
@DependsRules(ColumnPruning.class)
public class EliminateUnnecessaryProject implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        if (!plan.containsType(Project.class)) {
            return plan;
        }
        return rewrite(plan);
    }

    private Plan rewrite(Plan plan) {
        if (plan instanceof LogicalProject) {
            return rewriteProject((LogicalProject<?>) plan);
        } else {
            return rewriteChildren(plan);
        }
    }

    private Plan rewriteProject(LogicalProject<?> project) {
        if (project.child() instanceof LogicalEmptyRelation) {
            // eliminate unnecessary project
            return new LogicalEmptyRelation(StatementScopeIdGenerator.newRelationId(), project.getProjects());
        } else if (isPassThroughProject(project)) {
            // eliminate unnecessary project
            return rewrite(project.child());
        } else {
            return rewriteChildren(project);
        }
    }

    /**
     * 判断 project 是否只是把孩子的输出原样透传（只有这种 project 才是多余的）。
     *
     * <p>这里不能只比较 ExprId。ConstantPropagation 把 slot 替换成常量时会复用被替换 slot 的 ExprId
     * （见 ConstantPropagation#visitLogicalAggregate 中的
     * {@code new Alias(expr.getExprId(), replacedExpr, expr.getName())}），于是会构造出
     * “输出 ExprId 与孩子完全相同、但每一项都是字面量”的影子 project。例如 RQG case 11000258898：
     *
     * <pre>
     * BEFORE:
     * LogicalResultSink[61] ( outputExprs=[2004-04-28#55, 2014-07-07#56] )
     * +--LogicalLimit ( limit=1, offset=0, phase=GLOBAL )
     *    +--LogicalProject[260] ( projects=['2011-11-15 13:33:05.000' AS `2004-04-28`#55,
     *                                       '2011-11-15 13:33:05.000' AS `2014-07-07`#56] )  <- 每一项都是字面量，不是 slot
     *       +--LogicalIntersect ( qualifier=DISTINCT, outputs=[2004-04-28#55, 2014-07-07#56], ... )
     *                                                                     ^ 孩子输出的 ExprId 也是 {55, 56}
     * AFTER:  (下一条规则 OPERATIVE_COLUMN_DERIVE 的 BEFORE 里这个 Project 已经消失)
     * LogicalResultSink[61] ( outputExprs=[2004-04-28#55, 2014-07-07#56] )
     * +--LogicalLimit ( limit=1, offset=0, phase=GLOBAL )
     *    +--LogicalIntersect ( qualifier=DISTINCT, outputs=[2004-04-28#55, 2014-07-07#56], ... )
     * </pre>
     *
     * <p>此时 project 的输出 ExprId 集合 {55,56} 等于孩子 Intersect 的输出 ExprId 集合 {55,56}，
     * 只比较 ExprId 就会把它判成“多余”而删除。但这个 project 的 #55 是非空字面量
     * （cast('2011-11-15 13:33:05.000' as TEXT)，NOT NULL），孩子 Intersect 的 #55 却是 schema 级
     * nullable（第一个分支来自 nullable 列 col_datetime_3__undef_signed）。删掉之后，上层直接读到
     * Intersect 的 nullable slot，而上层引用在 rewrite 过程中已经按 NOT NULL 同步过，最终 AdjustNullable
     * 放宽该引用时会报
     * {@code AdjustNullable convert slot 2004-04-28#55 from not-nullable to nullable}。
     *
     * <p>因此，只有当 project 的每一项都是孩子输出 slot 的引用（SlotReference）时，才认为它的输出与孩子
     * 相同、可以删除。
     */
    private static boolean isPassThroughProject(LogicalProject<?> project) {
        for (NamedExpression namedExpression : project.getProjects()) {
            if (!(namedExpression instanceof SlotReference)) {
                return false;
            }
        }
        return project.getOutputExprIdBitSet().equals(project.getChildrenOutputExprIdBitSet());
    }

    private Plan rewriteChildren(Plan plan) {
        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
        boolean hasNewChildren = false;
        for (Plan child : plan.children()) {
            Plan newChild = rewrite(child);
            if (newChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(newChild);
        }
        return hasNewChildren ? plan.withChildren(newChildren.build()) : plan;
    }
}
