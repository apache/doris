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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitors;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ProjectToGlobalAggregate.
 * <p>
 * example sql:
 * <pre>
 * select sum(value)
 * from tbl
 * </pre>
 *
 * origin plan:                                                 transformed plan:
 * <p>
 * LogicalProject(projects=[sum(value)])                        LogicalAggregate(groupBy=[], output=[sum(value)])
 *            |                                      =>                              |
 *  LogicalOlapScan(table=tbl)                                                  LogicalOlapScan(table=tbl)
 */
public class ProjectToGlobalAggregate extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return RuleType.PROJECT_TO_GLOBAL_AGGREGATE.build(
           logicalProject().then(project -> {
               project = distinctConstantsToLimit1(project);
               Plan result = projectToAggregate(project);
               return distinctToAggregate(result, project);
           })
        );
    }

    // select distinct 1,2,3 from tbl
    //               ↓
    // select 1,2,3 from (select 1, 2, 3 from tbl limit 1) as tmp
    private static LogicalProject<Plan> distinctConstantsToLimit1(LogicalProject<Plan> project) {
        if (!project.isDistinct()) {
            return project;
        }

        boolean allSelectItemAreConstants = true;
        for (NamedExpression selectItem : project.getProjects()) {
            if (!selectItem.isConstant()) {
                allSelectItemAreConstants = false;
                break;
            }
        }

        if (allSelectItemAreConstants) {
            return new LogicalProject<>(
                    project.getProjects(),
                    new LogicalLimit<>(1, 0, LimitPhase.ORIGIN, project.child())
            );
        }
        return project;
    }

    // select avg(xxx) from tbl
    //         ↓
    // LogicalAggregate(groupBy=[], output=[avg(xxx)])
    private static Plan projectToAggregate(LogicalProject<Plan> project) {
        // contains aggregate functions, like sum, avg ?
        for (NamedExpression selectItem : project.getProjects()) {
            if (selectItem.accept(ExpressionVisitors.CONTAINS_AGGREGATE_CHECKER, null)) {
                return new LogicalAggregate<>(ImmutableList.of(), project.getProjects(), project.child());
            }
        }
        return project;
    }

    private static Plan distinctToAggregate(Plan result, LogicalProject<Plan> originProject) {
        if (!originProject.isDistinct()) {
            return result;
        }
        if (result instanceof LogicalProject) {
            // remove distinct: select distinct fun(xxx) as c1 from tbl
            //
            // LogicalProject(distinct=true, output=[fun(xxx) as c1])
            //                  ↓
            // LogicalAggregate(groupBy=[c1], output=[c1])
            //                  |
            //   LogicalProject(output=[fun(xxx) as c1])
            LogicalProject<?> project = (LogicalProject<?>) result;

            ImmutableList.Builder<NamedExpression> bottomProjectOutput
                    = ImmutableList.builderWithExpectedSize(project.getProjects().size());
            ImmutableList.Builder<NamedExpression> topAggOutput
                    = ImmutableList.builderWithExpectedSize(project.getProjects().size());

            boolean hasComplexExpr = false;
            for (NamedExpression selectItem : project.getProjects()) {
                if (selectItem.isSlot()) {
                    topAggOutput.add(selectItem);
                    bottomProjectOutput.add(selectItem);
                } else if (isAliasLiteral(selectItem)) {
                    // stay in agg, and eliminate by `ELIMINATE_GROUP_BY_CONSTANT`
                    topAggOutput.add(selectItem);
                } else {
                    // `FillUpMissingSlots` not support find complex expr in aggregate,
                    // so we should push down into the bottom project
                    hasComplexExpr = true;
                    topAggOutput.add(selectItem.toSlot());
                    bottomProjectOutput.add(selectItem);
                }
            }

            if (!hasComplexExpr) {
                List<Slot> projects = (List) project.getProjects();
                return new LogicalAggregate(projects, projects, project.child());
            }

            LogicalProject<?> removeDistinct = new LogicalProject<>(bottomProjectOutput.build(), project.child());
            ImmutableList<NamedExpression> aggOutput = topAggOutput.build();
            return new LogicalAggregate(aggOutput, aggOutput, removeDistinct);
        } else if (result instanceof LogicalAggregate) {
            // remove distinct: select distinct avg(xxx) as c1 from tbl
            //
            // LogicalProject(distinct=true, output=[avg(xxx) as c1])
            //                  ↓
            //  LogicalAggregate(output=[avg(xxx) as c1])
            return result;
        } else {
            // never reach
            throw new AnalysisException("Unsupported");
        }
    }

    private static boolean isAliasLiteral(NamedExpression selectItem) {
        return selectItem instanceof Alias && selectItem.child(0) instanceof Literal;
    }
}
