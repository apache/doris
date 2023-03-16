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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;

import com.google.common.collect.Lists;

/**
 * ProjectWithDistinctToAggregate.
 *
 * example sql:
 * <pre>
 * select distinct value
 * from tbl
 * </pre>
 *
 * origin plan:                                                 transformed plan:
 *
 * LogicalProject(projects=[distinct value])                        LogicalAggregate(groupBy=[value], output=[value])
 *            |                                      =>                              |
 *  LogicalOlapScan(table=tbl)                                                  LogicalOlapScan(table=tbl)
 */
public class ProjectWithDistinctToAggregate extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return RuleType.PROJECT_WITH_DISTINCT_TO_AGGREGATE.build(
                logicalProject().then(project -> {
                    if (project.isDistinct() && project.getProjects()
                            .stream()
                            .noneMatch(this::hasAggregateFunction)) {
                        return new LogicalAggregate<>(Lists.newArrayList(project.getProjects()), project.getProjects(),
                                project.child());
                    } else {
                        return project;
                    }
                })
        );
    }

    private boolean hasAggregateFunction(Expression expression) {
        return expression.anyMatch(AggregateFunction.class::isInstance);
    }
}
