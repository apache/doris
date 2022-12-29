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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * ProjectToGlobalAggregate.
 *
 * example sql:
 * <pre>
 * select sum(value)
 * from tbl
 * </pre>
 *
 * origin plan:                                                 transformed plan:
 *
 * LogicalProject(projects=[sum(value)])                        LogicalAggregate(groupBy=[], output=[sum(value)])
 *            |                                      =>                              |
 *  LogicalOlapScan(table=tbl)                                                  LogicalOlapScan(table=tbl)
 */
public class ProjectToGlobalAggregate extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return RuleType.PROJECT_TO_GLOBAL_AGGREGATE.build(
           logicalProject().then(project -> {
               boolean needGlobalAggregate = project.getProjects()
                       .stream()
                       .anyMatch(this::hasNonWindowedAggregateFunction);

               if (needGlobalAggregate) {
                   List<NamedExpression> output = project.getProjects().stream().map(ne -> {
                       if (ne.isAlias() && ((Alias) ne).child() instanceof NullableAggregateFunction) {
                           Alias alias = ((Alias) ne);
                           NullableAggregateFunction fn = ((NullableAggregateFunction) alias.child());
                           ne = alias.withChildren(ImmutableList.of(fn.withAlwaysNullable(true)));
                       }
                       return ne;
                   }).collect(Collectors.toList());
                   return new LogicalAggregate<>(ImmutableList.of(), output, project.child());
               } else {
                   return project;
               }
           })
        );
    }

    private boolean hasNonWindowedAggregateFunction(Expression expression) {
        // TODO: exclude windowed aggregate function
        return expression.anyMatch(AggregateFunction.class::isInstance);
    }
}
