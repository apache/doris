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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.InnerProductApproximate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2DistanceApproximate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * extract virtual column from filter and push down them into olap scan.
 */
public class PushDownVirtualColumnsIntoOlapScan implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalProject(logicalFilter(logicalOlapScan()
                        .when(s -> s.getVirtualColumns().isEmpty())))
                        .then(project -> {
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            return pushDown(filter, scan, Optional.of(project));
                        }).toRule(RuleType.PUSH_DOWN_VIRTUAL_COLUMNS_INTO_OLAP_SCAN),
                logicalFilter(logicalOlapScan()
                        .when(s -> s.getVirtualColumns().isEmpty()))
                        .then(filter -> {
                            LogicalOlapScan scan = filter.child();
                            return pushDown(filter, scan, Optional.empty());
                        }).toRule(RuleType.PUSH_DOWN_VIRTUAL_COLUMNS_INTO_OLAP_SCAN)

        );
    }

    private Plan pushDown(LogicalFilter<LogicalOlapScan> filter, LogicalOlapScan logicalOlapScan,
            Optional<LogicalProject<?>> optionalProject) {
        Map<Expression, Expression> replaceMap = Maps.newHashMap();
        ImmutableList.Builder<NamedExpression> virtualColumnsBuilder = ImmutableList.builder();
        // 1. extract filter l2_distance
        // 2. generate virtual column from l2_distance and add them to scan
        // 3. replace filter
        // 4. replace project
        for (Expression conjunct : filter.getConjuncts()) {
            Set<Expression> needPushDownFunctions = conjunct.collect(e -> e instanceof L2DistanceApproximate
                            || e instanceof InnerProductApproximate);
            for (Expression needPushDownFunction : needPushDownFunctions) {
                if (replaceMap.containsKey(needPushDownFunction)) {
                    continue;
                }
                Alias alias = new Alias(needPushDownFunction);
                replaceMap.put(needPushDownFunction, alias.toSlot());
                virtualColumnsBuilder.add(alias);
            }
        }

        if (optionalProject.isPresent()) {
            LogicalProject<?> project = optionalProject.get();
            for (NamedExpression namedExpr : project.getProjects()) {
                Set<Expression> scoreFunctions = namedExpr.collect(e -> e instanceof Score);
                for (Expression scoreFunc : scoreFunctions) {
                    if (replaceMap.containsKey(scoreFunc)) {
                        continue;
                    }
                    Alias alias = new Alias(scoreFunc);
                    replaceMap.put(scoreFunc, alias.toSlot());
                    virtualColumnsBuilder.add(alias);
                }
            }
        }

        if (replaceMap.isEmpty()) {
            return null;
        }
        logicalOlapScan = logicalOlapScan.withVirtualColumns(virtualColumnsBuilder.build());
        Set<Expression> conjuncts = ExpressionUtils.replace(filter.getConjuncts(), replaceMap);
        Plan plan = filter.withConjunctsAndChild(conjuncts, logicalOlapScan);
        if (optionalProject.isPresent()) {
            LogicalProject<?> project = optionalProject.get();
            List<NamedExpression> projections = ExpressionUtils.replace(
                    (List) project.getProjects(), replaceMap);
            plan = project.withProjectsAndChild(projections, plan);
        } else {
            plan = new LogicalProject<>((List) filter.getOutput(), plan);
        }
        return plan;
    }
}
