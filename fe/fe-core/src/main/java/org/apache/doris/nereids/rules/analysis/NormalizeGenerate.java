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
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * NormalizeGenerate
 */
public class NormalizeGenerate extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return logicalGenerate()
                .when(generate -> generate.getGenerators().stream()
                        .anyMatch(expr -> expr.containsType(SubqueryExpr.class)))
                .then(generate -> {
                    List<Expression> subqueries = ExpressionUtils.collectToList(
                            generate.getExpressions(), SubqueryExpr.class::isInstance);
                    Map<Expression, Expression> replaceMap = new HashMap<>();
                    ImmutableList.Builder<Alias> builder = ImmutableList.builder();
                    for (Expression expr : subqueries) {
                        Alias alias = new Alias(expr);
                        builder.add(alias);
                        replaceMap.put(expr, alias.toSlot());
                    }
                    LogicalProject logicalProject = new LogicalProject(builder.build(), generate.child());
                    List<Function> newGenerators = new ArrayList<>(generate.getGenerators().size());
                    for (Function function : generate.getGenerators()) {
                        newGenerators.add((Function) ExpressionUtils.replace(function, replaceMap));
                    }
                    return generate.withGenerators(newGenerators).withChildren(ImmutableList.of(logicalProject));
                })
                .toRule(RuleType.NORMALIZE_GENERATE);
    }
}
