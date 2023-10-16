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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitors;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * OneRowRelationExtractAggregate.
 * <p>
 * example sql:
 * <pre>
 * SELECT 1, 'a', COUNT();
 * </pre>
 * <p>
 * origin plan:
 * <p>
 * UnboundOneRowRelation ( relationId=RelationId#0, projects=[UnboundAlias(1), UnboundAlias('a'),
 *     UnboundAlias(COUNT())])
 * transformed plan:
 * <p>
 * LogicalAggregate[23] ( groupByExpr=[], outputExpr=[1 AS `1`#0, 'a' AS `'a'`#1, count(*) AS `count(*)`#2],
 *     hasRepeat=false )
 * LogicalOneRowRelation ( projects=[] )
 */
public class OneRowRelationExtractAggregate extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return RuleType.ONE_ROW_RELATION_EXTRACT_AGGREGATE.build(
                logicalOneRowRelation().then(relation -> {
                    List<NamedExpression> outputs = relation.getOutputs();
                    boolean needGlobalAggregate = outputs
                            .stream()
                            .anyMatch(p -> p.accept(ExpressionVisitors.CONTAINS_AGGREGATE_CHECKER, null));
                    if (needGlobalAggregate) {
                        LogicalRelation newRelation = new LogicalOneRowRelation(relation.getRelationId(),
                                ImmutableList.of());
                        return new LogicalAggregate<>(ImmutableList.of(), relation.getOutputs(), newRelation);
                    } else {
                        return relation;
                    }
                })
        );
    }
}
