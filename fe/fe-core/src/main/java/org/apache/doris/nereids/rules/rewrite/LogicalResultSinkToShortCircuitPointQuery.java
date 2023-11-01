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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * short circuit query optimization
 * pattern : select xxx from tbl where key = ?
 */
public class LogicalResultSinkToShortCircuitPointQuery extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalResultSink(logicalProject(logicalFilter(logicalOlapScan())))
                .when(r -> r.child().child().child().getTable().getEnableLightSchemaChange())
                .when(r -> r.child().child().child().getTable().getEnableUniqueKeyMergeOnWrite())
                .when(r -> r.child().child().child().getTable().storeRowColumn())
                // all conjuncts match with pattern `key = ?`
                .when(r -> r.child().child().getConjuncts().stream().allMatch(
                        expression -> (expression instanceof EqualTo)
                                && (expression.child(0).isKeyColumnFromTable()
                                    || ((SlotReference) expression.child(0)).getName().equals(Column.DELETE_SIGN))
                                && expression.child(1).isLiteral()))

                .thenApply(ctx -> {
                    OlapTable olapTable = ctx.root.child().child().child().getTable();
                    // All key columns in conjuncts
                    Set<String> colNames = Sets.newHashSet();
                    for (Expression expr : ctx.root.child().child().getConjuncts()) {
                        colNames.add(((SlotReference) (expr.child(0))).getName());
                    }
                    if (olapTable.getBaseSchemaKeyColumns().size() <= colNames.size()) {
                        ctx.statementContext.getParsedStatement().setPointQueryShortCircuit(true);
                    }
                    return ctx.root;
                }).toRule(RuleType.SHOR_CIRCUIT_POINT_QUERY);
    }
}
