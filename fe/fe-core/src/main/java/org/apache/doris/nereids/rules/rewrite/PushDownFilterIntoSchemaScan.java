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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalSchemaScan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * Used to push down catalog/db/table name to schema scan node.
 */
public class PushDownFilterIntoSchemaScan extends OneRewriteRuleFactory {

    public static ImmutableSet<String> SUPPOPRT_FRONTEND_CONJUNCTS_TABLES =
            ImmutableSet.of("view_dependency");

    @Override
    public Rule build() {
        return logicalFilter(logicalSchemaScan()).when(p -> !p.child().isFilterPushed()).thenApply(ctx -> {
            LogicalFilter<LogicalSchemaScan> filter = ctx.root;
            LogicalSchemaScan scan = filter.child();
            List<Optional<String>> fixedFilter = getFixedFilter(filter);
            List<Expression> commonFilter = getCommonFilter(filter);
            LogicalSchemaScan rewrittenScan = scan.withFrontendConjuncts(fixedFilter.get(0), fixedFilter.get(1),
                    fixedFilter.get(2), commonFilter);
            return filter.withChildren(ImmutableList.of(rewrittenScan));
        }).toRule(RuleType.PUSH_FILTER_INTO_SCHEMA_SCAN);
    }

    private List<Optional<String>> getFixedFilter(LogicalFilter<LogicalSchemaScan> filter) {
        Optional<String> schemaCatalog = Optional.empty();
        Optional<String> schemaDatabase = Optional.empty();
        Optional<String> schemaTable = Optional.empty();
        for (Expression expression : filter.getConjuncts()) {
            if (!(expression instanceof EqualTo)) {
                continue;
            }
            Expression slot = expression.child(0);
            if (!(slot instanceof SlotReference)) {
                continue;
            }
            Optional<Column> column = ((SlotReference) slot).getOriginalColumn();
            if (!column.isPresent()) {
                continue;
            }
            String columnName = column.get().getName();
            Expression slotValue = expression.child(1);
            if (!(slotValue instanceof VarcharLiteral)) {
                continue;
            }
            String columnValue = ((VarcharLiteral) slotValue).getValue();
            if ("TABLE_CATALOG".equals(columnName)) {
                schemaCatalog = Optional.of(columnValue);
            } else if ("TABLE_SCHEMA".equals(columnName)) {
                schemaDatabase = Optional.of(columnValue);
            } else if ("TABLE_NAME".equals(columnName)) {
                schemaTable = Optional.of(columnValue);
            }
        }
        return Lists.newArrayList(schemaCatalog, schemaDatabase, schemaTable);
    }

    private List<Expression> getCommonFilter(LogicalFilter<LogicalSchemaScan> filter) {
        List<Expression> res = Lists.newArrayList();
        if (!SUPPOPRT_FRONTEND_CONJUNCTS_TABLES.contains(filter.child().getTable().getName().toLowerCase())) {
            return res;
        }
        for (Expression expression : filter.getConjuncts()) {
            if (!supportOnFe(expression)) {
                continue;
            }
            res.add(expression);
        }
        return res;
    }

    private boolean supportOnFe(Expression expression) {
        return expression instanceof EqualTo
                || expression instanceof Or
                || expression instanceof InPredicate
                || expression instanceof LessThan
                || expression instanceof LessThanEqual
                || expression instanceof GreaterThan
                || expression instanceof GreaterThanEqual
                || expression instanceof Not;
    }
}
