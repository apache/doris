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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalSchemaScan;

import com.google.common.collect.ImmutableList;

import java.util.Optional;

/**
 * Used to push down catalog/db/table name to schema scan node.
 */
public class PushDownFilterIntoSchemaScan extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalSchemaScan()).when(p -> !p.child().isFilterPushed()).thenApply(ctx -> {
            LogicalFilter<LogicalSchemaScan> filter = ctx.root;
            LogicalSchemaScan scan = filter.child();
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
            LogicalSchemaScan rewrittenScan = scan.withSchemaIdentifier(schemaCatalog, schemaDatabase, schemaTable);
            return filter.withChildren(ImmutableList.of(rewrittenScan));
        }).toRule(RuleType.PUSH_FILTER_INTO_SCHEMA_SCAN);
    }
}
