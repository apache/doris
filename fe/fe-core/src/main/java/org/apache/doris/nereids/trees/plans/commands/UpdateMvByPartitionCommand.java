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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Update mv by partition
 */
public class UpdateMvByPartitionCommand extends InsertOverwriteTableCommand {
    private UpdateMvByPartitionCommand(LogicalPlan logicalQuery) {
        super(logicalQuery, Optional.empty());
    }

    /**
     * Construct command
     *
     * @param mv materialize view
     * @param partitions update partitions in mv and tables
     * @param tableWithPartKey the partitions key for different table
     * @return command
     */
    public static UpdateMvByPartitionCommand from(MTMV mv, Set<PartitionItem> partitions,
            Map<OlapTable, String> tableWithPartKey) {
        NereidsParser parser = new NereidsParser();
        Map<OlapTable, Set<Expression>> predicates =
                constructTableWithPredicates(partitions, tableWithPartKey);
        List<String> parts = constructPartsForMv(mv, partitions);
        Plan plan = parser.parseSingle(mv.getQuerySql());
        plan = plan.accept(new PredicateAdder(), predicates);
        UnboundTableSink<? extends Plan> sink =
                new UnboundTableSink<>(mv.getFullQualifiers(), ImmutableList.of(), ImmutableList.of(),
                        parts, plan);
        return new UpdateMvByPartitionCommand(sink);
    }

    private static List<String> constructPartsForMv(MTMV mv, Set<PartitionItem> partitions) {
        return mv.getPartitionNames().stream()
                .filter(name -> {
                    PartitionItem mvPartItem = mv.getPartitionInfo().getItem(mv.getPartition(name).getId());
                    return partitions.stream().anyMatch(p -> p.getIntersect(mvPartItem) != null);
                })
                .collect(ImmutableList.toImmutableList());
    }

    private static Map<OlapTable, Set<Expression>> constructTableWithPredicates(Set<PartitionItem> partitions,
            Map<OlapTable, String> tableWithPartKey) {
        ImmutableMap.Builder<OlapTable, Set<Expression>> builder = new ImmutableMap.Builder<>();
        tableWithPartKey.forEach((table, colName) ->
                builder.put(table, constructPredicates(partitions, colName))
        );
        return builder.build();
    }

    private static Set<Expression> constructPredicates(Set<PartitionItem> partitions, String colName) {
        UnboundSlot slot = new UnboundSlot(colName);
        return partitions.stream()
                .map(item -> convertPartitionItemToPredicate(item, slot))
                .collect(ImmutableSet.toImmutableSet());
    }

    private static Expression convertPartitionItemToPredicate(PartitionItem item, Slot col) {
        if (item instanceof ListPartitionItem) {
            List<Expression> inValues = ((ListPartitionItem) item).getItems().stream()
                    .map(key -> Literal.fromLegacyLiteral(key.getKeys().get(0),
                            Type.fromPrimitiveType(key.getTypes().get(0))))
                    .collect(ImmutableList.toImmutableList());
            return new InPredicate(col, inValues);
        } else {
            Range<PartitionKey> range = item.getItems();
            List<Expression> exprs = new ArrayList<>();
            if (range.hasLowerBound()) {
                PartitionKey key = range.lowerEndpoint();
                exprs.add(new GreaterThanEqual(col, Literal.fromLegacyLiteral(key.getKeys().get(0),
                        Type.fromPrimitiveType(key.getTypes().get(0)))));
            }
            if (range.hasUpperBound()) {
                PartitionKey key = range.upperEndpoint();
                exprs.add(new LessThan(col, Literal.fromLegacyLiteral(key.getKeys().get(0),
                        Type.fromPrimitiveType(key.getTypes().get(0)))));
            }
            return ExpressionUtils.and(exprs);
        }
    }

    static class PredicateAdder extends DefaultPlanRewriter<Map<OlapTable, Set<Expression>>> {
        @Override
        public Plan visitLogicalOlapScan(LogicalOlapScan scan, Map<OlapTable, Set<Expression>> predicates) {
            return new LogicalFilter<>(predicates.get(scan.getTable()), scan);
        }
    }
}
