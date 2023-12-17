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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.analyzer.UnboundRelation;
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
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;

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
     * @param partitionIds update partitions in mv and tables
     * @param tableWithPartKey the partitions key for different table
     * @return command
     */
    public static UpdateMvByPartitionCommand from(MTMV mv, Set<Long> partitionIds,
            Map<OlapTable, String> tableWithPartKey) {
        NereidsParser parser = new NereidsParser();
        Map<OlapTable, Set<Expression>> predicates =
                constructTableWithPredicates(mv, partitionIds, tableWithPartKey);
        List<String> parts = constructPartsForMv(mv, partitionIds);
        Plan plan = parser.parseSingle(mv.getQuerySql());
        plan = plan.accept(new PredicateAdder(), predicates);
        if (plan instanceof Sink) {
            plan = plan.child(0);
        }
        UnboundTableSink<? extends Plan> sink =
                new UnboundTableSink<>(mv.getFullQualifiers(), ImmutableList.of(), ImmutableList.of(),
                        parts, plan);
        return new UpdateMvByPartitionCommand(sink);
    }

    private static List<String> constructPartsForMv(MTMV mv, Set<Long> partitionIds) {
        return partitionIds.stream()
                .map(id -> mv.getPartition(id).getName())
                .collect(ImmutableList.toImmutableList());
    }

    private static Map<OlapTable, Set<Expression>> constructTableWithPredicates(MTMV mv,
            Set<Long> partitionIds, Map<OlapTable, String> tableWithPartKey) {
        Set<PartitionItem> items = partitionIds.stream()
                .map(id -> mv.getPartitionInfo().getItem(id))
                .collect(ImmutableSet.toImmutableSet());
        ImmutableMap.Builder<OlapTable, Set<Expression>> builder = new ImmutableMap.Builder<>();
        tableWithPartKey.forEach((table, colName) ->
                builder.put(table, constructPredicates(items, colName))
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
        public Plan visitUnboundRelation(UnboundRelation unboundRelation, Map<OlapTable, Set<Expression>> predicates) {
            List<String> tableQualifier = RelationUtil.getQualifierName(ConnectContext.get(),
                    unboundRelation.getNameParts());
            TableIf table = RelationUtil.getTable(tableQualifier, Env.getCurrentEnv());
            if (table instanceof OlapTable && predicates.containsKey(table)) {
                return new LogicalFilter<>(ImmutableSet.of(ExpressionUtils.or(predicates.get(table))),
                        unboundRelation);
            }
            return unboundRelation;
        }
    }
}
