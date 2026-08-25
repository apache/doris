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
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Rewrite MIN/MAX on a single external list partition column to constants from partition metadata.
 *
 * <p>For queries like {@code dt = (select max(dt) from hive_table)}, evaluating MAX(dt) by scanning
 * every partition blocks partition pruning for the outer scan. The selected partition map already
 * contains the exact list partition values, so this rule replaces the scalar aggregate with a
 * one-row constant relation before file-scan partition pruning runs.
 */
public class RewritePartitionColumnMinMaxToConstantRule implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate(logicalFileScan())
                        .thenApply(ctx -> {
                            LogicalAggregate<LogicalFileScan> agg = ctx.root;
                            LogicalFileScan scan = agg.child();
                            return tryRewrite(agg, scan, Optional.empty(), ctx.statementContext);
                        })
                        .toRule(RuleType.REWRITE_PARTITION_COLUMN_MIN_MAX_TO_CONSTANT),
                logicalAggregate(logicalProject(logicalFileScan()))
                        .thenApply(ctx -> {
                            LogicalAggregate<LogicalProject<LogicalFileScan>> agg = ctx.root;
                            LogicalProject<LogicalFileScan> project = agg.child();
                            LogicalFileScan scan = project.child();
                            return tryRewrite(agg, scan, Optional.of(project), ctx.statementContext);
                        })
                        .toRule(RuleType.REWRITE_PARTITION_COLUMN_MIN_MAX_TO_CONSTANT)
        );
    }

    private Plan tryRewrite(LogicalAggregate<?> agg, LogicalFileScan scan,
            Optional<LogicalProject<LogicalFileScan>> project, StatementContext statementContext) {
        if (scan.getTableSample().isPresent() || !agg.getGroupByExpressions().isEmpty()) {
            return null;
        }

        ExternalTable table = scan.getTable();
        if (!table.supportInternalPartitionPruned()) {
            return null;
        }

        List<Column> partitionColumns = table.getPartitionColumns(
                statementContext.getSnapshot(table, scan.getTableSnapshot(), scan.getScanParams()));
        if (partitionColumns.size() != 1) {
            return null;
        }
        Column partitionColumn = partitionColumns.get(0);

        Set<AggregateFunction> funcs = agg.getAggregateFunctions();
        if (funcs.isEmpty()) {
            return null;
        }
        for (AggregateFunction func : funcs) {
            if (!(func instanceof Min) && !(func instanceof Max)) {
                return null;
            }
        }

        List<NamedExpression> newOutputExprs = new ArrayList<>();
        for (NamedExpression outputExpr : agg.getOutputExpressions()) {
            if (!(outputExpr instanceof Alias)) {
                return null;
            }
            Alias alias = (Alias) outputExpr;
            Expression child = alias.child();
            if (!(child instanceof AggregateFunction)) {
                return null;
            }
            Optional<Literal> constant = tryGetConstant(
                    (AggregateFunction) child, partitionColumn, scan, project);
            if (!constant.isPresent()) {
                return null;
            }
            newOutputExprs.add(new Alias(alias.getExprId(), constant.get(), alias.getName()));
        }

        if (newOutputExprs.isEmpty()) {
            return null;
        }

        LogicalOneRowRelation oneRowRelation = new LogicalOneRowRelation(
                statementContext.getNextRelationId(),
                ImmutableList.of(new Alias(new NullLiteral(), "__dummy__")));
        return new LogicalProject<>(newOutputExprs, oneRowRelation);
    }

    private Optional<Literal> tryGetConstant(AggregateFunction func, Column partitionColumn, LogicalFileScan scan,
            Optional<LogicalProject<LogicalFileScan>> project) {
        if (func.isDistinct() || func.getArguments().size() != 1) {
            return Optional.empty();
        }
        Optional<SlotReference> slot = resolveSlot(func.getArguments().get(0), project);
        if (!slot.isPresent() || !isPartitionColumn(slot.get(), partitionColumn)) {
            return Optional.empty();
        }

        return findPartitionMinMaxLiteral(func instanceof Min, scan, partitionColumn);
    }

    private Optional<SlotReference> resolveSlot(Expression expression,
            Optional<LogicalProject<LogicalFileScan>> project) {
        Expression resolved = expression;
        if (project.isPresent() && expression instanceof Slot) {
            Map<Slot, Expression> aliasToProducer = ((Project) project.get()).getAliasToProducer();
            resolved = aliasToProducer.getOrDefault(expression, expression);
        }
        if (resolved instanceof SlotReference) {
            return Optional.of((SlotReference) resolved);
        }
        return Optional.empty();
    }

    private boolean isPartitionColumn(SlotReference slot, Column partitionColumn) {
        Optional<Column> originalColumn = slot.getOriginalColumn();
        if (originalColumn.isPresent()) {
            return originalColumn.get().getName().equalsIgnoreCase(partitionColumn.getName());
        }
        return slot.getName().equalsIgnoreCase(partitionColumn.getName());
    }

    private Optional<Literal> findPartitionMinMaxLiteral(boolean isMin, LogicalFileScan scan, Column partitionColumn) {
        PartitionKey selectedKey = null;
        for (PartitionItem item : scan.getSelectedPartitions().selectedPartitions.values()) {
            if (item.isDefaultPartition() || !(item instanceof ListPartitionItem)) {
                return Optional.empty();
            }
            for (PartitionKey key : ((ListPartitionItem) item).getItems()) {
                if (key.isDefaultListPartitionKey()) {
                    return Optional.empty();
                }
                org.apache.doris.analysis.LiteralExpr literalExpr = key.getKeys().get(0);
                if (literalExpr instanceof org.apache.doris.analysis.NullLiteral) {
                    continue;
                }
                if (selectedKey == null || (isMin ? key.compareTo(selectedKey) < 0 : key.compareTo(selectedKey) > 0)) {
                    selectedKey = key;
                }
            }
        }

        if (selectedKey == null) {
            return Optional.of(new NullLiteral(DataType.fromCatalogType(partitionColumn.getType())));
        }
        Type literalType = Type.fromPrimitiveType(selectedKey.getTypes().get(0));
        return Optional.of(Literal.fromLegacyLiteral(selectedKey.getKeys().get(0), literalType));
    }
}
