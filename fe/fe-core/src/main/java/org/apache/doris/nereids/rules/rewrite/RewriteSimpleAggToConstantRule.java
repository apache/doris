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

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.stats.SimpleAggCacheMgr;
import org.apache.doris.nereids.stats.SimpleAggCacheMgr.ColumnMinMax;
import org.apache.doris.nereids.stats.SimpleAggCacheMgr.ColumnMinMaxKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * For simple aggregation queries like
 * 'select count(*), count(not-null column), min(col), max(col) from olap_table',
 * rewrite them to return constants directly from FE metadata, completely bypassing BE.
 *
 * <p>COUNT uses table.getRowCount().
 * MIN/MAX uses an async FE-side cache ({@link SimpleAggCacheMgr}) that stores exact values
 * obtained via internal SQL queries, NOT sampled ColumnStatistic.
 *
 * <p>Conditions:
 * 1. DUP_KEYS table only (AGG_KEYS: rowCount inflated; UNIQUE_KEYS: min/max may be inaccurate
 *    in MoW model before compaction merges delete-marked rows)
 * 2. No GROUP BY
 * 3. Only COUNT / MIN / MAX aggregate functions
 * 4. COUNT(col): col must be NOT NULL (so count(col) == rowCount)
 * 5. MIN/MAX(col): col must be numeric or date type, and not aggregated
 */
public class RewriteSimpleAggToConstantRule implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // pattern: agg -> scan
                logicalAggregate(logicalOlapScan())
                        .thenApply(ctx -> {
                            LogicalAggregate<LogicalOlapScan> agg = ctx.root;
                            LogicalOlapScan olapScan = agg.child();
                            return tryRewrite(agg, olapScan, ctx.statementContext);
                        })
                        .toRule(RuleType.REWRITE_SIMPLE_AGG_TO_CONSTANT),
                // pattern: agg -> project -> scan
                logicalAggregate(logicalProject(logicalOlapScan()))
                        .thenApply(ctx -> {
                            LogicalAggregate<?> agg = ctx.root;
                            LogicalOlapScan olapScan = (LogicalOlapScan) ctx.root.child().child();
                            return tryRewrite(agg, olapScan, ctx.statementContext);
                        })
                        .toRule(RuleType.REWRITE_SIMPLE_AGG_TO_CONSTANT)
        );
    }

    private Plan tryRewrite(LogicalAggregate<?> agg, LogicalOlapScan olapScan,
            StatementContext statementContext) {
        if (olapScan.isIndexSelected()
                || !olapScan.getManuallySpecifiedPartitions().isEmpty()
                || !olapScan.getManuallySpecifiedTabletIds().isEmpty()
                || olapScan.getTableSample().isPresent()) {
            return null;
        }
        OlapTable table = olapScan.getTable();

        // Condition 1: DUP_KEYS only.
        // - DUP_KEYS: FE rowCount equals actual count(*); min/max are accurate.
        // - AGG_KEYS: rowCount may be inflated before full compaction.
        // - UNIQUE_KEYS: in MoW model, min/max may include values from delete-marked rows
        //   not yet compacted, so the result could be inaccurate.
        if (table.getKeysType() != KeysType.DUP_KEYS) {
            return null;
        }

        // Condition 2: No GROUP BY
        if (!agg.getGroupByExpressions().isEmpty()) {
            return null;
        }

        // Condition 3: Only COUNT / MIN / MAX aggregate functions.
        Set<AggregateFunction> funcs = agg.getAggregateFunctions();
        if (funcs.isEmpty()) {
            return null;
        }
        for (AggregateFunction func : funcs) {
            if (!(func instanceof Count) && !(func instanceof Min) && !(func instanceof Max)) {
                return null;
            }
        }

        // Try to compute a constant for each output expression.
        // If ANY one cannot be replaced, we give up the entire rewrite.
        List<NamedExpression> newOutputExprs = new ArrayList<>();
        for (NamedExpression outputExpr : agg.getOutputExpressions()) {
            if (!(outputExpr instanceof Alias)) {
                // Unexpected: for no-group-by aggregates, outputs should all be aliases over agg funcs
                return null;
            }
            Alias alias = (Alias) outputExpr;
            Expression child = alias.child();
            if (!(child instanceof AggregateFunction)) {
                return null;
            }
            AggregateFunction func = (AggregateFunction) child;
            Optional<Literal> constant = tryGetConstant(func, table);
            if (!constant.isPresent()) {
                // Cannot replace this agg function — give up
                return null;
            }
            newOutputExprs.add(new Alias(alias.getExprId(), constant.get(), alias.getName()));
        }

        if (newOutputExprs.isEmpty()) {
            return null;
        }

        // Build: LogicalProject(constants) -> LogicalOneRowRelation(dummy)
        // The OneRowRelation provides a single-row source; all real values come from the project.
        LogicalOneRowRelation oneRowRelation = new LogicalOneRowRelation(
                statementContext.getNextRelationId(),
                ImmutableList.of(new Alias(new NullLiteral(), "__dummy__")));
        return new LogicalProject<>(newOutputExprs, oneRowRelation);
    }

    /**
     * Try to compute a compile-time constant value for the given aggregate function,
     * using FE-side cached row-counts (for COUNT) or exact min/max cache (for MIN/MAX).
     * All values are obtained via internal SQL queries (SELECT count/min/max),
     * NOT from BE tablet stats reporting, to avoid delayed-reporting and version issues.
     */
    private Optional<Literal> tryGetConstant(AggregateFunction func, OlapTable table) {
        if (func.isDistinct()) {
            return Optional.empty();
        }

        long version;
        try {
            version = table.getVisibleVersion();
        } catch (RpcException e) {
            return Optional.empty();
        }
        // Bug: version not changed after truncating table
        if (table.selectNonEmptyPartitionIds(table.getPartitionIds()).isEmpty()) {
            return Optional.empty();
        }
        // --- COUNT ---
        if (func instanceof Count) {
            // Look up exact row count from the async cache.
            // The count is obtained by executing "SELECT count(*) FROM table" internally,
            // so it is accurate and versioned, unlike BE tablet stats reporting which
            // has delayed-reporting and version-mismatch issues.
            OptionalLong cachedCount = SimpleAggCacheMgr.internalInstance()
                    .getRowCount(table.getId(), version);
            if (!cachedCount.isPresent()) {
                return Optional.empty();
            }
            long rowCount = cachedCount.getAsLong();
            if (func.getArguments().isEmpty()) {
                // count(*) or count()
                return Optional.of(new BigIntLiteral(rowCount));
            }
            if (func.getArguments().size() == 1) {
                Expression arg = func.getArguments().get(0);
                if (arg instanceof SlotReference) {
                    Optional<Column> colOpt = ((SlotReference) arg).getOriginalColumn();
                    // count(not-null col) == rowCount
                    if (colOpt.isPresent() && !colOpt.get().isAllowNull()) {
                        return Optional.of(new BigIntLiteral(rowCount));
                    }
                }
            }
            return Optional.empty();
        }

        // --- MIN / MAX ---
        if (func instanceof Min || func instanceof Max) {
            if (func.getArguments().size() != 1) {
                return Optional.empty();
            }
            Expression arg = func.getArguments().get(0);
            if (!(arg instanceof SlotReference)) {
                return Optional.empty();
            }
            SlotReference slot = (SlotReference) arg;
            Optional<Column> colOpt = slot.getOriginalColumn();
            if (!colOpt.isPresent()) {
                return Optional.empty();
            }
            Column column = colOpt.get();
            // Only numeric and date/datetime columns are supported
            if (!column.getType().isNumericType() && !column.getType().isDateType()) {
                return Optional.empty();
            }
            // Aggregated columns cannot give correct min/max
            if (column.isAggregated()) {
                return Optional.empty();
            }

            // Look up exact min/max from the async cache
            ColumnMinMaxKey cacheKey = new ColumnMinMaxKey(table.getId(), column.getName());
            Optional<ColumnMinMax> minMax = SimpleAggCacheMgr.internalInstance().getStats(cacheKey, version);
            if (!minMax.isPresent()) {
                return Optional.empty();
            }

            // Convert the string value to a Nereids Literal
            try {
                String value = (func instanceof Min) ? minMax.get().minValue() : minMax.get().maxValue();
                LiteralExpr legacyLiteral = StatisticsUtil.readableValue(column.getType(), value);
                return Optional.of(Literal.fromLegacyLiteral(legacyLiteral, column.getType()));
            } catch (Exception e) {
                return Optional.empty();
            }
        }

        return Optional.empty();
    }
}
