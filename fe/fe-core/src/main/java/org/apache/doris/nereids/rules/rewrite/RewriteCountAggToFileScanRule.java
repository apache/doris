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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.BigIntType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Rewrite count(*)/count(1) over file scan to sum0(__count_from_metadata__).
 * This allows the BE scanner to directly return the row count from file metadata
 * (parquet/orc row group num_rows) instead of creating N empty rows.
 *
 * Only applies to HMS-based tables that use native Parquet/ORC readers:
 * - Hive non-Full-Acid tables
 * - Hudi COW tables
 *
 * Iceberg and Paimon are NOT handled by this rule; they use their own count pushdown
 * paths through AggregateStrategies / PhysicalStorageLayerAggregate with COUNT_FROM_METADATA.
 *
 * Supports GROUP BY on partition columns only, where partition column values come from
 * file paths and the count is read from metadata.
 *
 * Before (no group by):
 *   LogicalAggregate(output=[count(*)])
 *     +--LogicalFileScan(hive_table)
 *
 * After:
 *   LogicalAggregate(output=[sum0(__count_from_metadata__) AS count(*)])
 *     +--LogicalFileScan(hive_table, output=[__count_from_metadata__])
 *
 * Before (group by partition columns):
 *   LogicalAggregate(groupBy=[part_col], output=[part_col, count(*)])
 *     +--LogicalFileScan(hive_table)
 *
 * After:
 *   LogicalAggregate(groupBy=[part_col], output=[part_col, sum0(__count_from_metadata__) AS count(*)])
 *     +--LogicalFileScan(hive_table, output=[part_col, __count_from_metadata__])
 */
public class RewriteCountAggToFileScanRule implements RewriteRuleFactory {

    public static final String COUNT_FROM_METADATA_COL = "__count_from_metadata__";

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // Aggregate -> FileScan (no project)
                logicalAggregate(logicalFileScan())
                        .when(this::canRewrite)
                        .then(this::doRewriteWithoutProject)
                        .toRule(RuleType.REWRITE_COUNT_AGG_TO_FILE_SCAN),
                // Aggregate -> Project -> FileScan
                logicalAggregate(logicalProject(logicalFileScan()))
                        .when(this::canRewriteWithProject)
                        .then(this::doRewriteWithProject)
                        .toRule(RuleType.REWRITE_COUNT_AGG_TO_FILE_SCAN)
        );
    }

    /**
     * Get partition column names from the file scan's external table.
     */
    private Set<String> getPartitionColumnNames(LogicalFileScan fileScan) {
        ExternalTable table = fileScan.getTable();
        if (table instanceof HMSExternalTable) {
            return ((HMSExternalTable) table).getPartitionColumnNames();
        }
        // For other external tables, try getPartitionColumns
        List<Column> partitionColumns = table.getPartitionColumns(Optional.empty());
        if (partitionColumns == null) {
            return Sets.newHashSet();
        }
        return partitionColumns.stream().map(Column::getName).collect(Collectors.toSet());
    }

    /**
     * Check if all group by expressions reference partition columns only.
     */
    private boolean isGroupByOnPartitionColumnsOnly(LogicalAggregate<? extends Plan> agg,
            LogicalFileScan fileScan) {
        List<Expression> groupByExprs = agg.getGroupByExpressions();
        if (groupByExprs.isEmpty()) {
            return true;
        }
        Set<String> partitionColNames = getPartitionColumnNames(fileScan);
        if (partitionColNames.isEmpty()) {
            return false;
        }
        for (Expression expr : groupByExprs) {
            if (!(expr instanceof SlotReference)) {
                return false;
            }
            SlotReference slot = (SlotReference) expr;
            if (!partitionColNames.contains(slot.getName())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if all aggregate functions are count(*) or count(1) (non-distinct).
     */
    private boolean isAllCountStarOrCountLiteral(LogicalAggregate<? extends Plan> agg) {
        if (!agg.getDistinctArguments().isEmpty()) {
            return false;
        }
        Set<AggregateFunction> functions = agg.getAggregateFunctions();
        if (functions.isEmpty()) {
            return false;
        }
        for (AggregateFunction func : functions) {
            if (!(func instanceof Count)) {
                return false;
            }
            Count count = (Count) func;
            if (count.isDistinct()) {
                return false;
            }
            if (!count.isCountStar()) {
                return false;
            }
        }
        return true;
    }

    private boolean canRewrite(LogicalAggregate<LogicalFileScan> agg) {
        if (!isAllCountStarOrCountLiteral(agg)) {
            return false;
        }
        if (!isNativeReaderTable(agg.child())) {
            return false;
        }
        return isGroupByOnPartitionColumnsOnly(agg, agg.child());
    }

    private boolean canRewriteWithProject(LogicalAggregate<LogicalProject<LogicalFileScan>> agg) {
        if (!isAllCountStarOrCountLiteral(agg)) {
            return false;
        }
        if (!isNativeReaderTable(agg.child().child())) {
            return false;
        }
        return isGroupByOnPartitionColumnsOnly(agg, agg.child().child());
    }

    /**
     * Check if the table uses native Parquet/ORC reader so COUNT_FROM_METADATA works.
     * Only HMS-based tables that read files through native Parquet/ORC readers are supported:
     * - Hive tables with Parquet/ORC format (including Full-Acid, they use native ORC reader)
     * - Hudi COW tables (MOR tables use JNI reader)
     * HMS Iceberg tables are excluded because they have their own snapshot count path.
     * Non-HMS tables (Paimon, etc.) are excluded because they have their own count paths.
     * Text/CSV/JSON etc. Hive tables are excluded because they use non-native readers
     * and fall back to regular COUNT pushdown (not COUNT_FROM_METADATA).
     */
    private boolean isNativeReaderTable(LogicalFileScan fileScan) {
        ExternalTable table = fileScan.getTable();
        if (!(table instanceof HMSExternalTable)) {
            return false;
        }
        HMSExternalTable hmsTable = (HMSExternalTable) table;
        // HMS Iceberg tables use their own snapshot count path, not native Parquet/ORC reader
        if (hmsTable.getDlaType() == HMSExternalTable.DLAType.ICEBERG) {
            return false;
        }
        // Hudi COW tables use native Parquet reader
        if (hmsTable.isHoodieCowTable()) {
            return true;
        }
        // Hive tables with Parquet/ORC format use native reader (including Full-Acid)
        // Text/CSV/JSON etc. fall back to regular COUNT pushdown
        return hmsTable.getDlaType() == HMSExternalTable.DLAType.HIVE
                && hmsTable.isParquetOrOrcFormat();
    }

    /**
     * Collect partition column slots from the file scan that are referenced in group by expressions.
     */
    private List<SlotReference> collectPartitionSlots(LogicalAggregate<? extends Plan> agg,
            LogicalFileScan fileScan) {
        Set<String> partitionColNames = getPartitionColumnNames(fileScan);
        List<SlotReference> partitionSlots = Lists.newArrayList();
        for (Expression expr : agg.getGroupByExpressions()) {
            if (expr instanceof SlotReference) {
                SlotReference slot = (SlotReference) expr;
                if (partitionColNames.contains(slot.getName())) {
                    partitionSlots.add(slot);
                }
            }
        }
        return partitionSlots;
    }

    private Plan doRewriteWithoutProject(LogicalAggregate<LogicalFileScan> agg) {
        LogicalFileScan fileScan = agg.child();

        // Create __count_from_metadata__ slot with a Column object so that
        // the downstream SlotDescriptor.getColumn() does not return null.
        // The column is non-nullable since count from metadata is always a valid BIGINT value.
        SlotReference countFromMetadataSlot = createCountFromMetadataSlot();

        // Build new aggregate output expressions: count(*) -> sum0(__count_from_metadata__)
        List<NamedExpression> newOutputExpressions = Lists.newArrayList();
        for (NamedExpression outputExpr : agg.getOutputExpressions()) {
            Expression newExpr = rewriteCountToSum(outputExpr, countFromMetadataSlot);
            newOutputExpressions.add((NamedExpression) newExpr);
        }

        // Build project expressions: partition columns + __count_from_metadata__
        List<NamedExpression> newProjectExprs = Lists.newArrayList();
        List<SlotReference> partitionSlots = collectPartitionSlots(agg, fileScan);
        newProjectExprs.addAll(partitionSlots);
        newProjectExprs.add(countFromMetadataSlot);

        // Build file scan operative slots: partition columns + __count_from_metadata__
        // Operative slots control which columns are actually read from the file.
        List<Slot> operativeSlots = Lists.newArrayList();
        operativeSlots.addAll(partitionSlots);
        operativeSlots.add(countFromMetadataSlot);

        // Build cached outputs: all original output slots + __count_from_metadata__.
        // We must include __count_from_metadata__ because it is not in the table's base schema,
        // so computeOutput() won't include it. We must also keep all original output slots
        // so that the FileScan's output has more columns than the Project selects.
        // If we only include [partition_cols, __count_from_metadata__], the Project would output
        // the same as FileScan and get removed by RemoveUselessProjectPostProcessor,
        // causing the scan node to lose its final projections.
        List<Slot> cachedOutputs = Lists.newArrayList();
        cachedOutputs.addAll(fileScan.getOutput());
        cachedOutputs.add(countFromMetadataSlot);

        LogicalFileScan newFileScan = ((LogicalFileScan) fileScan
                .withOperativeSlots(operativeSlots))
                .withCachedOutput(cachedOutputs);

        LogicalProject<LogicalFileScan> newProject = new LogicalProject<>(
                newProjectExprs, newFileScan);

        return agg.withAggOutputChild(newOutputExpressions, newProject);
    }

    private Plan doRewriteWithProject(LogicalAggregate<LogicalProject<LogicalFileScan>> agg) {
        LogicalProject<LogicalFileScan> project = agg.child();
        LogicalFileScan fileScan = project.child();

        // Create __count_from_metadata__ slot with a Column object
        SlotReference countFromMetadataSlot = createCountFromMetadataSlot();

        // Build new aggregate output: count(*) -> sum0(__count_from_metadata__)
        List<NamedExpression> newOutputExpressions = Lists.newArrayList();
        for (NamedExpression outputExpr : agg.getOutputExpressions()) {
            Expression newExpr = rewriteCountToSum(outputExpr, countFromMetadataSlot);
            newOutputExpressions.add((NamedExpression) newExpr);
        }

        // The aggregate's group by slots are from Project's output.
        // Map them back to FileScan's slots through the Project.
        List<SlotReference> aggGroupBySlots = Lists.newArrayList();
        for (Expression groupByExpr : agg.getGroupByExpressions()) {
            if (groupByExpr instanceof SlotReference) {
                aggGroupBySlots.add((SlotReference) groupByExpr);
            }
        }

        // Build file scan operative slots: map from Aggregate's group by slots
        // to FileScan's slots through Project.
        List<Slot> operativeSlots = Lists.newArrayList();
        List<Slot> projectOutputSlots = project.getOutput();
        for (SlotReference aggGroupBySlot : aggGroupBySlots) {
            for (int i = 0; i < projectOutputSlots.size(); i++) {
                Slot outputSlot = projectOutputSlots.get(i);
                if (outputSlot.equals(aggGroupBySlot)) {
                    NamedExpression projectExpr = project.getProjects().get(i);
                    Expression childExpr = projectExpr;
                    while (childExpr instanceof Alias) {
                        childExpr = ((Alias) childExpr).child();
                    }
                    if (childExpr instanceof SlotReference) {
                        operativeSlots.add((SlotReference) childExpr);
                    }
                    break;
                }
            }
        }
        operativeSlots.add(countFromMetadataSlot);

        // Build cached outputs: all original output slots + __count_from_metadata__.
        List<Slot> cachedOutputs = Lists.newArrayList();
        cachedOutputs.addAll(fileScan.getOutput());
        cachedOutputs.add(countFromMetadataSlot);

        LogicalFileScan newFileScan = ((LogicalFileScan) fileScan
                .withOperativeSlots(operativeSlots))
                .withCachedOutput(cachedOutputs);

        // Build new project expressions: Aggregate's group by slots + __count_from_metadata__.
        List<NamedExpression> newProjectExprs = Lists.newArrayList();
        newProjectExprs.addAll(aggGroupBySlots);
        newProjectExprs.add(countFromMetadataSlot);

        LogicalProject<LogicalFileScan> newProject = new LogicalProject<>(
                newProjectExprs, newFileScan);

        return agg.withAggOutputChild(newOutputExpressions, newProject);
    }

    /**
     * Rewrite count(*) in expression to sum0(__count_from_metadata__).
     */
    private Expression rewriteCountToSum(Expression expr, SlotReference countFromMetadataSlot) {
        return expr.rewriteDownShortCircuit(e -> {
            if (e instanceof Count) {
                Count count = (Count) e;
                if (count.isCountStar() && !count.isDistinct()) {
                    Sum0 sum0 = new Sum0(countFromMetadataSlot);
                    if (e instanceof Alias) {
                        return new Alias(sum0, ((Alias) e).getName());
                    }
                    return sum0;
                }
            }
            return e;
        });
    }

    /**
     * Create the __count_from_metadata__ SlotReference with a Column object.
     * The Column is needed so that downstream SlotDescriptor.getColumn() does not return null,
     * which would cause NPE in FileQueryScanNode.initSchemaParams().
     * The slot is non-nullable because count from metadata is always a valid BIGINT value.
     */
    private SlotReference createCountFromMetadataSlot() {
        Column countColumn = new Column(COUNT_FROM_METADATA_COL, PrimitiveType.BIGINT, false);
        return new SlotReference(
                StatementScopeIdGenerator.newExprId(),
                COUNT_FROM_METADATA_COL, BigIntType.INSTANCE, false, ImmutableList.of(),
                null, countColumn, null, countColumn);
    }
}
