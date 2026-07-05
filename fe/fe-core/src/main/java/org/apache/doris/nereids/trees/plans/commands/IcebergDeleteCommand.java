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

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalIcebergDeleteSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * DELETE plan synthesizer for Iceberg tables, invoked by
 * IcebergRowLevelDmlTransform.synthesize via {@link #completeQueryPlan}.
 *
 * It rewrites a DELETE into an insert-shaped plan that generates
 * position DeleteFile entries instead of data files.
 *
 * Example:
 *   DELETE FROM iceberg_table WHERE id = 1
 *
 * This will:
 *   1. Scan rows matching the WHERE condition
 *   2. Generate DeleteFile containing the matching rows
 *   3. Commit the DeleteFile to Iceberg table using RowDelta API
 *
 * The legacy Command execution half was removed as dead post-cutover code.
 */
public class IcebergDeleteCommand {

    protected final List<String> nameParts;
    protected final String tableAlias;
    protected final boolean isTempPart;
    protected final List<String> partitions;
    protected final LogicalPlan logicalQuery;
    protected final DeleteCommandContext deleteCtx;

    /**
     * constructor
     */
    public IcebergDeleteCommand(
            List<String> nameParts,
            String tableAlias,
            boolean isTempPart,
            List<String> partitions,
            LogicalPlan logicalQuery,
            DeleteCommandContext deleteCtx) {
        this.nameParts = Utils.copyRequiredList(nameParts);
        this.tableAlias = tableAlias;
        this.isTempPart = isTempPart;
        this.partitions = Utils.copyRequiredList(partitions);
        this.logicalQuery = logicalQuery;
        this.deleteCtx = deleteCtx != null ? deleteCtx : new DeleteCommandContext();
    }

    /**
     * Complete the query plan by adding necessary columns for position delete operation.
     * Select $row_id (file_path, row_position, partition info).
     */
    // package-visible: the generic RowLevelDmlCommand shell delegates synthesis here (T07c).
    LogicalPlan completeQueryPlan(ConnectContext ctx, LogicalPlan logicalQuery,
                                         ExternalTable icebergTable) {
        LogicalPlan queryPlan = buildPositionDeletePlan(ctx, logicalQuery, icebergTable);

        // Convert output to NamedExpression list
        List<NamedExpression> outputExprs;
        if (!RowLevelDmlRowIdUtils.hasUnboundPlan(queryPlan)) {
            outputExprs = queryPlan.getOutput().stream()
                    .map(slot -> (NamedExpression) slot)
                    .collect(java.util.stream.Collectors.toList());
        } else if (queryPlan instanceof LogicalProject) {
            outputExprs = ((LogicalProject<?>) queryPlan).getProjects();
        } else {
            outputExprs = ImmutableList.of();
        }

        // Wrap query plan with LogicalIcebergDeleteSink
        LogicalIcebergDeleteSink<LogicalPlan> deleteSink = new LogicalIcebergDeleteSink<>(
                (ExternalDatabase) icebergTable.getDatabase(),
                icebergTable,
                icebergTable.getBaseSchema(true),  // cols
                outputExprs,  // outputExprs
                deleteCtx,
                Optional.empty(),  // groupExpression
                Optional.empty(),  // logicalProperties
                queryPlan  // child
        );

        return deleteSink;
    }

    /**
     * Build query plan for position delete.
     * Add $row_id column to select list.
     *
     * This follows Trino's approach:
     * 1. Original query filters rows based on WHERE clause
     * 2. We project $row_id metadata column from matching rows
     * 3. The $row_id contains (file_path, row_position, partition_spec_id, partition_data)
     * 4. These will be written to Position Delete file
     */
    private LogicalPlan buildPositionDeletePlan(ConnectContext ctx, LogicalPlan logicalQuery,
                                                ExternalTable icebergTable) {
        // Step 1: Inject $row_id metadata column into the scan
        LogicalPlan planWithRowId = RowLevelDmlRowIdUtils.injectRowIdColumn(logicalQuery);

        // Step 2: Project operation + __DORIS_ICEBERG_ROWID_COL__
        Optional<Slot> rowIdSlot = Optional.empty();
        if (!RowLevelDmlRowIdUtils.hasUnboundPlan(planWithRowId)) {
            rowIdSlot = RowLevelDmlRowIdUtils.findRowIdSlot(planWithRowId.getOutput());
        }
        NamedExpression operationColumn = new UnboundAlias(
                new TinyIntLiteral(MergeOperation.DELETE_OPERATION_NUMBER),
                MergeOperation.OPERATION_COLUMN);
        NamedExpression rowIdColumn = rowIdSlot.isPresent()
                ? (NamedExpression) rowIdSlot.get()
                : new UnboundSlot(Column.ICEBERG_ROWID_COL);
        List<NamedExpression> projectItems = ImmutableList.of(operationColumn, rowIdColumn);

        return new LogicalProject<>(projectItems, planWithRowId);
    }

    public DeleteCommandContext getDeleteCtx() {
        return deleteCtx;
    }
}
