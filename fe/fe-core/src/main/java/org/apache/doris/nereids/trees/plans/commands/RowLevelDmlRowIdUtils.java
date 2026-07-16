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
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Row-id injection utilities for row-level DML (DELETE/UPDATE/MERGE) commands.
 * Provides shared helpers for row-id injection (the SDK expression-conversion half was removed together
 * with its dead legacy callers; the live conversion lives in the connector's IcebergPredicateConverter).
 */
public class RowLevelDmlRowIdUtils {

    // ==================== Row-ID Injection Utilities ====================

    /**
     * Inject $row_id column into the plan for any Iceberg table scan.
     * Used by DELETE and UPDATE commands (single-table, no ambiguity).
     */
    public static LogicalPlan injectRowIdColumn(LogicalPlan plan) {
        if (hasUnboundPlan(plan)) {
            return plan;
        }
        return (LogicalPlan) plan.accept(new IcebergRowIdInjector(null), null);
    }

    /**
     * Inject $row_id column only for the specified target table.
     * Used by MERGE INTO where source may also be an Iceberg table.
     */
    public static LogicalPlan injectRowIdColumn(LogicalPlan plan, ExternalTable targetTable) {
        if (hasUnboundPlan(plan)) {
            return plan;
        }
        return (LogicalPlan) plan.accept(new IcebergRowIdInjector(targetTable), null);
    }

    /** Check if any slot in the list is the row-id column. */
    public static boolean hasRowIdSlot(List<Slot> slots) {
        return findRowIdSlot(slots).isPresent();
    }

    /** Find the row-id slot in the list, if present. */
    public static Optional<Slot> findRowIdSlot(List<Slot> slots) {
        for (Slot slot : slots) {
            if (Column.ICEBERG_ROWID_COL.equalsIgnoreCase(slot.getName())) {
                return Optional.of(slot);
            }
        }
        return Optional.empty();
    }

    /** Check if any project expression is the row-id column. */
    public static boolean hasRowIdProject(List<NamedExpression> projects) {
        for (NamedExpression project : projects) {
            if (project instanceof Slot
                    && Column.ICEBERG_ROWID_COL.equalsIgnoreCase(((Slot) project).getName())) {
                return true;
            }
        }
        return false;
    }

    /** Resolve the row-id Column definition from the table's full schema. */
    public static Column getRowIdColumn(ExternalTable table) {
        List<Column> fullSchema = table.getFullSchema();
        if (fullSchema != null) {
            for (Column column : fullSchema) {
                if (Column.ICEBERG_ROWID_COL.equalsIgnoreCase(column.getName())) {
                    return column;
                }
            }
        }
        return IcebergRowId.createHiddenColumn();
    }

    /**
     * Whether a scan's table is a row-id-injection target: an iceberg table is a
     * {@link PluginDrivenExternalTable}, identified by the neutral row-level-DML connector capability rather
     * than {@code instanceof Iceberg*} (iron-law: connector-capability-driven). Only the iceberg connector
     * declares supportsDelete/supportsMerge, so this precisely selects iceberg scans among a mixed plan
     * (e.g. a MERGE whose source is a different table type).
     */
    static boolean isRowIdInjectionTarget(ExternalTable table) {
        return table instanceof PluginDrivenExternalTable
                && pluginConnectorSupportsRowLevelDml((PluginDrivenExternalTable) table);
    }

    private static boolean pluginConnectorSupportsRowLevelDml(PluginDrivenExternalTable table) {
        // Resolved per-handle through the table's write-op probe (a heterogeneous gateway admits row-level DML
        // for its iceberg tables but not its hive tables). It degrades to the empty set on a dropped connector /
        // unresolvable handle (mirroring fetchSyntheticWriteColumns), so a mid-DML catalog drop is "not a target"
        // rather than an NPE.
        Set<WriteOperation> ops = table.connectorSupportedWriteOperations();
        return ops.contains(WriteOperation.DELETE) || ops.contains(WriteOperation.MERGE);
    }

    /** Check if a plan tree contains any unbound nodes or expressions. */
    public static boolean hasUnboundPlan(Plan plan) {
        return plan.anyMatch(node -> node instanceof Unbound || ((Plan) node).hasUnboundExpression());
    }

    /**
     * Plan rewriter that injects the $row_id hidden column into Iceberg scans and projects.
     *
     * <p>When {@code targetTable} is null, injects on ALL Iceberg scans (DELETE/UPDATE).
     * When non-null, only injects on the scan whose table ID matches (MERGE INTO).
     */
    private static class IcebergRowIdInjector extends DefaultPlanRewriter<Void> {
        @Nullable
        private final ExternalTable targetTable;

        IcebergRowIdInjector(@Nullable ExternalTable targetTable) {
            this.targetTable = targetTable;
        }

        @Override
        public Plan visitLogicalFileScan(LogicalFileScan scan, Void context) {
            if (!isRowIdInjectionTarget(scan.getTable())) {
                return scan;
            }
            if (targetTable != null
                    && scan.getTable().getId() != targetTable.getId()) {
                return scan;
            }
            if (hasRowIdSlot(scan.getOutput())) {
                return scan;
            }
            ExternalTable table = scan.getTable();
            Column rowIdColumn = getRowIdColumn(table);
            SlotReference rowIdSlot = SlotReference.fromColumn(
                    StatementScopeIdGenerator.newExprId(), table, rowIdColumn, scan.getQualifier());
            List<Slot> outputs = new ArrayList<>(scan.getOutput());
            outputs.add(rowIdSlot);
            return scan.withCachedOutput(outputs);
        }

        @Override
        public Plan visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
            project = (LogicalProject<? extends Plan>) visitChildren(this, project, context);
            Optional<Slot> rowIdSlot = findRowIdSlot(project.child().getOutput());
            if (!rowIdSlot.isPresent() || hasRowIdProject(project.getProjects())) {
                return project;
            }
            List<NamedExpression> newProjects = new ArrayList<>(project.getProjects());
            newProjects.add((NamedExpression) rowIdSlot.get());
            return project.withProjects(newProjects);
        }
    }
}
