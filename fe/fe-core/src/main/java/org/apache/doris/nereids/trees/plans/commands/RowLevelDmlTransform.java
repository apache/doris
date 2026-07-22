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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.BaseExternalTableInsertExecutor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;

import java.util.Optional;

/**
 * Per-table strategy that turns a row-level DML ({@code DELETE}/{@code UPDATE}/{@code MERGE INTO}) against an
 * external table into a synthesized INSERT-shaped plan plus the connector-specific wiring the generic
 * {@link RowLevelDmlCommand} shell drives. Implementations are registered in {@link RowLevelDmlRegistry}; the
 * dispatching commands look one up via {@link RowLevelDmlRegistry#find(TableIf)} instead of testing the table
 * type directly (the reverse {@code instanceof} moves into {@link #handles(TableIf)}).
 *
 * <p>The single live row-level-DML loop lives in {@link RowLevelDmlCommand}; this interface parameterizes the
 * six points that differ per table/operation (mode check, synthesis, required sink, executor factory, label
 * prefix, finalize) plus the connector-agnostic write-constraint extraction.</p>
 */
public interface RowLevelDmlTransform {

    /** Whether this transform handles the given target table (a connector-capability probe). */
    boolean handles(TableIf table);

    /** Reject unsupported table modes (e.g. copy-on-write) for the operation, mirroring legacy command checks. */
    void checkMode(TableIf table, RowLevelDmlOp op);

    /** Synthesize the logical plan (the table-sink-rooted INSERT-shaped plan) for the operation. */
    LogicalPlan synthesize(ConnectContext ctx, RowLevelDmlArgs args, RowLevelDmlOp op);

    /** Create the executor that performs the write for the operation. */
    BaseExternalTableInsertExecutor newExecutor(ConnectContext ctx, TableIf table, String label,
            NereidsPlanner planner, boolean emptyInsert, RowLevelDmlOp op);

    /** Locate and validate the required physical sink in the planned plan (throws with the legacy messages). */
    PhysicalSink<?> requirePhysicalSink(NereidsPlanner planner, RowLevelDmlOp op);

    /** The label prefix; the shell appends {@code _<hi>_<lo>}. Frozen for profile/txn parity. */
    String labelPrefix(RowLevelDmlOp op);

    /**
     * Legacy optimistic-conflict-detection wiring (kept live until P6.7): build the connector-specific
     * conflict filter from the analyzed plan and stash it on the executor for its {@code beforeExec}.
     */
    void setupConflictDetection(BaseExternalTableInsertExecutor executor, Plan analyzedPlan, TableIf table,
            RowLevelDmlOp op);

    /** Finalize the sink (op-specific; e.g. attaching rewritable delete-file metadata for the BE). */
    void finalizeSink(BaseExternalTableInsertExecutor executor, RowLevelDmlOp op, PlanFragment fragment,
            DataSink sink, PhysicalSink<?> physicalSink);

    /**
     * write-constraint extraction: the target-only predicate handed to a {@code ConnectorTransaction} via
     * {@code applyWriteConstraint}. Supplies the connector-specific synthetic-column exclusion. Returns empty
     * when no target-only conjunct survives.
     */
    Optional<ConnectorPredicate> extractWriteConstraint(Plan analyzedPlan, TableIf table);
}
