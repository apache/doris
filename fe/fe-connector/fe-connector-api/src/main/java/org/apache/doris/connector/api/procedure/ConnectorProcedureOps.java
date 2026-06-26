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

package org.apache.doris.connector.api.procedure;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;

import java.util.List;
import java.util.Map;

/**
 * Executes a connector's table procedures for {@code ALTER TABLE t EXECUTE <proc>(args)}.
 *
 * <p>This is the procedure-side analogue of
 * {@link org.apache.doris.connector.api.scan.ConnectorScanPlanProvider} and
 * {@link org.apache.doris.connector.api.write.ConnectorWritePlanProvider}: a connector that exposes
 * table procedures returns an implementation from
 * {@link org.apache.doris.connector.api.Connector#getProcedureOps()}; the engine routes
 * {@code ExecuteActionCommand} to {@link #execute} and wraps the returned rows into a result set.</p>
 *
 * <p><b>Engine/connector split.</b> The engine owns the command shell — the {@code ALTER} privilege
 * check, the {@code ResultSet} wrapping, and the edit-log refresh broadcast. The connector owns the
 * procedure body — argument validation, the underlying maintenance call, and the result schema/rows.
 * This mirrors Trino's {@code TableProcedureMetadata} model (procedure body in the connector,
 * orchestration in the engine), not the {@code CALL}/{@code MethodHandle} model.</p>
 *
 * <p>Maps to Doris's flat {@code ALTER TABLE EXECUTE} actions (one procedure per name); there is no
 * connector-level {@code CALL} procedure surface.</p>
 */
public interface ConnectorProcedureOps {

    /**
     * Returns the procedure names this connector supports for {@code ALTER TABLE EXECUTE}, used by the
     * engine for routing, validation, and {@code SHOW}-style discovery.
     */
    List<String> getSupportedProcedures();

    /**
     * Returns how the engine must drive {@code procedureName}: {@link ProcedureExecutionMode#SINGLE_CALL}
     * (the default — a synchronous body dispatched through {@link #execute}) or
     * {@link ProcedureExecutionMode#DISTRIBUTED} (an engine-orchestrated distributed rewrite that does NOT
     * go through {@link #execute}). This lets the engine route a distributed procedure (iceberg
     * {@code rewrite_data_files}) without hard-coding its name in a general engine class. The default covers
     * every pure-SDK procedure; a connector overrides it only for its distributed procedures.
     *
     * @param procedureName the procedure name (case-insensitive at the connector's discretion)
     */
    default ProcedureExecutionMode getExecutionMode(String procedureName) {
        return ProcedureExecutionMode.SINGLE_CALL;
    }

    /**
     * Executes a table procedure and returns its result (schema + rows) in an engine-neutral form; the
     * engine wraps these into a {@code ResultSet}.
     *
     * <p>The connector validates the {@code properties} against the procedure's own argument schema (the
     * engine does not know per-procedure argument shapes), performs the maintenance call, and returns the
     * result. Each procedure emits exactly one row today (see {@link ConnectorProcedureResult}).</p>
     *
     * @param session        the current session
     * @param table          the target table handle
     * @param procedureName  the procedure name (e.g. {@code rollback_to_snapshot})
     * @param properties     the procedure arguments as key/value pairs
     * @param whereCondition the engine-lowered {@code WHERE} predicate (only data-rewriting procedures
     *                       accept one; {@code null} when absent), or {@code null}
     * @param partitionNames the {@code PARTITION (...)} names (pass-through; all current procedures reject
     *                       a non-empty list), never {@code null}
     * @return the procedure result (column schema + rows)
     */
    ConnectorProcedureResult execute(
            ConnectorSession session,
            ConnectorTableHandle table,
            String procedureName,
            Map<String, String> properties,
            ConnectorPredicate whereCondition,
            List<String> partitionNames);
}
