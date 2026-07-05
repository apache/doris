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

package org.apache.doris.connector.iceberg.action;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.foundation.util.NamedArguments;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.iceberg.Table;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Abstract base for iceberg {@code ALTER TABLE EXECUTE} procedure bodies, run behind
 * {@link org.apache.doris.connector.iceberg.IcebergProcedureOps}.
 *
 * <p>Standalone connector port of legacy {@code datasource/iceberg/action/BaseIcebergAction} folded
 * together with the still-consumed half of {@code BaseExecuteAction}. It cannot extend the legacy base:
 * the import gate forbids {@code BaseExecuteAction}, {@code PartitionNamesInfo} and the nereids
 * {@code Expression}. The legacy types are therefore replaced by the SPI's engine-neutral carriers —
 * {@code List<String>} partition names, {@link ConnectorPredicate} where, and {@link ConnectorColumn}
 * result columns. The argument framework ({@link NamedArguments} / {@code ArgumentParsers}) is the shared
 * fe-foundation utility, so the engine and the connector validate against the same code.
 *
 * <p><b>Engine/connector split (D-062 §2).</b> The {@code ALTER} privilege check, the {@code ResultSet}
 * wrapping and the edit-log refresh stay in the engine; this base owns argument validation (§4 = 4-A) and
 * the single-row result contract. {@link #validate()} therefore performs no privilege check — only the
 * registered-argument validation plus the procedure-specific {@link #validateIcebergAction()} hook.
 *
 * <p><b>Single-row contract.</b> {@link #execute(Table)} mirrors {@code BaseExecuteAction.execute}: the
 * body returns one row whose width must equal the declared {@link #getResultSchema()} width
 * ({@code Preconditions.checkState}, legacy {@code BaseExecuteAction:106-108}).
 */
public abstract class BaseIcebergAction {

    protected final String actionType;
    protected final Map<String, String> properties;
    protected final List<String> partitionNames;
    protected final ConnectorPredicate whereCondition;

    // Named arguments for parameter validation. NamedArguments lives in fe-foundation (shared with the
    // engine); the connector still owns the per-procedure argument specs and runs the validation (§4 = 4-A).
    protected final NamedArguments namedArguments = new NamedArguments();

    // Result columns, captured once at construction (mirrors BaseExecuteAction's resultSetMetaData).
    private final List<ConnectorColumn> resultSchema;

    protected BaseIcebergAction(String actionType, Map<String, String> properties,
            List<String> partitionNames, ConnectorPredicate whereCondition) {
        this.actionType = actionType;
        this.properties = properties != null ? properties : Maps.newHashMap();
        this.partitionNames = partitionNames;
        this.whereCondition = whereCondition;

        // Register arguments specific to this action.
        registerIcebergArguments();

        // Capture the result schema once (subclasses provide constant columns).
        this.resultSchema = getResultSchema();
    }

    /**
     * Validates the procedure's arguments and runs its action-specific checks. The engine performs the
     * {@code ALTER} privilege check before dispatch, so it is intentionally not repeated here.
     */
    public final void validate() {
        // NamedArguments (fe-foundation) signals failures with an unchecked IllegalArgumentException;
        // re-wrap it as DorisConnectorException, keeping the message verbatim (T08 byte-parity).
        try {
            namedArguments.validate(properties);
        } catch (IllegalArgumentException e) {
            throw new DorisConnectorException(e.getMessage());
        }
        validateIcebergAction();
    }

    /**
     * Runs the procedure body and wraps its single row into a {@link ConnectorProcedureResult}. Enforces the
     * legacy single-row contract: the row width must equal the declared schema width.
     *
     * <p>{@code session} carries the connector execution context (most importantly the session time zone for
     * {@code rollback_to_timestamp}); the seven non-time-zone procedures ignore it. The legacy
     * {@code BaseExecuteAction} read the time zone from the thread-local {@code ConnectContext}; the connector
     * cannot, so it threads {@link ConnectorSession} here instead — the SPI ({@code ConnectorProcedureOps})
     * and factory signatures are unchanged.
     */
    public final ConnectorProcedureResult execute(Table table, ConnectorSession session) {
        List<String> resultRow = executeAction(table, session);
        if (resultSchema == null || resultSchema.isEmpty() || resultRow == null) {
            return new ConnectorProcedureResult(
                    resultSchema == null ? Collections.emptyList() : resultSchema,
                    Collections.emptyList());
        }
        Preconditions.checkState(resultSchema.size() == resultRow.size(),
                "Result row size does not match metadata column count");
        // The result is exactly one row, so we wrap it in a single-element list.
        return new ConnectorProcedureResult(resultSchema, Collections.singletonList(resultRow));
    }

    /**
     * Registers the arguments accepted by this procedure (into {@link #namedArguments}). Called once from
     * the constructor.
     */
    protected abstract void registerIcebergArguments();

    /**
     * Procedure-specific validation beyond argument parsing (e.g. partition/{@code WHERE} guards). Default
     * is a no-op.
     */
    protected void validateIcebergAction() {
        // Default implementation does nothing.
    }

    /**
     * The result-column schema, or an empty list when the procedure returns no rows. Subclasses override to
     * declare their columns. Captured once at construction.
     */
    protected List<ConnectorColumn> getResultSchema() {
        return Collections.emptyList();
    }

    /**
     * Runs the procedure against the loaded iceberg SDK table and returns its single result row (or
     * {@code null} when there is no result). {@code session} provides the execution context (e.g. the session
     * time zone consumed by {@code rollback_to_timestamp}); most procedures do not use it.
     */
    protected abstract List<String> executeAction(Table table, ConnectorSession session);

    public String getActionType() {
        return actionType;
    }

    /** Rejects a partition specification when the procedure does not support one. */
    protected void validateNoPartitions() {
        if (partitionNames != null && !partitionNames.isEmpty()) {
            throw new DorisConnectorException(
                    String.format("Action '%s' does not support partition specification", actionType));
        }
    }

    /** Rejects a {@code WHERE} condition when the procedure does not support one. */
    protected void validateNoWhereCondition() {
        if (whereCondition != null) {
            throw new DorisConnectorException(
                    String.format("Action '%s' does not support WHERE condition", actionType));
        }
    }

    /** Requires a {@code WHERE} condition to be present. */
    protected void validateRequiredWhereCondition() {
        if (whereCondition == null) {
            throw new DorisConnectorException(
                    String.format("Action '%s' requires WHERE condition", actionType));
        }
    }

    /** Requires a partition specification to be present. */
    protected void validateRequiredPartitions() {
        if (partitionNames == null || partitionNames.isEmpty()) {
            throw new DorisConnectorException(
                    String.format("Action '%s' requires partition specification", actionType));
        }
    }
}
