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

import org.apache.doris.connector.api.ConnectorColumn;

import java.util.List;
import java.util.Objects;

/**
 * The engine-neutral result of a table procedure: the result-column schema plus the row values.
 *
 * <p>Returned by {@link ConnectorProcedureOps#execute}. The engine maps each {@link ConnectorColumn}
 * to a {@code Column} (via the shared {@code ConnectorColumnConverter}) to build the result-set
 * metadata, then sends {@link #getRows()} unchanged. Decoupling the column metadata from the row
 * values keeps the SPI free of any engine result-set type.</p>
 *
 * <p>Every current procedure emits exactly one row whose size equals {@link #getResultSchema()} size
 * (legacy {@code BaseExecuteAction} enforces a single row of matching width). {@code List<List<String>>}
 * preserves that contract without locking the SPI to single-row results.</p>
 */
public final class ConnectorProcedureResult {

    private final List<ConnectorColumn> resultSchema;
    private final List<List<String>> rows;

    public ConnectorProcedureResult(List<ConnectorColumn> resultSchema, List<List<String>> rows) {
        this.resultSchema = Objects.requireNonNull(resultSchema, "resultSchema");
        this.rows = Objects.requireNonNull(rows, "rows");
    }

    /** The result-column schema (name + type per column). */
    public List<ConnectorColumn> getResultSchema() {
        return resultSchema;
    }

    /** The result rows; each row's size equals {@link #getResultSchema()} size. */
    public List<List<String>> getRows() {
        return rows;
    }
}
