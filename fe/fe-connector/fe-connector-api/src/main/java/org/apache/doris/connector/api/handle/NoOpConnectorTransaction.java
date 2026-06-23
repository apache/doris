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

package org.apache.doris.connector.api.handle;

/**
 * A degenerate {@link ConnectorTransaction} for connectors whose writes are
 * auto-committed by BE (e.g. jdbc, where each row is written through a
 * {@code PreparedStatement}) and therefore need no FE-side transaction
 * coordination. {@link #commit()} / {@link #rollback()} are no-ops; the engine
 * still routes every write through {@code beginTransaction} so the write
 * lifecycle is uniform across connectors (no per-connector transaction fork).
 *
 * <p>{@link #getUpdateCnt()} returns {@code -1} — "no row count is produced by
 * this transaction" — which signals the insert executor to keep the
 * coordinator's row counter (DPP_NORMAL_ALL) for affected-rows rather than
 * overwrite it with {@code 0}. {@code -1} is deliberately distinct from a
 * genuine zero-row write ({@code 0}).</p>
 */
public class NoOpConnectorTransaction implements ConnectorTransaction {

    private final long transactionId;
    private final String profileLabel;

    public NoOpConnectorTransaction(long transactionId, String profileLabel) {
        this.transactionId = transactionId;
        this.profileLabel = profileLabel;
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public void commit() {
        // no-op: the write is already durably committed by BE (auto-commit sink)
    }

    @Override
    public void rollback() {
        // no-op: there is nothing to undo on the FE side
    }

    @Override
    public void close() {
        // no-op: no resources are held
    }

    @Override
    public long getUpdateCnt() {
        // -1 = "no row count from this transaction; use the coordinator counter".
        // Distinct from 0 (a genuine zero-row write) so the executor does not
        // clobber loadedRows that BE already reported via DPP_NORMAL_ALL.
        return -1;
    }

    @Override
    public String profileLabel() {
        return profileLabel;
    }
}
