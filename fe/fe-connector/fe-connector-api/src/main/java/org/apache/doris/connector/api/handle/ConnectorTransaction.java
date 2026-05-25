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

import java.io.Closeable;

/**
 * A connector-managed transaction that scopes one or more write operations.
 *
 * <p>Lifecycle: the engine calls {@link #commit()} on success or
 * {@link #rollback()} on failure, then always calls {@link #close()} to
 * release resources. {@code rollback()} and {@code close()} are safe to
 * call multiple times.</p>
 *
 * <p>Extends the marker {@link ConnectorTransactionHandle} so that existing
 * APIs that traffic in opaque handles continue to work without change.</p>
 */
public interface ConnectorTransaction extends ConnectorTransactionHandle, Closeable {

    /** Stable transaction ID assigned by the connector. */
    long getTransactionId();

    /**
     * Commits all pending operations bound to this transaction.
     *
     * @throws org.apache.doris.connector.api.DorisConnectorException
     *         on conflict, IO failure, or external system error
     */
    void commit();

    /**
     * Aborts all pending operations and releases resources.
     * Safe to call multiple times; subsequent calls are no-ops.
     */
    void rollback();

    /** Called by the engine after commit OR rollback to release connections etc. */
    @Override
    void close();
}
