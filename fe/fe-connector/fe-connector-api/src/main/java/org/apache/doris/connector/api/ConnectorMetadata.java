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

package org.apache.doris.connector.api;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Central metadata interface that a connector must implement.
 *
 * <p>Extends the fine-grained sub-interfaces for schema, table,
 * pushdown, statistics, and write operations. Each sub-interface
 * provides sensible defaults so that connectors only need to
 * override the methods they actually support.</p>
 */
public interface ConnectorMetadata extends
        ConnectorSchemaOps,
        ConnectorTableOps,
        ConnectorPushdownOps,
        ConnectorStatisticsOps,
        ConnectorWriteOps,
        ConnectorIdentifierOps,
        Closeable {

    /** Returns connector-level properties. */
    default Map<String, String> getProperties() {
        return Collections.emptyMap();
    }

    // ──────────────────── MVCC Snapshots ────────────────────

    /**
     * Returns the current snapshot at query begin time, used as the MVCC pin
     * for all subsequent reads of {@code handle}.
     *
     * <p>Returning {@link Optional#empty()} means the connector does not
     * support MVCC and reads see whatever is current.</p>
     */
    default Optional<ConnectorMvccSnapshot> beginQuerySnapshot(
            ConnectorSession session, ConnectorTableHandle handle) {
        return Optional.empty();
    }

    /** Returns the snapshot at the given wall-clock time, or empty if none. */
    default Optional<ConnectorMvccSnapshot> getSnapshotAt(
            ConnectorSession session, ConnectorTableHandle handle,
            long timestampMillis) {
        return Optional.empty();
    }

    /** Returns the snapshot with the given id, or empty if none. */
    default Optional<ConnectorMvccSnapshot> getSnapshotById(
            ConnectorSession session, ConnectorTableHandle handle,
            long snapshotId) {
        return Optional.empty();
    }

    @Override
    default void close() throws IOException {
    }
}
