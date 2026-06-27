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
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    /**
     * Resolves an explicit time-travel spec (extracted from {@code FOR TIME AS OF} /
     * {@code FOR VERSION AS OF}, or the {@code @tag} / {@code @branch} / {@code @incr}
     * scan params) into a pinned snapshot.
     *
     * <p>The connector owns all provider-specific parsing of {@code spec} (snapshot-id
     * lookup, datetime parsing, tag/branch resolution, incremental-window validation).
     * The returned snapshot's {@link ConnectorMvccSnapshot#getProperties()} carries the
     * connector's scan options and its {@link ConnectorMvccSnapshot#getSchemaId()} is the
     * resolved schema version.</p>
     *
     * <p>Returns {@link Optional#empty()} when the spec is unsupported or the target is not
     * found, in which case the engine surfaces a user error. The default returns empty:
     * connectors without time-travel do not honor explicit specs.</p>
     */
    default Optional<ConnectorMvccSnapshot> resolveTimeTravel(
            ConnectorSession session, ConnectorTableHandle handle,
            ConnectorTimeTravelSpec spec) {
        return Optional.empty();
    }

    /**
     * Threads a pinned MVCC / time-travel {@code snapshot} into the table handle BEFORE
     * {@code planScan}, so an MVCC-capable connector can return a handle that reads at that
     * snapshot (mirrors the {@code applyFilter} / {@code applyProjection} handle-update pattern).
     *
     * <p>Contract for MVCC connectors: thread the FULL {@code snapshot.getProperties()}
     * (the scan-options map) into the returned handle so the read path sees exactly the
     * connector-resolved options. When {@code properties} is empty, fall back to setting
     * {@code scan.snapshot-id = snapshot.getSnapshotId()} (latest-pin parity).</p>
     *
     * <p>The default returns {@code handle} unchanged: connectors without time-travel ignore the
     * pin and read whatever is current.</p>
     */
    default ConnectorTableHandle applySnapshot(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
        return handle;  // default: connectors without time-travel ignore the pin
    }

    /**
     * Threads a per-group rewrite file scope into the table handle BEFORE {@code planScan}, so the
     * distributed {@code rewrite_data_files} driver can scope each per-group INSERT-SELECT scan to only the
     * data files that group bin-packed (mirrors {@link #applySnapshot} / {@code applyFilter} handle-update
     * pattern). {@code rawDataFilePaths} are the RAW file paths the connector's {@code planRewrite} emitted on
     * its {@link org.apache.doris.connector.api.procedure.ConnectorRewriteGroup}s; the connector matches its
     * re-enumerated scan tasks against the SAME raw paths (no normalization on either side — over-reading the
     * full table would make each group rewrite far beyond its bin-pack set and produce duplicate rows under
     * OCC).
     *
     * <p>The default returns {@code handle} unchanged: connectors without distributed rewrite ignore the
     * scope and scan the whole table. A {@code null}/empty set is also a no-op (no scope = full scan), so the
     * pin is only applied when a real per-group path set is present.</p>
     */
    default ConnectorTableHandle applyRewriteFileScope(ConnectorSession session,
            ConnectorTableHandle handle, Set<String> rawDataFilePaths) {
        return handle;  // default: connectors without distributed rewrite ignore the scope
    }

    @Override
    default void close() throws IOException {
    }
}
