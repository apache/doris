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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScopes;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Per-statement sharing of one table's {@link TableInfo} and readable lake boundary.
 *
 * <p>A single statement asks for the same table's metadata several times over — the handle, the
 * schema, the column handles, then split planning — and each of those is a coordinator round trip on
 * its own. Routing them through the statement scope collapses them to one fetch, and it also makes the
 * statement self-consistent: without it, a concurrent ALTER could land between two of those calls and
 * leave the plan built from two different schema versions.
 *
 * <p>The readable lake memo also caches {@link Optional#empty() absence}: if tiering publishes its first
 * snapshot while a statement is being planned, that statement must not bind one alias before the boundary
 * and another after it.
 *
 * <p>Under a {@code null} session or a statement scope of {@code NONE} (offline, no live statement)
 * the loader simply runs every time, which is what an untracked call did before.
 */
final class FlussStatementScope {

    /**
     * Namespace for fluss's per-statement {@link TableInfo} memo. Prefixed with this connector's type
     * name ("fluss") per the {@link ConnectorStatementScopes} convention, so a gateway statement
     * spanning two connectors cannot hand one of them the other's value.
     */
    static final String TABLE_INFO_NAMESPACE = "fluss.table_info";

    /** Namespace for the readable lake boundary (including its absence) shared by all read modes. */
    static final String LAKE_SNAPSHOT_NAMESPACE = "fluss.lake_snapshot";

    private FlussStatementScope() {
    }

    static TableInfo sharedTableInfo(ConnectorSession session, TablePath tablePath,
            Supplier<TableInfo> loader) {
        return ConnectorStatementScopes.resolveInStatement(
                session, TABLE_INFO_NAMESPACE,
                tablePath.getDatabaseName(), tablePath.getTableName(), loader);
    }

    static Optional<LakeSnapshot> sharedLakeSnapshot(ConnectorSession session, TablePath tablePath,
            Supplier<LakeSnapshot> loader) {
        return ConnectorStatementScopes.resolveInStatement(
                session, LAKE_SNAPSHOT_NAMESPACE,
                tablePath.getDatabaseName(), tablePath.getTableName(), () -> {
                    try {
                        return Optional.of(loader.get());
                    } catch (LakeTableSnapshotNotExistException e) {
                        // Absence is a real boundary state, not a cache miss. Memoizing Optional.empty()
                        // prevents two aliases in one statement from observing opposite sides of the
                        // first tiering commit.
                        return Optional.empty();
                    }
                });
    }
}
