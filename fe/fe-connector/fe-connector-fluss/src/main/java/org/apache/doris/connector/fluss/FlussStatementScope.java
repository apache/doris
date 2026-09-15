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

import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import java.util.function.Supplier;

/**
 * Per-statement sharing of one table's {@link TableInfo}.
 *
 * <p>A single statement asks for the same table's metadata several times over — the handle, the
 * schema, the column handles, then split planning — and each of those is a coordinator round trip on
 * its own. Routing them through the statement scope collapses them to one fetch, and it also makes the
 * statement self-consistent: without it, a concurrent ALTER could land between two of those calls and
 * leave the plan built from two different schema versions.
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

    private FlussStatementScope() {
    }

    static TableInfo sharedTableInfo(ConnectorSession session, TablePath tablePath,
            Supplier<TableInfo> loader) {
        return ConnectorStatementScopes.resolveInStatement(
                session, TABLE_INFO_NAMESPACE,
                tablePath.getDatabaseName(), tablePath.getTableName(), loader);
    }
}
