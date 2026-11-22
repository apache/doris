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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScopes;

import java.util.function.Supplier;

/**
 * The per-statement memo of table metadata. One statement asks for the same table version several times
 * (the query-begin pin, the schema, the column handles, the scan properties, the split plan), and each
 * miss opens the dataset again; the memo makes that one metadata read per table version per statement.
 */
final class LanceStatementScope {
    /**
     * Namespace for the per-statement table-metadata memo. Prefixed with the connector's type name so it
     * stays distinct from a sibling connector's memo inside a heterogeneous gateway; guarded by
     * {@code LanceStatementScopeTest}.
     */
    static final String TABLE_METADATA_NAMESPACE = "lance.table_metadata";

    private LanceStatementScope() {
    }

    /**
     * The metadata of {@code handle}'s table at its pinned version, or at the newest version when it has
     * none. A newest-version read is also remembered under the version it resolved to, so the pinned reads
     * that follow it in the statement reuse it.
     */
    static LanceTableMetadata sharedMetadata(ConnectorSession session, LanceTableHandle handle,
            Supplier<LanceTableMetadata> loader) {
        String table = handle.getTableName() + "@"
                + (handle.getVersion().isPresent() ? String.valueOf(handle.getVersion().getAsLong()) : "latest");
        LanceTableMetadata metadata = ConnectorStatementScopes.resolveInStatement(
                session, TABLE_METADATA_NAMESPACE, handle.getDbName(), table, loader);
        if (!handle.getVersion().isPresent()) {
            ConnectorStatementScopes.resolveInStatement(session, TABLE_METADATA_NAMESPACE, handle.getDbName(),
                    handle.getTableName() + "@" + metadata.getVersion(), () -> metadata);
        }
        return metadata;
    }
}
