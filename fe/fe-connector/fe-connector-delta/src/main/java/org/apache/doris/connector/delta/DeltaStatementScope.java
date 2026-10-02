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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScopes;

import java.util.Optional;

/** Shares the same authorized table version between statement metadata, scan and write planning. */
final class DeltaStatementScope {
    static final String TABLE_NAMESPACE = "delta.table";

    private DeltaStatementScope() {
    }

    static Optional<DeltaTableHandle> resolve(ConnectorSession session, DeltaCatalogAdapter adapter,
            String database, String table) {
        return ConnectorStatementScopes.resolveInStatement(
                session, TABLE_NAMESPACE, database, table, () -> adapter.getTableHandle(database, table));
    }
}
