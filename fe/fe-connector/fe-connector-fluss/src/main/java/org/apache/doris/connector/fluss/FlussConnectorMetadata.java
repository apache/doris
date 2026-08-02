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

import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;

import java.util.List;

/**
 * Fluss metadata for one statement: a thin mapping from the connector SPI onto {@link FlussAdminOps}.
 *
 * <p>Fluss has a real two-level namespace (database, table), so the listing calls are direct
 * pass-throughs and carry no Doris-side naming convention.
 */
public class FlussConnectorMetadata implements ConnectorMetadata {

    private final FlussAdminOps adminOps;

    public FlussConnectorMetadata(FlussAdminOps adminOps) {
        this.adminOps = adminOps;
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return adminOps.listDatabases();
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        return adminOps.databaseExists(dbName);
    }

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        return adminOps.listTables(dbName);
    }
}
