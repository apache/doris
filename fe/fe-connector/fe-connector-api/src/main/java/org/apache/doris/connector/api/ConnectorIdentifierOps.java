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

/**
 * Operations for mapping remote identifier names (databases, tables, columns)
 * to local (Doris-visible) names.
 *
 * <p>Connectors that support name mapping (e.g., lowercase conversion,
 * explicit JSON-based mapping) should override these methods.
 * Default implementations return the remote name unchanged.</p>
 */
public interface ConnectorIdentifierOps {

    /**
     * Maps a remote database name to its local representation.
     *
     * @param session the connector session
     * @param remoteDatabaseName the database name as reported by the remote source
     * @return the mapped local database name
     */
    default String fromRemoteDatabaseName(ConnectorSession session, String remoteDatabaseName) {
        return remoteDatabaseName;
    }

    /**
     * Maps a remote table name to its local representation.
     *
     * @param session the connector session
     * @param remoteDatabaseName the remote database containing the table
     * @param remoteTableName the table name as reported by the remote source
     * @return the mapped local table name
     */
    default String fromRemoteTableName(ConnectorSession session,
            String remoteDatabaseName, String remoteTableName) {
        return remoteTableName;
    }

    /**
     * Maps a remote column name to its local representation.
     *
     * @param session the connector session
     * @param remoteDatabaseName the remote database
     * @param remoteTableName the remote table containing the column
     * @param remoteColumnName the column name as reported by the remote source
     * @return the mapped local column name
     */
    default String fromRemoteColumnName(ConnectorSession session,
            String remoteDatabaseName, String remoteTableName,
            String remoteColumnName) {
        return remoteColumnName;
    }
}
