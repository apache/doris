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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Operations on databases (schemas) within a connector catalog.
 */
public interface ConnectorSchemaOps {

    /** Lists all database names in this catalog. */
    default List<String> listDatabaseNames(ConnectorSession session) {
        return Collections.emptyList();
    }

    /** Checks whether a database with the given name exists. */
    default boolean databaseExists(ConnectorSession session,
            String dbName) {
        return false;
    }

    /** Retrieves metadata for the specified database. */
    default ConnectorDatabaseMetadata getDatabase(
            ConnectorSession session, String dbName) {
        throw new DorisConnectorException(
                "getDatabase not implemented");
    }

    /** Creates a new database with the given name and properties. */
    default void createDatabase(ConnectorSession session,
            String dbName, Map<String, String> properties) {
        throw new DorisConnectorException(
                "CREATE DATABASE not supported");
    }

    /** Drops the specified database. */
    default void dropDatabase(ConnectorSession session,
            String dbName, boolean ifExists) {
        throw new DorisConnectorException(
                "DROP DATABASE not supported");
    }
}
