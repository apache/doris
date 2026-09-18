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

package org.apache.doris.connector.spi;

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

    /**
     * Retrieves metadata for the specified database. The default returns metadata with an empty
     * property map (so SHOW CREATE DATABASE renders no LOCATION/PROPERTIES for connectors with no
     * database-level metadata, matching their pre-flip behavior); connectors that expose namespace
     * metadata (e.g. iceberg's namespace location) override this. Mirrors the graceful empty defaults
     * of {@link #listDatabaseNames}/{@link #databaseExists} rather than throwing.
     */
    default ConnectorDatabaseMetadata getDatabase(
            ConnectorSession session, String dbName) {
        return new ConnectorDatabaseMetadata(dbName, Collections.emptyMap());
    }

    /**
     * Creates a new database with the given name and properties. The default throws, which IS the
     * declaration that this connector cannot create databases — there is no separate boolean to keep in
     * sync with it (a {@code supportsCreateDatabase()} switch existed and was removed: it could only
     * restate whether this method was overridden, and getting the two out of step broke
     * {@code CREATE DATABASE IF NOT EXISTS} with no compile error and no failing test).
     *
     * <p>{@code IF NOT EXISTS} is handled by the engine, not here: it consults {@link #databaseExists}
     * first and only calls this when the answer is no. A connector that cannot create databases therefore
     * still wants a truthful {@link #databaseExists} — that is what makes {@code IF NOT EXISTS} on an
     * already-existing database succeed rather than report "not supported".</p>
     */
    default void createDatabase(ConnectorSession session,
            String dbName, Map<String, String> properties) {
        throw new DorisConnectorException(
                "CREATE DATABASE not supported");
    }

    /**
     * Drops the specified database, cascading to its tables when {@code force} is true.
     *
     * <p>This is the only drop-database entry point: a 3-arg form without {@code force} used to exist and
     * this overload defaulted to it, so {@code DROP DATABASE ... FORCE} silently became a non-cascading drop
     * that then failed on a non-empty database. A connector that cannot cascade should reject {@code force}
     * explicitly instead.</p>
     */
    default void dropDatabase(ConnectorSession session,
            String dbName, boolean ifExists, boolean force) {
        throw new DorisConnectorException(
                "DROP DATABASE not supported");
    }
}
