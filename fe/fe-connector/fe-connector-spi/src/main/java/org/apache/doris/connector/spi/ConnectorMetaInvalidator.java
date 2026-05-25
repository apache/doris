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

import java.util.List;

/**
 * Callback the connector uses to notify the engine that external metadata
 * has changed and cached entries should be dropped (e.g. when an HMS
 * notification event reports a CREATE / ALTER / DROP).
 *
 * <p>Obtained from {@link ConnectorContext#getMetaInvalidator()}.</p>
 *
 * <p>Connectors that have no external change notifications can ignore this
 * interface entirely; the engine provides a {@link #NOOP} default.</p>
 */
public interface ConnectorMetaInvalidator {

    ConnectorMetaInvalidator NOOP = new ConnectorMetaInvalidator() { };

    /** Invalidates the entire catalog's metadata caches. */
    default void invalidateAll() { }

    /** Invalidates cached metadata for one database. */
    default void invalidateDatabase(String dbName) { }

    /** Invalidates cached metadata for one table. */
    default void invalidateTable(String dbName, String tableName) { }

    /**
     * Invalidates cached partition info for one partition.
     *
     * @param partitionValues partition column values in declared order
     *                        (e.g. {@code ["2024", "01"]} for a table
     *                        partitioned by {@code (year, month)})
     */
    default void invalidatePartition(String dbName, String tableName,
            List<String> partitionValues) { }

    /** Invalidates cached statistics for one table (without dropping schema cache). */
    default void invalidateStatistics(String dbName, String tableName) { }
}
