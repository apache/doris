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

package org.apache.doris.connector.hms;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * Clean interface for Hive MetaStore client operations.
 *
 * <p>This is the SPI-clean equivalent of fe-core's {@code HMSCachedClient}.
 * All return types use connector-SPI or JDK types — no fe-core dependency.
 * Connector plugins depend on this interface; the implementation
 * ({@link ThriftHmsClient}) handles Thrift pooling and auth.</p>
 *
 * <p>Methods are organized by lifecycle phase:</p>
 * <ul>
 *   <li>Phase 1: Read-only metadata (list, get, exists)</li>
 *   <li>Phase 2: Partition operations</li>
 *   <li>Phase 3: DDL / write operations (future)</li>
 *   <li>Phase 4: ACID + events (future)</li>
 * </ul>
 */
public interface HmsClient extends Closeable {

    // ========== Phase 1: Read-only metadata ==========

    /**
     * List all database names in the metastore.
     *
     * @return list of database names
     * @throws HmsClientException if the operation fails
     */
    List<String> listDatabases();

    /**
     * Get database metadata.
     *
     * @param dbName database name
     * @return database info
     * @throws HmsClientException if the database is not found or operation fails
     */
    HmsDatabaseInfo getDatabase(String dbName);

    /**
     * List all table names in a database.
     *
     * @param dbName database name
     * @return list of table names
     * @throws HmsClientException if the operation fails
     */
    List<String> listTables(String dbName);

    /**
     * Check whether a table exists.
     *
     * @param dbName    database name
     * @param tableName table name
     * @return true if the table exists
     * @throws HmsClientException if the operation fails
     */
    boolean tableExists(String dbName, String tableName);

    /**
     * Get full table metadata including columns, partition keys,
     * storage descriptor, and parameters.
     *
     * @param dbName    database name
     * @param tableName table name
     * @return table info with SPI-typed columns
     * @throws HmsClientException if the table is not found or operation fails
     */
    HmsTableInfo getTable(String dbName, String tableName);

    /**
     * Get default column values for a table.
     *
     * @param dbName    database name
     * @param tableName table name
     * @return map of column name to default value string
     * @throws HmsClientException if the operation fails
     */
    Map<String, String> getDefaultColumnValues(String dbName, String tableName);

    // ========== Phase 2: Partition operations ==========

    /**
     * List partition names for a table.
     *
     * @param dbName    database name
     * @param tableName table name
     * @param maxParts  maximum number of partitions to return
     * @return list of partition name strings (e.g. "dt=2024-01-01/region=us")
     * @throws HmsClientException if the operation fails
     */
    List<String> listPartitionNames(String dbName, String tableName,
            int maxParts);

    /**
     * Get partition metadata by partition names.
     *
     * @param dbName    database name
     * @param tableName table name
     * @param partNames partition name strings
     * @return list of partition info
     * @throws HmsClientException if the operation fails
     */
    List<HmsPartitionInfo> getPartitions(String dbName, String tableName,
            List<String> partNames);

    /**
     * Get a single partition by its values.
     *
     * @param dbName    database name
     * @param tableName table name
     * @param values    partition column values in declaration order
     * @return partition info
     * @throws HmsClientException if the partition is not found or operation fails
     */
    HmsPartitionInfo getPartition(String dbName, String tableName,
            List<String> values);
}
