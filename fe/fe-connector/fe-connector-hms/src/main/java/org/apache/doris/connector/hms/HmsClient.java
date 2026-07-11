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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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
 *   <li>Phase 3: DDL / write operations</li>
 *   <li>Phase 4: ACID transactions</li>
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
     * Lists partition names bypassing any decorator cache (a FRESH metastore listing). SHOW PARTITIONS and the
     * {@code partitions} metadata TVF need a fresh view (legacy read the raw pooled client, never the metadata
     * cache); the query-pruning path keeps {@link #listPartitionNames} (cached under {@code use_meta_cache}).
     *
     * <p>Default = the cached path: a non-decorating client (e.g. the raw {@link ThriftHmsClient}) has no cache
     * to bypass, so the two are identical for it. <b>Any caching {@code HmsClient} decorator MUST override this</b>
     * to reach its delegate directly, or SHOW PARTITIONS regresses to serving a stale cached list.
     *
     * @param dbName    database name
     * @param tableName table name
     * @param maxParts  maximum number of partitions to return
     * @return list of partition name strings (e.g. "dt=2024-01-01/region=us")
     * @throws HmsClientException if the operation fails
     */
    default List<String> listPartitionNamesFresh(String dbName, String tableName, int maxParts) {
        return listPartitionNames(dbName, tableName, maxParts);
    }

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

    /**
     * Returns HMS-recorded (no-scan) column statistics for the named columns, e.g. for the query-planner
     * column-statistics fast path.
     *
     * <p>Optional: defaults to an empty list so read-only test doubles and clients without column-stats
     * support need not implement it (the caller then degrades to "no stats"). The production
     * {@link ThriftHmsClient} overrides it.</p>
     *
     * @param dbName    database name
     * @param tableName table name
     * @param columns   column names to fetch stats for
     * @return one {@link HmsColumnStatistics} per column that HAS stats (may be shorter than {@code columns})
     */
    default List<HmsColumnStatistics> getTableColumnStatistics(String dbName, String tableName,
            List<String> columns) {
        return Collections.emptyList();
    }

    // ========== Phase 3: DDL / write operations ==========
    //
    // Optional operations: default to throwing so read-only implementations (the partition-pruning
    // test fakes, and connectors that never issue DDL) need not implement them. The production
    // {@link ThriftHmsClient} overrides all of them.

    /**
     * Create a database.
     *
     * @param request database create spec
     * @throws HmsClientException if the operation fails
     */
    default void createDatabase(HmsCreateDatabaseRequest request) {
        throw new UnsupportedOperationException("createDatabase is not supported by this client");
    }

    /**
     * Drop a database. Callers must have already handled IF EXISTS / cascade semantics.
     *
     * @param dbName database name
     * @throws HmsClientException if the operation fails
     */
    default void dropDatabase(String dbName) {
        throw new UnsupportedOperationException("dropDatabase is not supported by this client");
    }

    /**
     * Create a table. When any column carries a default value, it is registered as a Hive default
     * constraint (equivalent to legacy {@code createTableWithConstraints}).
     *
     * @param request table create spec
     * @throws HmsClientException if the table already exists or the operation fails
     */
    default void createTable(HmsCreateTableRequest request) {
        throw new UnsupportedOperationException("createTable is not supported by this client");
    }

    /**
     * Drop a table. Callers must have already handled IF EXISTS and transactional-table rejection.
     *
     * @param dbName    database name
     * @param tableName table name
     * @throws HmsClientException if the operation fails
     */
    default void dropTable(String dbName, String tableName) {
        throw new UnsupportedOperationException("dropTable is not supported by this client");
    }

    /**
     * Truncate a table, or the given partitions of it.
     *
     * @param dbName     database name
     * @param tableName  table name
     * @param partitions partition names to truncate; empty/null truncates the whole table
     * @throws HmsClientException if the operation fails
     */
    default void truncateTable(String dbName, String tableName, List<String> partitions) {
        throw new UnsupportedOperationException("truncateTable is not supported by this client");
    }

    /**
     * Add partitions to a table, stamping each partition's basic statistics onto its parameters.
     * Ports the legacy {@code HMSTransaction} add-partition commit; the implementation batches large
     * lists internally.
     *
     * @param dbName     database name
     * @param tableName  table name
     * @param partitions partitions to create, each with its data-layout and statistics
     * @throws HmsClientException if the operation fails
     */
    default void addPartitions(String dbName, String tableName,
            List<HmsPartitionWithStatistics> partitions) {
        throw new UnsupportedOperationException("addPartitions is not supported by this client");
    }

    /**
     * Read-modify-write a table's basic statistics parameters (numRows / totalSize / numFiles).
     *
     * @param dbName    database name
     * @param tableName table name
     * @param update    receives the table's current statistics and returns the new statistics
     * @throws HmsClientException if the operation fails
     */
    default void updateTableStatistics(String dbName, String tableName,
            Function<HmsPartitionStatistics, HmsPartitionStatistics> update) {
        throw new UnsupportedOperationException(
                "updateTableStatistics is not supported by this client");
    }

    /**
     * Read-modify-write a single partition's basic statistics parameters.
     *
     * @param dbName        database name
     * @param tableName     table name
     * @param partitionName partition name (e.g. "dt=2024-01-01")
     * @param update        receives the partition's current statistics and returns the new statistics
     * @throws HmsClientException if the operation fails
     */
    default void updatePartitionStatistics(String dbName, String tableName, String partitionName,
            Function<HmsPartitionStatistics, HmsPartitionStatistics> update) {
        throw new UnsupportedOperationException(
                "updatePartitionStatistics is not supported by this client");
    }

    /**
     * Drop a partition by its values.
     *
     * @param dbName          database name
     * @param tableName       table name
     * @param partitionValues partition column values in declaration order
     * @param deleteData      whether to also delete the partition's data files
     * @return true if a partition was dropped
     * @throws HmsClientException if the operation fails
     */
    default boolean dropPartition(String dbName, String tableName,
            List<String> partitionValues, boolean deleteData) {
        throw new UnsupportedOperationException("dropPartition is not supported by this client");
    }

    /**
     * Not-found-tolerant probe for whether a partition exists (used to downgrade a NEW-partition
     * write to an APPEND when the Doris cache missed a partition that already exists in HMS).
     *
     * @param dbName          database name
     * @param tableName       table name
     * @param partitionValues partition column values in declaration order
     * @return true if the partition exists in the metastore
     * @throws HmsClientException if the operation fails for a reason other than not-found
     */
    default boolean partitionExists(String dbName, String tableName,
            List<String> partitionValues) {
        throw new UnsupportedOperationException("partitionExists is not supported by this client");
    }

    // ========== Phase 4: ACID transactions ==========
    //
    // Read-side ACID primitives for transactional Hive tables. Like the Phase 3 block, they default
    // to throwing so read-only / non-transactional clients need not implement them.

    /**
     * Open a Hive ACID transaction.
     *
     * @param user the user opening the transaction
     * @return the new transaction id
     * @throws HmsClientException if the operation fails
     */
    default long openTxn(String user) {
        throw new UnsupportedOperationException("openTxn is not supported by this client");
    }

    /**
     * Commit a Hive ACID transaction (also releases the transaction's locks).
     *
     * @param txnId the transaction id
     * @throws HmsClientException if the operation fails
     */
    default void commitTxn(long txnId) {
        throw new UnsupportedOperationException("commitTxn is not supported by this client");
    }

    /**
     * Get the valid-transaction / valid-write-id snapshot for a transactional table, as the two
     * Hadoop-configuration entries ({@link HmsAcidConstants}) that BE consumes for ACID reads. On a
     * metastore-incompatibility error the implementation degrades to a max watermark rather than
     * failing the read.
     *
     * @param fullTableName        fully-qualified "db.table"
     * @param currentTransactionId the reader's open transaction id
     * @return map with {@link HmsAcidConstants#VALID_TXNS_KEY} and
     *         {@link HmsAcidConstants#VALID_WRITEIDS_KEY}
     * @throws HmsClientException if the operation fails
     */
    default Map<String, String> getValidWriteIds(String fullTableName, long currentTransactionId) {
        throw new UnsupportedOperationException("getValidWriteIds is not supported by this client");
    }

    /**
     * Acquire a shared (read) lock over a table and, if given, specific partitions, polling until the
     * lock is granted or the timeout elapses.
     *
     * @param queryId        the query id (lock owner)
     * @param txnId          the transaction id
     * @param user           the requesting user
     * @param dbName         database name
     * @param tableName      table name
     * @param partitionNames partitions to lock; empty locks the whole table
     * @param timeoutMs      maximum time to wait for the lock, in milliseconds
     * @throws HmsClientException if the lock cannot be acquired within the timeout
     */
    default void acquireSharedLock(String queryId, long txnId, String user, String dbName,
            String tableName, List<String> partitionNames, long timeoutMs) {
        throw new UnsupportedOperationException("acquireSharedLock is not supported by this client");
    }

    // ========== Phase 5: Metastore notification events (incremental metadata sync) ==========
    //
    // Optional: the incremental-metadata event feed. Only the production {@link ThriftHmsClient}
    // (and the {@link CachingHmsClient} pass-through) implement these; read-only test doubles and
    // connectors without an event feed keep the throwing defaults.

    /**
     * The metastore's current (latest) notification event id, or {@code -1} if unavailable. Used to
     * cheaply decide whether there is anything new to pull.
     *
     * @throws HmsClientException if the operation fails
     */
    default long getCurrentNotificationEventId() {
        throw new UnsupportedOperationException(
                "getCurrentNotificationEventId is not supported by this client");
    }

    /**
     * Fetch the next batch of notification events after {@code lastEventId} (exclusive), up to
     * {@code maxEvents}, as SPI-clean DTOs.
     *
     * @param lastEventId the last event id already consumed
     * @param maxEvents   maximum number of events to return
     * @return the events in ascending id order (empty when none)
     * @throws HmsClientException if the operation fails; when the metastore has trimmed its
     *         notification log past {@code lastEventId} the message carries the
     *         {@code REPL_EVENTS_MISSING_IN_METASTORE} sentinel so the caller can fall back to a
     *         full refresh
     */
    default List<HmsNotificationEvent> getNextNotification(long lastEventId, int maxEvents) {
        throw new UnsupportedOperationException("getNextNotification is not supported by this client");
    }
}
