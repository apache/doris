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

package org.apache.doris.datasource.fluss;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class FlussMetadataOps implements Closeable {
    private static final Logger LOG = LogManager.getLogger(FlussMetadataOps.class);

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long INITIAL_RETRY_DELAY_MS = 100;
    private static final long MAX_RETRY_DELAY_MS = 5000;

    private final FlussExternalCatalog catalog;
    private final String bootstrapServers;

    private final Map<String, FlussExternalTable.FlussTableMetadata> tableMetadataCache;
    private final Map<String, List<String>> databaseTablesCache;
    private final ReadWriteLock cacheLock = new ReentrantReadWriteLock();

    private volatile Connection connection;
    private volatile Admin admin;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object connectionLock = new Object();

    public FlussMetadataOps(FlussExternalCatalog catalog) {
        this.catalog = catalog;
        this.bootstrapServers = catalog.getBootstrapServers();
        this.tableMetadataCache = new HashMap<>();
        this.databaseTablesCache = new HashMap<>();
    }

    public FlussMetadataOps(org.apache.doris.datasource.ExternalCatalog catalog, Connection connection) {
        this.catalog = (FlussExternalCatalog) catalog;
        this.bootstrapServers = this.catalog.getBootstrapServers();
        this.tableMetadataCache = new HashMap<>();
        this.databaseTablesCache = new HashMap<>();
        this.connection = connection;
        this.admin = connection.getAdmin();
        this.initialized.set(true);
    }

    private void ensureConnection() {
        if (closed.get()) {
            throw new IllegalStateException("FlussMetadataOps is closed");
        }

        if (!initialized.get()) {
            synchronized (connectionLock) {
                if (!initialized.get() && !closed.get()) {
                    initConnectionWithRetry();
                    initialized.set(true);
                }
            }
        }
    }

    private void initConnectionWithRetry() {
        LOG.info("Initializing connection to Fluss cluster: {}", bootstrapServers);

        executeWithRetry(() -> {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrapServers);

            String securityProtocol = catalog.getSecurityProtocol();
            if (securityProtocol != null) {
                conf.setString("client.security.protocol", securityProtocol);
                String saslMechanism = catalog.getSaslMechanism();
                if (saslMechanism != null) {
                    conf.setString("client.security.sasl.mechanism", saslMechanism);
                }
                String saslUsername = catalog.getSaslUsername();
                if (saslUsername != null) {
                    conf.setString("client.security.sasl.username", saslUsername);
                }
                String saslPassword = catalog.getSaslPassword();
                if (saslPassword != null) {
                    conf.setString("client.security.sasl.password", saslPassword);
                }
            }

            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            return null;
        }, "initialize Fluss connection");

        LOG.info("Successfully connected to Fluss cluster: {}", bootstrapServers);
    }

    private <T> T executeWithRetry(Supplier<T> operation, String operationName) {
        int attempt = 0;
        long delayMs = INITIAL_RETRY_DELAY_MS;
        Exception lastException = null;

        while (attempt < MAX_RETRY_ATTEMPTS) {
            try {
                return operation.get();
            } catch (Exception e) {
                lastException = e;
                attempt++;

                if (attempt < MAX_RETRY_ATTEMPTS && isRetryable(e)) {
                    LOG.warn("Failed to {}, attempt {}/{}, retrying in {}ms",
                            operationName, attempt, MAX_RETRY_ATTEMPTS, delayMs, e);
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while retrying " + operationName, ie);
                    }
                    delayMs = Math.min(delayMs * 2, MAX_RETRY_DELAY_MS);
                } else {
                    break;
                }
            }
        }

        throw new RuntimeException("Failed to " + operationName + " after " + MAX_RETRY_ATTEMPTS
                + " attempts", lastException);
    }

    private boolean isRetryable(Exception e) {
        String message = e.getMessage();
        if (message == null) {
            return true;
        }
        String lowerMessage = message.toLowerCase();
        return lowerMessage.contains("timeout")
                || lowerMessage.contains("connection")
                || lowerMessage.contains("unavailable")
                || lowerMessage.contains("retry")
                || lowerMessage.contains("temporary");
    }

    public List<String> listDatabaseNames() {
        LOG.debug("Listing databases from Fluss catalog");
        ensureConnection();

        return executeWithRetry(() -> {
            try {
                return admin.listDatabases().get();
            } catch (Exception e) {
                throw new RuntimeException("Failed to list databases", e);
            }
        }, "list databases");
    }

    public boolean databaseExist(String dbName) {
        LOG.debug("Checking if database exists: {}", dbName);
        ensureConnection();

        return executeWithRetry(() -> {
            try {
                return admin.databaseExists(dbName).get();
            } catch (Exception e) {
                throw new RuntimeException("Failed to check database existence: " + dbName, e);
            }
        }, "check database existence");
    }

    public List<String> listTableNames(String dbName) {
        LOG.debug("Listing tables from database: {}", dbName);

        cacheLock.readLock().lock();
        try {
            List<String> cachedTables = databaseTablesCache.get(dbName);
            if (cachedTables != null) {
                return new ArrayList<>(cachedTables);
            }
        } finally {
            cacheLock.readLock().unlock();
        }

        ensureConnection();

        List<String> tables = executeWithRetry(() -> {
            try {
                return admin.listTables(dbName).get();
            } catch (Exception e) {
                throw new RuntimeException("Failed to list tables in database: " + dbName, e);
            }
        }, "list tables");

        cacheLock.writeLock().lock();
        try {
            databaseTablesCache.put(dbName, new ArrayList<>(tables));
        } finally {
            cacheLock.writeLock().unlock();
        }

        return tables;
    }

    public boolean tableExist(String dbName, String tableName) {
        LOG.debug("Checking if table exists: {}.{}", dbName, tableName);
        return listTableNames(dbName).contains(tableName);
    }

    public FlussExternalTable.FlussTableMetadata getTableMetadata(String dbName, String tableName) {
        String cacheKey = dbName + "." + tableName;

        cacheLock.readLock().lock();
        try {
            FlussExternalTable.FlussTableMetadata cached = tableMetadataCache.get(cacheKey);
            if (cached != null) {
                return cached;
            }
        } finally {
            cacheLock.readLock().unlock();
        }

        LOG.debug("Fetching metadata for table: {}.{}", dbName, tableName);

        FlussExternalTable.FlussTableMetadata metadata = new FlussExternalTable.FlussTableMetadata();
        
        try {
            org.apache.fluss.metadata.TablePath tablePath = 
                    org.apache.fluss.metadata.TablePath.of(dbName, tableName);
            org.apache.fluss.metadata.TableInfo tableInfo = admin.getTableInfo(tablePath).get();
            
            if (tableInfo != null) {
                // Determine table type based on primary keys
                List<String> primaryKeys = tableInfo.getPrimaryKeys();
                if (primaryKeys != null && !primaryKeys.isEmpty()) {
                    metadata.setTableType(FlussExternalTable.FlussTableType.PRIMARY_KEY_TABLE);
                    metadata.setPrimaryKeys(primaryKeys);
                } else {
                    metadata.setTableType(FlussExternalTable.FlussTableType.LOG_TABLE);
                    metadata.setPrimaryKeys(Collections.emptyList());
                }
                
                // Get partition keys
                List<String> partitionKeys = tableInfo.getPartitionKeys();
                metadata.setPartitionKeys(partitionKeys != null ? partitionKeys : Collections.emptyList());
                
                // Get bucket count
                int numBuckets = tableInfo.getNumBuckets();
                metadata.setNumBuckets(numBuckets > 0 ? numBuckets : 1);
            }
        } catch (Exception e) {
            LOG.warn("Failed to fetch table metadata for {}.{}, using defaults", dbName, tableName, e);
            metadata.setTableType(FlussExternalTable.FlussTableType.LOG_TABLE);
            metadata.setPrimaryKeys(Collections.emptyList());
            metadata.setPartitionKeys(Collections.emptyList());
            metadata.setNumBuckets(1);
        }

        cacheLock.writeLock().lock();
        try {
            tableMetadataCache.put(cacheKey, metadata);
        } finally {
            cacheLock.writeLock().unlock();
        }

        return metadata;
    }

    public org.apache.fluss.metadata.TableInfo getTableInfo(String dbName, String tableName) {
        LOG.debug("Fetching TableInfo for table: {}.{}", dbName, tableName);
        ensureConnection();
        
        return executeWithRetry(() -> {
            try {
                org.apache.fluss.metadata.TablePath tablePath = 
                        org.apache.fluss.metadata.TablePath.of(dbName, tableName);
                return admin.getTableInfo(tablePath).get();
            } catch (Exception e) {
                throw new RuntimeException("Failed to get table info: " + dbName + "." + tableName, e);
            }
        }, "get table info");
    }

    public List<Column> getTableSchema(String dbName, String tableName) {
        LOG.debug("Fetching schema for table: {}.{}", dbName, tableName);
        return new ArrayList<>();
    }

    public long getTableRowCount(String dbName, String tableName) {
        LOG.debug("Fetching row count for table: {}.{}", dbName, tableName);
        return -1;
    }

    public static Type flussTypeToDorisType(String flussType) {
        if (flussType == null) {
            return Type.STRING;
        }

        switch (flussType.toUpperCase()) {
            case "BOOLEAN":
                return Type.BOOLEAN;
            case "TINYINT":
            case "INT8":
                return Type.TINYINT;
            case "SMALLINT":
            case "INT16":
                return Type.SMALLINT;
            case "INT":
            case "INT32":
            case "INTEGER":
                return Type.INT;
            case "BIGINT":
            case "INT64":
                return Type.BIGINT;
            case "FLOAT":
                return Type.FLOAT;
            case "DOUBLE":
                return Type.DOUBLE;
            case "STRING":
            case "VARCHAR":
                return ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH);
            case "BINARY":
            case "BYTES":
                return Type.STRING;
            case "DATE":
                return ScalarType.createDateV2Type();
            case "TIME":
                return ScalarType.createTimeV2Type(0);
            case "TIMESTAMP":
            case "TIMESTAMP_WITHOUT_TIME_ZONE":
                return ScalarType.createDatetimeV2Type(6);
            case "TIMESTAMP_LTZ":
            case "TIMESTAMP_WITH_LOCAL_TIME_ZONE":
                return ScalarType.createDatetimeV2Type(6);
            case "DECIMAL":
                return ScalarType.createDecimalV3Type(38, 18);
            default:
                LOG.warn("Unknown Fluss type: {}, defaulting to STRING", flussType);
                return Type.STRING;
        }
    }

    public void invalidateTableCache(String dbName, String tableName) {
        String cacheKey = dbName + "." + tableName;
        cacheLock.writeLock().lock();
        try {
            tableMetadataCache.remove(cacheKey);
        } finally {
            cacheLock.writeLock().unlock();
        }
        LOG.debug("Invalidated cache for table: {}", cacheKey);
    }

    public void invalidateDatabaseCache(String dbName) {
        cacheLock.writeLock().lock();
        try {
            databaseTablesCache.remove(dbName);
            String prefix = dbName + ".";
            tableMetadataCache.entrySet().removeIf(entry -> entry.getKey().startsWith(prefix));
        } finally {
            cacheLock.writeLock().unlock();
        }
        LOG.debug("Invalidated cache for database: {}", dbName);
    }

    public void clearCache() {
        cacheLock.writeLock().lock();
        try {
            tableMetadataCache.clear();
            databaseTablesCache.clear();
        } finally {
            cacheLock.writeLock().unlock();
        }
        LOG.debug("Cleared all metadata cache");
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            LOG.info("Closing FlussMetadataOps");
            clearCache();

            synchronized (connectionLock) {
                if (admin != null) {
                    try {
                        admin.close();
                    } catch (Exception e) {
                        LOG.warn("Error closing Fluss admin", e);
                    }
                    admin = null;
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Exception e) {
                        LOG.warn("Error closing Fluss connection", e);
                    }
                    connection = null;
                }
                initialized.set(false);
            }
        }
    }

    public Connection getConnection() {
        ensureConnection();
        return connection;
    }
}
