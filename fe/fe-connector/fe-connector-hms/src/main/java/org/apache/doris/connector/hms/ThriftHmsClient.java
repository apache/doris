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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import shade.doris.hive.org.apache.thrift.TApplicationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Connection-pooled HMS Thrift client using connector-SPI types.
 *
 * <p>This mirrors fe-core's {@code ThriftHMSCachedClient} but returns
 * SPI types ({@link HmsTableInfo}, {@link ConnectorColumn}, etc.)
 * instead of fe-core's Column/DatabaseMetadata. It supports standard
 * HMS, Alibaba DLF, and AWS Glue metastores.</p>
 *
 * <p>Each method borrows a client from the pool, executes the HMS
 * operation, and returns the client. If an error occurs, the client is
 * tainted and destroyed rather than returned to the pool.</p>
 *
 * <p>Authentication is handled by an injectable {@link AuthAction}
 * functional interface, replacing fe-core's ExecutionAuthenticator.</p>
 */
public class ThriftHmsClient implements HmsClient {

    private static final Logger LOG = LogManager.getLogger(ThriftHmsClient.class);

    private static final HiveMetaHookLoader DUMMY_HOOK_LOADER = tbl -> null;
    private static final int MAX_LIST_PARTITION_NUM = Short.MAX_VALUE;
    private static final long POOL_BORROW_TIMEOUT_MS = 60_000L;

    private final HiveConf hiveConf;
    private final GenericObjectPool<PooledHmsClient> clientPool;
    private final AuthAction authAction;
    private final MetaStoreClientProvider clientProvider;
    private final HmsTypeMapping.Options typeMappingOptions;
    private volatile boolean closed;

    /**
     * Creates a new ThriftHmsClient.
     *
     * @param config     connection and pool configuration
     * @param authAction authentication wrapper (e.g. UGI doAs)
     */
    public ThriftHmsClient(HmsClientConfig config, AuthAction authAction) {
        this(config, authAction, new DefaultMetaStoreClientProvider(),
                HmsTypeMapping.Options.DEFAULT);
    }

    /**
     * Creates a new ThriftHmsClient with custom type mapping options.
     *
     * @param config             connection and pool configuration
     * @param authAction         authentication wrapper
     * @param typeMappingOptions controls binary/timestamptz mapping
     */
    public ThriftHmsClient(HmsClientConfig config, AuthAction authAction,
            HmsTypeMapping.Options typeMappingOptions) {
        this(config, authAction, new DefaultMetaStoreClientProvider(),
                typeMappingOptions);
    }

    /**
     * Package-private constructor for testing with injectable client provider.
     */
    ThriftHmsClient(HmsClientConfig config, AuthAction authAction,
            MetaStoreClientProvider clientProvider,
            HmsTypeMapping.Options typeMappingOptions) {
        this.hiveConf = HmsConfHelper.createHiveConf(config.getProperties());
        this.authAction = authAction != null ? authAction : Callable::call;
        this.clientProvider = clientProvider;
        this.typeMappingOptions = typeMappingOptions;
        if (config.getPoolSize() > 0) {
            this.clientPool = new GenericObjectPool<>(
                    new HmsClientFactory(), createPoolConfig(config.getPoolSize()));
        } else {
            this.clientPool = null;
        }
    }

    // ========== HmsClient implementation ==========

    @Override
    public List<String> listDatabases() {
        return execute(client -> client.getAllDatabases());
    }

    @Override
    public HmsDatabaseInfo getDatabase(String dbName) {
        return execute(client -> {
            Database db = client.getDatabase(dbName);
            return new HmsDatabaseInfo(
                    db.getName(),
                    db.getLocationUri(),
                    db.getDescription(),
                    db.getParameters());
        });
    }

    @Override
    public List<String> listTables(String dbName) {
        return execute(client -> client.getAllTables(dbName));
    }

    @Override
    public boolean tableExists(String dbName, String tableName) {
        return execute(client -> client.tableExists(dbName, tableName));
    }

    @Override
    public HmsTableInfo getTable(String dbName, String tableName) {
        return execute(client -> {
            Table table = client.getTable(dbName, tableName);
            return convertTable(table);
        });
    }

    @Override
    public Map<String, String> getDefaultColumnValues(String dbName,
            String tableName) {
        return execute(client -> {
            Map<String, String> defaults = new HashMap<>();
            try {
                org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest req =
                        new org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest(
                                "default", dbName, tableName);
                List<org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint>
                        constraints = client.getDefaultConstraints(req);
                for (org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint dc
                        : constraints) {
                    defaults.put(dc.getColumn_name(), dc.getDefault_value());
                }
            } catch (TApplicationException e) {
                LOG.debug("Hive metastore does not support getDefaultConstraints "
                        + "(probably Hive 2.x), returning empty defaults", e);
            }
            return defaults;
        });
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName,
            int maxParts) {
        int limit = maxParts <= 0 ? MAX_LIST_PARTITION_NUM : maxParts;
        return execute(
                client -> client.listPartitionNames(dbName, tableName,
                        (short) limit));
    }

    @Override
    public List<HmsPartitionInfo> getPartitions(String dbName,
            String tableName, List<String> partNames) {
        return execute(client -> {
            List<Partition> partitions =
                    client.getPartitionsByNames(dbName, tableName, partNames);
            return partitions.stream()
                    .map(ThriftHmsClient::convertPartition)
                    .collect(Collectors.toList());
        });
    }

    @Override
    public HmsPartitionInfo getPartition(String dbName, String tableName,
            List<String> values) {
        return execute(client -> {
            Partition partition =
                    client.getPartition(dbName, tableName, values);
            return convertPartition(partition);
        });
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (clientPool != null) {
            try {
                clientPool.close();
            } catch (Exception e) {
                LOG.warn("Failed to close HMS client pool", e);
            }
        }
    }

    // ========== Internal execution framework ==========

    private <T> T execute(HmsAction<T> action) {
        if (closed) {
            throw new HmsClientException("HMS client is closed");
        }
        try (PooledHmsClient pooled = borrowClient()) {
            try {
                return doAs(() -> action.call(pooled.client));
            } catch (Exception e) {
                pooled.taint(e);
                throw e;
            }
        } catch (HmsClientException e) {
            throw e;
        } catch (Exception e) {
            throw new HmsClientException("HMS operation failed: "
                    + e.getMessage(), e);
        }
    }

    private <T> T doAs(Callable<T> callable) throws Exception {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    ClassLoader.getSystemClassLoader());
            return authAction.execute(callable);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    // ========== Type conversion ==========

    private HmsTableInfo convertTable(Table table) {
        StorageDescriptor sd = table.getSd();
        List<ConnectorColumn> columns = convertFieldSchemas(
                sd != null ? sd.getCols() : Collections.emptyList());
        List<ConnectorColumn> partKeys = convertFieldSchemas(
                table.getPartitionKeys());

        HmsTableInfo.Builder builder = HmsTableInfo.builder()
                .dbName(table.getDbName())
                .tableName(table.getTableName())
                .tableType(table.getTableType())
                .parameters(table.getParameters())
                .columns(columns)
                .partitionKeys(partKeys);

        if (sd != null) {
            builder.location(sd.getLocation())
                    .inputFormat(sd.getInputFormat())
                    .outputFormat(sd.getOutputFormat());
            if (sd.getSerdeInfo() != null) {
                builder.serializationLib(
                        sd.getSerdeInfo().getSerializationLib());
                if (sd.getSerdeInfo().getParameters() != null) {
                    builder.sdParameters(sd.getSerdeInfo().getParameters());
                }
            }
        }
        return builder.build();
    }

    private List<ConnectorColumn> convertFieldSchemas(
            List<FieldSchema> schemas) {
        if (schemas == null || schemas.isEmpty()) {
            return Collections.emptyList();
        }
        List<ConnectorColumn> result = new ArrayList<>(schemas.size());
        for (FieldSchema fs : schemas) {
            ConnectorType type = HmsTypeMapping.toConnectorType(
                    fs.getType(), typeMappingOptions);
            result.add(new ConnectorColumn(
                    fs.getName(), type, fs.getComment(), true, null));
        }
        return result;
    }

    private static HmsPartitionInfo convertPartition(Partition partition) {
        StorageDescriptor sd = partition.getSd();
        return new HmsPartitionInfo(
                partition.getValues(),
                sd != null ? sd.getLocation() : null,
                sd != null ? sd.getInputFormat() : null,
                sd != null ? sd.getOutputFormat() : null,
                sd != null && sd.getSerdeInfo() != null
                        ? sd.getSerdeInfo().getSerializationLib() : null,
                partition.getParameters());
    }

    // ========== Pool management ==========

    private PooledHmsClient borrowClient() {
        if (clientPool == null) {
            return createFreshClient();
        }
        try {
            return clientPool.borrowObject();
        } catch (Exception e) {
            throw new HmsClientException("Failed to borrow HMS client "
                    + "from pool: " + e.getMessage(), e);
        }
    }

    private PooledHmsClient createFreshClient() {
        try {
            return doAs(() -> new PooledHmsClient(
                    clientProvider.create(hiveConf)));
        } catch (Exception e) {
            throw new HmsClientException("Failed to create HMS client: "
                    + e.getMessage(), e);
        }
    }

    private GenericObjectPoolConfig createPoolConfig(
            int poolSize) {
        GenericObjectPoolConfig config =
                new GenericObjectPoolConfig();
        config.setMaxTotal(poolSize);
        config.setMaxIdle(poolSize);
        config.setMinIdle(0);
        config.setBlockWhenExhausted(true);
        config.setMaxWaitMillis(POOL_BORROW_TIMEOUT_MS);
        config.setTestOnBorrow(false);
        config.setTestOnReturn(false);
        config.setTestWhileIdle(false);
        config.setTimeBetweenEvictionRunsMillis(-1L);
        return config;
    }

    // ========== Inner classes ==========

    /**
     * Wrapper around {@link IMetaStoreClient} with taint tracking.
     * When an exception occurs, the client is tainted and destroyed
     * on close rather than returned to the pool.
     */
    private class PooledHmsClient implements AutoCloseable {
        final IMetaStoreClient client;
        private volatile boolean destroyed;
        private volatile Throwable throwable;

        PooledHmsClient(IMetaStoreClient client) {
            this.client = client;
        }

        void taint(Throwable t) {
            this.throwable = t;
        }

        boolean isValid() {
            return !destroyed && throwable == null;
        }

        void destroy() {
            if (destroyed) {
                return;
            }
            destroyed = true;
            client.close();
        }

        @Override
        public void close() throws Exception {
            if (clientPool == null) {
                destroy();
                return;
            }
            if (closed) {
                destroy();
                return;
            }
            try {
                if (throwable != null) {
                    clientPool.invalidateObject(this);
                } else {
                    clientPool.returnObject(this);
                }
            } catch (IllegalStateException e) {
                destroy();
            } catch (Exception e) {
                destroy();
                throw e;
            }
        }
    }

    private class HmsClientFactory
            extends BasePooledObjectFactory<PooledHmsClient> {
        @Override
        public PooledHmsClient create() throws Exception {
            return createFreshClient();
        }

        @Override
        public PooledObject<PooledHmsClient> wrap(PooledHmsClient client) {
            return new DefaultPooledObject<>(client);
        }

        @Override
        public boolean validateObject(PooledObject<PooledHmsClient> p) {
            return !closed && p.getObject().isValid();
        }

        @Override
        public void destroyObject(PooledObject<PooledHmsClient> p)
                throws Exception {
            p.getObject().destroy();
        }
    }

    /**
     * Functional interface for HMS operations that can throw exceptions.
     */
    @FunctionalInterface
    interface HmsAction<T> {
        T call(IMetaStoreClient client) throws Exception;
    }

    /**
     * Functional interface for authentication delegation.
     * Replaces fe-core's ExecutionAuthenticator.
     *
     * <p>Example implementations:</p>
     * <ul>
     *   <li>Simple auth: {@code callable -> callable.call()}</li>
     *   <li>Kerberos: {@code callable -> ugi.doAs(callable)}</li>
     * </ul>
     */
    @FunctionalInterface
    public interface AuthAction {
        <T> T execute(Callable<T> callable) throws Exception;
    }

    /**
     * Factory for creating {@link IMetaStoreClient} instances.
     * Injectable for testing.
     */
    interface MetaStoreClientProvider {
        IMetaStoreClient create(HiveConf hiveConf) throws MetaException;
    }

    /**
     * Default provider using RetryingMetaStoreClient with
     * auto-detection of standard HMS, DLF, or Glue.
     */
    private static class DefaultMetaStoreClientProvider
            implements MetaStoreClientProvider {
        @Override
        public IMetaStoreClient create(HiveConf hiveConf)
                throws MetaException {
            return RetryingMetaStoreClient.getProxy(
                    hiveConf, DUMMY_HOOK_LOADER,
                    getMetastoreClientClassName(hiveConf));
        }
    }

    /** Alibaba Cloud DLF ProxyMetaStoreClient class name. */
    private static final String DLF_CLIENT_CLASS =
            "com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient";

    /** AWS Glue AWSCatalogMetastoreClient class name. */
    private static final String GLUE_CLIENT_CLASS =
            "com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient";

    static String getMetastoreClientClassName(HiveConf hiveConf) {
        String type = hiveConf.get(HmsClientConfig.METASTORE_TYPE_KEY);
        if (HmsClientConfig.METASTORE_TYPE_DLF.equalsIgnoreCase(type)) {
            return DLF_CLIENT_CLASS;
        } else if (HmsClientConfig.METASTORE_TYPE_GLUE.equalsIgnoreCase(type)) {
            return GLUE_CLIENT_CLASS;
        } else {
            return HiveMetaStoreClient.class.getName();
        }
    }
}
