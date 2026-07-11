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
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import shade.doris.hive.org.apache.thrift.TApplicationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;
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
    private static final long POOL_BORROW_TIMEOUT_MS = 60_000L;
    private static final int ADD_PARTITIONS_BATCH_SIZE = 20;
    private static final String TRANSIENT_LAST_DDL_TIME = "transient_lastDdlTime";

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
        return execute(
                client -> client.listPartitionNames(dbName, tableName,
                        toThriftMaxParts(maxParts)));
    }

    /**
     * Maps the connector's {@code maxParts} contract onto the {@code short max_parts} that HMS
     * {@code get_partition_names} accepts.
     *
     * <p>A non-positive {@code maxParts} means "all partitions" and maps to {@code -1}: HMS treats a
     * negative {@code max_parts} as unbounded. It must NOT be clamped to {@code Short.MAX_VALUE} — that
     * silently truncates a table with more than 32767 partitions and defeats the {@code -1} "unlimited"
     * contract the hive/hudi callers rely on (the legacy {@code ThriftHMSCachedClient} passed the
     * {@code Config.max_hive_list_partition_num} default of {@code -1} straight through to HMS, i.e.
     * unbounded).</p>
     *
     * <p>A positive value is passed through, narrowing to {@code short}; a value above
     * {@code Short.MAX_VALUE} narrows to a negative {@code short}, which HMS likewise treats as unbounded —
     * the pre-existing behavior of the callers that request up to {@code 100000} partitions.</p>
     */
    static short toThriftMaxParts(int maxParts) {
        return maxParts <= 0 ? (short) -1 : (short) maxParts;
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
    public List<HmsColumnStatistics> getTableColumnStatistics(String dbName, String tableName,
            List<String> columns) {
        List<ColumnStatisticsObj> stats = execute(
                client -> client.getTableColumnStatistics(dbName, tableName, columns));
        List<HmsColumnStatistics> result = new ArrayList<>(stats.size());
        for (ColumnStatisticsObj obj : stats) {
            result.add(convertColumnStatistics(obj));
        }
        return result;
    }

    /**
     * Extracts the neutral ndv / numNulls / avgColLen from a hive {@code ColumnStatisticsObj}, a byte-faithful
     * port of legacy {@code HMSExternalTable.setStatData}'s per-variant reads: {@code avgColLen} is captured
     * only for a string column (non-string columns leave it -1 so the consumer uses the fixed slot width), and
     * a variant the legacy reader does not handle (boolean / binary / timestamp) yields ndv=0 / numNulls=0.
     */
    static HmsColumnStatistics convertColumnStatistics(ColumnStatisticsObj obj) {
        long ndv = 0;
        long numNulls = 0;
        double avgColLen = -1;
        if (obj.isSetStatsData()) {
            ColumnStatisticsData data = obj.getStatsData();
            if (data.isSetStringStats()) {
                StringColumnStatsData stringStats = data.getStringStats();
                ndv = stringStats.getNumDVs();
                numNulls = stringStats.getNumNulls();
                avgColLen = stringStats.getAvgColLen();
            } else if (data.isSetLongStats()) {
                LongColumnStatsData longStats = data.getLongStats();
                ndv = longStats.getNumDVs();
                numNulls = longStats.getNumNulls();
            } else if (data.isSetDecimalStats()) {
                DecimalColumnStatsData decimalStats = data.getDecimalStats();
                ndv = decimalStats.getNumDVs();
                numNulls = decimalStats.getNumNulls();
            } else if (data.isSetDoubleStats()) {
                DoubleColumnStatsData doubleStats = data.getDoubleStats();
                ndv = doubleStats.getNumDVs();
                numNulls = doubleStats.getNumNulls();
            } else if (data.isSetDateStats()) {
                DateColumnStatsData dateStats = data.getDateStats();
                ndv = dateStats.getNumDVs();
                numNulls = dateStats.getNumNulls();
            }
        }
        return new HmsColumnStatistics(obj.getColName(), ndv, numNulls, avgColLen);
    }

    // ========== Phase 3: DDL / write operations ==========

    @Override
    public void createDatabase(HmsCreateDatabaseRequest request) {
        execute(client -> {
            client.createDatabase(HmsWriteConverter.toHiveDatabase(request));
            return null;
        });
    }

    @Override
    public void dropDatabase(String dbName) {
        execute(client -> {
            client.dropDatabase(dbName);
            return null;
        });
    }

    @Override
    public void createTable(HmsCreateTableRequest request) {
        if (tableExists(request.getDbName(), request.getTableName())) {
            throw new HmsClientException("Table '" + request.getTableName()
                    + "' has existed in '" + request.getDbName() + "'.");
        }
        Table hiveTable = HmsWriteConverter.toHiveTable(request);
        List<SQLDefaultConstraint> defaults = buildDefaultConstraints(request);
        execute(client -> {
            if (!defaults.isEmpty()) {
                // foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints
                client.createTableWithConstraints(hiveTable, null, null, null, null, defaults, null);
            } else {
                client.createTable(hiveTable);
            }
            return null;
        });
    }

    @Override
    public void dropTable(String dbName, String tableName) {
        execute(client -> {
            client.dropTable(dbName, tableName);
            return null;
        });
    }

    @Override
    public void truncateTable(String dbName, String tableName, List<String> partitions) {
        execute(client -> {
            client.truncateTable(dbName, tableName, partitions);
            return null;
        });
    }

    private static List<SQLDefaultConstraint> buildDefaultConstraints(HmsCreateTableRequest request) {
        List<SQLDefaultConstraint> defaults = new ArrayList<>();
        for (ConnectorColumn column : request.getColumns()) {
            String defaultValue = column.getDefaultValue();
            if (defaultValue != null) {
                SQLDefaultConstraint dv = new SQLDefaultConstraint();
                dv.setTable_db(request.getDbName());
                dv.setTable_name(request.getTableName());
                dv.setColumn_name(column.getName());
                dv.setDefault_value(defaultValue);
                dv.setDc_name(column.getName() + "_dv_constraint");
                defaults.add(dv);
            }
        }
        return defaults;
    }

    // ========== Phase 3: partition / statistics writes ==========

    @Override
    public void addPartitions(String dbName, String tableName,
            List<HmsPartitionWithStatistics> partitions) {
        execute(client -> {
            List<Partition> hivePartitions =
                    HmsWriteConverter.toHivePartitions(dbName, tableName, partitions);
            for (int i = 0; i < hivePartitions.size(); i += ADD_PARTITIONS_BATCH_SIZE) {
                int end = Math.min(i + ADD_PARTITIONS_BATCH_SIZE, hivePartitions.size());
                client.add_partitions(new ArrayList<>(hivePartitions.subList(i, end)));
            }
            return null;
        });
    }

    @Override
    public void updateTableStatistics(String dbName, String tableName,
            Function<HmsPartitionStatistics, HmsPartitionStatistics> update) {
        execute(client -> {
            Table originTable = client.getTable(dbName, tableName);
            Map<String, String> originParams = originTable.getParameters();
            HmsPartitionStatistics updatedStats =
                    update.apply(HmsWriteConverter.toPartitionStatistics(originParams));
            Table newTable = originTable.deepCopy();
            Map<String, String> newParams = HmsWriteConverter.toStatisticsParameters(
                    originParams, updatedStats.getCommonStatistics());
            newParams.put(TRANSIENT_LAST_DDL_TIME,
                    String.valueOf(System.currentTimeMillis() / 1000));
            newTable.setParameters(newParams);
            client.alter_table(dbName, tableName, newTable);
            return null;
        });
    }

    @Override
    public void updatePartitionStatistics(String dbName, String tableName, String partitionName,
            Function<HmsPartitionStatistics, HmsPartitionStatistics> update) {
        execute(client -> {
            List<Partition> partitions = client.getPartitionsByNames(
                    dbName, tableName, Collections.singletonList(partitionName));
            if (partitions.size() != 1) {
                throw new HmsClientException(
                        "Metastore returned multiple partitions for name: " + partitionName);
            }
            Partition originPartition = partitions.get(0);
            Map<String, String> originParams = originPartition.getParameters();
            HmsPartitionStatistics updatedStats =
                    update.apply(HmsWriteConverter.toPartitionStatistics(originParams));
            Partition modifiedPartition = originPartition.deepCopy();
            Map<String, String> newParams = HmsWriteConverter.toStatisticsParameters(
                    originParams, updatedStats.getCommonStatistics());
            newParams.put(TRANSIENT_LAST_DDL_TIME,
                    String.valueOf(System.currentTimeMillis() / 1000));
            modifiedPartition.setParameters(newParams);
            client.alter_partition(dbName, tableName, modifiedPartition);
            return null;
        });
    }

    @Override
    public boolean dropPartition(String dbName, String tableName,
            List<String> partitionValues, boolean deleteData) {
        return execute(client ->
                client.dropPartition(dbName, tableName, partitionValues, deleteData));
    }

    @Override
    public boolean partitionExists(String dbName, String tableName,
            List<String> partitionValues) {
        return execute(client -> {
            try {
                client.getPartition(dbName, tableName, partitionValues);
                return Boolean.TRUE;
            } catch (NoSuchObjectException e) {
                // Not found: a normal answer, not a client fault — do not taint the pooled client.
                return Boolean.FALSE;
            }
        });
    }

    // ========== Phase 4: ACID transactions ==========

    @Override
    public long openTxn(String user) {
        return execute(client -> client.openTxn(user));
    }

    @Override
    public void commitTxn(long txnId) {
        execute(client -> {
            client.commitTxn(txnId);
            return null;
        });
    }

    @Override
    public Map<String, String> getValidWriteIds(String fullTableName, long currentTransactionId) {
        Map<String, String> conf = new HashMap<>();
        try {
            return execute(client -> {
                // Use the recent snapshot of valid transactions (no currentTxn arg): passing
                // currentTransactionId here would break Hive's delta-directory listing if a major
                // compaction removed deltas for transactions that were valid when this txn opened.
                ValidTxnList validTransactions = client.getValidTxns();
                List<TableValidWriteIds> tableValidWriteIdsList = client.getValidWriteIds(
                        Collections.singletonList(fullTableName), validTransactions.toString());
                if (tableValidWriteIdsList.size() != 1) {
                    throw new HmsClientException("tableValidWriteIdsList's size should be 1");
                }
                ValidTxnWriteIdList validTxnWriteIdList = TxnUtils.createValidTxnWriteIdList(
                        currentTransactionId, tableValidWriteIdsList);
                ValidWriteIdList writeIdList =
                        validTxnWriteIdList.getTableValidWriteIdList(fullTableName);
                conf.put(HmsAcidConstants.VALID_TXNS_KEY, validTransactions.writeToString());
                conf.put(HmsAcidConstants.VALID_WRITEIDS_KEY, writeIdList.writeToString());
                return conf;
            });
        } catch (Exception e) {
            // Older / incompatible metastores may lack these APIs; degrade to a max watermark
            // instead of failing the scan (mirrors legacy ThriftHMSCachedClient).
            LOG.warn("failed to get valid write ids for {}, transaction {}",
                    fullTableName, currentTransactionId, e);
            ValidTxnList validTransactions = new ValidReadTxnList(
                    new long[0], new BitSet(), Long.MAX_VALUE, Long.MAX_VALUE);
            ValidWriteIdList writeIdList = new ValidReaderWriteIdList(
                    fullTableName, new long[0], new BitSet(), Long.MAX_VALUE);
            conf.put(HmsAcidConstants.VALID_TXNS_KEY, validTransactions.writeToString());
            conf.put(HmsAcidConstants.VALID_WRITEIDS_KEY, writeIdList.writeToString());
            return conf;
        }
    }

    @Override
    public void acquireSharedLock(String queryId, long txnId, String user, String dbName,
            String tableName, List<String> partitionNames, long timeoutMs) {
        LockRequestBuilder requestBuilder = new LockRequestBuilder(queryId)
                .setTransactionId(txnId)
                .setUser(user);
        for (LockComponent component
                : createLockComponentsForRead(dbName, tableName, partitionNames)) {
            requestBuilder.addLockComponent(component);
        }
        LockRequest lockRequest = requestBuilder.build();
        LockResponse response = execute(client -> client.lock(lockRequest));
        long start = System.currentTimeMillis();
        while (response.getState() == LockState.WAITING) {
            if (System.currentTimeMillis() - start > timeoutMs) {
                throw new HmsClientException("acquire lock timeout for txn " + txnId
                        + " of query " + queryId + ", timeout(ms): " + timeoutMs);
            }
            long lockId = response.getLockid();
            response = execute(client -> client.checkLock(lockId));
        }
        if (response.getState() != LockState.ACQUIRED) {
            throw new HmsClientException(
                    "failed to acquire lock, lock in state " + response.getState());
        }
    }

    @Override
    public long getCurrentNotificationEventId() {
        return execute(client -> {
            CurrentNotificationEventId id = client.getCurrentNotificationEventId();
            return id == null ? -1L : id.getEventId();
        });
    }

    @Override
    public List<HmsNotificationEvent> getNextNotification(long lastEventId, int maxEvents) {
        return execute(client -> {
            NotificationEventResponse response =
                    client.getNextNotification(lastEventId, maxEvents, null);
            List<HmsNotificationEvent> events = new ArrayList<>();
            if (response != null && response.getEvents() != null) {
                for (NotificationEvent event : response.getEvents()) {
                    events.add(new HmsNotificationEvent(event.getEventId(), event.getEventType(),
                            event.getDbName(), event.getTableName(), event.getMessage(),
                            event.getMessageFormat(), event.getEventTime()));
                }
            }
            return events;
        });
    }

    private static List<LockComponent> createLockComponentsForRead(String dbName, String tableName,
            List<String> partitionNames) {
        List<LockComponent> components = new ArrayList<>(
                partitionNames.isEmpty() ? 1 : partitionNames.size());
        if (partitionNames.isEmpty()) {
            components.add(createLockComponentForRead(dbName, tableName, null));
        } else {
            for (String partitionName : partitionNames) {
                components.add(createLockComponentForRead(dbName, tableName, partitionName));
            }
        }
        return components;
    }

    private static LockComponent createLockComponentForRead(String dbName, String tableName,
            String partitionName) {
        LockComponentBuilder builder = new LockComponentBuilder();
        builder.setShared();
        builder.setOperationType(DataOperationType.SELECT);
        builder.setDbName(dbName);
        builder.setTableName(tableName);
        if (partitionName != null) {
            builder.setPartitionName(partitionName);
        }
        builder.setIsTransactional(true);
        return builder.build();
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
            // Pin the TCCL to the plugin (child-first) classloader that loaded THIS client — NOT the system
            // classloader. Metastore client creation runs Hadoop's SecurityUtil.<clinit>, whose internal
            // `new Configuration()` captures the current TCCL and uses it to reflectively load
            // DNSDomainNameResolver. The system classloader holds fe-core's own hadoop copy, while
            // SecurityUtil/DomainNameResolver here resolve from the plugin's child-first copy — so a
            // system-loader TCCL loads the two from different loaders ("class DNSDomainNameResolver not
            // DomainNameResolver") and permanently poisons SecurityUtil JVM-wide. Pinning the plugin loader
            // (a strict superset of the system loader) keeps every reflective load on the plugin side. Mirrors
            // iceberg/paimon TcclPinningConnectorContext and HiveConnectorMetadata's stats pins; covers both the
            // non-Kerberos (context.executeAuthenticated) and Kerberos (ugi.doAs) authAction paths.
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
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
                // View text (null for a base table) — the hive VIEW SPI's isView signal + view-SQL source.
                .viewOriginalText(table.getViewOriginalText())
                .viewExpandedText(table.getViewExpandedText())
                .columns(columns)
                .partitionKeys(partKeys);

        if (sd != null) {
            builder.location(sd.getLocation())
                    .inputFormat(sd.getInputFormat())
                    .outputFormat(sd.getOutputFormat())
                    .bucketCols(sd.getBucketCols())
                    .numBuckets(sd.getNumBuckets());
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
            // isKey=true: external-table semantics (legacy HMSExternalTable and the iceberg connector both
            // mark external columns as key so DESC shows Key=true). The 5-arg ctor defaults isKey=false.
            result.add(new ConnectorColumn(
                    fs.getName(), type, fs.getComment(), true, null, true));
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
