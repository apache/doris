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

package org.apache.doris.datasource.hive;

import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.Config;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.datasource.DatabaseMetadata;
import org.apache.doris.datasource.TableMetadata;
import org.apache.doris.datasource.hive.event.MetastoreNotificationFetchException;
import org.apache.doris.datasource.property.constants.HMSProperties;

import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import shade.doris.hive.org.apache.thrift.TApplicationException;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class uses the thrift protocol to directly access the HiveMetaStore service
 * to obtain Hive metadata information
 */
public class ThriftHMSCachedClient implements HMSCachedClient {
    private static final Logger LOG = LogManager.getLogger(ThriftHMSCachedClient.class);

    private static final HiveMetaHookLoader DUMMY_HOOK_LOADER = t -> null;
    // -1 means no limit on the partitions returned.
    private static final short MAX_LIST_PARTITION_NUM = Config.max_hive_list_partition_num;

    private Queue<ThriftHMSClient> clientPool = new LinkedList<>();
    private boolean isClosed = false;
    private final int poolSize;
    private final HiveConf hiveConf;
    private HadoopAuthenticator hadoopAuthenticator;

    public ThriftHMSCachedClient(HiveConf hiveConf, int poolSize) {
        Preconditions.checkArgument(poolSize > 0, poolSize);
        if (hiveConf != null) {
            hiveConf.set(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.name(),
                    String.valueOf(Config.hive_metastore_client_timeout_second));
        }
        this.hiveConf = hiveConf;
        this.poolSize = poolSize;
        this.isClosed = false;
    }

    public void setHadoopAuthenticator(HadoopAuthenticator hadoopAuthenticator) {
        this.hadoopAuthenticator = hadoopAuthenticator;
    }

    @Override
    public void close() {
        synchronized (clientPool) {
            this.isClosed = true;
            while (!clientPool.isEmpty()) {
                try {
                    clientPool.poll().close();
                } catch (Exception e) {
                    LOG.warn("failed to close thrift client", e);
                }
            }
        }
    }

    @Override
    public List<String> getAllDatabases() {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(client.client::getAllDatabases);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get all database from hms client", e);
        }
    }

    @Override
    public List<String> getAllTables(String dbName) {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> client.client.getAllTables(dbName));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get all tables for db %s", e, dbName);
        }
    }

    @Override
    public void createDatabase(DatabaseMetadata db) {
        try (ThriftHMSClient client = getClient()) {
            try {
                if (db instanceof HiveDatabaseMetadata) {
                    HiveDatabaseMetadata hiveDb = (HiveDatabaseMetadata) db;
                    ugiDoAs(() -> {
                        client.client.createDatabase(HiveUtil.toHiveDatabase(hiveDb));
                        return null;
                    });
                }
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to create database from hms client", e);
        }
    }

    @Override
    public void createTable(TableMetadata tbl, boolean ignoreIfExists) {
        if (tableExists(tbl.getDbName(), tbl.getTableName())) {
            throw new HMSClientException("Table '" + tbl.getTableName()
                    + "' has existed in '" + tbl.getDbName() + "'.");
        }
        try (ThriftHMSClient client = getClient()) {
            try {
                // String location,
                if (tbl instanceof HiveTableMetadata) {
                    Table hiveTable = HiveUtil.toHiveTable((HiveTableMetadata) tbl);
                    List<Column> tableColumns = ((HiveTableMetadata) tbl).getColumns();
                    List<SQLDefaultConstraint> dvs = new ArrayList<>(tableColumns.size());
                    for (Column tableColumn : tableColumns) {
                        if (tableColumn.hasDefaultValue()) {
                            SQLDefaultConstraint dv = new SQLDefaultConstraint();
                            dv.setTable_db(tbl.getDbName());
                            dv.setTable_name(tbl.getTableName());
                            dv.setColumn_name(tableColumn.getName());
                            dv.setDefault_value(tableColumn.getDefaultValue());
                            dv.setDc_name(tableColumn.getName() + "_dv_constraint");
                            dvs.add(dv);
                        }
                    }
                    ugiDoAs(() -> {
                        if (!dvs.isEmpty()) {
                            // foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints
                            client.client.createTableWithConstraints(hiveTable, null,
                                    null, null, null, dvs, null);
                            return null;
                        }
                        client.client.createTable(hiveTable);
                        return null;
                    });
                }
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to create table from hms client", e);
        }
    }

    @Override
    public void dropDatabase(String dbName) {
        try (ThriftHMSClient client = getClient()) {
            try {
                ugiDoAs(() -> {
                    client.client.dropDatabase(dbName);
                    return null;
                });
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to drop database from hms client", e);
        }
    }

    @Override
    public void dropTable(String dbName, String tblName) {
        try (ThriftHMSClient client = getClient()) {
            try {
                ugiDoAs(() -> {
                    client.client.dropTable(dbName, tblName);
                    return null;
                });
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to drop database from hms client", e);
        }
    }

    @Override
    public void truncateTable(String dbName, String tblName, List<String> partitions) {
        try (ThriftHMSClient client = getClient()) {
            try {
                ugiDoAs(() -> {
                    client.client.truncateTable(dbName, tblName, partitions);
                    return null;
                });
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to truncate table %s in db %s.", e, tblName, dbName);
        }
    }

    @Override
    public boolean tableExists(String dbName, String tblName) {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> client.client.tableExists(dbName, tblName));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to check if table %s in db %s exists", e, tblName, dbName);
        }
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName) {
        return listPartitionNames(dbName, tblName, MAX_LIST_PARTITION_NUM);
    }

    public List<Partition> listPartitions(String dbName, String tblName) {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> client.client.listPartitions(dbName, tblName, MAX_LIST_PARTITION_NUM));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to list partitions in table '%s.%s'.", e, dbName, tblName);
        }
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, long maxListPartitionNum) {
        // list all parts when the limit is greater than the short maximum
        short limited = maxListPartitionNum <= Short.MAX_VALUE ? (short) maxListPartitionNum : MAX_LIST_PARTITION_NUM;
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> client.client.listPartitionNames(dbName, tblName, limited));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to list partition names for table %s in db %s", e, tblName, dbName);
        }
    }

    @Override
    public Partition getPartition(String dbName, String tblName, List<String> partitionValues) {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> client.client.getPartition(dbName, tblName, partitionValues));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get partition for table %s in db %s with value %s", e, tblName,
                    dbName, partitionValues);
        }
    }

    @Override
    public List<Partition> getPartitions(String dbName, String tblName, List<String> partitionNames) {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> client.client.getPartitionsByNames(dbName, tblName, partitionNames));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get partition for table %s in db %s with value %s", e, tblName,
                    dbName, partitionNames);
        }
    }

    @Override
    public Database getDatabase(String dbName) {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> client.client.getDatabase(dbName));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get database %s from hms client", e, dbName);
        }
    }

    public Map<String, String> getDefaultColumnValues(String dbName, String tblName) {
        Map<String, String> res = new HashMap<>();
        try (ThriftHMSClient client = getClient()) {
            try {
                DefaultConstraintsRequest req = new DefaultConstraintsRequest();
                req.setDb_name(dbName);
                req.setTbl_name(tblName);
                List<SQLDefaultConstraint> dvcs = ugiDoAs(() -> {
                    try {
                        return client.client.getDefaultConstraints(req);
                    } catch (TApplicationException e) {
                        if (e.getMessage().contains("Invalid method name: 'get_default_constraints'")) {
                            // the getDefaultConstraints method only supported on hive3
                            return ImmutableList.of();
                        }
                        throw e;
                    }
                });
                for (SQLDefaultConstraint dvc : dvcs) {
                    res.put(dvc.getColumn_name().toLowerCase(Locale.ROOT), dvc.getDefault_value());
                }
                return res;
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get table %s in db %s from hms client", e, tblName, dbName);
        }
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> client.client.getTable(dbName, tblName));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get table %s in db %s from hms client", e, tblName, dbName);
        }
    }

    @Override
    public List<FieldSchema> getSchema(String dbName, String tblName) {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> client.client.getSchema(dbName, tblName));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get schema for table %s in db %s", e, tblName, dbName);
        }
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tblName, List<String> columns) {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> client.client.getTableColumnStatistics(dbName, tblName, columns));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName, String tblName, List<String> partNames, List<String> columns) {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> client.client.getPartitionColumnStatistics(dbName, tblName, partNames, columns));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(client.client::getCurrentNotificationEventId);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            LOG.warn("Failed to fetch current notification event id", e);
            throw new MetastoreNotificationFetchException(
                    "Failed to get current notification event id. msg: " + e.getMessage());
        }
    }

    @Override
    public NotificationEventResponse getNextNotification(long lastEventId,
                                                         int maxEvents,
                                                         IMetaStoreClient.NotificationFilter filter)
            throws MetastoreNotificationFetchException {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> client.client.getNextNotification(lastEventId, maxEvents, filter));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            LOG.warn("Failed to get next notification based on last event id {}", lastEventId, e);
            throw new MetastoreNotificationFetchException(
                    "Failed to get next notification based on last event id: " + lastEventId + ". msg: " + e
                            .getMessage());
        }
    }

    @Override
    public long openTxn(String user) {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> client.client.openTxn(user));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to open transaction", e);
        }
    }

    @Override
    public void commitTxn(long txnId) {
        try (ThriftHMSClient client = getClient()) {
            try {
                ugiDoAs(() -> {
                    client.client.commitTxn(txnId);
                    return null;
                });
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to commit transaction " + txnId, e);
        }
    }

    @Override
    public void acquireSharedLock(String queryId, long txnId, String user, TableName tblName,
                                  List<String> partitionNames, long timeoutMs) {
        LockRequestBuilder request = new LockRequestBuilder(queryId).setTransactionId(txnId).setUser(user);
        List<LockComponent> lockComponents = createLockComponentsForRead(tblName, partitionNames);
        for (LockComponent component : lockComponents) {
            request.addLockComponent(component);
        }
        try (ThriftHMSClient client = getClient()) {
            LockResponse response;
            try {
                response = ugiDoAs(() -> client.client.lock(request.build()));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
            long start = System.currentTimeMillis();
            while (response.getState() == LockState.WAITING) {
                long lockId = response.getLockid();
                if (System.currentTimeMillis() - start > timeoutMs) {
                    throw new RuntimeException(
                            "acquire lock timeout for txn " + txnId + " of query " + queryId + ", timeout(ms): "
                                    + timeoutMs);
                }
                response = checkLock(lockId);
            }

            if (response.getState() != LockState.ACQUIRED) {
                throw new RuntimeException("failed to acquire lock, lock in state " + response.getState());
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to commit transaction " + txnId, e);
        }
    }

    @Override
    public ValidWriteIdList getValidWriteIds(String fullTableName, long currentTransactionId) {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> {
                    // Pass currentTxn as 0L to get the recent snapshot of valid transactions in Hive
                    // Do not pass currentTransactionId instead as
                    // it will break Hive's listing of delta directories if major compaction
                    // deletes delta directories for valid transactions that existed at the time transaction is opened
                    ValidTxnList validTransactions = client.client.getValidTxns();
                    List<TableValidWriteIds> tableValidWriteIdsList = client.client.getValidWriteIds(
                            Collections.singletonList(fullTableName), validTransactions.toString());
                    if (tableValidWriteIdsList.size() != 1) {
                        throw new Exception("tableValidWriteIdsList's size should be 1");
                    }
                    ValidTxnWriteIdList validTxnWriteIdList = TxnUtils.createValidTxnWriteIdList(currentTransactionId,
                            tableValidWriteIdsList);
                    ValidWriteIdList writeIdList = validTxnWriteIdList.getTableValidWriteIdList(fullTableName);
                    return writeIdList;
                });
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            // Ignore this exception when the version of hive is not compatible with these apis.
            // Currently, the workaround is using a max watermark.
            LOG.warn("failed to get valid write ids for {}, transaction {}", fullTableName, currentTransactionId, e);
            return new ValidReaderWriteIdList(fullTableName, new long[0], new BitSet(), Long.MAX_VALUE);
        }
    }

    private LockResponse checkLock(long lockId) {
        try (ThriftHMSClient client = getClient()) {
            try {
                return ugiDoAs(() -> client.client.checkLock(lockId));
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to check lock " + lockId, e);
        }
    }

    private static List<LockComponent> createLockComponentsForRead(TableName tblName, List<String> partitionNames) {
        List<LockComponent> components = Lists.newArrayListWithCapacity(
                partitionNames.isEmpty() ? 1 : partitionNames.size());
        if (partitionNames.isEmpty()) {
            components.add(createLockComponentForRead(tblName, Optional.empty()));
        } else {
            for (String partitionName : partitionNames) {
                components.add(createLockComponentForRead(tblName, Optional.of(partitionName)));
            }
        }
        return components;
    }

    private static LockComponent createLockComponentForRead(TableName tblName, Optional<String> partitionName) {
        LockComponentBuilder builder = new LockComponentBuilder();
        builder.setShared();
        builder.setOperationType(DataOperationType.SELECT);
        builder.setDbName(tblName.getDb());
        builder.setTableName(tblName.getTbl());
        partitionName.ifPresent(builder::setPartitionName);
        builder.setIsTransactional(true);
        return builder.build();
    }

    private class ThriftHMSClient implements AutoCloseable {
        private final IMetaStoreClient client;
        private volatile Throwable throwable;

        private ThriftHMSClient(HiveConf hiveConf) throws MetaException {
            String type = hiveConf.get(HMSProperties.HIVE_METASTORE_TYPE);
            if (HMSProperties.DLF_TYPE.equalsIgnoreCase(type)) {
                client = RetryingMetaStoreClient.getProxy(hiveConf, DUMMY_HOOK_LOADER,
                        ProxyMetaStoreClient.class.getName());
            } else if (HMSProperties.GLUE_TYPE.equalsIgnoreCase(type)) {
                client = RetryingMetaStoreClient.getProxy(hiveConf, DUMMY_HOOK_LOADER,
                        AWSCatalogMetastoreClient.class.getName());
            } else {
                client = RetryingMetaStoreClient.getProxy(hiveConf, DUMMY_HOOK_LOADER,
                        HiveMetaStoreClient.class.getName());
            }
        }

        public void setThrowable(Throwable throwable) {
            this.throwable = throwable;
        }

        @Override
        public void close() throws Exception {
            synchronized (clientPool) {
                if (isClosed || throwable != null || clientPool.size() > poolSize) {
                    client.close();
                } else {
                    clientPool.offer(this);
                }
            }
        }
    }

    private ThriftHMSClient getClient() throws MetaException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
            synchronized (clientPool) {
                ThriftHMSClient client = clientPool.poll();
                if (client == null) {
                    return ugiDoAs(() -> new ThriftHMSClient(hiveConf));
                }
                return client;
            }
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
    }

    @Override
    public String getCatalogLocation(String catalogName) {
        try (ThriftHMSClient client = getClient()) {
            try {
                Catalog catalog = ugiDoAs(() -> client.client.getCatalog(catalogName));
                return catalog.getLocationUri();
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get location for %s from hms client", e, catalogName);
        }
    }

    private <T> T ugiDoAs(PrivilegedExceptionAction<T> action) {
        try {
            return hadoopAuthenticator.doAs(action);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateTableStatistics(
            String dbName,
            String tableName,
            Function<HivePartitionStatistics, HivePartitionStatistics> update) {
        try (ThriftHMSClient client = getClient()) {

            Table originTable = getTable(dbName, tableName);
            Map<String, String> originParams = originTable.getParameters();
            HivePartitionStatistics updatedStats = update.apply(HiveUtil.toHivePartitionStatistics(originParams));

            Table newTable = originTable.deepCopy();
            Map<String, String> newParams =
                    HiveUtil.updateStatisticsParameters(originParams, updatedStats.getCommonStatistics());
            newParams.put("transient_lastDdlTime", String.valueOf(System.currentTimeMillis() / 1000));
            newTable.setParameters(newParams);
            client.client.alter_table(dbName, tableName, newTable);
        } catch (Exception e) {
            throw new RuntimeException("failed to update table statistics for " + dbName + "." + tableName, e);
        }
    }

    @Override
    public void updatePartitionStatistics(
            String dbName,
            String tableName,
            String partitionName,
            Function<HivePartitionStatistics, HivePartitionStatistics> update) {
        try (ThriftHMSClient client = getClient()) {
            List<Partition> partitions = client.client.getPartitionsByNames(
                    dbName, tableName, ImmutableList.of(partitionName));
            if (partitions.size() != 1) {
                throw new RuntimeException("Metastore returned multiple partitions for name: " + partitionName);
            }

            Partition originPartition = partitions.get(0);
            Map<String, String> originParams = originPartition.getParameters();
            HivePartitionStatistics updatedStats = update.apply(HiveUtil.toHivePartitionStatistics(originParams));

            Partition modifiedPartition = originPartition.deepCopy();
            Map<String, String> newParams =
                    HiveUtil.updateStatisticsParameters(originParams, updatedStats.getCommonStatistics());
            newParams.put("transient_lastDdlTime", String.valueOf(System.currentTimeMillis() / 1000));
            modifiedPartition.setParameters(newParams);
            client.client.alter_partition(dbName, tableName, modifiedPartition);
        } catch (Exception e) {
            throw new RuntimeException("failed to update table statistics for " + dbName + "." + tableName, e);
        }
    }

    @Override
    public void addPartitions(String dbName, String tableName, List<HivePartitionWithStatistics> partitions) {
        try (ThriftHMSClient client = getClient()) {
            List<Partition> hivePartitions = partitions.stream()
                    .map(HiveUtil::toMetastoreApiPartition)
                    .collect(Collectors.toList());
            client.client.add_partitions(hivePartitions);
        } catch (Exception e) {
            throw new RuntimeException("failed to add partitions for " + dbName + "." + tableName, e);
        }
    }

    @Override
    public void dropPartition(String dbName, String tableName, List<String> partitionValues, boolean deleteData) {
        try (ThriftHMSClient client = getClient()) {
            client.client.dropPartition(dbName, tableName, partitionValues, deleteData);
        } catch (Exception e) {
            throw new RuntimeException("failed to drop partition for " + dbName + "." + tableName, e);
        }
    }
}
