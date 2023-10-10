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
import org.apache.doris.common.Config;
import org.apache.doris.datasource.HMSClientException;
import org.apache.doris.datasource.hive.event.MetastoreNotificationFetchException;
import org.apache.doris.datasource.property.constants.HMSProperties;

import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import com.google.common.base.Preconditions;
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
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

/**
 * A hive metastore client pool for a specific catalog with hive configuration.
 */
public class PooledHiveMetaStoreClient {
    private static final Logger LOG = LogManager.getLogger(PooledHiveMetaStoreClient.class);

    private static final HiveMetaHookLoader DUMMY_HOOK_LOADER = t -> null;
    // -1 means no limit on the partitions returned.
    private static final short MAX_LIST_PARTITION_NUM = Config.max_hive_list_partition_num;

    private Queue<CachedClient> clientPool = new LinkedList<>();
    private final int poolSize;
    private final HiveConf hiveConf;

    public PooledHiveMetaStoreClient(HiveConf hiveConf, int pooSize) {
        Preconditions.checkArgument(pooSize > 0, pooSize);
        hiveConf.set(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.name(),
                String.valueOf(Config.hive_metastore_client_timeout_second));
        this.hiveConf = hiveConf;
        this.poolSize = pooSize;
    }

    public List<String> getAllDatabases() {
        try (CachedClient client = getClient()) {
            try {
                return client.client.getAllDatabases();
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get all database from hms client", e);
        }
    }

    public List<String> getAllTables(String dbName) {
        try (CachedClient client = getClient()) {
            try {
                return client.client.getAllTables(dbName);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get all tables for db %s", e, dbName);
        }
    }

    public boolean tableExists(String dbName, String tblName) {
        try (CachedClient client = getClient()) {
            try {
                return client.client.tableExists(dbName, tblName);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to check if table %s in db %s exists", e, tblName, dbName);
        }
    }

    public List<String> listPartitionNames(String dbName, String tblName) {
        try (CachedClient client = getClient()) {
            try {
                return client.client.listPartitionNames(dbName, tblName, MAX_LIST_PARTITION_NUM);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to list partition names for table %s in db %s", e, tblName, dbName);
        }
    }

    public Partition getPartition(String dbName, String tblName, List<String> partitionValues) {
        try (CachedClient client = getClient()) {
            try {
                return client.client.getPartition(dbName, tblName, partitionValues);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get partition for table %s in db %s with value %s", e, tblName,
                    dbName, partitionValues);
        }
    }

    public List<Partition> getPartitions(String dbName, String tblName, List<String> partitionNames) {
        try (CachedClient client = getClient()) {
            try {
                return client.client.getPartitionsByNames(dbName, tblName, partitionNames);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get partition for table %s in db %s with value %s", e, tblName,
                dbName, partitionNames);
        }
    }

    public List<Partition> getPartitionsByFilter(String dbName, String tblName, String filter) {
        try (CachedClient client = getClient()) {
            try {
                return client.client.listPartitionsByFilter(dbName, tblName, filter, MAX_LIST_PARTITION_NUM);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get partition by filter for table %s in db %s", e, tblName,
                    dbName);
        }
    }

    public Table getTable(String dbName, String tblName) {
        try (CachedClient client = getClient()) {
            try {
                return client.client.getTable(dbName, tblName);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get table %s in db %s from hms client", e, tblName, dbName);
        }
    }

    public List<FieldSchema> getSchema(String dbName, String tblName) {
        try (CachedClient client = getClient()) {
            try {
                return client.client.getSchema(dbName, tblName);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get schema for table %s in db %s", e, tblName, dbName);
        }
    }

    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tblName, List<String> columns) {
        try (CachedClient client = getClient()) {
            try {
                return client.client.getTableColumnStatistics(dbName, tblName, columns);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName, String tblName, List<String> partNames, List<String> columns) {
        try (CachedClient client = getClient()) {
            try {
                return client.client.getPartitionColumnStatistics(dbName, tblName, partNames, columns);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public CurrentNotificationEventId getCurrentNotificationEventId() {
        try (CachedClient client = getClient()) {
            try {
                return client.client.getCurrentNotificationEventId();
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

    public NotificationEventResponse getNextNotification(long lastEventId,
            int maxEvents,
            IMetaStoreClient.NotificationFilter filter)
            throws MetastoreNotificationFetchException {
        try (CachedClient client = getClient()) {
            try {
                return client.client.getNextNotification(lastEventId, maxEvents, filter);
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

    public long openTxn(String user) {
        try (CachedClient client = getClient()) {
            try {
                return client.client.openTxn(user);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to open transaction", e);
        }
    }

    public void commitTxn(long txnId) {
        try (CachedClient client = getClient()) {
            try {
                client.client.commitTxn(txnId);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to commit transaction " + txnId, e);
        }
    }

    public void acquireSharedLock(String queryId, long txnId, String user, TableName tblName,
            List<String> partitionNames, long timeoutMs) {
        LockRequestBuilder request = new LockRequestBuilder(queryId).setTransactionId(txnId).setUser(user);
        List<LockComponent> lockComponents = createLockComponentsForRead(tblName, partitionNames);
        for (LockComponent component : lockComponents) {
            request.addLockComponent(component);
        }
        try (CachedClient client = getClient()) {
            LockResponse response;
            try {
                response = client.client.lock(request.build());
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

    public ValidWriteIdList getValidWriteIds(String fullTableName, long currentTransactionId) {
        try (CachedClient client = getClient()) {
            try {
                // Pass currentTxn as 0L to get the recent snapshot of valid transactions in Hive
                // Do not pass currentTransactionId instead as it will break Hive's listing of delta directories if major compaction
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
        try (CachedClient client = getClient()) {
            try {
                return client.client.checkLock(lockId);
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

    public void heartbeatForTxn(long txnId) {
        try (CachedClient client = getClient()) {
            try {
                client.client.heartbeat(txnId, txnId);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to do heartbeat for transaction " + txnId, e);
        }
    }

    private class CachedClient implements AutoCloseable {
        private final IMetaStoreClient client;
        private volatile Throwable throwable;

        private CachedClient(HiveConf hiveConf) throws MetaException {
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
                if (throwable != null || clientPool.size() > poolSize) {
                    client.close();
                } else {
                    clientPool.offer(this);
                }
            }
        }
    }

    private CachedClient getClient() throws MetaException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
            synchronized (clientPool) {
                CachedClient client = clientPool.poll();
                if (client == null) {
                    return new CachedClient(hiveConf);
                }
                return client;
            }
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
    }
}

