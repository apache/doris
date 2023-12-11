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

import org.apache.doris.common.Config;
import org.apache.doris.datasource.property.constants.HMSProperties;

import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;

import java.util.List;
import java.util.Map;

public class IMetaStoreClientCachedClient implements CachedClient {
    private static final HiveMetaHookLoader DUMMY_HOOK_LOADER = t -> null;

    // -1 means no limit on the partitions returned.
    private static final short MAX_LIST_PARTITION_NUM = Config.max_hive_list_partition_num;

    private PooledHiveMetaStoreClient pooledHiveMetaStoreClient;

    private volatile Throwable throwable;

    private final IMetaStoreClient client;

    public IMetaStoreClientCachedClient(HiveConf hiveConf, PooledHiveMetaStoreClient pooledHiveMetaStoreClient)
            throws MetaException {
        Preconditions.checkNotNull(hiveConf);
        Preconditions.checkNotNull(pooledHiveMetaStoreClient);
        this.pooledHiveMetaStoreClient = pooledHiveMetaStoreClient;
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

    @Override
    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }


    @Override
    public void close() throws Exception {
        synchronized (pooledHiveMetaStoreClient.getClientPool()) {
            if (throwable != null
                    || pooledHiveMetaStoreClient.getClientPool().size() > pooledHiveMetaStoreClient.getPoolSize()) {
                client.close();
            } else {
                pooledHiveMetaStoreClient.getClientPool().offer(this);
            }
        }
    }

    @Override
    public Database getDatabase(String dbName) throws Exception {
        return client.getDatabase(dbName);
    }

    @Override
    public List<String> getAllDatabases() throws Exception {
        return client.getAllDatabases();
    }

    @Override
    public List<String> getAllTables(String dbName) throws Exception {
        return client.getAllTables(dbName);
    }

    @Override
    public boolean tableExists(String dbName, String tblName) throws Exception {
        return client.tableExists(dbName, tblName);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, short maxListPartitionNum) throws Exception {
        return client.listPartitionNames(dbName, tblName, MAX_LIST_PARTITION_NUM);
    }

    @Override
    public Partition getPartition(String dbName, String tblName, List<String> partitionValues) throws Exception {
        return client.getPartition(dbName, tblName, partitionValues);
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tblName, List<String> partitionNames)
            throws Exception {
        return client.getPartitionsByNames(dbName, tblName, partitionNames);
    }

    @Override
    public Table getTable(String dbName, String tblName) throws Exception {
        return client.getTable(dbName, tblName);
    }

    @Override
    public List<FieldSchema> getSchema(String dbName, String tblName) throws Exception {
        return client.getSchema(dbName, tblName);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tblName, List<String> columns)
            throws Exception {
        return client.getTableColumnStatistics(dbName, tblName, columns);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tblName,
            List<String> partNames, List<String> columns) throws Exception {
        return client.getPartitionColumnStatistics(dbName, tblName, partNames, columns);
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() throws Exception {
        return client.getCurrentNotificationEventId();
    }

    @Override
    public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents, NotificationFilter filter)
            throws Exception {
        return client.getNextNotification(lastEventId, maxEvents, filter);
    }

    @Override
    public long openTxn(String user) throws Exception {
        return client.openTxn(user);
    }

    @Override
    public void commitTxn(long txnId) throws Exception {
        client.commitTxn(txnId);
    }

    @Override
    public ValidTxnList getValidTxns() throws Exception {
        return client.getValidTxns();
    }

    @Override
    public List<TableValidWriteIds> getValidWriteIds(List<String> fullTableName, String validTransactions)
            throws Exception {
        return client.getValidWriteIds(fullTableName, validTransactions);
    }

    @Override
    public LockResponse checkLock(long lockId) throws Exception {
        return client.checkLock(lockId);
    }

    @Override
    public LockResponse lock(LockRequest lockRequest) throws Exception {
        return client.lock(lockRequest);
    }
}
