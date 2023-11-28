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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;

import java.util.List;
import java.util.Map;

public abstract class CachedClient implements AutoCloseable {
    private PooledHiveMetaStoreClient pooledHiveMetaStoreClient;

    private volatile Throwable throwable;

    public CachedClient(PooledHiveMetaStoreClient pooledHiveMetaStoreClient) {
        Preconditions.checkNotNull(pooledHiveMetaStoreClient);
        this.pooledHiveMetaStoreClient = pooledHiveMetaStoreClient;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    @Override
    public void close() throws Exception {
        synchronized (pooledHiveMetaStoreClient.getClientPool()) {
            if (throwable != null
                    || pooledHiveMetaStoreClient.getClientPool().size() > pooledHiveMetaStoreClient.getPoolSize()) {
                closeRealClient();
            } else {
                pooledHiveMetaStoreClient.getClientPool().offer(this);
            }
        }
    }

    protected abstract void closeRealClient();

    public abstract List<String> getAllDatabases() throws Exception;

    public abstract List<String> getAllTables(String dbName) throws Exception;

    public abstract boolean tableExists(String dbName, String tblName) throws Exception;

    public abstract List<String> listPartitionNames(String dbName, String tblName, short maxListPartitionNum)
            throws Exception;

    public abstract Partition getPartition(String dbName, String tblName, List<String> partitionValues)
            throws Exception;

    public abstract List<Partition> getPartitionsByNames(String dbName, String tblName, List<String> partitionNames)
            throws Exception;

    public abstract Table getTable(String dbName, String tblName) throws Exception;

    public abstract List<FieldSchema> getSchema(String dbName, String tblName) throws Exception;

    public abstract List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tblName,
            List<String> columns) throws Exception;

    public abstract Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName, String tblName, List<String> partNames, List<String> columns) throws Exception;

    public abstract CurrentNotificationEventId getCurrentNotificationEventId() throws Exception;

    public abstract NotificationEventResponse getNextNotification(long lastEventId,
            int maxEvents,
            IMetaStoreClient.NotificationFilter filter)
            throws Exception;

    public abstract long openTxn(String user) throws Exception;

    public abstract void commitTxn(long txnId) throws Exception;

    public abstract ValidTxnList getValidTxns() throws Exception;

    public abstract List<TableValidWriteIds> getValidWriteIds(List<String> fullTableName,
            String validTransactions) throws Exception;

    public abstract LockResponse checkLock(long lockId) throws Exception;

    public abstract LockResponse lock(LockRequest lockRequest) throws Exception;
}
