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

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;

import java.util.List;
import java.util.Map;

public interface CachedClient extends AutoCloseable {
    void setThrowable(Throwable throwable);

    Database getDatabase(String dbName) throws Exception;

    List<String> getAllDatabases() throws Exception;

    List<String> getAllTables(String dbName) throws Exception;

    boolean tableExists(String dbName, String tblName) throws Exception;

    List<String> listPartitionNames(String dbName, String tblName, short maxListPartitionNum)
            throws Exception;

    Partition getPartition(String dbName, String tblName, List<String> partitionValues)
            throws Exception;

    List<Partition> getPartitionsByNames(String dbName, String tblName, List<String> partitionNames)
            throws Exception;

    Table getTable(String dbName, String tblName) throws Exception;

    List<FieldSchema> getSchema(String dbName, String tblName) throws Exception;

    List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tblName,
            List<String> columns) throws Exception;

    Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName, String tblName, List<String> partNames, List<String> columns) throws Exception;

    CurrentNotificationEventId getCurrentNotificationEventId() throws Exception;

    NotificationEventResponse getNextNotification(long lastEventId,
            int maxEvents,
            IMetaStoreClient.NotificationFilter filter)
            throws Exception;

    long openTxn(String user) throws Exception;

    void commitTxn(long txnId) throws Exception;

    ValidTxnList getValidTxns() throws Exception;

    List<TableValidWriteIds> getValidWriteIds(List<String> fullTableName,
            String validTransactions) throws Exception;

    LockResponse checkLock(long lockId) throws Exception;

    LockResponse lock(LockRequest lockRequest) throws Exception;
}
