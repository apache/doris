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
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.DatabaseMetadata;
import org.apache.doris.datasource.TableMetadata;
import org.apache.doris.datasource.hive.event.MetastoreNotificationFetchException;
import org.apache.doris.datasource.property.metastore.HMSBaseProperties;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.BDPAuthContext;

import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
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
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class uses the thrift protocol to directly access the HiveMetaStore service
 * to obtain Hive metadata information
 */
public class ThriftHMSCachedClient implements HMSCachedClient {
    private static final Logger LOG = LogManager.getLogger(ThriftHMSCachedClient.class);

    private static final String JD_CONF_KEYS = "BEE_COMPUTE,BEE_BUSINESSID,BEE_SN,BEE_SCRIPT_ID,BEE_SCRIPT_V,"
            + "BUFFALO_ENV_ACTION_DEF_ID,BUFFALO_ENV_ACTION_INSTANCE_ID,BUFFALO_ENV_TASK_DEF_ID";
    private static final HiveMetaHookLoader DUMMY_HOOK_LOADER = t -> null;
    // -1 means no limit on the partitions returned.
    private static final short MAX_LIST_PARTITION_NUM = Config.max_hive_list_partition_num;

    private Multimap<String, ThriftHMSClient> clientPool = ArrayListMultimap.create();
    private PriorityQueue<Pair<String, Long>> priorityQueue = new PriorityQueue<>(
            Comparator.comparingLong(Pair::value)
    );
    private static final AtomicLong totalPooledClientCount = new AtomicLong();
    private final AtomicLong pooledClientCount = new AtomicLong();
    private boolean isClosed = false;
    private final int poolSize;
    private final HiveConf hiveConf;

    public ThriftHMSCachedClient(HiveConf hiveConf, int poolSize) {
        Preconditions.checkArgument(poolSize > 0, poolSize);
        this.hiveConf = hiveConf;
        this.poolSize = poolSize;
        this.isClosed = false;
    }

    @Override
    public void close() {
        synchronized (clientPool) {
            this.isClosed = true;
            clientPool.clear();
            priorityQueue.clear();
            totalPooledClientCount.addAndGet(-pooledClientCount.getAndSet(0));
        }
    }

    public static long getTotalPoolSize() {
        return totalPooledClientCount.get();
    }

    @Override
    public List<String> getAllDatabases() {
        try (ThriftHMSClient client = getClient()) {
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

    @Override
    public List<String> getAllTables(String dbName) {
        try (ThriftHMSClient client = getClient()) {
            long start = System.currentTimeMillis();
            try {
                return client.client.getAllTables(dbName);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - start;
                MetricRepo.HISTO_HMS_API_CALL_GET_ALL_TABLES.update(duration);
                logIfSlowHmsCall(duration, dbName, null);
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
                    client.client.createDatabase(HiveUtil.toHiveDatabase(hiveDb));
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
                    if (!dvs.isEmpty()) {
                        // foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints
                        client.client.createTableWithConstraints(hiveTable, null,
                                null, null, null, dvs, null);
                    } else {
                        client.client.createTable(hiveTable);
                    }
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
                client.client.dropDatabase(dbName);
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
                client.client.dropTable(dbName, tblName);
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
                client.client.truncateTable(dbName, tblName, partitions);
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
                return client.client.tableExists(dbName, tblName);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to check if table %s in db %s exists", e, tblName, dbName);
        }
    }

    @Override
    public List<String> listPartitionNamesFromView(String dbName, String tblName) {
        return listPartitionNamesFromView(dbName, tblName, MAX_LIST_PARTITION_NUM);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName) {
        return listPartitionNames(dbName, tblName, MAX_LIST_PARTITION_NUM);
    }

    public List<Partition> listPartitions(String dbName, String tblName) {
        try (ThriftHMSClient client = getClient()) {
            long start = System.currentTimeMillis();
            try {
                List<Partition> partitions = client.client.listPartitions(dbName, tblName, MAX_LIST_PARTITION_NUM);
                MetricRepo.HISTO_HMS_API_CALL_LIST_PARTITIONS_NUM.update(partitions.size());
                return partitions;
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - start;
                MetricRepo.HISTO_HMS_API_CALL_LIST_PARTITIONS.update(duration);
                logIfSlowHmsCall(duration, dbName, tblName);
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to list partitions in table '%s.%s'.", e, dbName, tblName);
        }
    }

    @Override
    public List<String> listPartitionNamesFromView(String dbName, String tblName, long maxListPartitionNum) {
        // list all parts when the limit is greater than the short maximum
        short limited = maxListPartitionNum <= Short.MAX_VALUE ? (short) maxListPartitionNum : MAX_LIST_PARTITION_NUM;
        try (ThriftHMSClient client = getClient()) {
            long start = System.currentTimeMillis();
            try {
                List<String> names = client.client.listPartitionNamesFromView(dbName, tblName, limited);
                MetricRepo.HISTO_HMS_API_CALL_LIST_PARTITIONS_NAMES_FROM_VIEW_NUM.update(names.size());
                return names;
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - start;
                MetricRepo.HISTO_HMS_API_CALL_LIST_PARTITIONS_NAMES_FROM_VIEW.update(duration);
                logIfSlowHmsCall(duration, dbName, tblName);
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to list partition names for table %s in db %s", e, tblName, dbName);
        }
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, long maxListPartitionNum) {
        // list all parts when the limit is greater than the short maximum
        short limited = maxListPartitionNum <= Short.MAX_VALUE ? (short) maxListPartitionNum : MAX_LIST_PARTITION_NUM;
        try (ThriftHMSClient client = getClient()) {
            long start = System.currentTimeMillis();
            try {
                List<String> names = client.client.listPartitionNames(dbName, tblName, limited);
                MetricRepo.HISTO_HMS_API_CALL_LIST_PARTITIONS_NAMES_NUM.update(names.size());
                return names;
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - start;
                MetricRepo.HISTO_HMS_API_CALL_LIST_PARTITIONS_NAMES.update(duration);
                logIfSlowHmsCall(duration, dbName, tblName);
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to list partition names for table %s in db %s", e, tblName, dbName);
        }
    }

    @Override
    public Partition getPartitionFromView(String dbName, String tblName, List<String> partitionValues) {
        try (ThriftHMSClient client = getClient()) {
            long start = System.currentTimeMillis();
            try {
                // TODO
                return client.client.getPartitionFromView(dbName, tblName, partitionValues);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - start;
                MetricRepo.HISTO_HMS_API_CALL_GET_PARTITION_FROM_VIEW.update(duration);
                logIfSlowHmsCall(duration, dbName, tblName);
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get partition for table %s in db %s with value %s", e, tblName,
                dbName, partitionValues);
        }
    }

    @Override
    public Partition getPartition(String dbName, String tblName, List<String> partitionValues) {
        try (ThriftHMSClient client = getClient()) {
            long start = System.currentTimeMillis();
            try {
                return client.client.getPartition(dbName, tblName, partitionValues);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - start;
                MetricRepo.HISTO_HMS_API_CALL_GET_PARTITION.update(duration);
                logIfSlowHmsCall(duration, dbName, tblName);
            }
        } catch (Exception e) {
            // Avoid printing too much log
            String partitionValuesMsg;
            if (partitionValues.size() <= 3) {
                partitionValuesMsg = partitionValues.toString();
            } else {
                partitionValuesMsg = partitionValues.subList(0, 3) + "... total: " + partitionValues.size();
            }
            throw new HMSClientException("failed to get partition for table %s in db %s with value [%s]", e, tblName,
                    dbName, partitionValuesMsg);
        }
    }

    @Override
    public List<Partition> getPartitionsFromView(String dbName, String tblName, List<String> partitionNames) {
        try (ThriftHMSClient client = getClient()) {
            long start = System.currentTimeMillis();
            try {
                List<Partition> partitions =
                        client.client.getPartitionsByNamesFromView(dbName, tblName, partitionNames);
                MetricRepo.HISTO_HMS_API_CALL_GET_PARTITIONS_FROM_VIEW_NUM.update(partitions.size());
                return partitions;
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - start;
                MetricRepo.HISTO_HMS_API_CALL_GET_PARTITIONS_FROM_VIEW.update(duration);
                logIfSlowHmsCall(duration, dbName, tblName);
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get partition for table %s in db %s with value %s", e, tblName,
                dbName, partitionNames);
        }
    }


    @Override
    public List<Partition> getPartitions(String dbName, String tblName, List<String> partitionNames) {
        try (ThriftHMSClient client = getClient()) {
            long start = System.currentTimeMillis();
            try {
                List<Partition> partitions = client.client.getPartitionsByNames(dbName, tblName, partitionNames);
                MetricRepo.HISTO_HMS_API_CALL_GET_PARTITIONS_NUM.update(partitions.size());
                return partitions;
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - start;
                MetricRepo.HISTO_HMS_API_CALL_GET_PARTITIONS.update(duration);
                logIfSlowHmsCall(duration, dbName, tblName);
            }
        } catch (Exception e) {
            // Avoid printing too much log
            String partitionNamesMsg;
            if (partitionNames.size() <= 3) {
                partitionNamesMsg = partitionNames.toString();
            } else {
                partitionNamesMsg = partitionNames.subList(0, 3) + "... total: " + partitionNames.size();
            }
            throw new HMSClientException("failed to get partitions for table %s in db %s with value [%s]", e, tblName,
                    dbName, partitionNamesMsg);
        }
    }

    @Override
    public Database getDatabase(String dbName) {
        try (ThriftHMSClient client = getClient()) {
            try {
                return client.client.getDatabase(dbName);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get database %s from hms client", e, dbName);
        }
    }

    public Map<String, String> getDefaultColumnValues(String dbName, String tblName) {
        return Maps.newHashMap();
    }

    @Override
    public Table getTableFromView(String dbName, String tblName) {
        try (ThriftHMSClient client = getClient()) {
            long start = System.currentTimeMillis();
            try {
                return client.client.getTableFromView(dbName, tblName);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - start;
                MetricRepo.HISTO_HMS_API_CALL_GET_TABLE_FROM_VIEW.update(duration);
                logIfSlowHmsCall(duration, dbName, tblName);
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get table %s in db %s from hms client", e, tblName, dbName);
        }
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        try (ThriftHMSClient client = getClient()) {
            long start = System.currentTimeMillis();
            try {
                return client.client.getTable(dbName, tblName);
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - start;
                MetricRepo.HISTO_HMS_API_CALL_GET_TABLE.update(duration);
                logIfSlowHmsCall(duration, dbName, tblName);
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get table %s in db %s from hms client", e, tblName, dbName);
        }
    }

    @Override
    public List<FieldSchema> getSchema(String dbName, String tblName) {
        try (ThriftHMSClient client = getClient()) {
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

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tblName, List<String> columns) {
        try (ThriftHMSClient client = getClient()) {
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

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName, String tblName, List<String> partNames, List<String> columns) {
        try (ThriftHMSClient client = getClient()) {
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

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() {
        try (ThriftHMSClient client = getClient()) {
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

    @Override
    public NotificationEventResponse getNextNotification(long lastEventId,
                                                         int maxEvents,
                                                         IMetaStoreClient.NotificationFilter filter)
            throws MetastoreNotificationFetchException {
        try (ThriftHMSClient client = getClient()) {
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

    @Override
    public long openTxn(String user) {
        try (ThriftHMSClient client = getClient()) {
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

    @Override
    public void commitTxn(long txnId) {
        try (ThriftHMSClient client = getClient()) {
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

    @Override
    public Map<String, String> getValidWriteIds(String fullTableName, long currentTransactionId) {
        Map<String, String> conf = new HashMap<>();
        try (ThriftHMSClient client = getClient()) {
            try {
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

                conf.put(AcidUtil.VALID_TXNS_KEY, validTransactions.writeToString());
                conf.put(AcidUtil.VALID_WRITEIDS_KEY, writeIdList.writeToString());
                return conf;
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            // Ignore this exception when the version of hive is not compatible with these apis.
            // Currently, the workaround is using a max watermark.
            LOG.warn("failed to get valid write ids for {}, transaction {}", fullTableName, currentTransactionId, e);

            ValidTxnList validTransactions = new ValidReadTxnList(
                    new long[0], new BitSet(), Long.MAX_VALUE, Long.MAX_VALUE);
            ValidWriteIdList writeIdList = new ValidReaderWriteIdList(
                    fullTableName, new long[0], new BitSet(), Long.MAX_VALUE);
            conf.put(AcidUtil.VALID_TXNS_KEY, validTransactions.writeToString());
            conf.put(AcidUtil.VALID_WRITEIDS_KEY, writeIdList.writeToString());
            return conf;
        }
    }

    private LockResponse checkLock(long lockId) {
        try (ThriftHMSClient client = getClient()) {
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

    private class ThriftHMSClient implements AutoCloseable {
        private final IMetaStoreClient client;
        private volatile Throwable throwable;
        private volatile boolean readyToClose;
        private String hmsClientKey;

        private ThriftHMSClient(BDPAuthContext bdpAuthContext, HiveConf hiveConf) throws MetaException {
            String type = hiveConf.get(HMSBaseProperties.HIVE_METASTORE_TYPE);
            this.hmsClientKey = bdpAuthContext.getHmsClientCacheKey();
            this.readyToClose = false;
            if (HMSBaseProperties.DLF_TYPE.equalsIgnoreCase(type)) {
                client = RetryingMetaStoreClient.getProxy(hiveConf, DUMMY_HOOK_LOADER,
                        ProxyMetaStoreClient.class.getName());
            } else if (HMSBaseProperties.GLUE_TYPE.equalsIgnoreCase(type)) {
                client = RetryingMetaStoreClient.getProxy(hiveConf, DUMMY_HOOK_LOADER,
                        AWSCatalogMetastoreClient.class.getName());
            } else {
                UserGroupInformation ugi = UserGroupInformation.createRemoteUser(bdpAuthContext.getHadoopUserName(),
                        null, bdpAuthContext.getUserToken());
                client = ugi.doAs((PrivilegedAction<IMetaStoreClient>) () -> {
                    try {
                        return RetryingMetaStoreClient.getProxy(hiveConf, DUMMY_HOOK_LOADER,
                            HiveMetaStoreClient.class.getName());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }

        public void setThrowable(Throwable throwable) {
            this.throwable = throwable;
            MetricRepo.COUNTER_HMS_CALL_ERROR.increase(1L);
        }

        public void setReadyToClose() {
            readyToClose = true;
        }

        @Override
        public void close() {
            synchronized (clientPool) {
                if (isClosed || throwable != null || readyToClose) {
                    readyToClose = true;
                } else {
                    clientPool.put(hmsClientKey, this);
                    pooledClientCount.incrementAndGet();
                    totalPooledClientCount.incrementAndGet();
                    priorityQueue.add(Pair.of(hmsClientKey, System.currentTimeMillis()));
                    if (clientPool.size() > poolSize) {
                        Pair<String, Long> pair = priorityQueue.poll();
                        List<ThriftHMSClient> clients = (List<ThriftHMSClient>) clientPool.get(pair.first);
                        pooledClientCount.decrementAndGet();
                        totalPooledClientCount.decrementAndGet();
                        try (ThriftHMSClient removeClient = clients.remove(clients.size() - 1)) {
                            removeClient.setReadyToClose();
                        } catch (Exception e) {
                            LOG.warn("failed to remove HMS client: ", e);
                        }
                    }
                }

            }
            if (readyToClose) {
                client.close();
            }
        }
    }

    private ThriftHMSClient getClient() throws MetaException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
            BDPAuthContext bdpAuthContext = BDPAuthContext.get();
            Preconditions.checkNotNull(bdpAuthContext, "bdp auth info cannot be null");
            ThriftHMSClient client = null;
            synchronized (clientPool) {
                try {
                    client = clientPool.get(bdpAuthContext.getHmsClientCacheKey()).stream()
                        .findFirst().orElse(null);
                    if (client != null) {
                        clientPool.remove(bdpAuthContext.getHmsClientCacheKey(), client);
                        pooledClientCount.decrementAndGet();
                        totalPooledClientCount.decrementAndGet();
                        Iterator<Pair<String, Long>> iterator = priorityQueue.iterator();
                        while (iterator.hasNext()) {
                            if (iterator.next().first.equals(bdpAuthContext.getHmsClientCacheKey())) {
                                iterator.remove();
                                break;
                            }
                        }
                        client.client.setMetaConf("BEE_SOURCE", bdpAuthContext.getSource());
                        client.client.setMetaConf("BEE_USER", bdpAuthContext.getErp());
                        client.client.setMetaConf("BEE_SN", bdpAuthContext.getQueryIdStr());
                        return client;
                    }
                    Preconditions.checkNotNull(bdpAuthContext.getErp(), "erp cannot be null");
                    Preconditions.checkNotNull(bdpAuthContext.getSource(), "source cannot be null");
                    long start = System.currentTimeMillis();
                    HiveConf conf = new HiveConf(hiveConf);
                    conf.set("BEE_SOURCE", bdpAuthContext.getSource());
                    conf.set("BEE_USER", bdpAuthContext.getErp());
                    conf.set("BEE_SN", bdpAuthContext.getQueryIdStr());
                    conf.set("hive.jd.conf.keys", JD_CONF_KEYS);
                    conf.set("BEE_COMPUTE", "Doris");
                    client = new ThriftHMSClient(bdpAuthContext, hiveConf);
                    if (bdpAuthContext.getUserType() != null && bdpAuthContext.getUserType().equalsIgnoreCase(
                            "dev_personal")) {
                        client.client.setMetaConf("USER_TYPE", bdpAuthContext.getUserType());
                        client.client.setMetaConf("BUSINESS_LINE", bdpAuthContext.getBusinessLine());
                    }
                    long duration = System.currentTimeMillis() - start;
                    MetricRepo.HISTO_HMS_CREATE_CLIENT.update(duration);
                    logIfSlowHmsCall(duration, null, null);
                    return client;
                } catch (Exception e) {
                    LOG.warn("failed to get hive client", e);
                    MetricRepo.COUNTER_HMS_CREATE_CLIENT_ERROR.increase(1L);
                    throw new MetaException(e.getMessage());
                }
            }
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
    }

    @Override
    public String getCatalogLocation(String catalogName) {
        try (ThriftHMSClient client = getClient()) {
            try {
                Catalog catalog = client.client.getCatalog(catalogName);
                return catalog.getLocationUri();
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            }
        } catch (Exception e) {
            throw new HMSClientException("failed to get location for %s from hms client", e, catalogName);
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

    @Override
    public int getNumPartitionsByFilter(String dbName, String tableName, String filter) {
        try (ThriftHMSClient client = getClient()) {
            int partitionsNum = client.client.getNumPartitionsByFilter(dbName, tableName, filter);
            MetricRepo.HISTO_HMS_API_CALL_GET_NUM_PARTITIONS_BY_FILTER.update(partitionsNum);
            return partitionsNum;
        } catch (Exception e) {
            throw new RuntimeException("failed to get num partitions by filter for " + dbName + "." + tableName, e);
        }
    }

    @Override
    public List<Partition> listPartitionsByFilterFromView(String dbName, String tableName, String filter,
                                                          short maxParts) {
        try (ThriftHMSClient client = getClient()) {
            long start = System.currentTimeMillis();
            try {
                List<Partition> partitions =
                        client.client.listPartitionsByFilterFromView(dbName, tableName, filter, maxParts);
                MetricRepo.HISTO_HMS_API_CALL_LIST_PARTITIONS_BY_FILTER_FROM_VIEW_NUM.update(partitions.size());
                logIfHugeHmsCall(partitions.size(), dbName, tableName);
                return partitions;
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - start;
                MetricRepo.HISTO_HMS_API_CALL_LIST_PARTITIONS_BY_FILTER_FROM_VIEW.update(duration);
                logIfSlowHmsCall(duration, dbName, tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to list partitions by filter for " + dbName + "." + tableName, e);
        }
    }

    @Override
    public List<Partition> listPartitionsByFilter(String dbName, String tableName, String filter, short maxParts) {
        try (ThriftHMSClient client = getClient()) {
            long start = System.currentTimeMillis();
            try {
                List<Partition> partitions = client.client.listPartitionsByFilter(dbName, tableName, filter, maxParts);
                MetricRepo.HISTO_HMS_API_CALL_LIST_PARTITIONS_BY_FILTER_NUM.update(partitions.size());
                logIfHugeHmsCall(partitions.size(), dbName, tableName);
                return partitions;
            } catch (Exception e) {
                client.setThrowable(e);
                throw e;
            } finally {
                long duration = System.currentTimeMillis() - start;
                MetricRepo.HISTO_HMS_API_CALL_LIST_PARTITIONS_BY_FILTER.update(duration);
                logIfSlowHmsCall(duration, dbName, tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to list partitions by filter for " + dbName + "." + tableName, e);
        }
    }

    public long getPoolSize() {
        return pooledClientCount.get();
    }

    private static void logIfSlowHmsCall(long duration, String dbName, String tableName) {
        if (duration <= Config.log_slow_hms_time_ms) {
            return;
        }
        BDPAuthContext context = BDPAuthContext.get();
        String db = (dbName != null) ? dbName : "N/A";
        String tbl = (tableName != null) ? tableName : "N/A";
        String logMsg = "Duration exceeds {} ms, dbName {}, tableName {}, HMS client information: {}";
        Object[] params = {
            Config.log_slow_hms_time_ms,
            db,
            tbl,
            (context != null) ? context : "N/A"
        };
        LOG.warn(logMsg, params);
    }

    @VisibleForTesting
    static void logIfHugeHmsCall(int partitionCount, String dbName, String tableName) {
        if (partitionCount <= Config.log_huge_hms_partition_num) {
            return;
        }

        BDPAuthContext context = BDPAuthContext.get();
        String db = (dbName != null) ? dbName : "N/A";
        String tbl = (tableName != null) ? tableName : "N/A";
        String logMsg = "Partition count exceeds {} partitions, "
                + "actual count {}, dbName {}, tableName {}, HMS client information: {}, query id: {}";

        Object[] params = {
            Config.log_huge_hms_partition_num,
            partitionCount,
            db,
            tbl,
            (context != null) ? context : "N/A",
            (context != null) ? context.getQueryIdStr() : "N/A"
        };

        LOG.warn(logMsg, params);
    }
}
