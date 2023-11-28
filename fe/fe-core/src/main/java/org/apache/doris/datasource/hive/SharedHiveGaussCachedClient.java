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

import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;
import org.apache.doris.datasource.jdbc.client.JdbcClientException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class SharedHiveGaussCachedClient extends JdbcPostgreSQLClientCachedClient {
    private static final Logger LOG = LogManager.getLogger(SharedHiveGaussCachedClient.class);

    private Map<String, JdbcPostgreSQLClientCachedClient> allClients = Maps.newHashMap();

    private String catalogName = "hive";

    public SharedHiveGaussCachedClient(PooledHiveMetaStoreClient pooledHiveMetaStoreClient,
            JdbcClientConfig jdbcClientConfig) {
        super(pooledHiveMetaStoreClient, jdbcClientConfig);
        initSharedClient(pooledHiveMetaStoreClient, jdbcClientConfig);
    }

    private void initSharedClient(PooledHiveMetaStoreClient pooledHiveMetaStoreClient,
            JdbcClientConfig jdbcClientConfig) {
        Map<String, String> customizedProperties = jdbcClientConfig.getCustomizedProperties();
        String catName = customizedProperties.getOrDefault("hive_catalog", "");
        LOG.debug("hive_catalog = " + catName);
        if (StringUtils.isNotEmpty(catName)) {
            this.catalogName = catName;
        }
        LOG.debug("this.catalogName = " + this.catalogName);

        String sharedJdbcUrl = customizedProperties.getOrDefault("shared_jdbc_url", "");
        LOG.debug("sharedJdbcUrl = " + sharedJdbcUrl);
        // shared jdbc_url key_value
        List<String> sharedJdbcUrls = Arrays.stream(sharedJdbcUrl.split(",")).collect(Collectors.toList());
        sharedJdbcUrls.stream().forEach(kv -> {
            String[] split = kv.trim().split("=", 2);
            if (split.length != 2) {
                throw new JdbcClientException("error when split sharedJdbcUrl, sharedJdbcUrl = kv");
            }
            LOG.debug("sharedJdbcUrl key = " + split[0] + ", value = " + split[1]);
            JdbcClientConfig sharedJdbcClientConfig = jdbcClientConfig.clone();
            sharedJdbcClientConfig.setJdbcUrl(split[1]);
            JdbcPostgreSQLClientCachedClient jdbcPostgreSQLClientCachedClient = new JdbcPostgreSQLClientCachedClient(
                    pooledHiveMetaStoreClient, sharedJdbcClientConfig);
            allClients.put(split[0], jdbcPostgreSQLClientCachedClient);
        });
    }

    private String getSharedRdbKey(String dbName, String tblName) throws Exception {
        String sql = "SELECT \"RDB_KEY\" FROM \"TABLES_RDB_MAPPING\""
                + " WHERE \"CAT_NAME\" = '" + this.catalogName
                + "' AND \"DB_NAME\" = '" + dbName + "' AND \"TBL_NAME\"='" + tblName + "';";
        LOG.debug("exec getSharedRdbKey sql: " + sql);
        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getString("RDB_KEY");
            }
            throw new Exception("Can not get slave RDB_KEY from PG databases. dbName = "
                    + dbName + ", tblName = " + tblName);
        }
    }

    @Override
    public List<String> getAllTables(String dbName) throws Exception {
        String sql = "SELECT \"TBL_NAME\" FROM \"TABLES_RDB_MAPPING\""
                + " WHERE \"TABLES_RDB_MAPPING\".\"DB_NAME\" = '" + dbName + "';";
        LOG.debug("getAllTables exec sql: {}", sql);

        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            Builder<String> builder = ImmutableList.builder();
            while (rs.next()) {
                String hiveDatabaseName = rs.getString("TBL_NAME");
                builder.add(hiveDatabaseName);
            }
            return builder.build();
        }
    }

    @Override
    public boolean tableExists(String dbName, String tblName) throws Exception {
        String rdbKey = getSharedRdbKey(dbName, tblName);
        return allClients.get(rdbKey).tableExists(dbName, tblName);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, short maxListPartitionNum) throws Exception {
        String rdbKey = getSharedRdbKey(dbName, tblName);
        return allClients.get(rdbKey).listPartitionNames(dbName, tblName, maxListPartitionNum);
    }

    // not used
    @Override
    public Partition getPartition(String dbName, String tblName, List<String> partitionValues) throws Exception {
        String rdbKey = getSharedRdbKey(dbName, tblName);
        return allClients.get(rdbKey).getPartition(dbName, tblName, partitionValues);
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tblName, List<String> partitionNames)
            throws Exception {
        String rdbKey = getSharedRdbKey(dbName, tblName);
        return allClients.get(rdbKey).getPartitionsByNames(dbName, tblName, partitionNames);
    }

    @Override
    public Table getTable(String dbName, String tblName) throws Exception {
        String rdbKey = getSharedRdbKey(dbName, tblName);
        return allClients.get(rdbKey).getTable(dbName, tblName);
    }

    @Override
    public List<FieldSchema> getSchema(String dbName, String tblName) throws Exception {
        String rdbKey = getSharedRdbKey(dbName, tblName);
        return allClients.get(rdbKey).getSchema(dbName, tblName);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tblName, List<String> columns)
            throws Exception {
        LOG.debug("getTableColumnStatistics is not supported");
        return ImmutableList.of();
    }

    // no use
    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tblName,
            List<String> partNames, List<String> columns) throws Exception {
        return null;
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() throws Exception {
        throw new Exception("Do not support in JdbcPostgreSQLClientCachedClient.");
    }

    @Override
    public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents, NotificationFilter filter)
            throws Exception {
        throw new Exception("Do not support in JdbcPostgreSQLClientCachedClient.");
    }

    @Override
    public long openTxn(String user) throws Exception {
        throw new Exception("Do not support in JdbcPostgreSQLClientCachedClient.");
    }

    @Override
    public void commitTxn(long txnId) throws Exception {
        throw new Exception("Do not support in JdbcPostgreSQLClientCachedClient.");
    }

    @Override
    public ValidTxnList getValidTxns() throws Exception {
        throw new Exception("Do not support in JdbcPostgreSQLClientCachedClient.");
    }

    @Override
    public List<TableValidWriteIds> getValidWriteIds(List<String> fullTableName, String validTransactions)
            throws Exception {
        throw new Exception("Do not support in JdbcPostgreSQLClientCachedClient.");
    }

    @Override
    public LockResponse checkLock(long lockId) throws Exception {
        throw new Exception("Do not support in JdbcPostgreSQLClientCachedClient.");
    }

    @Override
    public LockResponse lock(LockRequest lockRequest) throws Exception {
        throw new Exception("Do not support in JdbcPostgreSQLClientCachedClient.");
    }
}
