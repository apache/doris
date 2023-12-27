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
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.HMSClientException;
import org.apache.doris.datasource.hive.event.MetastoreNotificationFetchException;
import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;
import org.apache.doris.thrift.TOdbcTableType;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PostgreSQLJdbcHMSCachedClient extends JdbcHMSCachedClient {
    private static final Logger LOG = LogManager.getLogger(PostgreSQLJdbcHMSCachedClient.class);

    public PostgreSQLJdbcHMSCachedClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    public Database getDatabase(String dbName) {
        throw new HMSClientException("Do not support in PostgreSQLJdbcHMSCachedClient.");
    }

    @Override
    public List<String> getAllDatabases() {
        String nameFiled = JdbcTable.databaseProperName(TOdbcTableType.POSTGRESQL, "NAME");
        String tableName = JdbcTable.databaseProperName(TOdbcTableType.POSTGRESQL, "DBS");
        String sql = String.format("SELECT %s FROM %s;", nameFiled, tableName);
        LOG.debug("getAllDatabases exec sql: {}", sql);
        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            Builder<String> builder = ImmutableList.builder();
            while (rs.next()) {
                String hiveDatabaseName = rs.getString("NAME");
                builder.add(hiveDatabaseName);
            }
            return builder.build();
        } catch (Exception e) {
            throw new HMSClientException("failed to get all database from hms client", e);
        }
    }

    @Override
    public List<String> getAllTables(String dbName) {
        String sql = "SELECT \"TBL_NAME\" FROM \"TBLS\" join \"DBS\" on \"TBLS\".\"DB_ID\" = \"DBS\".\"DB_ID\""
                + " WHERE \"DBS\".\"NAME\" = '" + dbName + "';";
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
        } catch (Exception e) {
            throw new HMSClientException("failed to get all tables for db %s", e, dbName);
        }
    }

    @Override
    public boolean tableExists(String dbName, String tblName) {
        List<String> allTables = getAllTables(dbName);
        return allTables.contains(tblName);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName) {
        return listPartitionNames(dbName, tblName, (long) -1);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, long maxListPartitionNum) {
        String sql = String.format("SELECT \"PART_NAME\" from \"PARTITIONS\" WHERE \"TBL_ID\" = ("
                + "SELECT \"TBL_ID\" FROM \"TBLS\" join \"DBS\" on \"TBLS\".\"DB_ID\" = \"DBS\".\"DB_ID\""
                + " WHERE \"DBS\".\"NAME\" = '%s' AND \"TBLS\".\"TBL_NAME\"='%s');", dbName, tblName);
        LOG.debug("listPartitionNames exec sql: {}", sql);

        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            Builder<String> builder = ImmutableList.builder();
            while (rs.next()) {
                String hivePartitionName = rs.getString("PART_NAME");
                builder.add(hivePartitionName);
            }
            return builder.build();
        } catch (Exception e) {
            throw new HMSClientException("failed to list partition names for table %s in db %s", e, tblName, dbName);
        }
    }

    // not used
    @Override
    public Partition getPartition(String dbName, String tblName, List<String> partitionValues) {
        LOG.debug("getPartition partitionValues: {}", partitionValues);
        String partitionName = Joiner.on("/").join(partitionValues);
        ImmutableList<String> partitionNames = ImmutableList.of(partitionName);
        LOG.debug("getPartition partitionNames: {}", partitionNames);
        List<Partition> partitions = getPartitionsByNames(dbName, tblName, partitionNames);
        if (!partitions.isEmpty()) {
            return partitions.get(0);
        }
        throw new HMSClientException("Can not get partition of partitionName = " + partitionName
                + ", from " + dbName + "." + tblName);
    }

    @Override
    public List<Partition> getPartitions(String dbName, String tblName, List<String> partitionNames) {
        return getPartitionsByNames(dbName, tblName, partitionNames);
    }

    private List<Partition> getPartitionsByNames(String dbName, String tblName, List<String> partitionNames) {
        List<String> partitionNamesWithQuote = partitionNames.stream().map(partitionName -> "'" + partitionName + "'")
                .collect(Collectors.toList());
        String partitionNamesString = Joiner.on(", ").join(partitionNamesWithQuote);
        String sql = String.format("SELECT \"PART_ID\", \"PARTITIONS\".\"CREATE_TIME\","
                        + " \"PARTITIONS\".\"LAST_ACCESS_TIME\","
                        + " \"PART_NAME\", \"PARTITIONS\".\"SD_ID\" FROM \"PARTITIONS\""
                        + " join \"TBLS\" on \"TBLS\".\"TBL_ID\" = \"PARTITIONS\".\"TBL_ID\""
                        + " join \"DBS\" on \"TBLS\".\"DB_ID\" = \"DBS\".\"DB_ID\""
                        + " WHERE \"DBS\".\"NAME\" = '%s' AND \"TBLS\".\"TBL_NAME\"='%s'"
                        + " AND \"PART_NAME\" in (%s);",
                dbName, tblName, partitionNamesString);
        LOG.debug("getPartitionsByNames exec sql: {}", sql);

        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            Builder<Partition> builder = ImmutableList.builder();
            while (rs.next()) {
                Partition partition = new Partition();
                partition.setDbName(dbName);
                partition.setTableName(tblName);
                partition.setCreateTime(rs.getInt("CREATE_TIME"));
                partition.setLastAccessTime(rs.getInt("LAST_ACCESS_TIME"));

                // set partition values
                partition.setValues(getPartitionValues(rs.getInt("PART_ID")));

                // set SD
                StorageDescriptor storageDescriptor = getStorageDescriptor(rs.getInt("SD_ID"));
                partition.setSd(storageDescriptor);

                builder.add(partition);
            }
            return builder.build();
        } catch (Exception e) {
            throw new HMSClientException("failed to get partition for table %s in db %s with value %s", e, tblName,
                    dbName, partitionNames);
        }
    }

    private List<String> getPartitionValues(int partitionId) {
        String sql = String.format("SELECT \"PART_KEY_VAL\" FROM \"PARTITION_KEY_VALS\""
                + " WHERE \"PART_ID\" = " + partitionId);
        LOG.debug("getPartitionValues exec sql: {}", sql);
        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            Builder<String> builder = ImmutableList.builder();
            while (rs.next()) {
                builder.add(rs.getString("PART_KEY_VAL"));
            }
            return builder.build();
        } catch (Exception e) {
            throw new HMSClientException("failed to get partition Value for partitionId %s", e, partitionId);
        }
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        String sql = "SELECT \"TBL_ID\", \"TBL_NAME\", \"DBS\".\"NAME\", \"OWNER\", \"CREATE_TIME\","
                + " \"LAST_ACCESS_TIME\", \"LAST_ACCESS_TIME\", \"RETENTION\", \"TBLS\".\"SD_ID\", "
                + " \"IS_REWRITE_ENABLED\", \"VIEW_EXPANDED_TEXT\", \"VIEW_ORIGINAL_TEXT\", \"DBS\".\"OWNER_TYPE\""
                + " FROM \"TBLS\" join \"DBS\" on \"TBLS\".\"DB_ID\" = \"DBS\".\"DB_ID\" "
                + " WHERE \"DBS\".\"NAME\" = '" + dbName + "' AND \"TBLS\".\"TBL_NAME\"='" + tblName + "';";
        LOG.debug("getTable exec sql: {}", sql);

        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            Table hiveTable = new Table();
            if (rs.next()) {
                hiveTable.setTableName(rs.getString("TBL_NAME"));
                hiveTable.setDbName(rs.getString("NAME"));
                hiveTable.setOwner(rs.getString("OWNER"));
                hiveTable.setCreateTime(rs.getInt("CREATE_TIME"));
                hiveTable.setLastAccessTime(rs.getInt("LAST_ACCESS_TIME"));
                hiveTable.setRetention(rs.getInt("RETENTION"));
                hiveTable.setOwnerType(getOwnerType(rs.getString("OWNER_TYPE")));
                hiveTable.setRewriteEnabled(rs.getBoolean("IS_REWRITE_ENABLED"));
                hiveTable.setViewExpandedText(rs.getString("VIEW_EXPANDED_TEXT"));
                hiveTable.setViewOriginalText(rs.getString("VIEW_ORIGINAL_TEXT"));

                hiveTable.setSd(getStorageDescriptor(rs.getInt("SD_ID")));
                hiveTable.setParameters(getTableParameters(rs.getInt("TBL_ID")));
                hiveTable.setPartitionKeys(getTablePartitionKeys(rs.getInt("TBL_ID")));
                return hiveTable;
            }
            throw new HMSClientException("Can not get Table from PG databases. dbName = " + dbName
                    + ", tblName = " + tblName);
        } catch (Exception e) {
            throw new HMSClientException("failed to get table %s in db %s from hms client", e, tblName, dbName);
        }
    }

    private StorageDescriptor getStorageDescriptor(int sdId) {
        String sql = "SELECT * from \"SDS\" WHERE \"SD_ID\" = " + sdId;
        LOG.debug("getStorageDescriptorByDbAndTable exec sql: {}", sql);

        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(getSchemaExcludePartitionKeys(sdId));

        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                sd.setSerdeInfo(getSerdeInfo(rs.getInt("SERDE_ID")));
                sd.setInputFormat(rs.getString("INPUT_FORMAT"));
                sd.setCompressed(rs.getBoolean("IS_COMPRESSED"));
                sd.setLocation(rs.getString("LOCATION"));
                sd.setNumBuckets(rs.getInt("NUM_BUCKETS"));
                sd.setOutputFormat(rs.getString("OUTPUT_FORMAT"));
                sd.setStoredAsSubDirectories(rs.getBoolean("IS_STOREDASSUBDIRECTORIES"));
                return sd;
            }
            throw new HMSClientException("Can not get StorageDescriptor from PG, SD_ID = " + sdId);
        } catch (Exception e) {
            throw new HMSClientException("failed to get StorageDescriptor in sdId %s", e, sdId);
        }
    }

    private SerDeInfo getSerdeInfo(int serdeId) {
        String sql = "SELECT * FROM \"SERDES\" WHERE \"SERDE_ID\" = " + serdeId;
        LOG.debug("getSerdeInfo exec sql: {}", sql);

        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setParameters(getSerdeInfoParameters(serdeId));

        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                serDeInfo.setName(rs.getString("NAME"));
                serDeInfo.setSerializationLib(rs.getString("SLIB"));
                return serDeInfo;
            }
            throw new HMSClientException("Can not get SerDeInfo from PG databases, serdeId = " + serdeId + ".");
        } catch (Exception e) {
            throw new HMSClientException("failed to get SerdeInfo in serdeId %s", e, serdeId);
        }
    }

    private Map<String, String> getSerdeInfoParameters(int serdeId) {
        String sql = "SELECT \"PARAM_KEY\", \"PARAM_VALUE\" from \"SERDE_PARAMS\" WHERE \"SERDE_ID\" = " + serdeId;
        LOG.debug("getSerdeInfoParameters exec sql: {}", sql);

        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            while (rs.next()) {
                builder.put(rs.getString("PARAM_KEY"), rs.getString("PARAM_VALUE"));
            }
            return builder.build();
        } catch (Exception e) {
            throw new HMSClientException("failed to get SerdeInfoParameters in serdeId %s", e, serdeId);
        }
    }

    private List<FieldSchema> getTablePartitionKeys(int tableId) {
        String sql = "SELECT \"PKEY_NAME\", \"PKEY_TYPE\", \"PKEY_COMMENT\" from \"PARTITION_KEYS\""
                + " WHERE \"TBL_ID\"= " + tableId;
        LOG.debug("getTablePartitionKeys exec sql: {}", sql);

        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            Builder<FieldSchema> builder = ImmutableList.builder();
            while (rs.next()) {
                FieldSchema fieldSchema = new FieldSchema(rs.getString("PKEY_NAME"),
                        rs.getString("PKEY_TYPE"), rs.getString("PKEY_COMMENT"));
                builder.add(fieldSchema);
            }

            List<FieldSchema> fieldSchemas = builder.build();
            // must reverse fields
            List<FieldSchema> reversedFieldSchemas = Lists.newArrayList(fieldSchemas);
            Collections.reverse(reversedFieldSchemas);
            return reversedFieldSchemas;
        } catch (Exception e) {
            throw new HMSClientException("failed to get TablePartitionKeys in tableId %s", e, tableId);
        }
    }

    private Map<String, String> getTableParameters(int tableId) {
        String sql = "SELECT \"PARAM_KEY\", \"PARAM_VALUE\" from \"TABLE_PARAMS\" WHERE \"TBL_ID\" = " + tableId;
        LOG.debug("getParameters exec sql: {}", sql);

        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            while (rs.next()) {
                builder.put(rs.getString("PARAM_KEY"), rs.getString("PARAM_VALUE"));
            }
            return builder.build();
        } catch (Exception e) {
            throw new HMSClientException("failed to get TableParameters in tableId %s", e, tableId);
        }
    }

    private PrincipalType getOwnerType(String ownerTypeString) {
        switch (ownerTypeString) {
            case "USER":
                return PrincipalType.findByValue(1);
            case "ROLE":
                return PrincipalType.findByValue(2);
            case "GROUP":
                return PrincipalType.findByValue(3);
            default:
                throw new HMSClientException("Unknown owner type of this table");
        }
    }

    @Override
    public List<FieldSchema> getSchema(String dbName, String tblName) {
        String sql = "SELECT \"COLUMN_NAME\", \"TYPE_NAME\", \"COMMENT\", \"TBLS\".\"TBL_ID\""
                + " FROM \"TBLS\" join \"DBS\" on \"TBLS\".\"DB_ID\" = \"DBS\".\"DB_ID\""
                + " join \"SDS\" on \"SDS\".\"SD_ID\" = \"TBLS\".\"SD_ID\""
                + " join \"COLUMNS_V2\" on \"COLUMNS_V2\".\"CD_ID\" = \"SDS\".\"CD_ID\""
                + " WHERE \"DBS\".\"NAME\" = '" + dbName + "' AND \"TBLS\".\"TBL_NAME\"='" + tblName + "';";
        LOG.debug("getSchema exec sql: {}", sql);

        Builder<FieldSchema> builder = ImmutableList.builder();
        int tableId = -1;
        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                FieldSchema fieldSchema = new FieldSchema(rs.getString("COLUMN_NAME"),
                        rs.getString("TYPE_NAME"), rs.getString("COMMENT"));
                builder.add(fieldSchema);
                // actually, all resultSets have the same TBL_ID.
                tableId = rs.getInt("TBL_ID");
            }
        } catch (Exception e) {
            throw new HMSClientException("Can not get schema of db = " + dbName + ", table = " + tblName);
        }

        // add partition columns
        getTablePartitionKeys(tableId).stream().forEach(field -> builder.add(field));
        return builder.build();
    }

    private List<FieldSchema> getSchemaExcludePartitionKeys(int sdId) {
        String sql = "SELECT \"COLUMN_NAME\", \"TYPE_NAME\", \"COMMENT\""
                + " FROM \"SDS\" join \"COLUMNS_V2\" on \"COLUMNS_V2\".\"CD_ID\" = \"SDS\".\"CD_ID\""
                + " WHERE \"SDS\".\"SD_ID\" = " + sdId;
        LOG.debug("getSchema exec sql: {}", sql);

        Builder<FieldSchema> colsExcludePartitionKeys = ImmutableList.builder();
        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                FieldSchema fieldSchema = new FieldSchema(rs.getString("COLUMN_NAME"),
                        rs.getString("TYPE_NAME"), rs.getString("COMMENT"));
                colsExcludePartitionKeys.add(fieldSchema);
            }
        } catch (Exception e) {
            throw new HMSClientException("Can not get schema of SD_ID = " + sdId);
        }
        return colsExcludePartitionKeys.build();
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tblName, List<String> columns) {
        throw new HMSClientException("Do not support in PostgreSQLJdbcHMSCachedClient.");
    }

    // no use
    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tblName,
            List<String> partNames, List<String> columns) {
        throw new HMSClientException("Do not support in PostgreSQLJdbcHMSCachedClient.");
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() {
        throw new MetastoreNotificationFetchException("Do not support in PostgreSQLJdbcHMSCachedClient.");
    }

    @Override
    public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents, NotificationFilter filter) {
        throw new HMSClientException("Do not support in PostgreSQLJdbcHMSCachedClient.");
    }

    @Override
    public long openTxn(String user) {
        throw new HMSClientException("Do not support in PostgreSQLJdbcHMSCachedClient.");
    }

    @Override
    public void commitTxn(long txnId) {
        throw new HMSClientException("Do not support in PostgreSQLJdbcHMSCachedClient.");
    }

    @Override
    public ValidWriteIdList getValidWriteIds(String fullTableName, long currentTransactionId) {
        throw new HMSClientException("Do not support in PostgreSQLJdbcHMSCachedClient.");
    }

    @Override
    public void acquireSharedLock(String queryId, long txnId, String user, TableName tblName,
            List<String> partitionNames, long timeoutMs) {
        throw new HMSClientException("Do not support in PostgreSQLJdbcHMSCachedClient.");
    }

    @Override
    protected String getDatabaseQuery() {
        throw new HMSClientException("Do not support in PostgreSQLJdbcHMSCachedClient.");
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        throw new HMSClientException("Do not support in PostgreSQLJdbcHMSCachedClient.");
    }
}
