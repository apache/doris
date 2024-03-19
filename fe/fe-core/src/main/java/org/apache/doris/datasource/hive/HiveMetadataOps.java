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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;
import org.apache.doris.datasource.operations.ExternalMetadataOps;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
import org.apache.doris.thrift.THivePartitionUpdate;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class HiveMetadataOps implements ExternalMetadataOps {
    private static final Logger LOG = LogManager.getLogger(HiveMetadataOps.class);
    private static final int MIN_CLIENT_POOL_SIZE = 8;
    private JdbcClientConfig jdbcClientConfig;
    private HiveConf hiveConf;
    private HMSExternalCatalog catalog;
    private HMSCachedClient client;
    private final RemoteFileSystem fs;

    public HiveMetadataOps(HiveConf hiveConf, JdbcClientConfig jdbcClientConfig, HMSExternalCatalog catalog) {
        this.catalog = catalog;
        this.hiveConf = hiveConf;
        this.jdbcClientConfig = jdbcClientConfig;
        this.client = createCachedClient(hiveConf,
                Math.max(MIN_CLIENT_POOL_SIZE, Config.max_external_cache_loader_thread_pool_size), jdbcClientConfig);
        this.fs = new DFSFileSystem(catalog.getProperties());
    }

    public HMSCachedClient getClient() {
        return client;
    }

    public static HMSCachedClient createCachedClient(HiveConf hiveConf, int thriftClientPoolSize,
                                                     JdbcClientConfig jdbcClientConfig) {
        if (hiveConf != null) {
            return new ThriftHMSCachedClient(hiveConf, thriftClientPoolSize);
        }
        Preconditions.checkNotNull(jdbcClientConfig, "hiveConf and jdbcClientConfig are both null");
        String dbType = JdbcClient.parseDbType(jdbcClientConfig.getJdbcUrl());
        switch (dbType) {
            case JdbcResource.POSTGRESQL:
                return new PostgreSQLJdbcHMSCachedClient(jdbcClientConfig);
            default:
                throw new IllegalArgumentException("Unsupported DB type: " + dbType);
        }
    }

    @Override
    public void createDb(CreateDbStmt stmt) throws DdlException {
        String fullDbName = stmt.getFullDbName();
        Map<String, String> properties = stmt.getProperties();
        long dbId = Env.getCurrentEnv().getNextId();
        try {
            HiveDatabaseMetadata catalogDatabase = new HiveDatabaseMetadata();
            catalogDatabase.setDbName(fullDbName);
            catalogDatabase.setProperties(properties);
            if (properties.containsKey("location_uri")) {
                catalogDatabase.setLocationUri(properties.get("location_uri"));
            }
            catalogDatabase.setComment(properties.getOrDefault("comment", ""));
            client.createDatabase(catalogDatabase);
            catalog.onRefresh(true);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        LOG.info("createDb dbName = " + fullDbName + ", id = " + dbId);
    }

    @Override
    public void dropDb(DropDbStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        try {
            client.dropDatabase(dbName);
            catalog.onRefresh(true);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void createTable(CreateTableStmt stmt) throws UserException {
        String dbName = stmt.getDbName();
        String tblName = stmt.getTableName();
        ExternalDatabase<?> db = catalog.getDbNullable(dbName);
        if (db == null) {
            throw new UserException("Failed to get database: '" + dbName + "' in catalog: " + catalog.getName());
        }
        try {
            Map<String, String> props = stmt.getExtProperties();
            String fileFormat = props.getOrDefault("file_format", Config.hive_default_file_format);
            HiveTableMetadata catalogTable = HiveTableMetadata.of(dbName,
                    tblName,
                    stmt.getColumns(),
                    parsePartitionKeys(props),
                    props,
                    fileFormat);

            client.createTable(catalogTable, stmt.isSetIfNotExists());
            db.setUnInitialized(true);
        } catch (Exception e) {
            throw new UserException(e.getMessage(), e);
        }
    }

    private static List<FieldSchema> parsePartitionKeys(Map<String, String> props) {
        List<FieldSchema> parsedKeys = new ArrayList<>();
        String pkStr = props.getOrDefault("partition_keys", "");
        if (pkStr.isEmpty()) {
            return parsedKeys;
        } else {
            // TODO: parse string to partition keys list
            return parsedKeys;
        }
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        ExternalDatabase<?> db = catalog.getDbNullable(stmt.getDbName());
        if (db == null) {
            throw new DdlException("Failed to get database: '" + dbName + "' in catalog: " + catalog.getName());
        }
        try {
            client.dropTable(dbName, stmt.getTableName());
            db.setUnInitialized(true);
        } catch (Exception e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return client.getAllTables(dbName);
    }

    @Override
    public boolean tableExist(String dbName, String tblName) {
        return client.tableExists(dbName, tblName);
    }

    public List<String> listDatabaseNames() {
        return client.getAllDatabases();
    }

    public void commit(String dbName,
                       String tableName,
                       List<THivePartitionUpdate> hivePUs) {
        Table table = client.getTable(dbName, tableName);
        HMSCommitter hmsCommitter = new HMSCommitter(this, fs, table);
        hmsCommitter.commit(hivePUs);
        ExternalDatabase<?> db = catalog.getDbNullable(dbName);
        if (db == null) {
            LOG.warn("Failed to get database: '" + dbName + "' in catalog: " + catalog.getName());
        } else {
            db.setUnInitialized(true);
        }
    }

    public void updateTableStatistics(
            String dbName,
            String tableName,
            Function<HivePartitionStatistics, HivePartitionStatistics> update) {
        client.updateTableStatistics(dbName, tableName, update);
    }

    void updatePartitionStatistics(
            String dbName,
            String tableName,
            String partitionName,
            Function<HivePartitionStatistics, HivePartitionStatistics> update) {
        client.updatePartitionStatistics(dbName, tableName, partitionName, update);
    }

    public void addPartitions(String dbName, String tableName, List<HivePartitionWithStatistics> partitions) {
        client.addPartitions(dbName, tableName, partitions);
    }

    public void dropPartition(String dbName, String tableName, List<String> partitionValues, boolean deleteData) {
        client.dropPartition(dbName, tableName, partitionValues, deleteData);
    }
}
