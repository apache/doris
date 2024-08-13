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
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.info.SimpleTableInfo;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;
import org.apache.doris.datasource.operations.ExternalMetadataOps;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class HiveMetadataOps implements ExternalMetadataOps {
    public static final String LOCATION_URI_KEY = "location";
    public static final String FILE_FORMAT_KEY = "file_format";
    public static final Set<String> DORIS_HIVE_KEYS = ImmutableSet.of(FILE_FORMAT_KEY, LOCATION_URI_KEY);
    private static final Logger LOG = LogManager.getLogger(HiveMetadataOps.class);
    private static final int MIN_CLIENT_POOL_SIZE = 8;
    private final HMSCachedClient client;
    private final HMSExternalCatalog catalog;
    private HadoopAuthenticator hadoopAuthenticator;

    public HiveMetadataOps(HiveConf hiveConf, JdbcClientConfig jdbcClientConfig, HMSExternalCatalog catalog) {
        this(catalog, createCachedClient(hiveConf,
                Math.max(MIN_CLIENT_POOL_SIZE, Config.max_external_cache_loader_thread_pool_size),
                jdbcClientConfig));
        hadoopAuthenticator = catalog.getAuthenticator();
        client.setHadoopAuthenticator(hadoopAuthenticator);
    }

    @VisibleForTesting
    public HiveMetadataOps(HMSExternalCatalog catalog, HMSCachedClient client) {
        this.catalog = catalog;
        this.client = client;
    }

    public HMSCachedClient getClient() {
        return client;
    }

    public HMSExternalCatalog getCatalog() {
        return catalog;
    }

    private static HMSCachedClient createCachedClient(HiveConf hiveConf, int thriftClientPoolSize,
            JdbcClientConfig jdbcClientConfig) {
        if (hiveConf != null) {
            ThriftHMSCachedClient client = new ThriftHMSCachedClient(hiveConf, thriftClientPoolSize);
            return client;
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
        if (databaseExist(fullDbName)) {
            if (stmt.isSetIfNotExists()) {
                LOG.info("create database[{}] which already exists", fullDbName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, fullDbName);
            }
        }
        try {
            HiveDatabaseMetadata catalogDatabase = new HiveDatabaseMetadata();
            catalogDatabase.setDbName(fullDbName);
            if (properties.containsKey(LOCATION_URI_KEY)) {
                catalogDatabase.setLocationUri(properties.get(LOCATION_URI_KEY));
            }
            // remove it when set
            properties.remove(LOCATION_URI_KEY);
            catalogDatabase.setProperties(properties);
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
        if (!databaseExist(dbName)) {
            if (stmt.isSetIfExists()) {
                LOG.info("drop database[{}] which does not exist", dbName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
            }
        }
        try {
            client.dropDatabase(dbName);
            catalog.onRefresh(true);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws UserException {
        String dbName = stmt.getDbName();
        String tblName = stmt.getTableName();
        ExternalDatabase<?> db = catalog.getDbNullable(dbName);
        if (db == null) {
            throw new UserException("Failed to get database: '" + dbName + "' in catalog: " + catalog.getName());
        }
        if (tableExist(dbName, tblName)) {
            if (stmt.isSetIfNotExists()) {
                LOG.info("create table[{}] which already exists", tblName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tblName);
            }
        }
        try {
            Map<String, String> props = stmt.getProperties();
            String fileFormat = props.getOrDefault(FILE_FORMAT_KEY, Config.hive_default_file_format);
            Map<String, String> ddlProps = new HashMap<>();
            for (Map.Entry<String, String> entry : props.entrySet()) {
                String key = entry.getKey().toLowerCase();
                if (DORIS_HIVE_KEYS.contains(entry.getKey().toLowerCase())) {
                    ddlProps.put("doris." + key, entry.getValue());
                } else {
                    ddlProps.put(key, entry.getValue());
                }
            }
            List<String> partitionColNames = new ArrayList<>();
            if (stmt.getPartitionDesc() != null) {
                PartitionDesc partitionDesc = stmt.getPartitionDesc();
                if (partitionDesc.getType() == PartitionType.RANGE) {
                    throw new UserException("Only support 'LIST' partition type in hive catalog.");
                }
                partitionColNames.addAll(partitionDesc.getPartitionColNames());
                if (!partitionDesc.getSinglePartitionDescs().isEmpty()) {
                    throw new UserException("Partition values expressions is not supported in hive catalog.");
                }

            }
            String comment = stmt.getComment();
            Optional<String> location = Optional.ofNullable(props.getOrDefault(LOCATION_URI_KEY, null));
            HiveTableMetadata hiveTableMeta;
            DistributionDesc bucketInfo = stmt.getDistributionDesc();
            if (bucketInfo != null) {
                if (Config.enable_create_hive_bucket_table) {
                    if (bucketInfo instanceof HashDistributionDesc) {
                        hiveTableMeta = HiveTableMetadata.of(dbName,
                                tblName,
                                location,
                                stmt.getColumns(),
                                partitionColNames,
                                ((HashDistributionDesc) bucketInfo).getDistributionColumnNames(),
                                bucketInfo.getBuckets(),
                                ddlProps,
                                fileFormat,
                                comment);
                    } else {
                        throw new UserException("External hive table only supports hash bucketing");
                    }
                } else {
                    throw new UserException("Create hive bucket table need"
                            + " set enable_create_hive_bucket_table to true");
                }
            } else {
                hiveTableMeta = HiveTableMetadata.of(dbName,
                        tblName,
                        location,
                        stmt.getColumns(),
                        partitionColNames,
                        ddlProps,
                        fileFormat,
                        comment);
            }
            client.createTable(hiveTableMeta, stmt.isSetIfNotExists());
            db.setUnInitialized(true);
        } catch (Exception e) {
            throw new UserException(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        ExternalDatabase<?> db = catalog.getDbNullable(stmt.getDbName());
        if (db == null) {
            throw new DdlException("Failed to get database: '" + dbName + "' in catalog: " + catalog.getName());
        }
        if (!tableExist(dbName, stmt.getTableName())) {
            if (stmt.isSetIfExists()) {
                LOG.info("drop table[{}] which does not exist", dbName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TABLE, stmt.getTableName(), dbName);
            }
        }
        try {
            client.dropTable(dbName, stmt.getTableName());
            db.setUnInitialized(true);
        } catch (Exception e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    @Override
    public void truncateTable(String dbName, String tblName, List<String> partitions) throws DdlException {
        ExternalDatabase<?> db = catalog.getDbNullable(dbName);
        if (db == null) {
            throw new DdlException("Failed to get database: '" + dbName + "' in catalog: " + catalog.getName());
        }
        try {
            client.truncateTable(dbName, tblName, partitions);
        } catch (Exception e) {
            throw new DdlException(e.getMessage(), e);
        }
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(catalog.getId(), dbName, tblName);
        db.setLastUpdateTime(System.currentTimeMillis());
        db.setUnInitialized(true);
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return client.getAllTables(dbName);
    }

    @Override
    public boolean tableExist(String dbName, String tblName) {
        return client.tableExists(dbName, tblName);
    }

    @Override
    public boolean databaseExist(String dbName) {
        return listDatabaseNames().contains(dbName);
    }

    @Override
    public void close() {
        client.close();
    }

    public List<String> listDatabaseNames() {
        return client.getAllDatabases();
    }

    public void updateTableStatistics(
            SimpleTableInfo tableInfo,
            Function<HivePartitionStatistics, HivePartitionStatistics> update) {
        client.updateTableStatistics(tableInfo.getDbName(), tableInfo.getTbName(), update);
    }

    void updatePartitionStatistics(
            SimpleTableInfo tableInfo,
            String partitionName,
            Function<HivePartitionStatistics, HivePartitionStatistics> update) {
        client.updatePartitionStatistics(tableInfo.getDbName(), tableInfo.getTbName(), partitionName, update);
    }

    public void addPartitions(SimpleTableInfo tableInfo, List<HivePartitionWithStatistics> partitions) {
        client.addPartitions(tableInfo.getDbName(), tableInfo.getTbName(), partitions);
    }

    public void dropPartition(SimpleTableInfo tableInfo, List<String> partitionValues, boolean deleteData) {
        client.dropPartition(tableInfo.getDbName(), tableInfo.getTbName(), partitionValues, deleteData);
    }
}
