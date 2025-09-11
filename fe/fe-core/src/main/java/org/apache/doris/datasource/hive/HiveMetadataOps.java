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

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.operations.ExternalMetadataOps;
import org.apache.doris.datasource.property.metastore.HMSBaseProperties;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.AcidUtils;
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
    private static final Logger LOG = LogManager.getLogger(HiveMetadataOps.class);

    public static final String LOCATION_URI_KEY = "location";
    public static final String FILE_FORMAT_KEY = "file_format";
    public static final Set<String> DORIS_HIVE_KEYS = ImmutableSet.of(FILE_FORMAT_KEY, LOCATION_URI_KEY);
    private static final int MIN_CLIENT_POOL_SIZE = 8;
    private final HMSCachedClient client;
    private final HMSExternalCatalog catalog;

    public HiveMetadataOps(HiveConf hiveConf, HMSExternalCatalog catalog) {
        this(catalog, createCachedClient(hiveConf,
                Math.max(MIN_CLIENT_POOL_SIZE, Config.max_external_cache_loader_thread_pool_size),
                catalog.getExecutionAuthenticator()));
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
                                                      ExecutionAuthenticator executionAuthenticator) {
        Preconditions.checkNotNull(hiveConf, "HiveConf cannot be null");
        return  new ThriftHMSCachedClient(hiveConf, thriftClientPoolSize, executionAuthenticator);
    }

    @Override
    public boolean createDbImpl(String dbName, boolean ifNotExists, Map<String, String> properties)
            throws DdlException {
        ExternalDatabase dorisDb = catalog.getDbNullable(dbName);
        boolean exists = databaseExist(dbName);
        if (dorisDb != null || exists) {
            if (ifNotExists) {
                LOG.info("create database[{}] which already exists", dbName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, dbName);
            }
        }
        try {
            HiveDatabaseMetadata catalogDatabase = new HiveDatabaseMetadata();
            catalogDatabase.setDbName(dbName);
            if (properties.containsKey(LOCATION_URI_KEY)) {
                catalogDatabase.setLocationUri(properties.get(LOCATION_URI_KEY));
            }
            // remove it when set
            properties.remove(LOCATION_URI_KEY);
            catalogDatabase.setProperties(properties);
            catalogDatabase.setComment(properties.getOrDefault("comment", ""));
            client.createDatabase(catalogDatabase);
            LOG.info("successfully create hive database: {}", dbName);
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void afterCreateDb() {
        catalog.resetMetaCacheNames();
    }

    @Override
    public void dropDbImpl(String dbName, boolean ifExists, boolean force) throws DdlException {
        ExternalDatabase dorisDb = catalog.getDbNullable(dbName);
        if (dorisDb == null) {
            if (ifExists) {
                LOG.info("drop database[{}] which does not exist", dbName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
            }
        }
        try {
            if (force) {
                // try to drop all tables in the database
                List<String> remoteTableNames = listTableNames(dorisDb.getRemoteName());
                for (String remoteTableName : remoteTableNames) {
                    ExternalTable tbl = null;
                    try {
                        tbl = (ExternalTable) dorisDb.getTableOrDdlException(remoteTableName);
                    } catch (DdlException e) {
                        LOG.warn("failed to get table when force drop database [{}], table[{}], error: {}",
                                dbName, remoteTableName, e.getMessage());
                        continue;
                    }
                    dropTableImpl(tbl, true);
                }
                if (!remoteTableNames.isEmpty()) {
                    LOG.info("drop database[{}] with force, drop all tables, num: {}", dbName, remoteTableNames.size());
                }
            }
            client.dropDatabase(dorisDb.getRemoteName());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void afterDropDb(String dbName) {
        catalog.unregisterDatabase(dbName);
    }

    @Override
    public boolean createTableImpl(CreateTableInfo createTableInfo) throws UserException {
        String dbName = createTableInfo.getDbName();
        String tblName = createTableInfo.getTableName();
        ExternalDatabase<?> db = catalog.getDbNullable(dbName);
        if (db == null) {
            throw new UserException("Failed to get database: '" + dbName + "' in catalog: " + catalog.getName());
        }
        if (tableExist(db.getRemoteName(), tblName)) {
            if (createTableInfo.isIfNotExists()) {
                LOG.info("create table[{}] which already exists", tblName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tblName);
            }
        }
        try {
            Map<String, String> props = createTableInfo.getProperties();
            // set default owner
            if (!props.containsKey("owner")) {
                if (ConnectContext.get() != null) {
                    props.put("owner", ConnectContext.get().getCurrentUserIdentity().getUser());
                }
            }

            if (props.containsKey("transactional") && props.get("transactional").equalsIgnoreCase("true")) {
                throw new UserException("Not support create hive transactional table.");
                /*
                    CREATE TABLE trans6(
                      `col1` int,
                      `col2` int
                    )  ENGINE=hive
                    PROPERTIES (
                      'file_format'='orc',
                      'compression'='zlib',
                      'bucketing_version'='2',
                      'transactional'='true',
                      'transactional_properties'='default'
                    );
                    In hive, this table only can insert not update(not report error,but not actually updated).
                 */
            }

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
            PartitionDesc partitionDesc = createTableInfo.getPartitionDesc();
            if (partitionDesc != null) {
                if (partitionDesc.getType() == PartitionType.RANGE) {
                    throw new UserException("Only support 'LIST' partition type in hive catalog.");
                }
                partitionColNames.addAll(partitionDesc.getPartitionColNames());
                if (!partitionDesc.getSinglePartitionDescs().isEmpty()) {
                    throw new UserException("Partition values expressions is not supported in hive catalog.");
                }

            }
            Map<String, String> properties = catalog.getProperties();
            if (properties.containsKey(HMSBaseProperties.HIVE_METASTORE_TYPE)
                    && properties.get(HMSBaseProperties.HIVE_METASTORE_TYPE).equals(HMSBaseProperties.DLF_TYPE)) {
                for (Column column : createTableInfo.getColumns()) {
                    if (column.hasDefaultValue()) {
                        throw new UserException("Default values are not supported with `DLF` catalog.");
                    }
                }
            }
            String comment = createTableInfo.getComment();
            Optional<String> location = Optional.ofNullable(props.getOrDefault(LOCATION_URI_KEY, null));
            HiveTableMetadata hiveTableMeta;
            DistributionDesc bucketInfo = createTableInfo.getDistributionDesc();
            if (bucketInfo != null) {
                if (Config.enable_create_hive_bucket_table) {
                    if (bucketInfo instanceof HashDistributionDesc) {
                        hiveTableMeta = HiveTableMetadata.of(db.getRemoteName(),
                            tblName,
                            location,
                            createTableInfo.getColumns(),
                            partitionColNames,
                            bucketInfo.getDistributionColumnNames(),
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
                hiveTableMeta = HiveTableMetadata.of(db.getRemoteName(),
                    tblName,
                    location,
                    createTableInfo.getColumns(),
                    partitionColNames,
                    ddlProps,
                    fileFormat,
                    comment);
            }
            client.createTable(hiveTableMeta, createTableInfo.isIfNotExists());
            return false;
        } catch (Exception e) {
            throw new UserException(e.getMessage(), e);
        }
    }

    @Override
    public boolean createTableImpl(CreateTableStmt stmt) throws UserException {
        String dbName = stmt.getDbName();
        String tblName = stmt.getTableName();
        ExternalDatabase<?> db = catalog.getDbNullable(dbName);
        if (db == null) {
            throw new UserException("Failed to get database: '" + dbName + "' in catalog: " + catalog.getName());
        }
        if (tableExist(db.getRemoteName(), tblName)) {
            if (stmt.isSetIfNotExists()) {
                LOG.info("create table[{}] which already exists", tblName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tblName);
            }
        }
        try {
            Map<String, String> props = stmt.getProperties();
            // set default owner
            if (!props.containsKey("owner")) {
                if (ConnectContext.get() != null) {
                    props.put("owner", ConnectContext.get().getCurrentUserIdentity().getUser());
                }
            }

            if (props.containsKey("transactional") && props.get("transactional").equalsIgnoreCase("true")) {
                throw new UserException("Not support create hive transactional table.");
                /*
                    CREATE TABLE trans6(
                      `col1` int,
                      `col2` int
                    )  ENGINE=hive
                    PROPERTIES (
                      'file_format'='orc',
                      'compression'='zlib',
                      'bucketing_version'='2',
                      'transactional'='true',
                      'transactional_properties'='default'
                    );
                    In hive, this table only can insert not update(not report error,but not actually updated).
                 */
            }

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
            Map<String, String> properties = catalog.getProperties();
            if (properties.containsKey(HMSBaseProperties.HIVE_METASTORE_TYPE)
                    && properties.get(HMSBaseProperties.HIVE_METASTORE_TYPE).equals(HMSBaseProperties.DLF_TYPE)) {
                for (Column column : stmt.getColumns()) {
                    if (column.hasDefaultValue()) {
                        throw new UserException("Default values are not supported with `DLF` catalog.");
                    }
                }
            }
            String comment = stmt.getComment();
            Optional<String> location = Optional.ofNullable(props.getOrDefault(LOCATION_URI_KEY, null));
            HiveTableMetadata hiveTableMeta;
            DistributionDesc bucketInfo = stmt.getDistributionDesc();
            if (bucketInfo != null) {
                if (Config.enable_create_hive_bucket_table) {
                    if (bucketInfo instanceof HashDistributionDesc) {
                        hiveTableMeta = HiveTableMetadata.of(db.getRemoteName(),
                                tblName,
                                location,
                                stmt.getColumns(),
                                partitionColNames,
                                bucketInfo.getDistributionColumnNames(),
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
                hiveTableMeta = HiveTableMetadata.of(db.getRemoteName(),
                        tblName,
                        location,
                        stmt.getColumns(),
                        partitionColNames,
                        ddlProps,
                        fileFormat,
                        comment);
            }
            client.createTable(hiveTableMeta, stmt.isSetIfNotExists());
            return false;
        } catch (Exception e) {
            throw new UserException(e.getMessage(), e);
        }
    }

    @Override
    public void afterCreateTable(String dbName, String tblName) {
        Optional<ExternalDatabase<?>> db = catalog.getDbForReplay(dbName);
        if (db.isPresent()) {
            db.get().resetMetaCacheNames();
        }
        LOG.info("after create table {}.{}.{}, is db exists: {}",
                getCatalog().getName(), dbName, tblName, db.isPresent());
    }

    @Override
    public void dropTableImpl(ExternalTable dorisTable, boolean ifExists) throws DdlException {
        if (!tableExist(dorisTable.getRemoteDbName(), dorisTable.getRemoteName())) {
            if (ifExists) {
                LOG.info("drop table[{}] which does not exist", dorisTable.getRemoteDbName());
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TABLE,
                        dorisTable.getRemoteName(), dorisTable.getRemoteDbName());
            }
        }
        if (AcidUtils.isTransactionalTable(client.getTable(dorisTable.getRemoteDbName(), dorisTable.getRemoteName()))) {
            throw new DdlException("Not support drop hive transactional table.");
        }

        try {
            client.dropTable(dorisTable.getRemoteDbName(), dorisTable.getRemoteName());
        } catch (Exception e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    @Override
    public void afterDropTable(String dbName, String tblName) {
        Optional<ExternalDatabase<?>> db = catalog.getDbForReplay(dbName);
        if (db.isPresent()) {
            db.get().unregisterTable(tblName);
        }
        LOG.info("after drop table {}.{}.{}, is db exists: {}",
                getCatalog().getName(), dbName, tblName, db.isPresent());
    }

    @Override
    public void truncateTableImpl(ExternalTable dorisTable, List<String> partitions)
            throws DdlException {
        try {
            client.truncateTable(dorisTable.getRemoteDbName(), dorisTable.getRemoteName(), partitions);
        } catch (Exception e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    @Override
    public void afterTruncateTable(String dbName, String tblName) {
        try {
            // Invalidate cache.
            Optional<ExternalDatabase<?>> db = catalog.getDbForReplay(dbName);
            if (db.isPresent()) {
                Optional tbl = db.get().getTableForReplay(tblName);
                if (tbl.isPresent()) {
                    Env.getCurrentEnv().getRefreshManager()
                            .refreshTableInternal(db.get(), (ExternalTable) tbl.get(), 0);
                }
            }
        } catch (Exception e) {
            LOG.warn("exception when calling afterTruncateTable for db: {}, table: {}, error: {}",
                    dbName, tblName, e.getMessage(), e);
        }
    }

    @Override
    public void createOrReplaceBranchImpl(ExternalTable dorisTable, CreateOrReplaceBranchInfo branchInfo)
            throws UserException {
        throw new UserException("Not support create or replace branch in hive catalog.");
    }

    @Override
    public void createOrReplaceTagImpl(ExternalTable dorisTable, CreateOrReplaceTagInfo tagInfo)
            throws UserException {
        throw new UserException("Not support create or replace tag in hive catalog.");
    }

    @Override
    public void dropTagImpl(ExternalTable dorisTable, DropTagInfo tagInfo) throws UserException {
        throw new UserException("Not support drop tag in hive catalog.");
    }

    @Override
    public void dropBranchImpl(ExternalTable dorisTable, DropBranchInfo branchInfo) throws UserException {
        throw new UserException("Not support drop branch in hive catalog.");
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
        return listDatabaseNames().contains(dbName.toLowerCase());
    }

    @Override
    public void close() {
        client.close();
    }

    public List<String> listDatabaseNames() {
        return client.getAllDatabases();
    }

    public void updateTableStatistics(
            NameMapping nameMapping,
            Function<HivePartitionStatistics, HivePartitionStatistics> update) {
        client.updateTableStatistics(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(), update);
    }

    void updatePartitionStatistics(
            NameMapping nameMapping,
            String partitionName,
            Function<HivePartitionStatistics, HivePartitionStatistics> update) {
        client.updatePartitionStatistics(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(), partitionName,
                update);
    }

    public void addPartitions(NameMapping nameMapping, List<HivePartitionWithStatistics> partitions) {
        client.addPartitions(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(), partitions);
    }

    public void dropPartition(NameMapping nameMapping, List<String> partitionValues, boolean deleteData) {
        client.dropPartition(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(), partitionValues,
                deleteData);
    }
}
