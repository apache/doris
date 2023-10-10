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
//
// Copied from:
// https://github.com/aliyun/datalake-catalog-metastore-client/blob/master/metastore-client-hive/metastore-client-hive2/src/main/java/com/aliyun/datalake/metastore/hive2/ProxyMetaStoreClient.java
// 3c6f5905

package com.aliyun.datalake.metastore.hive2;

import shade.doris.com.aliyun.datalake.metastore.common.DataLakeConfig;
import shade.doris.com.aliyun.datalake.metastore.common.ProxyMode;
import shade.doris.com.aliyun.datalake.metastore.common.Version;
import shade.doris.com.aliyun.datalake.metastore.common.functional.FunctionalUtils;
import shade.doris.com.aliyun.datalake.metastore.common.functional.ThrowingConsumer;
import shade.doris.com.aliyun.datalake.metastore.common.functional.ThrowingFunction;
import shade.doris.com.aliyun.datalake.metastore.common.functional.ThrowingRunnable;
import shade.doris.com.aliyun.datalake.metastore.common.util.DataLakeUtil;
import shade.doris.com.aliyun.datalake.metastore.common.util.ProxyLogUtils;
import shade.doris.com.aliyun.datalake.metastore.hive.common.utils.ClientUtils;
import shade.doris.com.aliyun.datalake.metastore.hive.common.utils.ConfigUtils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.hadoop.hive.ql.session.SessionState;
import shade.doris.com.aliyun.datalake.metastore.hive2.DlfSessionMetaStoreClient;
import shade.doris.hive.org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ProxyMetaStoreClient implements IMetaStoreClient {
    private static final Logger logger = LoggerFactory.getLogger(ProxyMetaStoreClient.class);
    private final static String HIVE_FACTORY_CLASS = "org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClientFactory";

    private final ProxyMode proxyMode;

    // Dlf Client
    private IMetaStoreClient dlfSessionMetaStoreClient;

    // Hive Client
    private IMetaStoreClient hiveSessionMetaStoreClient;

    // ReadWrite Client
    private IMetaStoreClient readWriteClient;

    // Extra Write Client
    private Optional<IMetaStoreClient> extraClient;

    // Allow failure
    private boolean allowFailure = false;

    // copy Hive conf
    private HiveConf hiveConf;

    private final String readWriteClientType;

    public ProxyMetaStoreClient(HiveConf hiveConf) throws MetaException {
        this(hiveConf, null, false);
    }

    // morningman: add this constructor to avoid NoSuchMethod exception
    public ProxyMetaStoreClient(Configuration conf, HiveMetaHookLoader hiveMetaHookLoader, Boolean allowEmbedded)
            throws MetaException {
        this((HiveConf) conf, hiveMetaHookLoader, allowEmbedded);
    }

    public ProxyMetaStoreClient(HiveConf hiveConf, HiveMetaHookLoader hiveMetaHookLoader, Boolean allowEmbedded)
            throws MetaException {
        long startTime = System.currentTimeMillis();
        logger.info("ProxyMetaStoreClient start, datalake-metastore-client-version:{}",
                Version.DATALAKE_METASTORE_CLIENT_VERSION);
        this.hiveConf = new HiveConf(hiveConf);

        proxyMode = ConfigUtils.getProxyMode(hiveConf);

        // init logging if needed
        ProxyLogUtils.initLogUtils(proxyMode, hiveConf.get(DataLakeConfig.CATALOG_PROXY_LOGSTORE,
                        ConfigUtils.getUserId(hiveConf)), hiveConf.getBoolean(DataLakeConfig.CATALOG_ACTION_LOG_ENABLED,
                        DataLakeConfig.DEFAULT_CATALOG_ACTION_LOG_ENABLED),
                hiveConf.getBoolean(DataLakeConfig.CATALOG_LOG_ENABLED, DataLakeConfig.DEFAULT_CATALOG_LOG_ENABLED));

        // init Dlf Client if any
        createClient(true, () -> initDlfClient(hiveConf, hiveMetaHookLoader, allowEmbedded, new ConcurrentHashMap<>()));

        // init Hive Client if any
        createClient(false, () -> initHiveClient(hiveConf, hiveMetaHookLoader, allowEmbedded, new ConcurrentHashMap<>()));

        // init extraClient
        initClientByProxyMode();

        readWriteClientType = this.readWriteClient instanceof DlfSessionMetaStoreClient ? "dlf" : "hive";

        logger.info("ProxyMetaStoreClient end, cost:{}ms", System.currentTimeMillis() - startTime);
    }

    public static Map<String, org.apache.hadoop.hive.ql.metadata.Table> getTempTablesForDatabase(String dbName) {
        return getTempTables().get(dbName);
    }

    public static Map<String, Map<String, org.apache.hadoop.hive.ql.metadata.Table>> getTempTables() {
        SessionState ss = SessionState.get();
        if (ss == null) {
            return Collections.emptyMap();
        }
        return ss.getTempTables();
    }

    public HiveConf getHiveConf() {
        return hiveConf;
    }

    public void initClientByProxyMode() throws MetaException {
        switch (proxyMode) {
            case METASTORE_ONLY:
                this.readWriteClient = hiveSessionMetaStoreClient;
                this.extraClient = Optional.empty();
                break;
            case METASTORE_DLF_FAILURE:
                this.allowFailure = true;
                this.readWriteClient = hiveSessionMetaStoreClient;
                this.extraClient = Optional.ofNullable(dlfSessionMetaStoreClient);
                break;
            case METASTORE_DLF_SUCCESS:
                this.readWriteClient = hiveSessionMetaStoreClient;
                this.extraClient = Optional.of(dlfSessionMetaStoreClient);
                break;
            case DLF_METASTORE_SUCCESS:
                this.readWriteClient = dlfSessionMetaStoreClient;
                this.extraClient = Optional.of(hiveSessionMetaStoreClient);
                break;
            case DLF_METASTORE_FAILURE:
                this.allowFailure = true;
                this.readWriteClient = dlfSessionMetaStoreClient;
                this.extraClient = Optional.ofNullable(hiveSessionMetaStoreClient);
                break;
            case DLF_ONLY:
                this.readWriteClient = dlfSessionMetaStoreClient;
                this.extraClient = Optional.empty();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + proxyMode);
        }
    }

    private void createClient(boolean isDlf, ThrowingRunnable<MetaException> createClient) throws MetaException {
        try {
            createClient.run();
        } catch (Exception e) {
            if ((isDlf && proxyMode == ProxyMode.METASTORE_DLF_FAILURE)) {
                dlfSessionMetaStoreClient = null;
            } else if (!isDlf && proxyMode == ProxyMode.DLF_METASTORE_FAILURE) {
                hiveSessionMetaStoreClient = null;
            } else {
                throw DataLakeUtil.throwException(new MetaException(e.getMessage()), e);
            }
        }
    }

    public void initHiveClient(HiveConf hiveConf, HiveMetaHookLoader hiveMetaHookLoader, boolean allowEmbedded,
            ConcurrentHashMap<String, Long> metaCallTimeMap) throws MetaException {
        switch (proxyMode) {
            case METASTORE_ONLY:
            case METASTORE_DLF_FAILURE:
            case METASTORE_DLF_SUCCESS:
            case DLF_METASTORE_SUCCESS:
            case DLF_METASTORE_FAILURE:
                this.hiveSessionMetaStoreClient = ClientUtils.createMetaStoreClient(HIVE_FACTORY_CLASS,
                        hiveConf, hiveMetaHookLoader, allowEmbedded, metaCallTimeMap);
                break;
            case DLF_ONLY:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + proxyMode);
        }
    }

    public void initDlfClient(HiveConf hiveConf, HiveMetaHookLoader hiveMetaHookLoader, boolean allowEmbedded,
            ConcurrentHashMap<String, Long> metaCallTimeMap) throws MetaException {
        switch (proxyMode) {
            case METASTORE_ONLY:
                break;
            case METASTORE_DLF_FAILURE:
            case METASTORE_DLF_SUCCESS:
            case DLF_METASTORE_SUCCESS:
            case DLF_METASTORE_FAILURE:
            case DLF_ONLY:
                this.dlfSessionMetaStoreClient = new DlfSessionMetaStoreClient(hiveConf, hiveMetaHookLoader, allowEmbedded);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + proxyMode);
        }
    }

    @Override
    public boolean isCompatibleWith(Configuration conf) {
        try {
            return call(this.readWriteClient, client -> client.isCompatibleWith(conf), "isCompatibleWith", conf);
        } catch (TException e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public void setHiveAddedJars(String s) {
        try {
            run(client -> client.setHiveAddedJars(s), "setHiveAddedJars", s);
        } catch (TException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public boolean isLocalMetaStore() {
        return !extraClient.isPresent() && readWriteClient.isLocalMetaStore();
    }

    @Override
    public void reconnect() throws MetaException {
        if (hiveSessionMetaStoreClient != null) {
            hiveSessionMetaStoreClient.reconnect();
        }
    }

    @Override
    public void close() {
        if (hiveSessionMetaStoreClient != null) {
            hiveSessionMetaStoreClient.close();
        }
    }

    @Override
    public void createDatabase(Database database)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        run(client -> client.createDatabase(database), "createDatabase", database);
    }

    @Override
    public Database getDatabase(String name) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getDatabase(name), "getDatabase", name);
    }

    @Override
    public Database getDatabase(String catalogId, String databaseName) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getDatabase(catalogId, databaseName), "getDatabase", catalogId, databaseName);
    }

    @Override
    public List<String> getDatabases(String pattern) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getDatabases(pattern), "getDatabases", pattern);
    }

    @Override
    public List<String> getDatabases(String catalogId, String databasePattern) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getDatabases(catalogId, databasePattern), "getDatabases", catalogId, databasePattern);
    }

    @Override
    public List<String> getAllDatabases() throws MetaException, TException {
        return getDatabases(".*");
    }

    @Override
    public List<String> getAllDatabases(String catalogId) throws MetaException, TException {
        return getDatabases(catalogId);
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
            throws NoSuchObjectException, MetaException, TException {
        run(client -> client.alterDatabase(databaseName, database), "alterDatabase", databaseName, database);
    }

    @Override
    public void alterDatabase(String catalogId, String databaseName, Database database) throws NoSuchObjectException, MetaException, TException {
        run(client -> client.alterDatabase(catalogId, databaseName, database), "alterDatabase", catalogId, databaseName, database);
    }

    @Override
    public void dropDatabase(String name)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        dropDatabase(name, true, false, false);
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        dropDatabase(name, deleteData, ignoreUnknownDb, false);
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        run(client -> client.dropDatabase(name, deleteData, ignoreUnknownDb, cascade), "dropDatabase", name, deleteData,
                ignoreUnknownDb, cascade);
    }

    @Override
    public void dropDatabase(String catalogId, String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        run(client -> client.dropDatabase(catalogId, name, deleteData, ignoreUnknownDb, cascade), "dropDatabase", catalogId, name, deleteData, ignoreUnknownDb, cascade);
    }

    @Override
    public Partition add_partition(Partition partition)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return call(client -> client.add_partition(partition), "add_partition", partition);
    }

    @Override
    public int add_partitions(List<Partition> partitions)
            throws InvalidObjectException, AlreadyExistsException, MetaException,
            TException {
        return call(client -> client.add_partitions(partitions), "add_partitions", partitions);
    }

    @Override
    public List<Partition> add_partitions(
            List<Partition> partitions,
            boolean ifNotExists,
            boolean needResult
    ) throws TException {
        return call(client -> client.add_partitions(partitions, ifNotExists, needResult), "add_partitions", partitions, ifNotExists, needResult);
    }

    @Override
    public int add_partitions_pspec(PartitionSpecProxy pSpec)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return call(client -> client.add_partitions_pspec(pSpec), "add_partitions_pspec", pSpec);
    }

    @Override
    public void alterFunction(String dbName, String functionName, Function newFunction)
            throws InvalidObjectException, MetaException, TException {
        run(client -> client.alterFunction(dbName, functionName, newFunction), "alterFunction", dbName, functionName, newFunction);
    }

    @Override
    public void alterFunction(String catalogId, String dbName, String functionName, Function newFunction) throws InvalidObjectException, MetaException, TException {
        run(client -> client.alterFunction(catalogId, dbName, functionName, newFunction), "alterFunction", catalogId, dbName, functionName, newFunction);
    }

    @Override
    public void alter_partition(String dbName, String tblName, Partition partition)
            throws InvalidOperationException, MetaException, TException {
        run(client -> client.alter_partition(dbName, tblName, partition), "alter_partition", dbName, tblName, partition);
    }

    @Override
    public void alter_partition(
            String dbName,
            String tblName,
            Partition partition,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
        run(client -> client.alter_partition(dbName, tblName, partition, environmentContext), "alter_partition", dbName, tblName, partition, environmentContext);
    }

    @Override
    public void alter_partition(String catalogId, String dbName, String tblName, Partition partition, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
        run(client -> client.alter_partition(catalogId, dbName, tblName, partition, environmentContext), "alter_partition", catalogId, dbName, tblName, partition, environmentContext);
    }

    @Override
    public void alter_partitions(
            String dbName,
            String tblName,
            List<Partition> partitions
    ) throws InvalidOperationException, MetaException, TException {
        run(client -> client.alter_partitions(dbName, tblName, partitions), "alter_partitions", dbName, tblName, partitions);
    }

    @Override
    public void alter_partitions(
            String dbName,
            String tblName,
            List<Partition> partitions,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
        run(client -> client.alter_partitions(dbName, tblName, partitions, environmentContext), "alter_partitions", dbName, tblName, partitions, environmentContext);
    }

    @Override
    public void alter_partitions(String catalogId, String dbName, String tblName, List<Partition> partitions, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
        run(client -> client.alter_partitions(catalogId, dbName, tblName, partitions, environmentContext), "alter_partitions", catalogId, dbName, tblName, partitions, environmentContext);
    }

    @Override
    public void alter_table(String dbName, String tblName, Table table)
            throws InvalidOperationException, MetaException, TException {
        if (table.isTemporary()) {
            run(this.readWriteClient, client -> client.alter_table(dbName, tblName, table), "alter_table", dbName, tblName, table);
        } else {
            run(client -> client.alter_table(dbName, tblName, table), "alter_table", dbName, tblName, table);
        }
    }

    @Override
    public void alter_table(String catalogId, String dbName, String tblName, Table table, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
        if (table.isTemporary()) {
            run(this.readWriteClient, client -> client.alter_table(catalogId, dbName, tblName, table, environmentContext), "alter_table", catalogId, dbName, tblName, table, environmentContext);
        } else {
            run(client -> client.alter_table(catalogId, dbName, tblName, table, environmentContext), "alter_table", catalogId, dbName, tblName, table, environmentContext);
        }
    }

    @Override
    @Deprecated
    public void alter_table(
            String dbName,
            String tblName,
            Table table,
            boolean cascade
    ) throws InvalidOperationException, MetaException, TException {
        if (table.isTemporary()) {
            run(this.readWriteClient, client -> client.alter_table(dbName, tblName, table, cascade), "alter_table", dbName, tblName, table, cascade);
        } else {
            run(client -> client.alter_table(dbName, tblName, table, cascade), "alter_table", dbName, tblName, table, cascade);
        }
    }

    @Override
    public void alter_table_with_environmentContext(
            String dbName,
            String tblName,
            Table table,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
        if (table.isTemporary()) {
            run(this.readWriteClient, client -> client.alter_table_with_environmentContext(dbName, tblName, table, environmentContext), "alter_table_with_environmentContext", dbName, tblName, table, environmentContext);
        } else {
            run(client -> client.alter_table_with_environmentContext(dbName, tblName, table, environmentContext), "alter_table_with_environmentContext", dbName,
                    tblName, table, environmentContext);
        }
    }

    @Override
    public Partition appendPartition(String dbName, String tblName, List<String> values)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return call(client -> client.appendPartition(dbName, tblName, values), "appendPartition", dbName, tblName, values);
    }

    @Override
    public Partition appendPartition(String catalogId, String dbName, String tblName, List<String> values) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return call(client -> client.appendPartition(catalogId, dbName, tblName, values), "appendPartition", catalogId, dbName, tblName, values);
    }

    @Override
    public Partition appendPartition(String dbName, String tblName, String partitionName)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return call(client -> client.appendPartition(dbName, tblName, partitionName), "appendPartition", dbName, tblName, partitionName);
    }

    @Override
    public Partition appendPartition(String catalogId, String dbName, String tblName, String partitionName) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return call(client -> client.appendPartition(catalogId, dbName, tblName, partitionName), "appendPartition", catalogId, dbName, tblName, partitionName);
    }

    @Override
    public boolean create_role(Role role) throws MetaException, TException {
        return call(client -> client.create_role(role), "create_role", role);
    }

    @Override
    public boolean drop_role(String roleName) throws MetaException, TException {
        return call(client -> client.drop_role(roleName), "drop_role", roleName);
    }

    @Override
    public List<Role> list_roles(
            String principalName, PrincipalType principalType
    ) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.list_roles(principalName, principalType), "list_roles", principalName, principalType);
    }

    @Override
    public List<String> listRoleNames() throws MetaException, TException {
        return call(this.readWriteClient, client -> client.listRoleNames(), "listRoleNames");
    }

    @Override
    public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest request)
            throws MetaException, TException {
        return call(this.readWriteClient, client -> client.get_principals_in_role(request), "get_principals_in_role", request);
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
            GetRoleGrantsForPrincipalRequest request) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.get_role_grants_for_principal(request), "get_role_grants_for_principal", request);
    }

    @Override
    public boolean grant_role(
            String roleName,
            String userName,
            PrincipalType principalType,
            String grantor,
            PrincipalType grantorType,
            boolean grantOption
    ) throws MetaException, TException {
        return call(client -> client.grant_role(roleName, userName, principalType, grantor, grantorType, grantOption)
                , "grant_role", roleName, userName, principalType, grantor, grantorType);
    }

    @Override
    public boolean revoke_role(
            String roleName,
            String userName,
            PrincipalType principalType,
            boolean grantOption
    ) throws MetaException, TException {
        return call(client -> client.revoke_role(roleName, userName, principalType, grantOption), "revoke_role", roleName, userName,
                principalType, grantOption);
    }

    @Override
    public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
        run(client -> client.cancelDelegationToken(tokenStrForm), "cancelDelegationToken", tokenStrForm);
    }

    @Override
    public String getTokenStrForm() throws IOException {
        try {
            return call(this.readWriteClient, client -> {
                try {
                    return client.getTokenStrForm();
                } catch (IOException e) {
                    throw new TException(e.getMessage(), e);
                }
            }, "getTokenStrForm");
        } catch (TException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
        return call(client -> client.addToken(tokenIdentifier, delegationToken), "addToken", tokenIdentifier, delegationToken);
    }

    @Override
    public boolean removeToken(String tokenIdentifier) throws TException {
        return call(client -> client.removeToken(tokenIdentifier), "removeToken", tokenIdentifier);
    }

    @Override
    public String getToken(String tokenIdentifier) throws TException {
        return call(this.readWriteClient, client -> client.getToken(tokenIdentifier), "getToken", tokenIdentifier);
    }

    @Override
    public List<String> getAllTokenIdentifiers() throws TException {
        return call(this.readWriteClient, client -> client.getAllTokenIdentifiers(), "getAllTokenIdentifiers");
    }

    @Override
    public int addMasterKey(String key) throws MetaException, TException {
        return call(client -> client.addMasterKey(key), "addMasterKey", key);
    }

    @Override
    public void updateMasterKey(Integer seqNo, String key)
            throws NoSuchObjectException, MetaException, TException {
        run(client -> client.updateMasterKey(seqNo, key), "updateMasterKey", key);
    }

    @Override
    public boolean removeMasterKey(Integer keySeq) throws TException {
        return call(client -> client.removeMasterKey(keySeq), "removeMasterKey", keySeq);
    }

    @Override
    public String[] getMasterKeys() throws TException {
        return call(this.readWriteClient, client -> client.getMasterKeys(), "getMasterKeys");
    }

    @Override
    public LockResponse checkLock(long lockId)
            throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
        return call(this.readWriteClient, client -> client.checkLock(lockId), "checkLock", lockId);
    }

    @Override
    public void commitTxn(long txnId) throws NoSuchTxnException, TxnAbortedException, TException {
        run(client -> client.commitTxn(txnId), "commitTxn", txnId);
    }

    @Override
    public void replCommitTxn(long srcTxnId, String replPolicy) throws NoSuchTxnException, TxnAbortedException, TException {
        run(client -> client.replCommitTxn(srcTxnId, replPolicy), "replCommitTxn", srcTxnId, replPolicy);
    }

    @Override
    public void abortTxns(List<Long> txnIds) throws TException {
        run(client -> client.abortTxns(txnIds), "abortTxns", txnIds);
    }

    @Override
    public long allocateTableWriteId(long txnId, String dbName, String tableName) throws TException {
        return call(client -> client.allocateTableWriteId(txnId, dbName, tableName), "allocateTableWriteId", txnId, dbName, tableName);
    }

    @Override
    public void replTableWriteIdState(String validWriteIdList, String dbName, String tableName, List<String> partNames) throws TException {
        run(client -> client.replTableWriteIdState(validWriteIdList, dbName, tableName, partNames), "replTableWriteIdState", validWriteIdList, dbName, tableName, partNames);
    }

    @Override
    public List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> txnIds, String dbName, String tableName) throws TException {
        return call(client -> client.allocateTableWriteIdsBatch(txnIds, dbName, tableName), "allocateTableWriteIdsBatch", txnIds, dbName, tableName);
    }

    @Override
    public List<TxnToWriteId> replAllocateTableWriteIdsBatch(String dbName, String tableName,
            String replPolicy, List<TxnToWriteId> srcTxnToWriteIdList) throws TException {
        return call(client -> client.replAllocateTableWriteIdsBatch(dbName, tableName, replPolicy, srcTxnToWriteIdList), "replAllocateTableWriteIdsBatch", dbName, tableName, replPolicy, srcTxnToWriteIdList);
    }

    @Override
    @Deprecated
    public void compact(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType
    ) throws TException {
        run(client -> client.compact(dbName, tblName, partitionName, compactionType), "compact", dbName, tblName, partitionName, compactionType);
    }

    @Override
    @Deprecated
    public void compact(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType,
            Map<String, String> tblProperties
    ) throws TException {
        run(client -> client.compact(dbName, tblName, partitionName, compactionType, tblProperties), "compact", dbName, tblName, partitionName, compactionType, tblProperties);
    }

    @Override
    public CompactionResponse compact2(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType,
            Map<String, String> tblProperties
    ) throws TException {
        return call(client -> client.compact2(dbName, tblName, partitionName, compactionType, tblProperties), "compact2", dbName, tblName, partitionName, compactionType, tblProperties);
    }

    @Override
    public void createFunction(Function function) throws InvalidObjectException, MetaException, TException {
        run(client -> client.createFunction(function), "createFunction", function);
    }

    @Override
    public void createTable(Table tbl)
            throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        createTable(tbl, null);
    }

    public void createTable(Table tbl, EnvironmentContext envContext) throws AlreadyExistsException,
            InvalidObjectException, MetaException, NoSuchObjectException, TException {
        // Subclasses can override this step (for example, for temporary tables)
        if (tbl.isTemporary()) {
            run(this.readWriteClient, client -> client.createTable(tbl), "createTable", tbl);
        } else {
            run(client -> client.createTable(tbl), "createTable", tbl);
        }
    }

    @Override
    public boolean deletePartitionColumnStatistics(
            String dbName, String tableName, String partName, String colName
    ) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return call(client -> client.deletePartitionColumnStatistics(dbName, tableName, partName, colName), "deletePartitionColumnStatistics", dbName,
                tableName, partName, colName);
    }

    @Override
    public boolean deletePartitionColumnStatistics(String catalogId, String dbName, String tableName, String partName, String colName) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return call(client -> client.deletePartitionColumnStatistics(catalogId, dbName, tableName, partName, colName), "deletePartitionColumnStatistics", catalogId, dbName, tableName, partName, colName);
    }

    @Override
    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException,
            TException, InvalidInputException {
        if (getTempTable(dbName, tableName) != null) {
            return call(this.readWriteClient, client -> client.deleteTableColumnStatistics(dbName, tableName, colName), "deleteTableColumnStatistics", dbName, tableName, colName);
        } else {
            return call(client -> client.deleteTableColumnStatistics(dbName, tableName, colName), "deleteTableColumnStatistics", dbName, tableName, colName);
        }
    }

    @Override
    public boolean deleteTableColumnStatistics(String catalogId, String dbName, String tableName, String colName) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        if (getTempTable(dbName, tableName) != null) {
            return call(this.readWriteClient, client -> client.deleteTableColumnStatistics(catalogId, dbName, tableName, colName), "deleteTableColumnStatistics", catalogId, dbName, tableName, colName);
        } else {
            return call(client -> client.deleteTableColumnStatistics(catalogId, dbName, tableName, colName), "deleteTableColumnStatistics", catalogId, dbName, tableName, colName);
        }
    }

    @Override
    public void dropFunction(String dbName, String functionName) throws MetaException, NoSuchObjectException,
            InvalidObjectException, InvalidInputException, TException {
        run(client -> client.dropFunction(dbName, functionName), "dropFunction", dbName, functionName);
    }

    @Override
    public void dropFunction(String catalogId, String dbName, String functionName) throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
        run(client -> client.dropFunction(catalogId, dbName, functionName), "dropFunction", catalogId, dbName, functionName);
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, List<String> values, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        return call(client -> client.dropPartition(dbName, tblName, values, deleteData), "dropPartition", dbName, tblName, values, deleteData);
    }

    @Override
    public boolean dropPartition(String catalogId, String dbName, String tblName, List<String> values, boolean deleteData) throws NoSuchObjectException, MetaException, TException {
        return call(client -> client.dropPartition(catalogId, dbName, tblName, values, deleteData), "dropPartition", catalogId, dbName, tblName, values, deleteData);
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, List<String> values, PartitionDropOptions options)
            throws TException {
        return call(client -> client.dropPartition(dbName, tblName, values, options), "dropPartition", dbName, tblName, values, options);
    }

    @Override
    public boolean dropPartition(String catalogId, String dbName, String tblName, List<String> values, PartitionDropOptions options) throws NoSuchObjectException, MetaException, TException {
        return call(client -> client.dropPartition(catalogId, dbName, tblName, values, options), "dropPartition", catalogId, dbName, tblName, values, options);
    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName,
            List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
            boolean ifExists) throws NoSuchObjectException, MetaException, TException {
        return call(client -> client.dropPartitions(dbName, tblName, partExprs, deleteData, ifExists), "dropPartitions", dbName, tblName, partExprs, deleteData, ifExists);
    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName,
            List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
            boolean ifExists, boolean needResult) throws NoSuchObjectException, MetaException, TException {
        return call(client -> client.dropPartitions(dbName, tblName, partExprs, deleteData, ifExists, needResult), "dropPartitions", dbName, tblName, partExprs, deleteData, ifExists, needResult);
    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, PartitionDropOptions partitionDropOptions) throws NoSuchObjectException, MetaException, TException {
        return call(client -> client.dropPartitions(dbName, tblName, partExprs, partitionDropOptions), "dropPartitions", dbName, tblName, partExprs, partitionDropOptions);
    }

    @Override
    public List<Partition> dropPartitions(String catalogId, String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, PartitionDropOptions partitionDropOptions) throws NoSuchObjectException, MetaException, TException {
        return call(client -> client.dropPartitions(catalogId, dbName, tblName, partExprs, partitionDropOptions), "dropPartitions", catalogId, dbName, tblName, partExprs, partitionDropOptions);
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, String partitionName, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        return call(client -> client.dropPartition(dbName, tblName, partitionName, deleteData), "dropPartition", dbName, tblName,
                partitionName, deleteData);
    }

    @Override
    public boolean dropPartition(String catName, String dbName, String tblName, String partitionName,
            boolean deleteData) throws NoSuchObjectException, MetaException, TException {
        return call(client -> client.dropPartition(catName, dbName, tblName, partitionName, deleteData), "dropPartition", catName, dbName, tblName, partitionName, deleteData);
    }

    private Table getTempTable(String dbName, String tableName) {
        Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables = getTempTablesForDatabase(dbName.toLowerCase());
        if (tables != null) {
            org.apache.hadoop.hive.ql.metadata.Table table = tables.get(tableName.toLowerCase());
            if (table != null) {
                return table.getTTable();
            }
        }
        return null;
    }

    @Override
    public void dropTable(String dbname, String tableName)
            throws MetaException, TException, NoSuchObjectException {
        Table table = getTempTable(dbname, tableName);
        if (table != null) {
            run(this.readWriteClient, client -> client.dropTable(dbname, tableName), "dropTable", dbname, tableName);
        } else {
            run(client -> client.dropTable(dbname, tableName), "dropTable", dbname, tableName);
        }
    }

    @Override
    public void dropTable(String catalogId, String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge) throws MetaException, NoSuchObjectException, TException {
        Table table = getTempTable(dbname, tableName);
        if (table != null) {
            run(this.readWriteClient, client -> client.dropTable(catalogId, dbname, tableName, deleteData, ignoreUnknownTab, ifPurge), "dropTable", catalogId, dbname, tableName, deleteData, ignoreUnknownTab, ifPurge);
        } else {
            run(client -> client.dropTable(catalogId, dbname, tableName, deleteData, ignoreUnknownTab, ifPurge), "dropTable", catalogId, dbname, tableName, deleteData, ignoreUnknownTab, ifPurge);
        }
    }

    @Override
    public void truncateTable(String dbName, String tableName, List<String> partNames) throws MetaException, TException {
        run(client -> client.truncateTable(dbName, tableName, partNames), "truncateTable", dbName, tableName, partNames);
    }

    @Override
    public void truncateTable(String catName, String dbName, String tableName, List<String> partNames) throws MetaException, TException {
        run(client -> client.truncateTable(catName, dbName, tableName, partNames), "truncateTable", catName, dbName, tableName, partNames);
    }

    @Override
    public CmRecycleResponse recycleDirToCmPath(CmRecycleRequest cmRecycleRequest) throws MetaException, TException {
        return call(client -> client.recycleDirToCmPath(cmRecycleRequest), "recycleDirToCmPath", cmRecycleRequest);
    }

    @Override
    public void dropTable(
            String dbname,
            String tableName,
            boolean deleteData,
            boolean ignoreUnknownTab
    ) throws MetaException, TException, NoSuchObjectException {
        Table table = getTempTable(dbname, tableName);
        if (table != null) {
            run(this.readWriteClient, client -> client.dropTable(dbname, tableName, deleteData, ignoreUnknownTab), "dropTable", dbname, tableName, deleteData, ignoreUnknownTab);
        } else {
            run(client -> client.dropTable(dbname, tableName, deleteData, ignoreUnknownTab), "dropTable", dbname, tableName, deleteData, ignoreUnknownTab);
        }
    }

    @Override
    public void dropTable(
            String dbname,
            String tableName,
            boolean deleteData,
            boolean ignoreUnknownTable,
            boolean ifPurge
    ) throws MetaException, TException, NoSuchObjectException {
        Table table = getTempTable(dbname, tableName);
        if (table != null) {
            run(this.readWriteClient, client -> client.dropTable(dbname, tableName, deleteData, ignoreUnknownTable, ifPurge), "dropTable", dbname, tableName, deleteData, ignoreUnknownTable, ifPurge);
        } else {
            run(client -> client.dropTable(dbname, tableName, deleteData, ignoreUnknownTable, ifPurge), "dropTable", dbname, tableName, deleteData, ignoreUnknownTable, ifPurge);
        }
    }

    @Override
    public Partition exchange_partition(
            Map<String, String> partitionSpecs,
            String srcDb,
            String srcTbl,
            String dstDb,
            String dstTbl
    ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return call(client -> client.exchange_partition(partitionSpecs, srcDb, srcTbl, dstDb, dstTbl), "exchange_partition", partitionSpecs
                , srcDb, srcTbl, dstDb, dstTbl);
    }

    @Override
    public Partition exchange_partition(Map<String, String> partitionSpecs, String srcCatalogId, String srcDb, String srcTbl, String descCatalogId, String dstDb, String dstTbl) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return call(client -> client.exchange_partition(partitionSpecs, srcCatalogId, srcDb, srcTbl, descCatalogId, dstDb, dstTbl), "exchange_partition", partitionSpecs, srcCatalogId, srcDb, srcTbl, descCatalogId, dstDb, dstTbl);
    }

    @Override
    public List<Partition> exchange_partitions(
            Map<String, String> partitionSpecs,
            String sourceDb,
            String sourceTbl,
            String destDb,
            String destTbl
    ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return call(client -> client.exchange_partitions(partitionSpecs, sourceDb, sourceTbl, destDb, destTbl), "exchange_partitions",
                partitionSpecs, sourceDb, sourceTbl, destDb, destTbl);
    }

    @Override
    public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String srcCatalogId, String sourceDb, String sourceTbl, String dstCatalogId, String destDb, String destTbl) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return call(client -> client.exchange_partitions(partitionSpecs, srcCatalogId, sourceDb, sourceTbl, dstCatalogId, destDb, destTbl), "exchange_partitions", partitionSpecs, srcCatalogId, sourceDb, sourceTbl, dstCatalogId, destDb, destTbl);
    }

    @Override
    public AggrStats getAggrColStatsFor(
            String dbName,
            String tblName,
            List<String> colNames,
            List<String> partName
    ) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getAggrColStatsFor(dbName, tblName, colNames, partName), "getAggrColStatsFor", dbName, tblName, colNames, partName);
    }

    @Override
    public AggrStats getAggrColStatsFor(
            String catalogId,
            String dbName,
            String tblName,
            List<String> colNames,
            List<String> partName
    ) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getAggrColStatsFor(catalogId, dbName, tblName, colNames, partName), "getAggrColStatsFor", catalogId, dbName, tblName, colNames, partName);
    }

    @Override
    public List<String> getAllTables(String dbname)
            throws MetaException, TException, UnknownDBException {
        return getTables(dbname, ".*");
    }

    @Override
    public List<String> getAllTables(String catalogId, String dbName) throws MetaException, TException, UnknownDBException {
        return getTables(catalogId, dbName);
    }

    @Override
    public String getConfigValue(String name, String defaultValue)
            throws TException, ConfigValSecurityException {
        return call(this.readWriteClient, client -> client.getConfigValue(name, defaultValue), "getConfigValue", name, defaultValue);
    }

    @Override
    public String getDelegationToken(String owner, String renewerKerberosPrincipalName)
            throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getDelegationToken(owner, renewerKerberosPrincipalName), "getDelegationToken", owner, renewerKerberosPrincipalName);
    }

    @Override
    public List<FieldSchema> getFields(String db, String tableName) throws TException {
        return call(this.readWriteClient, client -> client.getFields(db, tableName), "getFields", db, tableName);
    }

    @Override
    public List<FieldSchema> getFields(String catalogId, String db, String tableName) throws MetaException, TException, UnknownTableException, UnknownDBException {
        return call(this.readWriteClient, client -> client.getFields(catalogId, db, tableName), "getFields", catalogId, db, tableName);
    }

    @Override
    public Function getFunction(String dbName, String functionName) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getFunction(dbName, functionName), "getFunction", dbName, functionName);
    }

    @Override
    public Function getFunction(String catalogId, String dbName, String funcName) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getFunction(catalogId, dbName, funcName), "getFunction", catalogId, dbName, funcName);
    }

    @Override
    public List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getFunctions(dbName, pattern), "getFunctions", dbName, pattern);
    }

    @Override
    public List<String> getFunctions(String catalogId, String dbName, String pattern) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getFunctions(catalogId, dbName, pattern), "getFunctions", catalogId, dbName, pattern);
    }

    @Override
    public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getAllFunctions(), "getAllFunctions");
    }

    @Override
    public String getMetaConf(String key) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getMetaConf(key), "getMetaConf", key);
    }

    @Override
    public void createCatalog(Catalog catalog) throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
        run(client -> client.createCatalog(catalog), "createCatalog", catalog);
    }

    @Override
    public void alterCatalog(String catalogName, Catalog newCatalog) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        run(client -> client.alterCatalog(catalogName, newCatalog), "alterCatalog", catalogName, newCatalog);
    }

    @Override
    public Catalog getCatalog(String catalogId) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getCatalog(catalogId), "getCatalog", catalogId);
    }

    @Override
    public List<String> getCatalogs() throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getCatalogs(), "getCatalogs");
    }

    @Override
    public void dropCatalog(String catalogId) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        run(client -> client.dropCatalog(catalogId), "dropCatalog", catalogId);
    }

    @Override
    public Partition getPartition(String dbName, String tblName, List<String> values)
            throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getPartition(dbName, tblName, values), "getPartition", dbName, tblName, values);
    }

    @Override
    public Partition getPartition(String catalogId, String dbName, String tblName, List<String> values) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getPartition(catalogId, dbName, tblName, values), "getPartition", catalogId, dbName, tblName, values);
    }

    @Override
    public Partition getPartition(String dbName, String tblName, String partitionName)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getPartition(dbName, tblName, partitionName), "getPartition", dbName, tblName, partitionName);
    }

    @Override
    public Partition getPartition(String catalogId, String dbName, String tblName, String partitionName) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getPartition(catalogId, dbName, tblName, partitionName), "getPartition", catalogId, dbName, tblName, partitionName);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName,
            String tableName,
            List<String> partitionNames,
            List<String> columnNames
    ) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getPartitionColumnStatistics(dbName, tableName, partitionNames, columnNames), "getPartitionColumnStatistics", dbName, tableName, partitionNames, columnNames);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String catalogId,
            String dbName,
            String tableName,
            List<String> partitionNames,
            List<String> columnNames
    ) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getPartitionColumnStatistics(catalogId, dbName, tableName, partitionNames, columnNames), "getPartitionColumnStatistics", catalogId, dbName, tableName, partitionNames, columnNames);
    }

    @Override
    public Partition getPartitionWithAuthInfo(
            String databaseName,
            String tableName,
            List<String> values,
            String userName,
            List<String> groupNames
    ) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getPartitionWithAuthInfo(databaseName, tableName, values, userName, groupNames), "getPartitionWithAuthInfo", databaseName, tableName, values, userName, groupNames);
    }

    @Override
    public Partition getPartitionWithAuthInfo(
            String catalogId,
            String databaseName,
            String tableName,
            List<String> values,
            String userName,
            List<String> groupNames) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getPartitionWithAuthInfo(catalogId, databaseName, tableName, values, userName, groupNames), "getPartitionWithAuthInfo", catalogId, databaseName, tableName, values, userName, groupNames);
    }

    @Override
    public List<Partition> getPartitionsByNames(
            String databaseName,
            String tableName,
            List<String> partitionNames
    ) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getPartitionsByNames(databaseName, tableName, partitionNames), "getPartitionsByNames", databaseName, tableName, partitionNames);
    }

    @Override
    public List<Partition> getPartitionsByNames(
            String catalogId,
            String databaseName,
            String tableName,
            List<String> partitionNames
    ) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getPartitionsByNames(catalogId, databaseName, tableName, partitionNames), "getPartitionsByNames", catalogId, databaseName, tableName, partitionNames);
    }

    @Override
    public List<FieldSchema> getSchema(String db, String tableName) throws TException {
        return call(this.readWriteClient, client -> client.getSchema(db, tableName), "getSchema", db, tableName);
    }

    @Override
    public List<FieldSchema> getSchema(String catalogId, String db, String tableName) throws MetaException, TException, UnknownTableException, UnknownDBException {
        return call(this.readWriteClient, client -> client.getSchema(catalogId, db, tableName), "getSchema", catalogId, db, tableName);
    }

    @Override
    public Table getTable(String dbName, String tableName)
            throws MetaException, TException, NoSuchObjectException {
        return call(this.readWriteClient, client -> client.getTable(dbName, tableName), "getTable", dbName, tableName);
    }

    @Override
    public Table getTable(String catalogId, String dbName, String tableName) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getTable(catalogId, dbName, tableName), "getTable", catalogId, dbName, tableName);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(
            String dbName,
            String tableName,
            List<String> colNames
    ) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getTableColumnStatistics(dbName, tableName, colNames), "getTableColumnStatistics", dbName, tableName, colNames);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(
            String catalogId,
            String dbName,
            String tableName,
            List<String> colNames
    ) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getTableColumnStatistics(catalogId, dbName, tableName, colNames), "getTableColumnStatistics", catalogId, dbName, tableName, colNames);
    }

    @Override
    public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return call(this.readWriteClient, client -> client.getTableObjectsByName(dbName, tableNames), "getTableObjectsByName", dbName, tableNames);
    }

    @Override
    public List<Table> getTableObjectsByName(String catalogId, String dbName, List<String> tableNames) throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return call(this.readWriteClient, client -> client.getTableObjectsByName(catalogId, dbName, tableNames), "getTableObjectsByName", catalogId, dbName, tableNames);
    }

    @Override
    public Materialization getMaterializationInvalidationInfo(CreationMetadata creationMetadata, String validTxnList) throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return call(this.readWriteClient, client -> client.getMaterializationInvalidationInfo(creationMetadata, validTxnList), "getMaterializationInvalidationInfo", creationMetadata, validTxnList);
    }

    @Override
    public void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm) throws MetaException, TException {
        run(client -> client.updateCreationMetadata(dbName, tableName, cm), "updateCreationMetadata", dbName, tableName, cm);
    }

    @Override
    public void updateCreationMetadata(String catalogId, String dbName, String tableName, CreationMetadata cm) throws MetaException, TException {
        run(client -> client.updateCreationMetadata(catalogId, dbName, tableName, cm), "updateCreationMetadata", catalogId, dbName, tableName, cm);
    }

    @Override
    public List<String> getTables(String dbname, String tablePattern)
            throws MetaException, TException, UnknownDBException {
        return call(this.readWriteClient, client -> client.getTables(dbname, tablePattern), "getTables", dbname, tablePattern);
    }

    @Override
    public List<String> getTables(String catalogId, String dbname, String tablePattern) throws MetaException, TException, UnknownDBException {
        return call(this.readWriteClient, client -> client.getTables(catalogId, dbname, tablePattern), "getTables", catalogId, dbname, tablePattern);
    }

    @Override
    public List<String> getTables(String dbname, String tablePattern, TableType tableType)
            throws MetaException, TException, UnknownDBException {
        return call(this.readWriteClient, client -> client.getTables(dbname, tablePattern, tableType), "getTables", dbname, tablePattern, tableType);
    }

    @Override
    public List<String> getTables(String catalogId, String dbname, String tablePattern, TableType tableType) throws MetaException, TException, UnknownDBException {
        return call(this.readWriteClient, client -> client.getTables(catalogId, dbname, tablePattern, tableType), "getTables", catalogId, dbname, tablePattern, tableType);
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String dbName) throws MetaException, TException, UnknownDBException {
        return call(this.readWriteClient, client -> client.getMaterializedViewsForRewriting(dbName), "getMaterializedViewsForRewriting", dbName);
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String catalogId, String dbName) throws MetaException, TException, UnknownDBException {
        return call(this.readWriteClient, client -> client.getMaterializedViewsForRewriting(catalogId, dbName), "getMaterializedViewsForRewriting", catalogId, dbName);
    }

    @Override
    public List<TableMeta> getTableMeta(
            String dbPatterns,
            String tablePatterns,
            List<String> tableTypes
    ) throws MetaException, TException, UnknownDBException, UnsupportedOperationException {
        return call(this.readWriteClient, client -> client.getTableMeta(dbPatterns, tablePatterns, tableTypes), "getTableMeta", dbPatterns, tablePatterns, tableTypes);
    }

    @Override
    public List<TableMeta> getTableMeta(String catalogId, String dbPatterns, String tablePatterns, List<String> tableTypes) throws MetaException, TException, UnknownDBException {
        return call(this.readWriteClient, client -> client.getTableMeta(catalogId, dbPatterns, tablePatterns, tableTypes), "getTableMeta", catalogId, dbPatterns, tablePatterns, tableTypes);
    }

    @Override
    public ValidTxnList getValidTxns() throws TException {
        return call(this.readWriteClient, client -> client.getValidTxns(), "getValidTxns");
    }

    @Override
    public ValidTxnList getValidTxns(long currentTxn) throws TException {
        return call(this.readWriteClient, client -> client.getValidTxns(currentTxn), "getValidTxns", currentTxn);
    }

    @Override
    public ValidWriteIdList getValidWriteIds(String fullTableName) throws TException {
        return call(this.readWriteClient, client -> client.getValidWriteIds(fullTableName), "getValidWriteIds", fullTableName);
    }

    @Override
    public List<TableValidWriteIds> getValidWriteIds(List<String> tablesList, String validTxnList) throws TException {
        return call(this.readWriteClient, client -> client.getValidWriteIds(tablesList, validTxnList), "getValidWriteIds", tablesList, validTxnList);
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef obj, String user, List<String> groups)
            throws MetaException, TException {
        return call(this.readWriteClient, client -> client.get_privilege_set(obj, user, groups), "get_privilege_set", obj, user, groups);
    }

    @Override
    public boolean grant_privileges(PrivilegeBag privileges)
            throws MetaException, TException {
        return call(client -> client.grant_privileges(privileges), "grant_privileges", privileges);
    }

    @Override
    public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption)
            throws MetaException, TException {
        return call(client -> client.revoke_privileges(privileges, grantOption), "revoke_privileges", privileges, grantOption);
    }

    @Override
    public boolean refresh_privileges(HiveObjectRef objToRefresh, String authorizer, PrivilegeBag grantPrivileges) throws MetaException, TException {
        return call(client -> client.refresh_privileges(objToRefresh, authorizer, grantPrivileges), "refresh_privileges", objToRefresh, authorizer, grantPrivileges);
    }

    @Override
    public void heartbeat(long txnId, long lockId)
            throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
        run(client -> client.heartbeat(txnId, lockId), "heartbeat", txnId, lockId);
    }

    @Override
    public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
        return call(client -> client.heartbeatTxnRange(min, max), "heartbeatTxnRange", min, max);
    }

    @Override
    public boolean isPartitionMarkedForEvent(
            String dbName,
            String tblName,
            Map<String, String> partKVs,
            PartitionEventType eventType
    ) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        return call(this.readWriteClient, client -> client.isPartitionMarkedForEvent(dbName, tblName, partKVs, eventType), "isPartitionMarkedForEvent", dbName, tblName, partKVs, eventType);
    }

    @Override
    public boolean isPartitionMarkedForEvent(String catalogId, String db_name, String tbl_name, Map<String, String> partKVs, PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
        return call(this.readWriteClient, client -> client.isPartitionMarkedForEvent(catalogId, db_name, tbl_name, partKVs, eventType), "isPartitionMarkedForEvent", catalogId, db_name, tbl_name, partKVs, eventType);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, short max)
            throws MetaException, TException {
        return call(this.readWriteClient, client -> client.listPartitionNames(dbName, tblName, max), "listPartitionNames", dbName, tblName, max);
    }

    @Override
    public List<String> listPartitionNames(String catalogId, String dbName, String tblName, int max) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.listPartitionNames(catalogId, dbName, tblName, max), "listPartitionNames", catalogId, dbName, tblName, max);
    }

    @Override
    public List<String> listPartitionNames(
            String databaseName,
            String tableName,
            List<String> values,
            short max
    ) throws MetaException, TException, NoSuchObjectException {
        return call(this.readWriteClient, client -> client.listPartitionNames(databaseName, tableName, values, max), "listPartitionNames", databaseName, tableName, values, max);
    }

    @Override
    public List<String> listPartitionNames(
            String catalogId,
            String databaseName,
            String tableName,
            List<String> values,
            int max
    ) throws MetaException, TException, NoSuchObjectException {
        return call(this.readWriteClient, client -> client.listPartitionNames(catalogId, databaseName, tableName, values, max), "listPartitionNames", catalogId, databaseName, tableName, values, max);
    }

    @Override
    public PartitionValuesResponse listPartitionValues(PartitionValuesRequest partitionValuesRequest) throws MetaException, TException, NoSuchObjectException {
        return call(this.readWriteClient, client -> client.listPartitionValues(partitionValuesRequest), "listPartitionValues", partitionValuesRequest);
    }

    @Override
    public int getNumPartitionsByFilter(
            String dbName,
            String tableName,
            String filter
    ) throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getNumPartitionsByFilter(dbName, tableName, filter), "getNumPartitionsByFilter", dbName, tableName, filter);
    }

    @Override
    public int getNumPartitionsByFilter(
            String catalogId,
            String dbName,
            String tableName,
            String filter
    ) throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getNumPartitionsByFilter(catalogId, dbName, tableName, filter), "getNumPartitionsByFilter", catalogId, dbName, tableName, filter);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(
            String dbName,
            String tblName,
            int max
    ) throws TException {
        return call(this.readWriteClient, client -> client.listPartitionSpecs(dbName, tblName, max), "listPartitionSpecs", dbName, tblName, max);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(
            String catalogId,
            String dbName,
            String tblName,
            int max
    ) throws TException {
        return call(this.readWriteClient, client -> client.listPartitionSpecs(catalogId, dbName, tblName, max), "listPartitionSpecs", catalogId, dbName, tblName, max);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(
            String dbName,
            String tblName,
            String filter,
            int max
    ) throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.listPartitionSpecsByFilter(dbName, tblName, filter, max), "listPartitionSpecsByFilter", dbName, tblName, filter, max);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(
            String catalogId,
            String dbName,
            String tblName,
            String filter,
            int max
    ) throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.listPartitionSpecsByFilter(catalogId, dbName, tblName, filter, max), "listPartitionSpecsByFilter", catalogId, dbName, tblName, filter, max);
    }

    @Override
    public List<Partition> listPartitions(String dbName, String tblName, short max)
            throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.listPartitions(dbName, tblName, max), "listPartitions", dbName, tblName, max);
    }

    @Override
    public List<Partition> listPartitions(String catalogId, String dbName, String tblName, int max) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.listPartitions(catalogId, dbName, tblName, max), "listPartitions", catalogId, dbName, tblName, max);
    }

    @Override
    public List<Partition> listPartitions(
            String databaseName,
            String tableName,
            List<String> values,
            short max
    ) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.listPartitions(databaseName, tableName, values, max), "listPartitions", databaseName, tableName, values, max);
    }

    @Override
    public List<Partition> listPartitions(String catalogId, String databaseName, String tableName, List<String> values, int max) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.listPartitions(catalogId, databaseName, tableName, values, max), "listPartitions", catalogId, databaseName, tableName, values, max);
    }

    @Override
    public boolean listPartitionsByExpr(
            String databaseName,
            String tableName,
            byte[] expr,
            String defaultPartitionName,
            short max,
            List<Partition> result
    ) throws TException {
        return call(this.readWriteClient, client -> client.listPartitionsByExpr(databaseName, tableName, expr, defaultPartitionName, max, result), "listPartitionsByExpr", databaseName, tableName, expr, defaultPartitionName, max, result);
    }

    @Override
    public boolean listPartitionsByExpr(
            String catalogId,
            String databaseName,
            String tableName,
            byte[] expr,
            String defaultPartitionName,
            int max,
            List<Partition> result
    ) throws TException {
        return call(this.readWriteClient, client -> client.listPartitionsByExpr(catalogId, databaseName, tableName, expr, defaultPartitionName, max, result), "listPartitionsByExpr", catalogId, databaseName, tableName, expr, defaultPartitionName, max, result);
    }

    @Override
    public List<Partition> listPartitionsByFilter(
            String databaseName,
            String tableName,
            String filter,
            short max
    ) throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.listPartitionsByFilter(databaseName, tableName, filter, max), "listPartitionsByFilter", databaseName, tableName, filter, max);
    }

    @Override
    public List<Partition> listPartitionsByFilter(
            String catalogId,
            String databaseName,
            String tableName,
            String filter,
            int max
    ) throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.listPartitionsByFilter(catalogId, databaseName, tableName, filter, max), "listPartitionsByFilter", catalogId, databaseName, tableName, filter, max);
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(
            String database,
            String table,
            short maxParts,
            String user,
            List<String> groups
    ) throws MetaException, TException, NoSuchObjectException {
        return call(this.readWriteClient, client -> client.listPartitionsWithAuthInfo(database, table, maxParts, user, groups), "listPartitionsWithAuthInfo", database, table, maxParts, user, groups);
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(
            String catalogId,
            String database,
            String table,
            int maxParts,
            String user,
            List<String> groups
    ) throws MetaException, TException, NoSuchObjectException {
        return call(this.readWriteClient, client -> client.listPartitionsWithAuthInfo(catalogId, database, table, maxParts, user, groups), "listPartitionsWithAuthInfo", catalogId, database, table, maxParts, user, groups);
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(
            String database,
            String table,
            List<String> partVals,
            short maxParts,
            String user,
            List<String> groups
    ) throws MetaException, TException, NoSuchObjectException {
        return call(this.readWriteClient, client -> client.listPartitionsWithAuthInfo(database, table, partVals, maxParts, user, groups), "listPartitionsWithAuthInfo", database, table, partVals, maxParts, user, groups);
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String catalogId, String database, String table, List<String> partVals, int maxParts, String user, List<String> groups) throws MetaException, TException, NoSuchObjectException {
        return call(this.readWriteClient, client -> client.listPartitionsWithAuthInfo(catalogId, database, table, partVals, maxParts, user, groups), "listPartitionsWithAuthInfo", catalogId, database, table, partVals, maxParts, user, groups);
    }

    @Override
    public List<String> listTableNamesByFilter(
            String dbName,
            String filter,
            short maxTables
    ) throws MetaException, TException, InvalidOperationException, UnknownDBException, UnsupportedOperationException {
        return call(this.readWriteClient, client -> client.listTableNamesByFilter(dbName, filter, maxTables), "listTableNamesByFilter", dbName, filter, maxTables);
    }

    @Override
    public List<String> listTableNamesByFilter(String catalogId, String dbName, String filter, int maxTables) throws TException, InvalidOperationException, UnknownDBException {
        return call(this.readWriteClient, client -> client.listTableNamesByFilter(catalogId, dbName, filter, maxTables), "listTableNamesByFilter", catalogId, dbName, filter, maxTables);
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(
            String principal,
            PrincipalType principalType,
            HiveObjectRef objectRef
    ) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.list_privileges(principal, principalType, objectRef), "list_privileges", principal, principalType, objectRef);
    }

    @Override
    public LockResponse lock(LockRequest lockRequest) throws NoSuchTxnException, TxnAbortedException, TException {
        return call(client -> client.lock(lockRequest), "lock", lockRequest);
    }

    @Override
    public void markPartitionForEvent(
            String dbName,
            String tblName,
            Map<String, String> partKVs,
            PartitionEventType eventType
    ) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        run(client -> client.markPartitionForEvent(dbName, tblName, partKVs, eventType), "markPartitionForEvent", dbName, tblName, partKVs, eventType);
    }

    @Override
    public void markPartitionForEvent(
            String catalogId,
            String dbName,
            String tblName,
            Map<String, String> partKVs,
            PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
        run(client -> client.markPartitionForEvent(catalogId, dbName, tblName, partKVs, eventType), "markPartitionForEvent", catalogId, dbName, tblName, partKVs, eventType);
    }

    @Override
    public long openTxn(String user) throws TException {
        return call(client -> client.openTxn(user), "openTxn", user);
    }

    @Override
    public List<Long> replOpenTxn(String replPolicy, List<Long> srcTxnIds, String user) throws TException {
        return call(client -> client.replOpenTxn(replPolicy, srcTxnIds, user), "replOpenTxn", replPolicy, srcTxnIds, user);
    }

    @Override
    public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
        return call(client -> client.openTxns(user, numTxns), "openTxns", numTxns);
    }

    @Override
    public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.partitionNameToSpec(name), "partitionNameToSpec", name);
    }

    @Override
    public List<String> partitionNameToVals(String name) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.partitionNameToVals(name), "partitionNameToVals", name);
    }

    @Override
    public void renamePartition(
            String dbName,
            String tblName,
            List<String> partitionValues,
            Partition newPartition
    ) throws InvalidOperationException, MetaException, TException {
        run(client -> client.renamePartition(dbName, tblName, partitionValues, newPartition), "renamePartition", dbName, tblName,
                partitionValues, newPartition);
    }

    @Override
    public void renamePartition(String catalogId, String dbName, String tblName, List<String> partitionValues, Partition newPartition) throws InvalidOperationException, MetaException, TException {
        run(client -> client.renamePartition(catalogId, dbName, tblName, partitionValues, newPartition), "renamePartition", catalogId, dbName, tblName, partitionValues, newPartition);
    }

    @Override
    public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
        return call(client -> client.renewDelegationToken(tokenStrForm), "renewDelegationToken", tokenStrForm);
    }

    @Override
    public void rollbackTxn(long txnId) throws NoSuchTxnException, TException {
        run(client -> client.rollbackTxn(txnId), "rollbackTxn", txnId);
    }

    @Override
    public void replRollbackTxn(long srcTxnId, String replPolicy) throws NoSuchTxnException, TException {
        run(client -> client.replRollbackTxn(srcTxnId, replPolicy), "replRollbackTxn", srcTxnId, replPolicy);
    }

    @Override
    public void setMetaConf(String key, String value) throws MetaException, TException {
        run(client -> client.setMetaConf(key, value), "setMetaConf", key, value);
    }

    @Override
    public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request)
            throws NoSuchObjectException, InvalidObjectException,
            MetaException, TException, InvalidInputException {
        if (request.getColStatsSize() == 1) {
            ColumnStatistics colStats = request.getColStatsIterator().next();
            ColumnStatisticsDesc desc = colStats.getStatsDesc();
            String dbName = desc.getDbName().toLowerCase();
            String tableName = desc.getTableName().toLowerCase();
            if (getTempTable(dbName, tableName) != null) {
                return call(this.readWriteClient, client -> client.setPartitionColumnStatistics(request), "setPartitionColumnStatistics", request);
            }
        }
        SetPartitionsStatsRequest deepCopy = request.deepCopy();
        boolean result = readWriteClient.setPartitionColumnStatistics(deepCopy);
        if (extraClient.isPresent()) {
            try {
                extraClient.get().setPartitionColumnStatistics(request);
            } catch (Exception e) {
                FunctionalUtils.collectLogs(e, "setPartitionColumnStatistics", request);
                if (!allowFailure) {
                    throw e;
                }
            }
        }
        return result;
    }

    @Override
    public void flushCache() {
        try {
            run(client -> client.flushCache(), "flushCache");
        } catch (TException e) {
            logger.info(e.getMessage(), e);
        }
    }

    @Override
    public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
        return call(this.readWriteClient, client -> client.getFileMetadata(fileIds), "getFileMetadata", fileIds);
    }

    @Override
    public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
            List<Long> fileIds,
            ByteBuffer sarg,
            boolean doGetFooters
    ) throws TException {
        return call(this.readWriteClient, client -> client.getFileMetadataBySarg(fileIds, sarg, doGetFooters), "getFileMetadataBySarg", fileIds, sarg, doGetFooters);
    }

    @Override
    public void clearFileMetadata(List<Long> fileIds) throws TException {
        run(client -> client.clearFileMetadata(fileIds), "clearFileMetadata", fileIds);
    }

    @Override
    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
        run(client -> client.putFileMetadata(fileIds, metadata), "putFileMetadata", fileIds, metadata);
    }

    @Override
    public boolean isSameConfObj(Configuration conf) {
        try {
            return call(this.readWriteClient, client -> client.isSameConfObj(conf), "isSameConfObj", conf);
        } catch (TException e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public boolean cacheFileMetadata(
            String dbName,
            String tblName,
            String partName,
            boolean allParts
    ) throws TException {
        return call(client -> client.cacheFileMetadata(dbName, tblName, partName, allParts), "cacheFileMetadata", dbName, tblName, partName, allParts);
    }

    @Override
    public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest primaryKeysRequest)
            throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getPrimaryKeys(primaryKeysRequest), "getPrimaryKeys", primaryKeysRequest);
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest foreignKeysRequest)
            throws MetaException, NoSuchObjectException, TException {
        // PrimaryKeys are currently unsupported
        //return null to allow DESCRIBE (FORMATTED | EXTENDED)
        return call(this.readWriteClient, client -> client.getForeignKeys(foreignKeysRequest), "getForeignKeys", foreignKeysRequest);
    }

    @Override
    public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest uniqueConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getUniqueConstraints(uniqueConstraintsRequest), "getUniqueConstraints", uniqueConstraintsRequest);
    }

    @Override
    public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest notNullConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getNotNullConstraints(notNullConstraintsRequest), "getNotNullConstraints", notNullConstraintsRequest);
    }

    @Override
    public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest defaultConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getDefaultConstraints(defaultConstraintsRequest), "getDefaultConstraints", defaultConstraintsRequest);
    }

    @Override
    public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest checkConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getCheckConstraints(checkConstraintsRequest), "getCheckConstraints", checkConstraintsRequest);
    }

    @Override
    public void createTableWithConstraints(
            Table tbl,
            List<SQLPrimaryKey> primaryKeys,
            List<SQLForeignKey> foreignKeys,
            List<SQLUniqueConstraint> uniqueConstraints,
            List<SQLNotNullConstraint> notNullConstraints,
            List<SQLDefaultConstraint> defaultConstraints,
            List<SQLCheckConstraint> checkConstraints
    ) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        run(client -> client.createTableWithConstraints(tbl, primaryKeys, foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints), "createTableWithConstraints", tbl, primaryKeys, foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints);
    }

    @Override
    public void dropConstraint(
            String dbName,
            String tblName,
            String constraintName
    ) throws MetaException, NoSuchObjectException, TException {
        run(client -> client.dropConstraint(dbName, tblName, constraintName), "dropConstraint", dbName, tblName, constraintName);
    }

    @Override
    public void dropConstraint(String catalogId, String dbName, String tableName, String constraintName) throws MetaException, NoSuchObjectException, TException {
        run(client -> client.dropConstraint(catalogId, dbName, tableName, constraintName), "dropConstraint", catalogId, dbName, tableName, constraintName);
    }

    @Override
    public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols)
            throws MetaException, NoSuchObjectException, TException {
        run(client -> client.addPrimaryKey(primaryKeyCols), "addPrimaryKey", primaryKeyCols);
    }

    @Override
    public void addForeignKey(List<SQLForeignKey> foreignKeyCols)
            throws MetaException, NoSuchObjectException, TException {
        run(client -> client.addForeignKey(foreignKeyCols), "addForeignKey", foreignKeyCols);
    }

    @Override
    public void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols) throws MetaException, NoSuchObjectException, TException {
        run(client -> client.addUniqueConstraint(uniqueConstraintCols), "addUniqueConstraint", uniqueConstraintCols);
    }

    @Override
    public void addNotNullConstraint(List<SQLNotNullConstraint> notNullConstraintCols) throws MetaException, NoSuchObjectException, TException {
        run(client -> client.addNotNullConstraint(notNullConstraintCols), "addNotNullConstraint", notNullConstraintCols);
    }

    @Override
    public void addDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints) throws MetaException, NoSuchObjectException, TException {
        run(client -> client.addDefaultConstraint(defaultConstraints), "addDefaultConstraint", defaultConstraints);
    }

    @Override
    public void addCheckConstraint(List<SQLCheckConstraint> checkConstraints) throws MetaException, NoSuchObjectException, TException {
        run(client -> client.addCheckConstraint(checkConstraints), "addCheckConstraint", checkConstraints);
    }

    @Override
    public String getMetastoreDbUuid() throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getMetastoreDbUuid(), "getMetastoreDbUuid");
    }

    @Override
    public void createResourcePlan(WMResourcePlan wmResourcePlan, String copyFromName) throws InvalidObjectException, MetaException, TException {
        run(client -> client.createResourcePlan(wmResourcePlan, copyFromName), "createResourcePlan", wmResourcePlan, copyFromName);
    }

    @Override
    public WMFullResourcePlan getResourcePlan(String resourcePlanName) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getResourcePlan(resourcePlanName), "getResourcePlan", resourcePlanName);
    }

    @Override
    public List<WMResourcePlan> getAllResourcePlans() throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getAllResourcePlans(), "getAllResourcePlans");
    }

    @Override
    public void dropResourcePlan(String resourcePlanName) throws NoSuchObjectException, MetaException, TException {
        run(client -> client.dropResourcePlan(resourcePlanName), "dropResourcePlan", resourcePlanName);
    }

    @Override
    public WMFullResourcePlan alterResourcePlan(String resourcePlanName, WMNullableResourcePlan wmNullableResourcePlan, boolean canActivateDisabled, boolean isForceDeactivate, boolean isReplace) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        return call(client -> client.alterResourcePlan(resourcePlanName, wmNullableResourcePlan, canActivateDisabled, isForceDeactivate, isReplace), "alterResourcePlan", resourcePlanName, wmNullableResourcePlan, canActivateDisabled, isForceDeactivate, isReplace);
    }

    @Override
    public WMFullResourcePlan getActiveResourcePlan() throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getActiveResourcePlan(), "getActiveResourcePlan");
    }

    @Override
    public WMValidateResourcePlanResponse validateResourcePlan(String resourcePlanName) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.validateResourcePlan(resourcePlanName), "validateResourcePlan", resourcePlanName);
    }

    @Override
    public void createWMTrigger(WMTrigger wmTrigger) throws InvalidObjectException, MetaException, TException {
        run(client -> client.createWMTrigger(wmTrigger), "createWMTrigger", wmTrigger);
    }

    @Override
    public void alterWMTrigger(WMTrigger wmTrigger) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        run(client -> client.alterWMTrigger(wmTrigger), "alterWMTrigger", wmTrigger);
    }

    @Override
    public void dropWMTrigger(String resourcePlanName, String triggerName) throws NoSuchObjectException, MetaException, TException {
        run(client -> client.dropWMTrigger(resourcePlanName, triggerName), "dropWMTrigger", resourcePlanName, triggerName);
    }

    @Override
    public List<WMTrigger> getTriggersForResourcePlan(String resourcePlan) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getTriggersForResourcePlan(resourcePlan), "getTriggersForResourcePlan", resourcePlan);
    }

    @Override
    public void createWMPool(WMPool wmPool) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        run(client -> client.createWMPool(wmPool), "createWMPool", wmPool);
    }

    @Override
    public void alterWMPool(WMNullablePool wmNullablePool, String poolPath) throws NoSuchObjectException, InvalidObjectException, TException {
        run(client -> client.alterWMPool(wmNullablePool, poolPath), "alterWMPool", wmNullablePool, poolPath);
    }

    @Override
    public void dropWMPool(String resourcePlanName, String poolPath) throws TException {
        run(client -> client.dropWMPool(resourcePlanName, poolPath), "dropWMPool", resourcePlanName, poolPath);
    }

    @Override
    public void createOrUpdateWMMapping(WMMapping wmMapping, boolean isUpdate) throws TException {
        run(client -> client.createOrUpdateWMMapping(wmMapping, isUpdate), "createOrUpdateWMMapping", wmMapping, isUpdate);
    }

    @Override
    public void dropWMMapping(WMMapping wmMapping) throws TException {
        run(client -> client.dropWMMapping(wmMapping), "dropWMMapping", wmMapping);
    }

    @Override
    public void createOrDropTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath, boolean shouldDrop) throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
        run(client -> client.createOrDropTriggerToPoolMapping(resourcePlanName, triggerName, poolPath, shouldDrop), "createOrDropTriggerToPoolMapping", resourcePlanName, triggerName, poolPath, shouldDrop);
    }

    @Override
    public void createISchema(ISchema iSchema) throws TException {
        run(client -> client.createISchema(iSchema), "createISchema", iSchema);
    }

    @Override
    public void alterISchema(String catName, String dbName, String schemaName, ISchema newSchema) throws TException {
        run(client -> client.alterISchema(catName, dbName, schemaName, newSchema), "alterISchema", catName, dbName, schemaName, newSchema);
    }

    @Override
    public ISchema getISchema(String catalogId, String dbName, String name) throws TException {
        return call(this.readWriteClient, client -> client.getISchema(catalogId, dbName, name), "getISchema", catalogId, dbName, name);
    }

    @Override
    public void dropISchema(String catalogId, String dbName, String name) throws TException {
        run(client -> client.dropISchema(catalogId, dbName, name), "dropISchema", catalogId, dbName, name);
    }

    @Override
    public void addSchemaVersion(SchemaVersion schemaVersion) throws TException {
        run(client -> client.addSchemaVersion(schemaVersion), "addSchemaVersion", schemaVersion);
    }

    @Override
    public SchemaVersion getSchemaVersion(String catalogId, String dbName, String schemaName, int version) throws TException {
        return call(this.readWriteClient, client -> client.getSchemaVersion(catalogId, dbName, schemaName, version), "getSchemaVersion", catalogId, dbName, schemaName, version);
    }

    @Override
    public SchemaVersion getSchemaLatestVersion(String catalogId, String dbName, String schemaName) throws TException {
        return call(this.readWriteClient, client -> client.getSchemaLatestVersion(catalogId, dbName, schemaName), "getSchemaLatestVersion", catalogId, dbName, schemaName);
    }

    @Override
    public List<SchemaVersion> getSchemaAllVersions(String catalogId, String dbName, String schemaName) throws TException {
        return call(this.readWriteClient, client -> client.getSchemaAllVersions(catalogId, dbName, schemaName), "getSchemaAllVersions", catalogId, dbName, schemaName);
    }

    @Override
    public void dropSchemaVersion(String catalogId, String dbName, String schemaName, int version) throws TException {
        run(client -> client.dropSchemaVersion(catalogId, dbName, schemaName, version), "dropSchemaVersion", catalogId, dbName, schemaName, version);
    }

    @Override
    public FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst findSchemasByColsRqst) throws TException {
        return call(this.readWriteClient, client -> client.getSchemaByCols(findSchemasByColsRqst), "getSchemaByCols", findSchemasByColsRqst);
    }

    @Override
    public void mapSchemaVersionToSerde(String catalogId, String dbName, String schemaName, int version, String serdeName) throws TException {
        run(client -> client.mapSchemaVersionToSerde(catalogId, dbName, schemaName, version, serdeName), "mapSchemaVersionToSerde", catalogId, dbName, schemaName, version, serdeName);
    }

    @Override
    public void setSchemaVersionState(String catalogId, String dbName, String schemaName, int version, SchemaVersionState state) throws TException {
        run(client -> client.setSchemaVersionState(catalogId, dbName, schemaName, version, state), "setSchemaVersionState", catalogId, dbName, schemaName, version, state);
    }

    @Override
    public void addSerDe(SerDeInfo serDeInfo) throws TException {
        run(client -> client.addSerDe(serDeInfo), "addSerDe", serDeInfo);
    }

    @Override
    public SerDeInfo getSerDe(String serDeName) throws TException {
        return call(this.readWriteClient, client -> client.getSerDe(serDeName), "getSerDe", serDeName);
    }

    @Override
    public LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
        return call(client -> client.lockMaterializationRebuild(dbName, tableName, txnId), "lockMaterializationRebuild", dbName, tableName, txnId);
    }

    @Override
    public boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
        return call(client -> client.heartbeatLockMaterializationRebuild(dbName, tableName, txnId), "heartbeatLockMaterializationRebuild", dbName, tableName, txnId);
    }

    @Override
    public void addRuntimeStat(RuntimeStat runtimeStat) throws TException {
        run(client -> client.addRuntimeStat(runtimeStat), "addRuntimeStat", runtimeStat);
    }

    @Override
    public List<RuntimeStat> getRuntimeStats(int maxWeight, int maxCreateTime) throws TException {
        return call(this.readWriteClient, client -> client.getRuntimeStats(maxWeight, maxCreateTime), "getRuntimeStats", maxWeight, maxCreateTime);
    }

    @Override
    public ShowCompactResponse showCompactions() throws TException {
        return call(this.readWriteClient, client -> client.showCompactions(), "showCompactions");
    }

    @Override
    public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName, List<String> partNames) throws TException {
        run(client -> client.addDynamicPartitions(txnId, writeId, dbName, tableName, partNames), "addDynamicPartitions", txnId, writeId, dbName, tableName, partNames);
    }

    @Override
    public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName, List<String> partNames, DataOperationType operationType) throws TException {
        run(client -> client.addDynamicPartitions(txnId, writeId, dbName, tableName, partNames, operationType), "addDynamicPartitions", txnId, writeId, dbName, tableName, partNames, operationType);
    }

    @Override
    public void insertTable(Table table, boolean overwrite) throws MetaException {
        try {
            run(client -> client.insertTable(table, overwrite), "insertTable", table, overwrite);
        } catch (TException e) {
            throw DataLakeUtil.throwException(new MetaException(e.getMessage()), e);
        }
    }

    @Override
    public NotificationEventResponse getNextNotification(
            long lastEventId,
            int maxEvents,
            NotificationFilter notificationFilter
    ) throws TException {
        return call(this.readWriteClient, client -> client.getNextNotification(lastEventId, maxEvents, notificationFilter), "getNextNotification", lastEventId, maxEvents, notificationFilter);
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
        return call(this.readWriteClient, client -> client.getCurrentNotificationEventId(), "getCurrentNotificationEventId");
    }

    @Override
    public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest notificationEventsCountRequest) throws TException {
        return call(this.readWriteClient, client -> client.getNotificationEventsCount(notificationEventsCountRequest), "getNotificationEventsCount", notificationEventsCountRequest);
    }

    @Override
    public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
        return call(this.readWriteClient, client -> client.fireListenerEvent(fireEventRequest), "fireListenerEvent", fireEventRequest);
    }

    @Override
    @Deprecated
    public ShowLocksResponse showLocks() throws TException {
        return call(this.readWriteClient, client -> client.showLocks(), "showLocks");
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
        return call(this.readWriteClient, client -> client.showLocks(showLocksRequest), "showLocks", showLocksRequest);
    }

    @Override
    public GetOpenTxnsInfoResponse showTxns() throws TException {
        return call(this.readWriteClient, client -> client.showTxns(), "showTxns");
    }

    @Override
    public boolean tableExists(String databaseName, String tableName)
            throws MetaException, TException, UnknownDBException {
        return call(this.readWriteClient, client -> client.tableExists(databaseName, tableName), "tableExists", databaseName, tableName);
    }

    @Override
    public boolean tableExists(String catalogId, String databaseName, String tableName) throws MetaException, TException, UnknownDBException {
        return call(this.readWriteClient, client -> client.tableExists(catalogId, databaseName, tableName), "tableExists", catalogId, databaseName, tableName);
    }

    @Override
    public void unlock(long lockId) throws NoSuchLockException, TxnOpenException, TException {
        run(client -> client.unlock(lockId), "unlock", lockId);
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        return call(client -> client.updatePartitionColumnStatistics(columnStatistics), "updatePartitionColumnStatistics", columnStatistics);
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        if (getTempTable(columnStatistics.getStatsDesc().getDbName(), columnStatistics.getStatsDesc().getTableName()) != null) {
            return call(this.readWriteClient, client -> client.updateTableColumnStatistics(columnStatistics), "updateTableColumnStatistics", columnStatistics);
        } else {
            return call(client -> client.updateTableColumnStatistics(columnStatistics), "updateTableColumnStatistics", columnStatistics);
        }
    }

    @Override
    public void validatePartitionNameCharacters(List<String> part_vals) throws TException, MetaException {
        run(this.readWriteClient, client -> client.validatePartitionNameCharacters(part_vals), "validatePartitionNameCharacters", part_vals);
    }

    @VisibleForTesting
    public IMetaStoreClient getDlfSessionMetaStoreClient() {
        return dlfSessionMetaStoreClient;
    }

    @VisibleForTesting
    public IMetaStoreClient getHiveSessionMetaStoreClient() {
        return hiveSessionMetaStoreClient;
    }

    @VisibleForTesting
    boolean isAllowFailure() {
        return allowFailure;
    }

    public void run(ThrowingConsumer<IMetaStoreClient, TException> consumer, String actionName, Object... parameters) throws TException {
        FunctionalUtils.run(this.readWriteClient, extraClient, allowFailure, consumer, this.readWriteClientType, actionName, parameters);
    }

    public void run(IMetaStoreClient client, ThrowingConsumer<IMetaStoreClient, TException> consumer,
            String actionName, Object... parameters) throws TException {
        FunctionalUtils.run(client, Optional.empty(), allowFailure, consumer, this.readWriteClientType, actionName, parameters);
    }

    public <R> R call(ThrowingFunction<IMetaStoreClient, R, TException> consumer,
            String actionName, Object... parameters) throws TException {
        return FunctionalUtils.call(this.readWriteClient, extraClient, allowFailure, consumer,
                this.readWriteClientType, actionName, parameters);
    }

    public <R> R call(IMetaStoreClient client, ThrowingFunction<IMetaStoreClient, R, TException> consumer,
            String actionName, Object... parameters) throws TException {
        return FunctionalUtils.call(client, Optional.empty(), allowFailure, consumer, this.readWriteClientType,
                actionName, parameters);
    }
}