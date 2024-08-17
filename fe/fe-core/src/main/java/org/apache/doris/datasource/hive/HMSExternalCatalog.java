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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;
import org.apache.doris.datasource.operations.ExternalMetadataOperations;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.fs.FileSystemProvider;
import org.apache.doris.fs.FileSystemProviderImpl;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
import org.apache.doris.transaction.TransactionManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.Getter;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * External catalog for hive metastore compatible data sources.
 */
public class HMSExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(HMSExternalCatalog.class);

    public static final String FILE_META_CACHE_TTL_SECOND = "file.meta.cache.ttl-second";
    // broker name for file split and query scan.
    public static final String BIND_BROKER_NAME = "broker.name";

    // -1 means file cache no ttl set
    public static final int FILE_META_CACHE_NO_TTL = -1;
    // 0 means file cache is disabled; >0 means file cache with ttl;
    public static final int FILE_META_CACHE_TTL_DISABLE_CACHE = 0;

    private static final int FILE_SYSTEM_EXECUTOR_THREAD_NUM = 16;
    private ThreadPoolExecutor fileSystemExecutor;
    @Getter
    private HadoopAuthenticator authenticator;

    private int hmsEventsBatchSizePerRpc = -1;
    private boolean enableHmsEventsIncrementalSync = false;


    @VisibleForTesting
    public HMSExternalCatalog() {
        catalogProperty = new CatalogProperty(null, null);
    }

    /**
     * Default constructor for HMSExternalCatalog.
     */
    public HMSExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, InitCatalogLog.Type.HMS, comment);
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
        AuthenticationConfig config = AuthenticationConfig.getKerberosConfig(getConfiguration());
        authenticator = HadoopAuthenticator.getHadoopAuthenticator(config);
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        // check file.meta.cache.ttl-second parameter
        String fileMetaCacheTtlSecond = catalogProperty.getOrDefault(FILE_META_CACHE_TTL_SECOND, null);
        if (Objects.nonNull(fileMetaCacheTtlSecond) && NumberUtils.toInt(fileMetaCacheTtlSecond, FILE_META_CACHE_NO_TTL)
                < FILE_META_CACHE_TTL_DISABLE_CACHE) {
            throw new DdlException(
                    "The parameter " + FILE_META_CACHE_TTL_SECOND + " is wrong, value is " + fileMetaCacheTtlSecond);
        }
        Map<String, String> properties = catalogProperty.getProperties();
        if (properties.containsKey(HMSProperties.ENABLE_HMS_EVENTS_INCREMENTAL_SYNC)) {
            enableHmsEventsIncrementalSync =
                    properties.get(HMSProperties.ENABLE_HMS_EVENTS_INCREMENTAL_SYNC).equals("true");
        } else {
            enableHmsEventsIncrementalSync = Config.enable_hms_events_incremental_sync;
        }

        if (properties.containsKey(HMSProperties.HMS_EVENTIS_BATCH_SIZE_PER_RPC)) {
            hmsEventsBatchSizePerRpc = Integer.valueOf(properties.get(HMSProperties.HMS_EVENTIS_BATCH_SIZE_PER_RPC));
        } else {
            hmsEventsBatchSizePerRpc = Config.hms_events_batch_size_per_rpc;
        }

        // check the dfs.ha properties
        // 'dfs.nameservices'='your-nameservice',
        // 'dfs.ha.namenodes.your-nameservice'='nn1,nn2',
        // 'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',
        // 'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',
        // 'dfs.client.failover.proxy.provider.your-nameservice'='xxx'
        String dfsNameservices = catalogProperty.getOrDefault(HdfsResource.DSF_NAMESERVICES, "");
        if (Strings.isNullOrEmpty(dfsNameservices)) {
            return;
        }

        String[] nameservices = dfsNameservices.split(",");
        for (String dfsservice : nameservices) {
            String namenodes = catalogProperty.getOrDefault("dfs.ha.namenodes." + dfsservice, "");
            if (Strings.isNullOrEmpty(namenodes)) {
                throw new DdlException("Missing dfs.ha.namenodes." + dfsservice + " property");
            }
            String[] names = namenodes.split(",");
            for (String name : names) {
                String address = catalogProperty.getOrDefault("dfs.namenode.rpc-address." + dfsservice + "." + name,
                        "");
                if (Strings.isNullOrEmpty(address)) {
                    throw new DdlException(
                            "Missing dfs.namenode.rpc-address." + dfsservice + "." + name + " property");
                }
            }
            String failoverProvider = catalogProperty.getOrDefault("dfs.client.failover.proxy.provider." + dfsservice,
                    "");
            if (Strings.isNullOrEmpty(failoverProvider)) {
                throw new DdlException(
                        "Missing dfs.client.failover.proxy.provider." + dfsservice + " property");
            }
        }
    }

    @Override
    protected void initLocalObjectsImpl() {
        if (authenticator == null) {
            AuthenticationConfig config = AuthenticationConfig.getKerberosConfig(getConfiguration());
            authenticator = HadoopAuthenticator.getHadoopAuthenticator(config);
        }

        HiveConf hiveConf = null;
        JdbcClientConfig jdbcClientConfig = null;
        String hiveMetastoreType = catalogProperty.getOrDefault(HMSProperties.HIVE_METASTORE_TYPE, "");
        if (hiveMetastoreType.equalsIgnoreCase("jdbc")) {
            jdbcClientConfig = new JdbcClientConfig();
            jdbcClientConfig.setUser(catalogProperty.getOrDefault("user", ""));
            jdbcClientConfig.setPassword(catalogProperty.getOrDefault("password", ""));
            jdbcClientConfig.setJdbcUrl(catalogProperty.getOrDefault("jdbc_url", ""));
            jdbcClientConfig.setDriverUrl(catalogProperty.getOrDefault("driver_url", ""));
            jdbcClientConfig.setDriverClass(catalogProperty.getOrDefault("driver_class", ""));
        } else {
            hiveConf = new HiveConf();
            for (Map.Entry<String, String> kv : catalogProperty.getHadoopProperties().entrySet()) {
                hiveConf.set(kv.getKey(), kv.getValue());
            }
            hiveConf.set(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.name(),
                    String.valueOf(Config.hive_metastore_client_timeout_second));
        }
        HiveMetadataOps hiveOps = ExternalMetadataOperations.newHiveMetadataOps(hiveConf, jdbcClientConfig, this);
        FileSystemProvider fileSystemProvider = new FileSystemProviderImpl(Env.getCurrentEnv().getExtMetaCacheMgr(),
                this.bindBrokerName(), this.catalogProperty.getHadoopProperties());
        this.fileSystemExecutor = ThreadPoolManager.newDaemonFixedThreadPool(FILE_SYSTEM_EXECUTOR_THREAD_NUM,
                Integer.MAX_VALUE, String.format("hms_committer_%s_file_system_executor_pool", name), true);
        transactionManager = TransactionManagerFactory.createHiveTransactionManager(hiveOps, fileSystemProvider,
                fileSystemExecutor);
        metadataOps = hiveOps;
    }

    @Override
    public void onRefresh(boolean invalidCache) {
        super.onRefresh(invalidCache);
        if (metadataOps != null) {
            metadataOps.close();
        }
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        return metadataOps.listTableNames(ClusterNamespace.getNameFromFullName(dbName));
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        return metadataOps.tableExist(ClusterNamespace.getNameFromFullName(dbName), tblName);
    }

    @Override
    public boolean tableExistInLocal(String dbName, String tblName) {
        makeSureInitialized();
        HMSExternalDatabase hmsExternalDatabase = (HMSExternalDatabase) getDbNullable(dbName);
        if (hmsExternalDatabase == null) {
            return false;
        }
        return hmsExternalDatabase.getTable(ClusterNamespace.getNameFromFullName(tblName)).isPresent();
    }

    public HMSCachedClient getClient() {
        makeSureInitialized();
        return ((HiveMetadataOps) metadataOps).getClient();
    }

    @Override
    public void unregisterDatabase(String dbName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("drop database [{}]", dbName);
        }
        if (useMetaCache.get()) {
            if (isInitialized()) {
                metaCache.invalidate(dbName, Util.genIdByName(getQualifiedName(dbName)));
            }
        } else {
            Long dbId = dbNameToId.remove(dbName);
            if (dbId == null) {
                LOG.warn("drop database [{}] failed", dbName);
            }
            idToDb.remove(dbId);
        }
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateDbCache(getId(), dbName);
    }

    @Override
    public void registerDatabase(long dbId, String dbName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("create database [{}]", dbName);
        }

        ExternalDatabase<? extends ExternalTable> db = buildDbForInit(dbName, dbId, logType);
        if (useMetaCache.get()) {
            if (isInitialized()) {
                metaCache.updateCache(dbName, db, Util.genIdByName(getQualifiedName(dbName)));
            }
        } else {
            dbNameToId.put(dbName, dbId);
            idToDb.put(dbId, db);
        }
    }

    @Override
    public void notifyPropertiesUpdated(Map<String, String> updatedProps) {
        super.notifyPropertiesUpdated(updatedProps);
        String fileMetaCacheTtl = updatedProps.getOrDefault(FILE_META_CACHE_TTL_SECOND, null);
        if (Objects.nonNull(fileMetaCacheTtl)) {
            Env.getCurrentEnv().getExtMetaCacheMgr().getMetaStoreCache(this).setNewFileCache();
        }
    }

    @Override
    public void setDefaultPropsIfMissing(boolean isReplay) {
        super.setDefaultPropsIfMissing(isReplay);
        if (ifNotSetFallbackToSimpleAuth()) {
            // always allow fallback to simple auth, so to support both kerberos and simple auth
            catalogProperty.addProperty(DFSFileSystem.PROP_ALLOW_FALLBACK_TO_SIMPLE_AUTH, "true");
        }
    }

    public String getHiveMetastoreUris() {
        return catalogProperty.getOrDefault(HMSProperties.HIVE_METASTORE_URIS, "");
    }

    public String getHiveVersion() {
        return catalogProperty.getOrDefault(HMSProperties.HIVE_VERSION, "");
    }

    public int getHmsEventsBatchSizePerRpc() {
        return hmsEventsBatchSizePerRpc;
    }

    public boolean isEnableHmsEventsIncrementalSync() {
        return enableHmsEventsIncrementalSync;
    }
}
