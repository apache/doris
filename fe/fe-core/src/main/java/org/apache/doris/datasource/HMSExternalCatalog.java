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

package org.apache.doris.datasource;

import org.apache.doris.catalog.AuthType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.catalog.external.HMSExternalDatabase;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.hive.HMSCachedClient;
import org.apache.doris.datasource.hive.HMSCachedClientFactory;
import org.apache.doris.datasource.hive.event.MetastoreNotificationFetchException;
import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.HMSProperties;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * External catalog for hive metastore compatible data sources.
 */
public class HMSExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(HMSExternalCatalog.class);

    private static final int MIN_CLIENT_POOL_SIZE = 8;
    protected HMSCachedClient client;
    // Record the latest synced event id when processing hive events
    // Must set to -1 otherwise client.getNextNotification will throw exception
    // Reference to https://github.com/apDdlache/doris/issues/18251
    private long lastSyncedEventId = -1L;
    public static final String ENABLE_SELF_SPLITTER = "enable.self.splitter";
    public static final String FILE_META_CACHE_TTL_SECOND = "file.meta.cache.ttl-second";
    // broker name for file split and query scan.
    public static final String BIND_BROKER_NAME = "broker.name";
    private static final String PROP_ALLOW_FALLBACK_TO_SIMPLE_AUTH = "ipc.client.fallback-to-simple-auth-allowed";

    // -1 means file cache no ttl set
    public static final int FILE_META_CACHE_NO_TTL = -1;
    // 0 means file cache is disabled; >0 means file cache with ttl;
    public static final int FILE_META_CACHE_TTL_DISABLE_CACHE = 0;

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

    public String getHiveMetastoreUris() {
        return catalogProperty.getOrDefault(HMSProperties.HIVE_METASTORE_URIS, "");
    }

    public String getHiveVersion() {
        return catalogProperty.getOrDefault(HMSProperties.HIVE_VERSION, "");
    }

    protected List<String> listDatabaseNames() {
        return client.getAllDatabases();
    }

    @Override
    protected void initLocalObjectsImpl() {
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
            String authentication = catalogProperty.getOrDefault(
                    HdfsResource.HADOOP_SECURITY_AUTHENTICATION, "");
            if (AuthType.KERBEROS.getDesc().equals(authentication)) {
                hiveConf.set(HdfsResource.HADOOP_SECURITY_AUTHENTICATION, authentication);
                UserGroupInformation.setConfiguration(hiveConf);
                try {
                    /**
                     * Because metastore client is created by using
                     * {@link org.apache.hadoop.hive.metastore.RetryingMetaStoreClient#getProxy}
                     * it will relogin when TGT is expired, so we don't need to relogin manually.
                     */
                    UserGroupInformation.loginUserFromKeytab(
                            catalogProperty.getOrDefault(HdfsResource.HADOOP_KERBEROS_PRINCIPAL, ""),
                            catalogProperty.getOrDefault(HdfsResource.HADOOP_KERBEROS_KEYTAB, ""));
                } catch (IOException e) {
                    throw new HMSClientException("login with kerberos auth failed for catalog %s", e, this.getName());
                }
            }
        }
        client = HMSCachedClientFactory.createCachedClient(hiveConf,
                Math.max(MIN_CLIENT_POOL_SIZE, Config.max_external_cache_loader_thread_pool_size), jdbcClientConfig);
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        HMSExternalDatabase hmsExternalDatabase = (HMSExternalDatabase) idToDb.get(dbNameToId.get(dbName));
        if (hmsExternalDatabase != null && hmsExternalDatabase.isInitialized()) {
            List<String> names = Lists.newArrayList();
            hmsExternalDatabase.getTables().forEach(table -> names.add(table.getName()));
            return names;
        } else {
            return client.getAllTables(getRealTableName(dbName));
        }
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        return client.tableExists(getRealTableName(dbName), tblName);
    }

    @Override
    public boolean tableExistInLocal(String dbName, String tblName) {
        makeSureInitialized();
        HMSExternalDatabase hmsExternalDatabase = (HMSExternalDatabase) idToDb.get(dbNameToId.get(dbName));
        if (hmsExternalDatabase == null) {
            return false;
        }
        return hmsExternalDatabase.getTable(getRealTableName(tblName)).isPresent();
    }

    public HMSCachedClient getClient() {
        makeSureInitialized();
        return client;
    }

    public void setLastSyncedEventId(long lastSyncedEventId) {
        this.lastSyncedEventId = lastSyncedEventId;
    }

    public NotificationEventResponse getNextEventResponse(HMSExternalCatalog hmsExternalCatalog)
            throws MetastoreNotificationFetchException {
        makeSureInitialized();
        long currentEventId = getCurrentEventId();
        if (lastSyncedEventId < 0) {
            refreshCatalog(hmsExternalCatalog);
            // invoke getCurrentEventId() and save the event id before refresh catalog to avoid missing events
            // but set lastSyncedEventId to currentEventId only if there is not any problems when refreshing catalog
            lastSyncedEventId = currentEventId;
            LOG.info(
                    "First pulling events on catalog [{}],refreshCatalog and init lastSyncedEventId,"
                            + "lastSyncedEventId is [{}]",
                    hmsExternalCatalog.getName(), lastSyncedEventId);
            return null;
        }

        LOG.debug("Catalog [{}] getNextEventResponse, currentEventId is {},lastSyncedEventId is {}",
                hmsExternalCatalog.getName(), currentEventId, lastSyncedEventId);
        if (currentEventId == lastSyncedEventId) {
            LOG.info("Event id not updated when pulling events on catalog [{}]", hmsExternalCatalog.getName());
            return null;
        }

        try {
            return client.getNextNotification(lastSyncedEventId, Config.hms_events_batch_size_per_rpc, null);
        } catch (MetastoreNotificationFetchException e) {
            // Need a fallback to handle this because this error state can not be recovered until restarting FE
            if (StringUtils.isNotEmpty(e.getMessage())
                        && e.getMessage().contains(HiveMetaStoreClient.REPL_EVENTS_MISSING_IN_METASTORE)) {
                refreshCatalog(hmsExternalCatalog);
                // set lastSyncedEventId to currentEventId after refresh catalog successfully
                lastSyncedEventId = currentEventId;
                LOG.warn("Notification events are missing, maybe an event can not be handled "
                        + "or processing rate is too low, fallback to refresh the catalog");
                return null;
            }
            throw e;
        }
    }

    private void refreshCatalog(HMSExternalCatalog hmsExternalCatalog) {
        CatalogLog log = new CatalogLog();
        log.setCatalogId(hmsExternalCatalog.getId());
        log.setInvalidCache(true);
        Env.getCurrentEnv().getCatalogMgr().refreshCatalog(log);
    }

    private long getCurrentEventId() {
        makeSureInitialized();
        CurrentNotificationEventId currentNotificationEventId = client.getCurrentNotificationEventId();
        if (currentNotificationEventId == null) {
            LOG.warn("Get currentNotificationEventId is null");
            return -1;
        }
        return currentNotificationEventId.getEventId();
    }

    @Override
    public void dropDatabaseForReplay(String dbName) {
        LOG.debug("drop database [{}]", dbName);
        Long dbId = dbNameToId.remove(dbName);
        if (dbId == null) {
            LOG.warn("drop database [{}] failed", dbName);
        }
        idToDb.remove(dbId);
    }

    @Override
    public void createDatabaseForReplay(long dbId, String dbName) {
        LOG.debug("create database [{}]", dbName);
        dbNameToId.put(dbName, dbId);
        ExternalDatabase<? extends ExternalTable> db = getDbForInit(dbName, dbId, logType);
        idToDb.put(dbId, db);
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
    public void setDefaultPropsWhenCreating(boolean isReplay) {
        if (isReplay) {
            return;
        }
        if (catalogProperty.getOrDefault(PROP_ALLOW_FALLBACK_TO_SIMPLE_AUTH, "").isEmpty()) {
            // always allow fallback to simple auth, so to support both kerberos and simple auth
            catalogProperty.addProperty(PROP_ALLOW_FALLBACK_TO_SIMPLE_AUTH, "true");
        }
    }
}
