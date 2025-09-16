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
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.iceberg.IcebergMetadataOps;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.operations.ExternalMetadataOperations;
import org.apache.doris.datasource.property.metastore.AbstractHMSProperties;
import org.apache.doris.fs.FileSystemProvider;
import org.apache.doris.fs.FileSystemProviderImpl;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
import org.apache.doris.transaction.TransactionManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.iceberg.hive.HiveCatalog;
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
    public static final String PARTITION_CACHE_TTL_SECOND = "partition.cache.ttl-second";
    // broker name for file split and query scan.
    public static final String BIND_BROKER_NAME = "broker.name";
    // Default is false, if set to true, will get table schema from "remoteTable" instead of from hive metastore.
    // This is because for some forward compatibility issue of hive metastore, there maybe
    // "storage schema reading not support" error being thrown.
    // set this to true can avoid this error.
    // But notice that if set to true, the default value of column will be ignored because we cannot get default value
    // from remoteTable object.
    public static final String GET_SCHEMA_FROM_TABLE = "get_schema_from_table";

    private static final int FILE_SYSTEM_EXECUTOR_THREAD_NUM = 16;
    private ThreadPoolExecutor fileSystemExecutor;

    //for "type" = "hms" , but is iceberg table.
    private IcebergMetadataOps icebergMetadataOps;

    private volatile AbstractHMSProperties hmsProperties;

    /**
     * Lazily initializes HMSProperties from catalog properties.
     * This method is thread-safe using double-checked locking.
     * <p>
     * TODO: After all metastore integrations are completed,
     * consider moving this initialization logic into the superclass constructor
     * for unified management.
     * NOTE: Alter operations are temporarily not handled here.
     * We will consider a unified solution for alter support later,
     * as it's currently not feasible to handle it in a common/shared location.
     */
    public AbstractHMSProperties getHmsProperties() {
        makeSureInitialized();
        return hmsProperties;
    }

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
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        // check file.meta.cache.ttl-second parameter
        String fileMetaCacheTtlSecond = catalogProperty.getOrDefault(FILE_META_CACHE_TTL_SECOND, null);
        if (Objects.nonNull(fileMetaCacheTtlSecond) && NumberUtils.toInt(fileMetaCacheTtlSecond, CACHE_NO_TTL)
                < CACHE_TTL_DISABLE_CACHE) {
            throw new DdlException(
                    "The parameter " + FILE_META_CACHE_TTL_SECOND + " is wrong, value is " + fileMetaCacheTtlSecond);
        }

        // check partition.cache.ttl-second parameter
        String partitionCacheTtlSecond = catalogProperty.getOrDefault(PARTITION_CACHE_TTL_SECOND, null);
        if (Objects.nonNull(partitionCacheTtlSecond) && NumberUtils.toInt(partitionCacheTtlSecond, CACHE_NO_TTL)
                < CACHE_TTL_DISABLE_CACHE) {
            throw new DdlException(
                    "The parameter " + PARTITION_CACHE_TTL_SECOND + " is wrong, value is " + partitionCacheTtlSecond);
        }
        catalogProperty.checkMetaStoreAndStorageProperties(AbstractHMSProperties.class);
    }

    @Override
    protected synchronized void initPreExecutionAuthenticator() {
        if (executionAuthenticator == null) {
            executionAuthenticator = hmsProperties.getExecutionAuthenticator();
        }
    }

    @Override
    protected void initLocalObjectsImpl() {
        this.hmsProperties = (AbstractHMSProperties) catalogProperty.getMetastoreProperties();
        initPreExecutionAuthenticator();
        HiveMetadataOps hiveOps = ExternalMetadataOperations.newHiveMetadataOps(hmsProperties.getHiveConf(), this);
        threadPoolWithPreAuth = ThreadPoolManager.newDaemonFixedThreadPoolWithPreAuth(
                ICEBERG_CATALOG_EXECUTOR_THREAD_NUM,
                Integer.MAX_VALUE,
                String.format("hms_iceberg_catalog_%s_executor_pool", name),
                true,
                executionAuthenticator);
        FileSystemProvider fileSystemProvider = new FileSystemProviderImpl(Env.getCurrentEnv().getExtMetaCacheMgr(),
                this.catalogProperty.getStoragePropertiesMap());
        this.fileSystemExecutor = ThreadPoolManager.newDaemonFixedThreadPool(FILE_SYSTEM_EXECUTOR_THREAD_NUM,
                Integer.MAX_VALUE, String.format("hms_committer_%s_file_system_executor_pool", name), true);
        transactionManager = TransactionManagerFactory.createHiveTransactionManager(hiveOps, fileSystemProvider,
                fileSystemExecutor);
        metadataOps = hiveOps;
    }

    @Override
    public synchronized void resetToUninitialized(boolean invalidCache) {
        super.resetToUninitialized(invalidCache);
        if (metadataOps != null) {
            metadataOps.close();
        }
    }

    @Override
    public void onClose() {
        super.onClose();
        if (null != fileSystemExecutor) {
            ThreadPoolManager.shutdownExecutorService(fileSystemExecutor);
        }
        if (null != icebergMetadataOps) {
            icebergMetadataOps.close();
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
    public void registerDatabase(long dbId, String dbName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("create database [{}]", dbName);
        }

        ExternalDatabase<? extends ExternalTable> db = buildDbForInit(dbName, null, dbId, logType, false);
        if (useMetaCache.get()) {
            if (isInitialized()) {
                metaCache.updateCache(db.getRemoteName(), db.getFullName(), db,
                        Util.genIdByName(name, db.getFullName()));
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
        String partitionCacheTtl = updatedProps.getOrDefault(PARTITION_CACHE_TTL_SECOND, null);
        if (Objects.nonNull(fileMetaCacheTtl) || Objects.nonNull(partitionCacheTtl)) {
            Env.getCurrentEnv().getExtMetaCacheMgr().getMetaStoreCache(this).init();
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

    public IcebergMetadataOps getIcebergMetadataOps() {
        makeSureInitialized();
        if (icebergMetadataOps == null) {
            HiveCatalog icebergHiveCatalog = IcebergUtils.createIcebergHiveCatalog(this, getName());
            icebergMetadataOps = ExternalMetadataOperations.newIcebergMetadataOps(this, icebergHiveCatalog);
        }
        return icebergMetadataOps;
    }
}

