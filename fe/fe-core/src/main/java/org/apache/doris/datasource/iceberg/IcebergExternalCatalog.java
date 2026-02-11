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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.operations.ExternalMetadataOperations;
import org.apache.doris.datasource.property.metastore.AbstractIcebergProperties;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplacePartitionFieldOp;
import org.apache.doris.transaction.TransactionManagerFactory;

import org.apache.iceberg.catalog.Catalog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class IcebergExternalCatalog extends ExternalCatalog {

    private static final Logger LOG = LogManager.getLogger(IcebergExternalCatalog.class);
    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    public static final String ICEBERG_REST = "rest";
    public static final String ICEBERG_HMS = "hms";
    public static final String ICEBERG_HADOOP = "hadoop";
    public static final String ICEBERG_GLUE = "glue";
    public static final String ICEBERG_DLF = "dlf";
    public static final String ICEBERG_JDBC = "jdbc";
    public static final String ICEBERG_S3_TABLES = "s3tables";
    public static final String EXTERNAL_CATALOG_NAME = "external_catalog.name";
    public static final String ICEBERG_TABLE_CACHE_ENABLE = "meta.cache.iceberg.table.enable";
    public static final String ICEBERG_TABLE_CACHE_TTL_SECOND = "meta.cache.iceberg.table.ttl-second";
    public static final String ICEBERG_TABLE_CACHE_CAPACITY = "meta.cache.iceberg.table.capacity";
    public static final String ICEBERG_MANIFEST_CACHE_ENABLE = "meta.cache.iceberg.manifest.enable";
    public static final String ICEBERG_MANIFEST_CACHE_TTL_SECOND = "meta.cache.iceberg.manifest.ttl-second";
    public static final String ICEBERG_MANIFEST_CACHE_CAPACITY = "meta.cache.iceberg.manifest.capacity";
    public static final boolean DEFAULT_ICEBERG_MANIFEST_CACHE_ENABLE = false;
    public static final long DEFAULT_ICEBERG_MANIFEST_CACHE_CAPACITY = 1024;
    public static final long DEFAULT_ICEBERG_MANIFEST_CACHE_TTL_SECOND = 48 * 60 * 60;
    protected String icebergCatalogType;
    protected Catalog catalog;

    private AbstractIcebergProperties msProperties;

    public IcebergExternalCatalog(long catalogId, String name, String comment) {
        super(catalogId, name, InitCatalogLog.Type.ICEBERG, comment);
    }

    // Create catalog based on catalog type
    protected void initCatalog() {
        try {
            msProperties = (AbstractIcebergProperties) catalogProperty.getMetastoreProperties();
            this.catalog = msProperties.initializeCatalog(getName(), catalogProperty
                    .getOrderedStoragePropertiesList());

            this.icebergCatalogType = msProperties.getIcebergCatalogType();
        } catch (ClassCastException e) {
            throw new RuntimeException("Invalid properties for Iceberg catalog: " + getProperties(), e);
        } catch (Exception e) {
            throw new RuntimeException("Unexpected error while initializing Iceberg catalog: " + e.getMessage(), e);
        }
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        CacheSpec.checkBooleanProperty(catalogProperty.getOrDefault(ICEBERG_TABLE_CACHE_ENABLE, null),
                ICEBERG_TABLE_CACHE_ENABLE);
        CacheSpec.checkLongProperty(catalogProperty.getOrDefault(ICEBERG_TABLE_CACHE_TTL_SECOND, null),
                -1L, ICEBERG_TABLE_CACHE_TTL_SECOND);
        CacheSpec.checkLongProperty(catalogProperty.getOrDefault(ICEBERG_TABLE_CACHE_CAPACITY, null),
                0L, ICEBERG_TABLE_CACHE_CAPACITY);

        CacheSpec.checkBooleanProperty(catalogProperty.getOrDefault(ICEBERG_MANIFEST_CACHE_ENABLE, null),
                ICEBERG_MANIFEST_CACHE_ENABLE);
        CacheSpec.checkLongProperty(catalogProperty.getOrDefault(ICEBERG_MANIFEST_CACHE_TTL_SECOND, null),
                -1L, ICEBERG_MANIFEST_CACHE_TTL_SECOND);
        CacheSpec.checkLongProperty(catalogProperty.getOrDefault(ICEBERG_MANIFEST_CACHE_CAPACITY, null),
                0L, ICEBERG_MANIFEST_CACHE_CAPACITY);
        catalogProperty.checkMetaStoreAndStorageProperties(AbstractIcebergProperties.class);
    }

    @Override
    public void notifyPropertiesUpdated(Map<String, String> updatedProps) {
        super.notifyPropertiesUpdated(updatedProps);
        String tableCacheEnable = updatedProps.getOrDefault(ICEBERG_TABLE_CACHE_ENABLE, null);
        String tableCacheTtl = updatedProps.getOrDefault(ICEBERG_TABLE_CACHE_TTL_SECOND, null);
        String tableCacheCapacity = updatedProps.getOrDefault(ICEBERG_TABLE_CACHE_CAPACITY, null);
        String manifestCacheEnable = updatedProps.getOrDefault(ICEBERG_MANIFEST_CACHE_ENABLE, null);
        String manifestCacheCapacity = updatedProps.getOrDefault(ICEBERG_MANIFEST_CACHE_CAPACITY, null);
        String manifestCacheTtl = updatedProps.getOrDefault(ICEBERG_MANIFEST_CACHE_TTL_SECOND, null);
        if (Objects.nonNull(tableCacheEnable) || Objects.nonNull(tableCacheTtl) || Objects.nonNull(tableCacheCapacity)
                || Objects.nonNull(manifestCacheEnable) || Objects.nonNull(manifestCacheCapacity)
                || Objects.nonNull(manifestCacheTtl)) {
            Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache(this).init();
        }
    }

    @Override
    protected synchronized void initPreExecutionAuthenticator() {
        if (executionAuthenticator == null) {
            executionAuthenticator = msProperties.getExecutionAuthenticator();
        }
    }

    @Override
    protected void initLocalObjectsImpl() {
        initCatalog();
        initPreExecutionAuthenticator();
        IcebergMetadataOps ops = ExternalMetadataOperations.newIcebergMetadataOps(this, catalog);
        transactionManager = TransactionManagerFactory.createIcebergTransactionManager(ops);
        threadPoolWithPreAuth = ThreadPoolManager.newDaemonFixedThreadPoolWithPreAuth(
                ICEBERG_CATALOG_EXECUTOR_THREAD_NUM,
                Integer.MAX_VALUE,
                String.format("iceberg_catalog_%s_executor_pool", name),
                true,
                executionAuthenticator);
        metadataOps = ops;
    }

    /**
     * Returns the underlying {@link Catalog} instance used by this external catalog.
     *
     * <p><strong>Warning:</strong> This method does not handle any authentication logic. If the
     * returned catalog implementation relies on external systems
     * that require authentication — especially in environments where Kerberos is enabled — the caller is
     * fully responsible for ensuring the appropriate authentication has been performed <em>before</em>
     * invoking this method.
     * <p>Failing to authenticate beforehand may result in authorization errors or IO failures.
     *
     * @return the underlying catalog instance
     */
    public Catalog getCatalog() {
        makeSureInitialized();
        return ((IcebergMetadataOps) metadataOps).getCatalog();
    }

    public String getIcebergCatalogType() {
        makeSureInitialized();
        return icebergCatalogType;
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return metadataOps.tableExist(dbName, tblName);
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        // On the Doris side, the result of SHOW TABLES for Iceberg external tables includes both tables and views,
        // so the combined set of tables and views is used here.
        List<String> tableNames = metadataOps.listTableNames(dbName);
        List<String> viewNames = metadataOps.listViewNames(dbName);
        tableNames.addAll(viewNames);
        return tableNames;
    }

    @Override
    public void onClose() {
        super.onClose();
        if (null != catalog) {
            try {
                if (catalog instanceof AutoCloseable) {
                    ((AutoCloseable) catalog).close();
                }
                catalog = null;
            } catch (Exception e) {
                LOG.warn("Failed to close iceberg catalog: {}", getName(), e);
            }
        }
    }

    @Override
    public boolean viewExists(String dbName, String viewName) {
        return metadataOps.viewExists(dbName, viewName);
    }

    /**
     * Add partition field to Iceberg table for partition evolution
     */
    public void addPartitionField(IcebergExternalTable table, AddPartitionFieldOp op, long updateTime)
            throws UserException {
        makeSureInitialized();
        if (metadataOps == null) {
            throw new UserException("Add partition field operation is not supported for catalog: " + getName());
        }
        ((IcebergMetadataOps) metadataOps).addPartitionField(table, op, updateTime);
        Env.getCurrentEnv().getEditLog()
                .logRefreshExternalTable(
                        ExternalObjectLog.createForRefreshTable(table.getCatalog().getId(),
                                table.getDbName(), table.getName(), updateTime));
    }

    /**
     * Drop partition field from Iceberg table for partition evolution
     */
    public void dropPartitionField(IcebergExternalTable table, DropPartitionFieldOp op, long updateTime)
            throws UserException {
        makeSureInitialized();
        if (metadataOps == null) {
            throw new UserException("Drop partition field operation is not supported for catalog: " + getName());
        }
        ((IcebergMetadataOps) metadataOps).dropPartitionField(table, op, updateTime);
        Env.getCurrentEnv().getEditLog()
                .logRefreshExternalTable(
                        ExternalObjectLog.createForRefreshTable(table.getCatalog().getId(),
                                table.getDbName(), table.getName(), updateTime));
    }

    /**
     * Replace partition field in Iceberg table for partition evolution
     */
    public void replacePartitionField(IcebergExternalTable table,
            ReplacePartitionFieldOp op, long updateTime) throws UserException {
        makeSureInitialized();
        if (metadataOps == null) {
            throw new UserException("Replace partition field operation is not supported for catalog: " + getName());
        }
        ((IcebergMetadataOps) metadataOps).replacePartitionField(table, op, updateTime);
        Env.getCurrentEnv().getEditLog()
                .logRefreshExternalTable(
                        ExternalObjectLog.createForRefreshTable(table.getCatalog().getId(),
                                table.getDbName(), table.getName(), updateTime));
    }
}
