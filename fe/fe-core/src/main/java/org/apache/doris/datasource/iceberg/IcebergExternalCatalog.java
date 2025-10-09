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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.operations.ExternalMetadataOperations;
import org.apache.doris.datasource.property.metastore.AbstractIcebergProperties;
import org.apache.doris.transaction.TransactionManagerFactory;

import org.apache.iceberg.catalog.Catalog;

import java.util.ArrayList;
import java.util.List;

public abstract class IcebergExternalCatalog extends ExternalCatalog {

    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    public static final String ICEBERG_REST = "rest";
    public static final String ICEBERG_HMS = "hms";
    public static final String ICEBERG_HADOOP = "hadoop";
    public static final String ICEBERG_GLUE = "glue";
    public static final String ICEBERG_DLF = "dlf";
    public static final String ICEBERG_S3_TABLES = "s3tables";
    public static final String EXTERNAL_CATALOG_NAME = "external_catalog.name";
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
            this.catalog = msProperties.initializeCatalog(getName(), new ArrayList<>(catalogProperty
                    .getStoragePropertiesMap().values()));

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
        catalogProperty.checkMetaStoreAndStorageProperties(AbstractIcebergProperties.class);
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
            catalog = null;
        }
    }

    @Override
    public boolean viewExists(String dbName, String viewName) {
        return metadataOps.viewExists(dbName, viewName);
    }
}
