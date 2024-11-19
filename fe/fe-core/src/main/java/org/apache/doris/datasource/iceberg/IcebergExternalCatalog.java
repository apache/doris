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

import org.apache.doris.common.security.authentication.PreExecutionAuthenticator;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.operations.ExternalMetadataOperations;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.transaction.TransactionManagerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.iceberg.catalog.Catalog;

import java.util.List;
import java.util.Map;

public abstract class IcebergExternalCatalog extends ExternalCatalog {

    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    public static final String ICEBERG_REST = "rest";
    public static final String ICEBERG_HMS = "hms";
    public static final String ICEBERG_HADOOP = "hadoop";
    public static final String ICEBERG_GLUE = "glue";
    public static final String ICEBERG_DLF = "dlf";
    public static final String EXTERNAL_CATALOG_NAME = "external_catalog.name";
    protected String icebergCatalogType;
    protected Catalog catalog;

    public IcebergExternalCatalog(long catalogId, String name, String comment) {
        super(catalogId, name, InitCatalogLog.Type.ICEBERG, comment);
    }

    // Create catalog based on catalog type
    protected abstract void initCatalog();

    @Override
    protected void initLocalObjectsImpl() {
        preExecutionAuthenticator = new PreExecutionAuthenticator();
        initCatalog();
        IcebergMetadataOps ops = ExternalMetadataOperations.newIcebergMetadataOps(this, catalog);
        transactionManager = TransactionManagerFactory.createIcebergTransactionManager(ops);
        metadataOps = ops;
    }

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
        return metadataOps.listTableNames(dbName);
    }

    protected void initS3Param(Configuration conf) {
        Map<String, String> properties = catalogProperty.getHadoopProperties();
        conf.set(Constants.AWS_CREDENTIALS_PROVIDER, PropertyConverter.getAWSCredentialsProviders(properties));
    }
}
