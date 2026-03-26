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

package org.apache.doris.datasource.deltalake;

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.metastore.AbstractHiveProperties;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * External catalog for Delta Lake data sources.
 * Uses HMS to discover databases and tables, then reads Delta Lake metadata
 * via Delta Kernel API.
 */
public class DeltaLakeExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeExternalCatalog.class);

    private volatile AbstractHiveProperties hmsProperties;

    public DeltaLakeExternalCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment) {
        super(catalogId, name, InitCatalogLog.Type.DELTALAKE, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        catalogProperty.checkMetaStoreAndStorageProperties(AbstractHiveProperties.class);
    }

    @Override
    protected synchronized void initPreExecutionAuthenticator() {
        if (executionAuthenticator == null) {
            executionAuthenticator = hmsProperties.getExecutionAuthenticator();
        }
    }

    @Override
    protected void initLocalObjectsImpl() {
        this.hmsProperties = (AbstractHiveProperties) catalogProperty.getMetastoreProperties();
        initPreExecutionAuthenticator();
        metadataOps = new DeltaLakeMetadataOps(this, hmsProperties);
    }

    @Override
    public void onClose() {
        super.onClose();
        if (metadataOps != null) {
            metadataOps.close();
            metadataOps = null;
        }
    }

    @Override
    protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
        return metadataOps.listTableNames(dbName);
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        return metadataOps.tableExist(dbName, tblName);
    }

    @Override
    public void setDefaultPropsIfMissing(boolean isReplay) {
        super.setDefaultPropsIfMissing(isReplay);
        if (ifNotSetFallbackToSimpleAuth()) {
            catalogProperty.addProperty(DFSFileSystem.PROP_ALLOW_FALLBACK_TO_SIMPLE_AUTH, "true");
        }
    }

    public AbstractHiveProperties getHmsProperties() {
        makeSureInitialized();
        return hmsProperties;
    }
}
