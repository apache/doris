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

package org.apache.doris.datasource.paimon;

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.operations.ExternalMetadataOperations;
import org.apache.doris.datasource.property.metastore.AbstractPaimonProperties;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

// The subclasses of this class are all deprecated, only for meta persistence compatibility.
public class PaimonExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(PaimonExternalCatalog.class);
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";
    public static final String PAIMON_FILESYSTEM = "filesystem";
    public static final String PAIMON_HMS = "hms";
    public static final String PAIMON_DLF = "dlf";
    public static final String PAIMON_REST = "rest";
    public static final String PAIMON_TABLE_CACHE_ENABLE = "meta.cache.paimon.table.enable";
    public static final String PAIMON_TABLE_CACHE_TTL_SECOND = "meta.cache.paimon.table.ttl-second";
    public static final String PAIMON_TABLE_CACHE_CAPACITY = "meta.cache.paimon.table.capacity";
    protected String catalogType;
    protected Catalog catalog;

    private AbstractPaimonProperties paimonProperties;

    public PaimonExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
                                 String comment) {
        super(catalogId, name, InitCatalogLog.Type.PAIMON, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        paimonProperties = (AbstractPaimonProperties) catalogProperty.getMetastoreProperties();
        catalogType = paimonProperties.getPaimonCatalogType();
        catalog = createCatalog();
        initPreExecutionAuthenticator();
        metadataOps = ExternalMetadataOperations.newPaimonMetaOps(this, catalog);
    }

    @Override
    protected synchronized void initPreExecutionAuthenticator() {
        if (executionAuthenticator == null) {
            executionAuthenticator = paimonProperties.getExecutionAuthenticator();
        }
    }

    public String getCatalogType() {
        makeSureInitialized();
        return catalogType;
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

    public List<Partition> getPaimonPartitions(NameMapping nameMapping) {
        makeSureInitialized();
        try {
            return executionAuthenticator.execute(() -> {
                List<Partition> partitions = new ArrayList<>();
                try {
                    partitions = catalog.listPartitions(Identifier.create(nameMapping.getRemoteDbName(),
                            nameMapping.getRemoteTblName()));
                } catch (Catalog.TableNotExistException e) {
                    LOG.warn("TableNotExistException", e);
                }
                return partitions;
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Paimon table partitions:" + getName() + "."
                    + nameMapping.getRemoteDbName() + "." + nameMapping.getRemoteTblName() + ", because "
                    + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    public org.apache.paimon.table.Table getPaimonTable(NameMapping nameMapping) {
        return getPaimonTable(nameMapping, null, null);
    }

    public org.apache.paimon.table.Table getPaimonTable(NameMapping nameMapping, String branch,
            String queryType) {
        makeSureInitialized();
        try {
            Identifier identifier;
            if (branch != null && queryType != null) {
                identifier = new Identifier(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(),
                        branch, queryType);
            } else if (branch != null) {
                identifier = new Identifier(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(),
                        branch);
            } else if (queryType != null) {
                identifier = new Identifier(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(),
                        "main", queryType);
            } else {
                identifier = new Identifier(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName());
            }
            return executionAuthenticator.execute(() -> catalog.getTable(identifier));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Paimon table:" + getName() + "."
                    + nameMapping.getRemoteDbName() + "." + nameMapping.getRemoteTblName() + "$" + queryType
                    + ", because " + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    protected Catalog createCatalog() {
        try {
            return paimonProperties.initializeCatalog(getName(), new ArrayList<>(catalogProperty
                    .getOrderedStoragePropertiesList()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create catalog, catalog name: " + getName() + ", exception: "
                    + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    public Map<String, String> getPaimonOptionsMap() {
        makeSureInitialized();
        return paimonProperties.getCatalogOptionsMap();
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        CacheSpec.checkBooleanProperty(catalogProperty.getOrDefault(PAIMON_TABLE_CACHE_ENABLE, null),
                PAIMON_TABLE_CACHE_ENABLE);
        CacheSpec.checkLongProperty(catalogProperty.getOrDefault(PAIMON_TABLE_CACHE_TTL_SECOND, null),
                -1L, PAIMON_TABLE_CACHE_TTL_SECOND);
        CacheSpec.checkLongProperty(catalogProperty.getOrDefault(PAIMON_TABLE_CACHE_CAPACITY, null),
                0L, PAIMON_TABLE_CACHE_CAPACITY);
        catalogProperty.checkMetaStoreAndStorageProperties(AbstractPaimonProperties.class);
    }

    @Override
    public void notifyPropertiesUpdated(Map<String, String> updatedProps) {
        super.notifyPropertiesUpdated(updatedProps);
        String tableCacheEnable = updatedProps.getOrDefault(PAIMON_TABLE_CACHE_ENABLE, null);
        String tableCacheTtl = updatedProps.getOrDefault(PAIMON_TABLE_CACHE_TTL_SECOND, null);
        String tableCacheCapacity = updatedProps.getOrDefault(PAIMON_TABLE_CACHE_CAPACITY, null);
        if (Objects.nonNull(tableCacheEnable) || Objects.nonNull(tableCacheTtl)
                || Objects.nonNull(tableCacheCapacity)) {
            PaimonUtils.getPaimonMetadataCache(this).init();
        }
    }

    @Override
    public void onClose() {
        super.onClose();
        if (null != catalog) {
            try {
                catalog.close();
            } catch (Exception e) {
                LOG.warn("Failed to close paimon catalog: {}", getName(), e);
            }
        }
    }
}
