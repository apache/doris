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
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.metastore.AbstractPaimonProperties;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// The subclasses of this class are all deprecated, only for meta persistence compatibility.
public class PaimonExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(PaimonExternalCatalog.class);
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";
    public static final String PAIMON_FILESYSTEM = "filesystem";
    public static final String PAIMON_HMS = "hms";
    public static final String PAIMON_DLF = "dlf";
    public static final String PAIMON_REST = "rest";
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
        try {
            paimonProperties = (AbstractPaimonProperties) MetastoreProperties.create(catalogProperty.getProperties());
        } catch (UserException e) {
            throw new IllegalArgumentException("Failed to create Paimon properties from catalog properties,exception: "
                    + ExceptionUtils.getRootCauseMessage(e), e);
        }
        catalogType = paimonProperties.getPaimonCatalogType();
        catalog = createCatalog();
        initPreExecutionAuthenticator();
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

    protected List<String> listDatabaseNames() {
        try {
            return executionAuthenticator.execute(() -> new ArrayList<>(catalog.listDatabases()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to list databases names, catalog name: " + getName(), e);
        }
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        try {
            return executionAuthenticator.execute(() -> {
                try {
                    catalog.getTable(Identifier.create(dbName, tblName));
                    return true;
                } catch (TableNotExistException e) {
                    return false;
                }
            });

        } catch (Exception e) {
            throw new RuntimeException("Failed to check table existence, catalog name: " + getName()
                    + "error message is:" + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        try {
            return executionAuthenticator.execute(() -> {
                List<String> tableNames = null;
                try {
                    tableNames = catalog.listTables(dbName);
                } catch (Catalog.DatabaseNotExistException e) {
                    LOG.warn("DatabaseNotExistException", e);
                }
                return tableNames;
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to list table names, catalog name: " + getName(), e);
        }
    }

    public org.apache.paimon.table.Table getPaimonTable(NameMapping nameMapping) {
        makeSureInitialized();
        try {
            return executionAuthenticator.execute(() -> catalog.getTable(Identifier.create(nameMapping
                    .getRemoteDbName(), nameMapping.getRemoteTblName())));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Paimon table:" + getName() + "." + nameMapping.getLocalDbName()
                    + "." + nameMapping.getLocalTblName() + ", because " + ExceptionUtils.getRootCauseMessage(e), e);
        }
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

    public org.apache.paimon.table.Table getPaimonSystemTable(NameMapping nameMapping, String queryType) {
        return getPaimonSystemTable(nameMapping, null, queryType);
    }

    public org.apache.paimon.table.Table getPaimonSystemTable(NameMapping nameMapping,
                                                              String branch, String queryType) {
        makeSureInitialized();
        try {
            return executionAuthenticator.execute(() -> catalog.getTable(new Identifier(nameMapping.getRemoteDbName(),
                    nameMapping.getRemoteTblName(), branch, queryType)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Paimon system table:" + getName() + "."
                    + nameMapping.getRemoteDbName() + "." + nameMapping.getRemoteTblName() + "$" + queryType
                    + ", because " + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }


    protected Catalog createCatalog() {
        try {
            return paimonProperties.initializeCatalog(getName(), new ArrayList<>(catalogProperty
                    .getStoragePropertiesMap().values()));
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
        if (null != paimonProperties) {
            try {
                this.paimonProperties = (AbstractPaimonProperties) MetastoreProperties
                        .create(catalogProperty.getProperties());
            } catch (UserException e) {
                throw new DdlException("Failed to create Paimon properties from catalog properties, exception: "
                        + ExceptionUtils.getRootCauseMessage(e), e);
            }
        }
    }
}
