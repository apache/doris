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
import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.security.authentication.HadoopUGI;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.datasource.property.constants.PaimonProperties;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class PaimonExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(PaimonExternalCatalog.class);
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";
    public static final String PAIMON_FILESYSTEM = "filesystem";
    public static final String PAIMON_HMS = "hms";
    protected String catalogType;
    protected Catalog catalog;
    protected AuthenticationConfig authConf;

    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            PaimonProperties.WAREHOUSE
    );

    public PaimonExternalCatalog(long catalogId, String name, String resource,
                                 Map<String, String> props, String comment) {
        super(catalogId, name, InitCatalogLog.Type.PAIMON, comment);
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        Configuration conf = DFSFileSystem.getHdfsConf(ifNotSetFallbackToSimpleAuth());
        for (Map.Entry<String, String> propEntry : this.catalogProperty.getHadoopProperties().entrySet()) {
            conf.set(propEntry.getKey(), propEntry.getValue());
        }
        authConf = AuthenticationConfig.getKerberosConfig(conf,
                AuthenticationConfig.HADOOP_KERBEROS_PRINCIPAL,
                AuthenticationConfig.HADOOP_KERBEROS_KEYTAB);
    }

    public String getCatalogType() {
        makeSureInitialized();
        return catalogType;
    }

    protected List<String> listDatabaseNames() {
        return HadoopUGI.ugiDoAs(authConf, () -> new ArrayList<>(catalog.listDatabases()));
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return HadoopUGI.ugiDoAs(authConf, () -> catalog.tableExists(Identifier.create(dbName, tblName)));
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        return HadoopUGI.ugiDoAs(authConf, () -> {
            List<String> tableNames = null;
            try {
                tableNames = catalog.listTables(dbName);
            } catch (Catalog.DatabaseNotExistException e) {
                LOG.warn("DatabaseNotExistException", e);
            }
            return tableNames;
        });
    }

    public org.apache.paimon.table.Table getPaimonTable(String dbName, String tblName) {
        makeSureInitialized();
        return HadoopUGI.ugiDoAs(authConf, () -> {
            org.apache.paimon.table.Table table = null;
            try {
                table = catalog.getTable(Identifier.create(dbName, tblName));
            } catch (Catalog.TableNotExistException e) {
                LOG.warn("TableNotExistException", e);
            }
            return table;
        });
    }

    protected String getPaimonCatalogType(String catalogType) {
        if (PAIMON_HMS.equalsIgnoreCase(catalogType)) {
            return PaimonProperties.PAIMON_HMS_CATALOG;
        } else {
            return PaimonProperties.PAIMON_FILESYSTEM_CATALOG;
        }
    }

    protected Catalog createCatalog() {
        return HadoopUGI.ugiDoAs(authConf, () -> {
            Options options = new Options();
            Map<String, String> paimonOptionsMap = getPaimonOptionsMap();
            for (Map.Entry<String, String> kv : paimonOptionsMap.entrySet()) {
                options.set(kv.getKey(), kv.getValue());
            }
            CatalogContext context = CatalogContext.create(options, getConfiguration());
            return createCatalogImpl(context);
        });
    }

    protected Catalog createCatalogImpl(CatalogContext context) {
        return CatalogFactory.createCatalog(context);
    }

    public Map<String, String> getPaimonOptionsMap() {
        Map<String, String> properties = catalogProperty.getHadoopProperties();
        Map<String, String> options = Maps.newHashMap();
        options.put(PaimonProperties.WAREHOUSE, properties.get(PaimonProperties.WAREHOUSE));
        setPaimonCatalogOptions(properties, options);
        setPaimonExtraOptions(properties, options);
        return options;
    }

    protected abstract void setPaimonCatalogOptions(Map<String, String> properties, Map<String, String> options);

    protected void setPaimonExtraOptions(Map<String, String> properties, Map<String, String> options) {
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            if (kv.getKey().startsWith(PaimonProperties.PAIMON_PREFIX)) {
                options.put(kv.getKey().substring(PaimonProperties.PAIMON_PREFIX.length()), kv.getValue());
            }
        }

        // hive version.
        // This property is used for both FE and BE, so it has no "paimon." prefix.
        // We need to handle it separately.
        if (properties.containsKey(HMSProperties.HIVE_VERSION)) {
            options.put(HMSProperties.HIVE_VERSION, properties.get(HMSProperties.HIVE_VERSION));
        }
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        for (String requiredProperty : REQUIRED_PROPERTIES) {
            if (!catalogProperty.getProperties().containsKey(requiredProperty)) {
                throw new DdlException("Required property '" + requiredProperty + "' is missing");
            }
        }
    }
}
