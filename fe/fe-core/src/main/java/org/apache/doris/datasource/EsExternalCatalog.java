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


import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EsResource;
import org.apache.doris.catalog.external.EsExternalDatabase;
import org.apache.doris.external.elasticsearch.EsRestClient;
import org.apache.doris.external.elasticsearch.EsUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * External catalog for elasticsearch
 */
@Getter
public class EsExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(EsExternalCatalog.class);

    public static final String DEFAULT_DB = "default_db";

    private EsRestClient esRestClient;

    /**
     * Default constructor for EsExternalCatalog.
     */
    public EsExternalCatalog(long catalogId, String name, String resource, Map<String, String> props) {
        super(catalogId, name);
        this.type = "es";
        this.catalogProperty = new CatalogProperty(resource, processCompatibleProperties(props));
    }

    private Map<String, String> processCompatibleProperties(Map<String, String> props) {
        // Compatible with "Doris On ES" interfaces
        Map<String, String> properties = Maps.newHashMap();
        for (Map.Entry<String, String> kv : props.entrySet()) {
            properties.put(StringUtils.removeStart(kv.getKey(), EsResource.ES_PROPERTIES_PREFIX), kv.getValue());
        }
        // nodes = properties.get(EsResource.HOSTS).trim().split(",");
        if (properties.containsKey("ssl")) {
            properties.put(EsResource.HTTP_SSL_ENABLED, properties.remove("ssl"));
        }
        if (!properties.containsKey(EsResource.HTTP_SSL_ENABLED)) {
            properties.put(EsResource.HTTP_SSL_ENABLED, String.valueOf(false));
        }
        if (properties.containsKey("username")) {
            properties.put(EsResource.USER, properties.remove("username"));
        }
        if (properties.containsKey("doc_value_scan")) {
            properties.put(EsResource.DOC_VALUE_SCAN, properties.remove("doc_value_scan"));
        }
        if (!properties.containsKey(EsResource.DOC_VALUE_SCAN)) {
            properties.put(EsResource.DOC_VALUE_SCAN, "true");
        }
        if (properties.containsKey("keyword_sniff")) {
            properties.put(EsResource.KEYWORD_SNIFF, properties.remove("keyword_sniff"));
        }
        if (!properties.containsKey(EsResource.KEYWORD_SNIFF)) {
            properties.put(EsResource.KEYWORD_SNIFF, "true");
        }
        if (!properties.containsKey(EsResource.NODES_DISCOVERY)) {
            properties.put(EsResource.NODES_DISCOVERY, "true");
        }
        return properties;
    }

    public String[] getNodes() {
        String hosts = catalogProperty.getOrDefault(EsResource.HOSTS, "");
        return hosts.trim().split(",");
    }

    public String getUsername() {
        return catalogProperty.getOrDefault(EsResource.USER, "");
    }

    public String getPassword() {
        return catalogProperty.getOrDefault(EsResource.PASSWORD, "");
    }

    public boolean enableDocValueScan() {
        return Boolean.valueOf(catalogProperty.getOrDefault(EsResource.DOC_VALUE_SCAN, "true"));
    }

    public boolean enableKeywordSniff() {
        return Boolean.valueOf(catalogProperty.getOrDefault(EsResource.KEYWORD_SNIFF, "true"));
    }

    public boolean enableSsl() {
        return Boolean.valueOf(catalogProperty.getOrDefault(EsResource.HTTP_SSL_ENABLED, "false"));
    }

    public boolean enableNodesDiscovery() {
        return Boolean.valueOf(catalogProperty.getOrDefault(EsResource.NODES_DISCOVERY, "true"));
    }

    @Override
    protected void initLocalObjectsImpl() {
        esRestClient = new EsRestClient(getNodes(), getUsername(), getPassword(), enableSsl());
    }

    @Override
    protected void init() {
        InitCatalogLog initCatalogLog = new InitCatalogLog();
        initCatalogLog.setCatalogId(id);
        initCatalogLog.setType(InitCatalogLog.Type.ES);
        if (dbNameToId != null && dbNameToId.containsKey(DEFAULT_DB)) {
            idToDb.get(dbNameToId.get(DEFAULT_DB)).setUnInitialized(invalidCacheInInit);
            initCatalogLog.addRefreshDb(dbNameToId.get(DEFAULT_DB));
        } else {
            dbNameToId = Maps.newConcurrentMap();
            idToDb = Maps.newConcurrentMap();
            long defaultDbId = Env.getCurrentEnv().getNextId();
            dbNameToId.put(DEFAULT_DB, defaultDbId);
            EsExternalDatabase db = new EsExternalDatabase(this, defaultDbId, DEFAULT_DB);
            idToDb.put(defaultDbId, db);
            initCatalogLog.addCreateDb(defaultDbId, DEFAULT_DB);
        }
        Env.getCurrentEnv().getEditLog().logInitCatalog(initCatalogLog);
    }

    @Override
    public List<String> listDatabaseNames(SessionContext ctx) {
        makeSureInitialized();
        return new ArrayList<>(dbNameToId.keySet());
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        EsExternalDatabase db = (EsExternalDatabase) idToDb.get(dbNameToId.get(dbName));
        if (db != null && db.isInitialized()) {
            List<String> names = Lists.newArrayList();
            db.getTables().forEach(table -> names.add(table.getName()));
            return names;
        } else {
            return esRestClient.listTable();
        }
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        return esRestClient.existIndex(this.esRestClient.getClient(), tblName);
    }

    @Override
    public List<Column> getSchema(String dbName, String tblName) {
        makeSureInitialized();
        return EsUtil.genColumnsFromEs(getEsRestClient(), tblName, null);
    }
}
