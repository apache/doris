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
import org.apache.doris.catalog.external.EsExternalDatabase;
import org.apache.doris.common.DdlException;
import org.apache.doris.external.elasticsearch.EsRestClient;
import org.apache.doris.external.elasticsearch.EsUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
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

    public static final String PROP_HOSTS = "elasticsearch.hosts";
    public static final String PROP_SSL = "elasticsearch.ssl";
    public static final String PROP_USERNAME = "elasticsearch.username";
    public static final String PROP_PASSWORD = "elasticsearch.password";
    public static final String PROP_DOC_VALUE_SCAN = "elasticsearch.doc_value_scan";
    public static final String PROP_KEYWORD_SNIFF = "elasticsearch.keyword_sniff";
    public static final String PROP_NODES_DISCOVERY = "elasticsearch.nodes_discovery";

    private EsRestClient esRestClient;

    private String[] nodes;

    private String username = null;

    private String password = null;

    private boolean enableDocValueScan = true;

    private boolean enableKeywordSniff = true;

    private boolean enableSsl = false;

    private boolean enableNodesDiscovery = true;

    /**
     * Default constructor for EsExternalCatalog.
     */
    public EsExternalCatalog(long catalogId, String name, Map<String, String> props) {
        this.id = catalogId;
        this.name = name;
        this.type = "es";
        setProperties(props);
        this.catalogProperty = new CatalogProperty();
        this.catalogProperty.setProperties(props);
    }

    private void setProperties(Map<String, String> properties) {
        try {
            nodes = properties.get(PROP_HOSTS).trim().split(",");
            if (properties.containsKey(PROP_SSL)) {
                enableSsl = EsUtil.getBoolean(properties, PROP_SSL);
            } else {
                properties.put(PROP_SSL, String.valueOf(enableSsl));
            }

            if (StringUtils.isNotBlank(properties.get(PROP_USERNAME))) {
                username = properties.get(PROP_USERNAME).trim();
            }

            if (StringUtils.isNotBlank(properties.get(PROP_PASSWORD))) {
                password = properties.get(PROP_PASSWORD).trim();
            }

            if (properties.containsKey(PROP_DOC_VALUE_SCAN)) {
                enableDocValueScan = EsUtil.getBoolean(properties, PROP_DOC_VALUE_SCAN);
            } else {
                properties.put(PROP_DOC_VALUE_SCAN, String.valueOf(enableDocValueScan));
            }

            if (properties.containsKey(PROP_KEYWORD_SNIFF)) {
                enableKeywordSniff = EsUtil.getBoolean(properties, PROP_KEYWORD_SNIFF);
            } else {
                properties.put(PROP_KEYWORD_SNIFF, String.valueOf(enableKeywordSniff));
            }

            if (properties.containsKey(PROP_NODES_DISCOVERY)) {
                enableNodesDiscovery = EsUtil.getBoolean(properties, PROP_NODES_DISCOVERY);
            } else {
                properties.put(PROP_NODES_DISCOVERY, String.valueOf(enableNodesDiscovery));
            }
        } catch (DdlException e) {
            // should not happen. the properties are already checked in analysis phase.
            throw new RuntimeException("should not happen", e);
        }
    }

    @Override
    protected void initLocalObjectsImpl() {
        esRestClient = new EsRestClient(this.nodes, this.username, this.password, this.enableSsl);
    }

    @Override
    protected void init() {
        InitCatalogLog initCatalogLog = new InitCatalogLog();
        this.esRestClient = new EsRestClient(this.nodes, this.username, this.password, this.enableSsl);
        initCatalogLog.setCatalogId(id);
        initCatalogLog.setType(InitCatalogLog.Type.ES);
        if (dbNameToId != null && dbNameToId.containsKey(DEFAULT_DB)) {
            idToDb.get(dbNameToId.get(DEFAULT_DB)).setUnInitialized();
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
            db.getTables().stream().forEach(table -> names.add(table.getName()));
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
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        setProperties(this.catalogProperty.getProperties());
    }

    @Override
    public List<Column> getSchema(String dbName, String tblName) {
        makeSureInitialized();
        return EsUtil.genColumnsFromEs(getEsRestClient(), tblName, null);
    }
}
