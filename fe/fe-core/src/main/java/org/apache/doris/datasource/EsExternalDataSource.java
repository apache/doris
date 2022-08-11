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


import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.external.EsExternalDatabase;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.external.elasticsearch.EsRestClient;
import org.apache.doris.external.elasticsearch.EsUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * External data source for elasticsearch
 */
@Getter
public class EsExternalDataSource extends ExternalDataSource {

    public static final String DEFAULT_DB = "default_db";
    private static final Logger LOG = LogManager.getLogger(EsExternalDataSource.class);
    private static final String PROP_HOSTS = "elasticsearch.hosts";
    private static final String PROP_USERNAME = "elasticsearch.username";
    private static final String PROP_PASSWORD = "elasticsearch.password";
    private static final String PROP_DOC_VALUE_SCAN = "elasticsearch.doc_value_scan";
    private static final String PROP_KEYWORD_SNIFF = "elasticsearch.keyword_sniff";
    private static final String PROP_NODES_DISCOVERY = "elasticsearch.nodes_discovery";
    private static final String PROP_SSL = "elasticsearch.ssl";

    // Cache of db name to db id.
    private Map<String, Long> dbNameToId;
    private Map<Long, EsExternalDatabase> idToDb;

    private EsRestClient esRestClient;

    private String[] nodes;

    private String username = null;

    private String password = null;

    private boolean enableDocValueScan = true;

    private boolean enableKeywordSniff = true;

    private boolean enableSsl = false;

    private boolean enableNodesDiscovery = true;

    /**
     * Default constructor for EsExternalDataSource.
     */
    public EsExternalDataSource(long catalogId, String name, Map<String, String> props) throws DdlException {
        this.id = catalogId;
        this.name = name;
        this.type = "es";
        validate(props);
        this.dsProperty = new DataSourceProperty();
        this.dsProperty.setProperties(props);
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException(
                    "Please set properties of elasticsearch table, " + "they are: hosts, user, password, index");
        }

        if (StringUtils.isBlank(properties.get(PROP_HOSTS))) {
            throw new DdlException("Hosts of ES table is null.");
        }
        nodes = properties.get(PROP_HOSTS).trim().split(",");
        // check protocol
        for (String seed : nodes) {
            if (!seed.startsWith("http")) {
                throw new DdlException("the protocol must be used");
            }
            if (properties.containsKey(PROP_SSL)) {
                enableSsl = EsUtil.getBoolean(properties, PROP_SSL);
                if (enableSsl && seed.startsWith("http://")) {
                    throw new DdlException("if ssl_enabled is true, the https protocol must be used");
                }
                if (!enableSsl && seed.startsWith("https://")) {
                    throw new DdlException("if ssl_enabled is false, the http protocol must be used");
                }
            }
        }

        if (StringUtils.isNotBlank(properties.get(PROP_USERNAME))) {
            username = properties.get(PROP_USERNAME).trim();
        }

        if (StringUtils.isNotBlank(properties.get(PROP_PASSWORD))) {
            password = properties.get(PROP_PASSWORD).trim();
        }

        if (properties.containsKey(PROP_DOC_VALUE_SCAN)) {
            enableDocValueScan = EsUtil.getBoolean(properties, PROP_DOC_VALUE_SCAN);
        }

        if (properties.containsKey(PROP_KEYWORD_SNIFF)) {
            enableKeywordSniff = EsUtil.getBoolean(properties, PROP_KEYWORD_SNIFF);
        }

        if (properties.containsKey(PROP_NODES_DISCOVERY)) {
            enableNodesDiscovery = EsUtil.getBoolean(properties, PROP_NODES_DISCOVERY);
        }

    }

    /**
     * Datasource can't be init when creating because the external datasource may depend on third system.
     * So you have to make sure the client of third system is initialized before any method was called.
     */
    private synchronized void makeSureInitialized() {
        if (!initialized) {
            init();
            initialized = true;
        }
    }

    private void init() {
        try {
            validate(this.dsProperty.getProperties());
        } catch (DdlException e) {
            LOG.warn("validate error", e);
        }
        dbNameToId = Maps.newConcurrentMap();
        idToDb = Maps.newConcurrentMap();
        this.esRestClient = new EsRestClient(this.nodes, this.username, this.password, this.enableSsl);
        long defaultDbId = Env.getCurrentEnv().getNextId();
        dbNameToId.put(DEFAULT_DB, defaultDbId);
        idToDb.put(defaultDbId, new EsExternalDatabase(this, defaultDbId, DEFAULT_DB));
    }

    @Override
    public List<String> listDatabaseNames(SessionContext ctx) {
        makeSureInitialized();
        return new ArrayList<>(dbNameToId.keySet());
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        return esRestClient.listTable();
    }

    @Nullable
    @Override
    public ExternalDatabase getDbNullable(String dbName) {
        makeSureInitialized();
        String realDbName = ClusterNamespace.getNameFromFullName(dbName);
        if (!dbNameToId.containsKey(realDbName)) {
            return null;
        }
        return idToDb.get(dbNameToId.get(realDbName));
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        return esRestClient.existIndex(this.esRestClient.getClient(), tblName);
    }

    @Override
    public List<Long> getDbIds() {
        return Lists.newArrayList(dbNameToId.values());
    }
}
