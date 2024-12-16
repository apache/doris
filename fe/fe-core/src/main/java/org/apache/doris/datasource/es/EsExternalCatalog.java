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

package org.apache.doris.datasource.es;

import org.apache.doris.catalog.EsResource;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * External catalog for elasticsearch
 */
@Getter
public class EsExternalCatalog extends ExternalCatalog {
    public static final String DEFAULT_DB = "default_db";

    private static final Logger LOG = LogManager.getLogger(EsExternalCatalog.class);
    private EsRestClient esRestClient;
    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            EsResource.HOSTS
    );

    /**
     * Default constructor for EsExternalCatalog.
     */
    public EsExternalCatalog(long catalogId, String name, String resource, Map<String, String> props, String comment) {
        super(catalogId, name, InitCatalogLog.Type.ES, comment);
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
        if (properties.containsKey("username")) {
            properties.put(EsResource.USER, properties.remove("username"));
        }
        return properties;
    }

    public String[] getNodes() {
        String hosts = catalogProperty.getOrDefault(EsResource.HOSTS, "");
        String sslEnabled =
                catalogProperty.getOrDefault(EsResource.HTTP_SSL_ENABLED, EsResource.HTTP_SSL_ENABLED_DEFAULT_VALUE);
        String[] hostUrls = hosts.trim().split(",");
        EsResource.fillUrlsWithSchema(hostUrls, Boolean.parseBoolean(sslEnabled));
        return hostUrls;
    }

    public String getUsername() {
        return catalogProperty.getOrDefault(EsResource.USER, "");
    }

    public String getPassword() {
        return catalogProperty.getOrDefault(EsResource.PASSWORD, "");
    }

    public boolean enableDocValueScan() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(EsResource.DOC_VALUE_SCAN,
                EsResource.DOC_VALUE_SCAN_DEFAULT_VALUE));
    }

    public boolean enableKeywordSniff() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(EsResource.KEYWORD_SNIFF,
                EsResource.KEYWORD_SNIFF_DEFAULT_VALUE));
    }

    public boolean enableSsl() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(EsResource.HTTP_SSL_ENABLED,
                EsResource.HTTP_SSL_ENABLED_DEFAULT_VALUE));
    }

    public boolean enableNodesDiscovery() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(EsResource.NODES_DISCOVERY,
                EsResource.NODES_DISCOVERY_DEFAULT_VALUE));
    }

    public boolean enableMappingEsId() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(EsResource.MAPPING_ES_ID,
                EsResource.MAPPING_ES_ID_DEFAULT_VALUE));
    }

    public boolean enableLikePushDown() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(EsResource.LIKE_PUSH_DOWN,
                EsResource.LIKE_PUSH_DOWN_DEFAULT_VALUE));
    }

    public boolean enableIncludeHiddenIndex() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(EsResource.INCLUDE_HIDDEN_INDEX,
                EsResource.INCLUDE_HIDDEN_INDEX_DEFAULT_VALUE));
    }

    @Override
    protected void initLocalObjectsImpl() {
        esRestClient = new EsRestClient(getNodes(), getUsername(), getPassword(), enableSsl());
        if (!esRestClient.health()) {
            throw new DorisEsException("Failed to connect to ES cluster,"
                    + " please check your ES cluster or your ES catalog configuration.");
        }
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        return esRestClient.listTable(enableIncludeHiddenIndex());
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        return esRestClient.existIndex(this.esRestClient.getClient(), tblName);
    }

    @Override
    protected List<String> listDatabaseNames() {
        return Lists.newArrayList(DEFAULT_DB);
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
