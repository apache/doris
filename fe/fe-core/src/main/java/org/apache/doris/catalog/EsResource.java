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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.external.elasticsearch.EsUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * ES resource
 * <p>
 * Syntax:
 * CREATE RESOURCE "remote_es"
 * PROPERTIES
 * (
 * "type" = "es",
 * "hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
 * "index" = "test",
 * "type" = "doc",
 * "user" = "root",
 * "password" = "root"
 * );
 */
public class EsResource extends Resource {
    public static final String ES_PROPERTIES_PREFIX = "elasticsearch.";

    public static final String HOSTS = "hosts";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String INDEX = "index";
    public static final String TYPE = "type";
    public static final String VERSION = "version";
    public static final String DOC_VALUES_MODE = "doc_values_mode";

    public static final String DOC_VALUE_SCAN = "enable_docvalue_scan";
    public static final String KEYWORD_SNIFF = "enable_keyword_sniff";
    public static final String MAX_DOCVALUE_FIELDS = "max_docvalue_fields";
    public static final String NODES_DISCOVERY = "nodes_discovery";
    public static final String HTTP_SSL_ENABLED = "http_ssl_enabled";
    public static final String QUERY_DSL = "query_dsl";

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public EsResource(String name) {
        super(name, Resource.ResourceType.ES);
        properties = Maps.newHashMap();
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        valid(properties, true);
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
        }
        super.modifyProperties(properties);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        valid(properties, false);
        this.properties = properties;
    }

    public static void valid(Map<String, String> properties, boolean isAlter) throws DdlException {
        if (!isAlter) {
            if (StringUtils.isEmpty(properties.get(HOSTS))) {
                throw new DdlException("Hosts of ES table is null. "
                        + "Please add properties('hosts'='xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx') when create table");
            }

            if (properties.containsKey(HTTP_SSL_ENABLED)) {
                boolean httpSslEnabled = EsUtil.getBoolean(properties, HTTP_SSL_ENABLED);
                // check protocol
                String[] seeds = properties.get(HOSTS).trim().split(",");
                for (String seed : seeds) {
                    if (httpSslEnabled && seed.startsWith("http://")) {
                        throw new DdlException("if http_ssl_enabled is true, the https protocol must be used");
                    }
                    if (!httpSslEnabled && seed.startsWith("https://")) {
                        throw new DdlException("if http_ssl_enabled is false, the http protocol must be used");
                    }
                }
            }
        }

        if (properties.containsKey(EsResource.HTTP_SSL_ENABLED)) {
            EsUtil.getBoolean(properties, EsResource.HTTP_SSL_ENABLED);
        }
        if (properties.containsKey(EsResource.DOC_VALUE_SCAN)) {
            EsUtil.getBoolean(properties, EsResource.DOC_VALUE_SCAN);
        }
        if (properties.containsKey(EsResource.KEYWORD_SNIFF)) {
            EsUtil.getBoolean(properties, EsResource.KEYWORD_SNIFF);
        }
        if (properties.containsKey(EsResource.NODES_DISCOVERY)) {
            EsUtil.getBoolean(properties, EsResource.NODES_DISCOVERY);
        }
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(properties);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }
}
