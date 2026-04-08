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

import org.apache.doris.common.DdlException;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Elasticsearch property constants and validation utilities.
 *
 * <p>This class centralizes all ES configuration property keys, default values,
 * and validation logic used by ES Catalog and related components.</p>
 */
public class EsProperties {

    public static final String ES_PROPERTIES_PREFIX = "elasticsearch.";

    // Property keys
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
    public static final String MAPPING_ES_ID = "mapping_es_id";

    public static final String LIKE_PUSH_DOWN = "like_push_down";
    public static final String QUERY_DSL = "query_dsl";

    public static final String INCLUDE_HIDDEN_INDEX = "include_hidden_index";

    // Default values
    public static final String DOC_VALUE_SCAN_DEFAULT_VALUE = "true";
    public static final String KEYWORD_SNIFF_DEFAULT_VALUE = "true";
    public static final String HTTP_SSL_ENABLED_DEFAULT_VALUE = "false";
    public static final String NODES_DISCOVERY_DEFAULT_VALUE = "true";
    public static final String MAPPING_ES_ID_DEFAULT_VALUE = "false";
    public static final String LIKE_PUSH_DOWN_DEFAULT_VALUE = "true";
    public static final String INCLUDE_HIDDEN_INDEX_DEFAULT_VALUE = "false";

    private EsProperties() {
        // utility class
    }

    /**
     * Validate ES properties.
     */
    public static void valid(Map<String, String> properties, boolean isAlter) throws DdlException {
        if (!isAlter) {
            if (StringUtils.isEmpty(properties.get(HOSTS))) {
                throw new DdlException("Hosts of ES table is null. "
                        + "Please add properties('hosts'='xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx') when create table");
            }

            if (properties.containsKey(HTTP_SSL_ENABLED)) {
                boolean httpSslEnabled = EsUtil.getBoolean(properties, HTTP_SSL_ENABLED);
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

        if (properties.containsKey(HTTP_SSL_ENABLED)) {
            EsUtil.getBoolean(properties, HTTP_SSL_ENABLED);
        }
        if (properties.containsKey(DOC_VALUE_SCAN)) {
            EsUtil.getBoolean(properties, DOC_VALUE_SCAN);
        }
        if (properties.containsKey(KEYWORD_SNIFF)) {
            EsUtil.getBoolean(properties, KEYWORD_SNIFF);
        }
        if (properties.containsKey(NODES_DISCOVERY)) {
            EsUtil.getBoolean(properties, NODES_DISCOVERY);
        }
        if (properties.containsKey(MAPPING_ES_ID)) {
            EsUtil.getBoolean(properties, MAPPING_ES_ID);
        }
        if (properties.containsKey(LIKE_PUSH_DOWN)) {
            EsUtil.getBoolean(properties, LIKE_PUSH_DOWN);
        }
        if (properties.containsKey(INCLUDE_HIDDEN_INDEX)) {
            EsUtil.getBoolean(properties, INCLUDE_HIDDEN_INDEX);
        }
    }

    /**
     * Fill URLs with http/https schema if not present.
     */
    public static void fillUrlsWithSchema(String[] urls, boolean isSslEnabled) {
        for (int i = 0; i < urls.length; i++) {
            String seed = urls[i].trim();
            if (!seed.startsWith("http://") && !seed.startsWith("https://")) {
                urls[i] = (isSslEnabled ? "https://" : "http://") + seed;
            }
        }
    }
}
