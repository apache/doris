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

package org.apache.doris.connector.es;

import java.util.HashMap;
import java.util.Map;

/**
 * ES catalog property constants and utility methods.
 * Adapted from fe-core's EsProperties — zero fe-core dependency.
 */
public final class EsConnectorProperties {

    public static final String ES_PROPERTIES_PREFIX = "elasticsearch.";
    public static final String HOSTS = "hosts";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String DOC_VALUE_SCAN = "doc_values_mode";
    public static final String DOC_VALUE_SCAN_DEFAULT = "true";
    public static final String KEYWORD_SNIFF = "keyword_sniff";
    public static final String KEYWORD_SNIFF_DEFAULT = "true";
    public static final String NODES_DISCOVERY = "nodes_discovery";
    public static final String NODES_DISCOVERY_DEFAULT = "true";
    public static final String HTTP_SSL_ENABLED = "http_ssl_enabled";
    public static final String HTTP_SSL_ENABLED_DEFAULT = "false";
    public static final String MAPPING_ES_ID = "mapping_es_id";
    public static final String MAPPING_ES_ID_DEFAULT = "false";
    public static final String LIKE_PUSH_DOWN = "like_push_down";
    public static final String LIKE_PUSH_DOWN_DEFAULT = "true";
    public static final String INCLUDE_HIDDEN_INDEX = "include_hidden_index";
    public static final String INCLUDE_HIDDEN_INDEX_DEFAULT = "false";
    public static final String MAPPING_TYPE = "mapping_type";
    public static final String MAX_DOCVALUE_FIELDS = "max_docvalue_fields";
    public static final int MAX_DOCVALUE_FIELDS_DEFAULT = 20;

    // Legacy key names (from old EsProperties) that must still be recognized
    private static final String LEGACY_DOC_VALUE_SCAN = "enable_docvalue_scan";
    private static final String LEGACY_KEYWORD_SNIFF = "enable_keyword_sniff";

    private EsConnectorProperties() {
    }

    /**
     * Processes compatibility transformations from legacy "Doris On ES" properties.
     * Strips the "elasticsearch." prefix and maps legacy key names.
     */
    public static Map<String, String> processCompatible(Map<String, String> props) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> kv : props.entrySet()) {
            String key = kv.getKey();
            if (key.startsWith(ES_PROPERTIES_PREFIX)) {
                key = key.substring(ES_PROPERTIES_PREFIX.length());
            }
            result.put(key, kv.getValue());
        }
        if (result.containsKey("ssl")) {
            result.put(HTTP_SSL_ENABLED, result.remove("ssl"));
        }
        if (result.containsKey("username")) {
            result.put(USER, result.remove("username"));
        }
        // Map legacy property keys to new canonical names
        if (result.containsKey(LEGACY_DOC_VALUE_SCAN) && !result.containsKey(DOC_VALUE_SCAN)) {
            result.put(DOC_VALUE_SCAN, result.remove(LEGACY_DOC_VALUE_SCAN));
        }
        if (result.containsKey(LEGACY_KEYWORD_SNIFF) && !result.containsKey(KEYWORD_SNIFF)) {
            result.put(KEYWORD_SNIFF, result.remove(LEGACY_KEYWORD_SNIFF));
        }
        return result;
    }

    /**
     * Fills URL array with http:// or https:// schema if missing.
     */
    public static void fillUrlsWithSchema(String[] urls, boolean ssl) {
        String schema = ssl ? "https://" : "http://";
        for (int i = 0; i < urls.length; i++) {
            String trimmed = urls[i].trim();
            if (!trimmed.startsWith("http://") && !trimmed.startsWith("https://")) {
                urls[i] = schema + trimmed;
            } else {
                urls[i] = trimmed;
            }
        }
    }
}
