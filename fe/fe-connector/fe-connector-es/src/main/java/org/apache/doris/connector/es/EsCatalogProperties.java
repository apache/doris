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

import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Everything a user writes in {@code CREATE CATALOG} for an Elasticsearch catalog, bound and checked.
 *
 * <p>fe-core parses no connector properties, so this class is where they are interpreted. It carries
 * the longest compatibility history of any connector here: keys reached Doris first as "Doris On ES"
 * table properties, and both an {@code elasticsearch.} prefix and four older names are still accepted.
 * The prefix is stripped on the way in ({@link #stripEsPrefix}) because it applies to every key alike;
 * the four renames are expressed as {@code names} on the field that owns them, so each alias sits next
 * to the property it is an alias for.
 *
 * <p>{@link #of(Map)} strips, binds, derives and validates in one step, so an instance that exists has
 * valid properties and every reader downstream uses a getter. It performs no I/O and is idempotent: it
 * runs at {@code CREATE CATALOG}, again on the merged candidate when {@code ALTER CATALOG} validates,
 * and once more every time the connector is rebuilt -- including on an FE replaying the edit log.
 * Checks that do need the cluster stay in {@code EsConnector#testConnection}.
 *
 * <p><b>Unknown keys are accepted, always.</b> The same map carries engine keys ({@code type},
 * {@code meta.cache.*}, ...) and storage keys, and {@code ALTER CATALOG} merges properties: it can
 * overwrite a key but never remove one, so a key refused here would leave a catalog that no statement
 * could repair. Bad <i>values</i> are refused; unrecognized <i>names</i> are not.
 */
public final class EsCatalogProperties {

    /**
     * The prefix every key could carry when these were table properties of a "Doris On ES" table. It is
     * stripped from every key, so it is not an alias of any one property and cannot be expressed as one.
     */
    public static final String ES_PROPERTIES_PREFIX = "elasticsearch.";

    /** Comma-separated ES HTTP endpoints. A missing scheme is filled in from {@link #HTTP_SSL_ENABLED}. */
    public static final String HOSTS = "hosts";

    public static final String USER = "user";
    public static final String PASSWORD = "password";

    /**
     * Read values from doc_values instead of _source where the mapping allows it. Also gated per query
     * by {@link #MAX_DOCVALUE_FIELDS} and by whether every selected field actually has a doc_values
     * mapping.
     */
    public static final String DOC_VALUE_SCAN = "doc_values_mode";

    /** Resolve a {@code text} field's {@code keyword} sub-field so equality can be pushed down. */
    public static final String KEYWORD_SNIFF = "keyword_sniff";

    /** Ask the cluster for its node topology and scan the data nodes directly. */
    public static final String NODES_DISCOVERY = "nodes_discovery";

    /** Talk https rather than http. Also decides the scheme filled into a bare {@link #HOSTS} entry. */
    public static final String HTTP_SSL_ENABLED = "http_ssl_enabled";

    /** Expose the document {@code _id} as a column. */
    public static final String MAPPING_ES_ID = "mapping_es_id";

    /** Push {@code LIKE} down as an ES wildcard query. */
    public static final String LIKE_PUSH_DOWN = "like_push_down";

    /** List indices whose name starts with a dot, which ES hides by default. */
    public static final String INCLUDE_HIDDEN_INDEX = "include_hidden_index";

    /**
     * The mapping type to read, for the ES versions that still had one. <b>Absent is meaningful</b> --
     * it selects the type-less (ES 7+) mapping shape and leaves the type out of the BE payload -- so
     * this one property defaults to {@code null} rather than to the empty string.
     */
    public static final String MAPPING_TYPE = "mapping_type";

    /** The most selected fields a query may have before doc_values reading is given up on. */
    public static final String MAX_DOCVALUE_FIELDS = "max_docvalue_fields";

    // Older names, each expressed as an alias on the field it renames. Two of them win over the current
    // name and two lose to it -- see the fields for why the orders differ.
    private static final String LEGACY_HTTP_SSL_ENABLED = "ssl";
    private static final String LEGACY_USER = "username";
    private static final String LEGACY_DOC_VALUE_SCAN = "enable_docvalue_scan";
    private static final String LEGACY_KEYWORD_SNIFF = "enable_keyword_sniff";

    @ConnectorProperty(names = {HOSTS}, description = "comma-separated ES HTTP endpoints")
    private String hosts;

    // "username" first: before this class the rename overwrote whatever "user" held, so a catalog
    // carrying both has always been served by the older name. Reversing it now would silently switch
    // which credential such a catalog logs in with.
    @ConnectorProperty(names = {LEGACY_USER, USER}, required = false, description = "ES user name")
    private String user = "";

    @ConnectorProperty(names = {PASSWORD}, required = false, sensitive = true, description = "ES password")
    private String password = "";

    // "ssl" first, for the same reason as "username" above.
    @ConnectorProperty(names = {LEGACY_HTTP_SSL_ENABLED, HTTP_SSL_ENABLED}, required = false,
            description = "talk https rather than http")
    private boolean httpSslEnabled;

    // Current name first: this rename only ever applied when the current name was absent, so the
    // current name has always won.
    @ConnectorProperty(names = {DOC_VALUE_SCAN, LEGACY_DOC_VALUE_SCAN}, required = false,
            description = "read values from doc_values where the mapping allows it")
    private boolean docValuesMode = true;

    // Current name first, as for doc_values_mode above.
    @ConnectorProperty(names = {KEYWORD_SNIFF, LEGACY_KEYWORD_SNIFF}, required = false,
            description = "resolve a text field's keyword sub-field for pushdown")
    private boolean keywordSniff = true;

    @ConnectorProperty(names = {NODES_DISCOVERY}, required = false,
            description = "scan the data nodes directly, using the cluster's node topology")
    private boolean nodesDiscovery = true;

    @ConnectorProperty(names = {MAPPING_ES_ID}, required = false,
            description = "expose the document _id as a column")
    private boolean mappingEsId;

    @ConnectorProperty(names = {LIKE_PUSH_DOWN}, required = false,
            description = "push LIKE down as an ES wildcard query")
    private boolean likePushDown = true;

    @ConnectorProperty(names = {INCLUDE_HIDDEN_INDEX}, required = false,
            description = "list indices whose name starts with a dot")
    private boolean includeHiddenIndex;

    // No initializer on purpose: null means "type-less mapping", which is a different shape, not a
    // default value. See MAPPING_TYPE above.
    @ConnectorProperty(names = {MAPPING_TYPE}, required = false,
            description = "the mapping type to read, for ES versions that had one")
    private String mappingType;

    @ConnectorProperty(names = {MAX_DOCVALUE_FIELDS}, required = false,
            description = "the most selected fields a query may have before doc_values reading is given up")
    private int maxDocValueFields = 20;

    private String[] hostUrls;
    private String[] seeds;

    private EsCatalogProperties() {
    }

    public static EsCatalogProperties of(Map<String, String> properties) {
        EsCatalogProperties p = new EsCatalogProperties();
        ConnectorPropertiesUtils.bindConnectorProperties(p, stripEsPrefix(properties));
        new ParamRules()
                .require(p.hosts, "Required property '" + HOSTS + "' is missing")
                .validate();
        p.seeds = p.hosts.split(",");
        p.hostUrls = fillUrlsWithSchema(p.hosts.trim().split(","), p.httpSslEnabled);
        return p;
    }

    /**
     * Returns the properties with {@link #ES_PROPERTIES_PREFIX} removed from every key that carries it.
     *
     * <p>Where a catalog somehow holds both spellings of one key, the unprefixed one wins. Before this
     * class the winner was whichever spelling the hash map's iteration happened to reach last, i.e. not
     * a decision anyone made; picking the current spelling makes it one.
     */
    private static Map<String, String> stripEsPrefix(Map<String, String> properties) {
        Map<String, String> stripped = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (!entry.getKey().startsWith(ES_PROPERTIES_PREFIX)) {
                stripped.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(ES_PROPERTIES_PREFIX)) {
                stripped.putIfAbsent(entry.getKey().substring(ES_PROPERTIES_PREFIX.length()),
                        entry.getValue());
            }
        }
        return stripped;
    }

    /** Fills in http:// or https:// on any host written without a scheme. */
    private static String[] fillUrlsWithSchema(String[] urls, boolean ssl) {
        String schema = ssl ? "https://" : "http://";
        for (int i = 0; i < urls.length; i++) {
            String trimmed = urls[i].trim();
            if (!trimmed.startsWith("http://") && !trimmed.startsWith("https://")) {
                urls[i] = schema + trimmed;
            } else {
                urls[i] = trimmed;
            }
        }
        return urls;
    }

    public String getHosts() {
        return hosts;
    }

    /**
     * The hosts as URLs the REST client can call: trimmed and given a scheme. A fresh array per call,
     * since the caller owns it.
     */
    public String[] getHostUrls() {
        return hostUrls.clone();
    }

    /**
     * The hosts as node-discovery seeds -- split only, deliberately not the same derivation as
     * {@link #getHostUrls()}, which is what the discovery path has always been given.
     */
    public String[] getSeeds() {
        return seeds.clone();
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public boolean isHttpSslEnabled() {
        return httpSslEnabled;
    }

    public boolean isDocValuesMode() {
        return docValuesMode;
    }

    public boolean isKeywordSniff() {
        return keywordSniff;
    }

    public boolean isNodesDiscovery() {
        return nodesDiscovery;
    }

    public boolean isMappingEsId() {
        return mappingEsId;
    }

    public boolean isLikePushDown() {
        return likePushDown;
    }

    public boolean isIncludeHiddenIndex() {
        return includeHiddenIndex;
    }

    /** Null when the catalog names no mapping type, which is its own shape -- see {@link #MAPPING_TYPE}. */
    public String getMappingType() {
        return mappingType;
    }

    public int getMaxDocValueFields() {
        return maxDocValueFields;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
