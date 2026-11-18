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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Everything a user writes in {@code CREATE CATALOG} for a Lance catalog, bound and checked.
 *
 * <p>The keys and their rules are the ones Lance catalogs have had since 4.1, unchanged: images written by
 * those versions persist the raw property map, and this class must accept it as it is. Settings that are
 * one-per-FE rather than one-per-catalog live in the plugin's {@code lance.conf} ({@link LanceConf}).
 *
 * <p>{@link #of(Map)} touches neither storage nor any {@code org.lance.*} type: it runs at CREATE, again on
 * the merged candidate at ALTER, and every time the connector is rebuilt, including while an FE replays the
 * edit log. Unknown keys are accepted, always: the same map carries engine and storage keys, and ALTER
 * CATALOG can overwrite a key but never remove one.
 *
 * <p>Only the filesystem flavor is served by this build. A {@code rest} catalog is still bound here, so one
 * created by an earlier version replays and keeps its secrets masked, but {@code CREATE CATALOG} refuses it
 * and its first use fails with {@link #requireFilesystem()}.
 */
public final class LanceCatalogProperties {

    public static final String CATALOG_TYPE = "lance.catalog.type";
    public static final String FILESYSTEM = "filesystem";
    public static final String REST = "rest";

    public static final String WAREHOUSE = "warehouse";

    public static final String NAMESPACE_PARENT = "lance.namespace.parent";
    public static final String NAMESPACE_DELIMITER = "lance.namespace.delimiter";
    public static final String ROOT_DATABASE = "lance.namespace.root_database";
    public static final String DEFAULT_DELIMITER = "$";
    public static final String DEFAULT_ROOT_DATABASE = "default";

    /** Every REST-only key starts with this; a filesystem catalog rejects all of them. */
    public static final String REST_PREFIX = "lance.rest.";
    /** The REST credentials. Declared to the engine as secrets so SHOW CREATE CATALOG masks them. */
    public static final String REST_BEARER_TOKEN = "lance.rest.bearer-token";
    public static final String REST_API_KEY = "lance.rest.api-key";

    @ConnectorProperty(names = {CATALOG_TYPE}, required = false,
            description = "The Lance namespace implementation: filesystem or rest. Default: filesystem.")
    private String catalogType = FILESYSTEM;

    @ConnectorProperty(names = {WAREHOUSE}, required = false,
            description = "The local, file or S3 warehouse containing Lance datasets.")
    private String warehouse;

    @ConnectorProperty(names = {NAMESPACE_PARENT}, required = false,
            description = "The optional Lance namespace under which Doris databases are exposed.")
    private String namespaceParent = "";

    @ConnectorProperty(names = {NAMESPACE_DELIMITER}, required = false,
            description = "The delimiter used in an escaped parent namespace. Default: $.")
    private String namespaceDelimiter = DEFAULT_DELIMITER;

    @ConnectorProperty(names = {ROOT_DATABASE}, required = false,
            description = "The Doris database name representing the catalog's root namespace. Default: default.")
    private String rootDatabase = DEFAULT_ROOT_DATABASE;

    private final Map<String, String> raw;
    private List<String> parentNamespace;

    private LanceCatalogProperties(Map<String, String> properties) {
        this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    public static LanceCatalogProperties of(Map<String, String> properties) {
        LanceCatalogProperties p = new LanceCatalogProperties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(p, properties);
        // The binder trims and treats a blank value as unset. These keys keep their value as written: the
        // type is matched trimmed and lower-cased, the warehouse is handed to Lance as is, and namespace
        // whitespace and explicit empty values are meaningful to the escaping rules, so they must be
        // validated rather than silently replaced by defaults.
        p.catalogType = properties.getOrDefault(CATALOG_TYPE, FILESYSTEM).trim().toLowerCase(Locale.ROOT);
        p.warehouse = properties.get(WAREHOUSE);
        p.namespaceParent = properties.getOrDefault(NAMESPACE_PARENT, "");
        p.namespaceDelimiter = properties.getOrDefault(NAMESPACE_DELIMITER, DEFAULT_DELIMITER);
        p.rootDatabase = properties.getOrDefault(ROOT_DATABASE, DEFAULT_ROOT_DATABASE);
        if (!FILESYSTEM.equals(p.catalogType) && !REST.equals(p.catalogType)) {
            throw new IllegalArgumentException("Property '" + CATALOG_TYPE
                    + "' must be 'filesystem' or 'rest', but was '" + p.catalogType + "'");
        }
        p.validateCommonProperties();
        if (FILESYSTEM.equals(p.catalogType)) {
            p.validateFilesystemProperties();
        }
        return p;
    }

    private void validateCommonProperties() {
        if (namespaceDelimiter.isEmpty() || namespaceDelimiter.indexOf('\\') >= 0) {
            throw new IllegalArgumentException("Property '" + NAMESPACE_DELIMITER
                    + "' cannot be empty or contain the escape character '\\'");
        }
        if (StringUtils.isBlank(rootDatabase)) {
            throw new IllegalArgumentException("Property '" + ROOT_DATABASE + "' must be non-empty");
        }
        parentNamespace = Collections.unmodifiableList(
                LanceNamespaceName.parseParentNamespace(namespaceParent, namespaceDelimiter));
    }

    private void validateFilesystemProperties() {
        if (StringUtils.isBlank(warehouse)) {
            throw new IllegalArgumentException(
                    "Missing required property 'warehouse' for Lance filesystem catalog");
        }
        validateWarehouse(warehouse);
        for (String key : raw.keySet()) {
            if (key.startsWith(REST_PREFIX)) {
                throw new IllegalArgumentException(
                        "Property '" + key + "' is not valid for Lance filesystem catalog");
            }
        }
    }

    private static void validateWarehouse(String warehouse) {
        final URI uri;
        try {
            uri = URI.create(warehouse);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Lance warehouse URI: " + warehouse, e);
        }
        if (uri.getScheme() == null) {
            Path path = Paths.get(warehouse);
            if (!path.isAbsolute()) {
                throw new IllegalArgumentException(
                        "Local Lance warehouse must be an absolute path: " + warehouse);
            }
            return;
        }
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        if (!"file".equals(scheme) && !"s3".equals(scheme)) {
            throw new IllegalArgumentException("Unsupported Lance filesystem warehouse scheme '" + scheme
                    + "'; supported schemes are local/file and s3");
        }
        // An object-store root names its bucket in the authority. Lance reads that authority as the
        // bucket and fails deep inside the store when it is absent, so reject the no-authority form
        // here where the message can still name the property.
        if ("s3".equals(scheme) && StringUtils.isBlank(uri.getAuthority())) {
            throw new IllegalArgumentException(
                    "Lance " + scheme + " warehouse must name a bucket, as in " + scheme
                            + "://bucket/path, but was: " + warehouse);
        }
    }

    /**
     * Fails for a flavor this build cannot serve. Called where a catalog would first reach its namespace,
     * so a {@code rest} catalog carried over from an earlier version stays loadable, droppable and alterable,
     * and only reading through it fails, saying why.
     */
    public void requireFilesystem() {
        if (!FILESYSTEM.equals(catalogType)) {
            throw new DorisConnectorException("Lance " + catalogType + " catalogs are not supported yet;"
                    + " only '" + CATALOG_TYPE + "'='" + FILESYSTEM + "' catalogs can be read");
        }
    }

    public String getCatalogType() {
        return catalogType;
    }

    /** The warehouse a filesystem catalog's directory namespace roots at; null for a REST catalog. */
    public String getWarehouse() {
        return warehouse;
    }

    public String getNamespaceDelimiter() {
        return namespaceDelimiter;
    }

    public String getRootDatabase() {
        return rootDatabase;
    }

    /** The configured parent namespace, already parsed and validated; empty when none is configured. */
    public List<String> getParentNamespace() {
        return parentNamespace;
    }

    /**
     * The catalog's properties as written, unmodifiable. It contains credentials: pass it only to code
     * that keeps it internal.
     */
    public Map<String, String> getRaw() {
        return raw;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
