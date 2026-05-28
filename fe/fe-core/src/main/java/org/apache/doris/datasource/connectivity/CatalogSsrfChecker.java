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

package org.apache.doris.datasource.connectivity;

import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates user-supplied catalog URIs against SSRF attacks before a catalog is created.
 *
 * <p>Unlike {@link CatalogConnectivityTestCoordinator}, this checker opens no network
 * connections; it only invokes {@link SecurityChecker#startSSRFChecking(String)} on each URI
 * so the platform's SSRF rule engine can reject internal / private / loopback hosts.
 * Because no connectivity probe is required, the check runs unconditionally on every CREATE
 * CATALOG, regardless of the {@code test_connection} property.
 *
 * <p>Discovery is driven by the {@link ConnectorProperty#checkSsrf()} flag — to extend
 * coverage to a new property class, simply set {@code checkSsrf = true} on its endpoint /
 * URI field; no change to this class is required.
 */
public class CatalogSsrfChecker {
    private static final Logger LOG = LogManager.getLogger(CatalogSsrfChecker.class);

    /** Reflection traversal only descends into property classes under this package prefix. */
    private static final String PROPERTY_PACKAGE_PREFIX = "org.apache.doris.datasource.property.";

    /**
     * HDFS HA exposes one rpc-address per namenode under dynamic keys
     * (e.g. {@code dfs.namenode.rpc-address.<nameservice>.<nn>}); these are stored in
     * {@link HdfsProperties#getBackendConfigProperties()} rather than as declared fields,
     * so {@link ConnectorProperty#checkSsrf()} cannot reach them. They are collected
     * separately.
     */
    private static final String HDFS_NAMENODE_RPC_ADDRESS_PREFIX = "dfs.namenode.rpc-address.";

    private CatalogSsrfChecker() {}

    /**
     * Validate every user-supplied URI on the given catalog properties.
     *
     * @throws DdlException if any URI fails the SSRF check
     */
    public static void check(String catalogName,
                             MetastoreProperties metastoreProperties,
                             Map<StorageProperties.Type, StorageProperties> storagePropertiesMap)
            throws DdlException {
        List<String> uris = new ArrayList<>();
        Set<Object> visited = Sets.newIdentityHashSet();

        if (metastoreProperties != null) {
            collectAnnotatedUris(metastoreProperties, visited, uris);
        }
        if (storagePropertiesMap != null) {
            for (StorageProperties sp : storagePropertiesMap.values()) {
                collectStorageUris(sp, visited, uris);
            }
        }
        for (String uri : uris) {
            checkSingleUri(catalogName, uri);
        }
    }

    /**
     * Collect every URI worth SSRF-checking on a single storage property object.
     *
     * <p>Auto-fallback HDFS storage ({@code explicitlyConfigured=false}) is dropped wholesale
     * — both its annotated {@code fs.defaultFS} field and any dynamic namenode rpc-address
     * entries — because that {@link HdfsProperties} instance was synthesised by the
     * framework for catalogs whose user never configured HDFS, and so its values shouldn't
     * surface as user-supplied URIs.
     */
    private static void collectStorageUris(StorageProperties sp, Set<Object> visited, List<String> uris) {
        if (sp instanceof HdfsProperties && !((HdfsProperties) sp).isExplicitlyConfigured()) {
            return;
        }
        collectAnnotatedUris(sp, visited, uris);
        collectDynamicUris(sp, uris);
    }

    /**
     * Walk the object graph rooted at {@code root}, collecting String values of any field
     * annotated with {@link ConnectorProperty#checkSsrf()}{@code  = true}. Recurses through
     * fields whose declared type lives under {@link #PROPERTY_PACKAGE_PREFIX}; an identity
     * set prevents revisits.
     */
    private static void collectAnnotatedUris(Object root, Set<Object> visited, List<String> uris) {
        if (root == null || !visited.add(root)) {
            return;
        }
        Class<?> clazz = root.getClass();
        while (clazz != null && clazz != Object.class) {
            for (Field field : clazz.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                ConnectorProperty annotation = field.getAnnotation(ConnectorProperty.class);
                Object value = readField(field, root);
                if (value == null) {
                    continue;
                }
                if (annotation != null && annotation.checkSsrf() && value instanceof String) {
                    String s = (String) value;
                    if (StringUtils.isNotBlank(s)) {
                        uris.add(s);
                    }
                }
                if (isPropertyContainer(value)) {
                    collectAnnotatedUris(value, visited, uris);
                }
            }
            clazz = clazz.getSuperclass();
        }
    }

    /**
     * Collect URIs that live behind dynamic property keys (those not bound to a static
     * field via {@code @ConnectorProperty}). Currently this means HDFS namenode rpc-address
     * entries on {@link HdfsProperties}. The {@code isExplicitlyConfigured} gating is
     * applied by {@link #collectStorageUris} before reaching here.
     */
    private static void collectDynamicUris(StorageProperties props, List<String> uris) {
        if (!(props instanceof HdfsProperties)) {
            return;
        }
        Map<String, String> backendConfig = ((HdfsProperties) props).getBackendConfigProperties();
        if (backendConfig == null) {
            return;
        }
        for (Map.Entry<String, String> e : backendConfig.entrySet()) {
            if (e.getKey() != null && e.getKey().startsWith(HDFS_NAMENODE_RPC_ADDRESS_PREFIX)
                    && StringUtils.isNotBlank(e.getValue())) {
                uris.add(e.getValue());
            }
        }
    }

    private static Object readField(Field field, Object obj) {
        try {
            field.setAccessible(true);
            return field.get(obj);
        } catch (IllegalAccessException e) {
            LOG.warn("Failed to read field {} on {}", field.getName(), obj.getClass().getName(), e);
            return null;
        }
    }

    /**
     * True if {@code value} is itself a Doris property POJO that we should recurse into
     * (e.g. {@code HMSBaseProperties} embedded inside {@code HiveHMSProperties}).
     * Returns false for Strings, primitives, collections, framework types, etc.
     */
    private static boolean isPropertyContainer(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof CharSequence || value instanceof Number || value instanceof Boolean
                || value instanceof Collection || value instanceof Map) {
            return false;
        }
        Class<?> c = value.getClass();
        if (c.isPrimitive() || c.isArray() || c.isEnum()) {
            return false;
        }
        String name = c.getName();
        return name.startsWith(PROPERTY_PACKAGE_PREFIX);
    }

    /**
     * Run a single URI through the SSRF check. URI may be a full URL ({@code thrift://h:p},
     * {@code hdfs://h:p}, {@code https://...}) or a bare {@code host[:port]}. We normalize to
     * {@code http://...} solely so the underlying SSRF rule engine can parse it; no HTTP
     * connection is opened here.
     *
     * <p>HMS allows comma-separated URIs (e.g. {@code thrift://h1:p,thrift://h2:p}); each is
     * validated independently so a single bad host fails the catalog creation.
     */
    private static void checkSingleUri(String catalogName, String uri) throws DdlException {
        if (StringUtils.isBlank(uri)) {
            return;
        }
        for (String token : uri.split(",")) {
            String single = token.trim();
            if (single.isEmpty()) {
                continue;
            }
            String urlStr = normalizeToHttpUrl(single);
            if (urlStr == null) {
                continue;
            }
            try {
                SecurityChecker.getInstance().startSSRFChecking(urlStr);
            } catch (Exception e) {
                LOG.warn("SSRF check failed for catalog '{}', uri '{}'", catalogName, single, e);
                throw new DdlException("SSRF check failed for catalog '" + catalogName
                        + "', uri '" + single + "': " + e.getMessage());
            } finally {
                SecurityChecker.getInstance().stopSSRFChecking();
            }
        }
    }

    private static String normalizeToHttpUrl(String uri) {
        String s = uri;
        int schemeIdx = s.indexOf("://");
        if (schemeIdx != -1) {
            s = s.substring(schemeIdx + 3);
        }
        int slash = s.indexOf('/');
        if (slash != -1) {
            s = s.substring(0, slash);
        }
        int qmark = s.indexOf('?');
        if (qmark != -1) {
            s = s.substring(0, qmark);
        }
        if (StringUtils.isBlank(s)) {
            return null;
        }
        return "http://" + s;
    }
}
