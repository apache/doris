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

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.credentials.AbstractVendedCredentialsProvider;
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * CatalogProperty to store the properties for catalog.
 * the properties in "properties" will overwrite properties in "resource"
 */
public class CatalogProperty {
    private static final Logger LOG = LogManager.getLogger(CatalogProperty.class);

    // Default: false, mapping BINARY types to STRING for compatibility
    public static final String ENABLE_MAPPING_VARBINARY = "enable.mapping.varbinary";
    // Default: false, mapping TIMESTAMP_TZ types to DATETIME for compatibility
    public static final String ENABLE_MAPPING_TIMESTAMP_TZ = "enable.mapping.timestamp_tz";

    @Deprecated
    @SerializedName(value = "resource")
    private String resource;

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    /**
     * An ordered list of all initialized {@link StorageAdapter} bindings.
     * <p>
     * The order of this list is significant:
     * <ul>
     *   <li>The default HDFS binding (if auto-created) is always inserted at index 0.</li>
     *   <li>Explicitly configured storage providers follow in the order they are detected.</li>
     *   <li>Callers rely on this deterministic ordering for selecting or iterating through
     *       storage backends.</li>
     * </ul>
     * <p>
     * Declared as {@code volatile} to ensure visibility across threads once initialized.
     */
    private volatile StorageBindings storageBindings;

    /**
     * Immutable pair of the ordered adapter list and its type-keyed map, published through a
     * single volatile field so a concurrent {@link #resetAllCaches()} can never let a reader
     * observe one half initialized and the other nulled (the legacy two-field twin could
     * return a stale list; two independently nulled fields could return null instead).
     */
    private static final class StorageBindings {
        private final List<StorageAdapter> ordered;
        private final Map<StorageTypeId, StorageAdapter> byType;

        StorageBindings(List<StorageAdapter> ordered, Map<StorageTypeId, StorageAdapter> byType) {
            this.ordered = ordered;
            this.byType = byType;
        }
    }

    // Lazy-loaded metastore properties, using volatile to ensure visibility
    private volatile MetastoreProperties metastoreProperties;

    // Lazy-loaded backend storage properties, using volatile to ensure visibility
    private volatile Map<String, String> backendStorageProperties;

    // Lazy-loaded Hadoop properties, using volatile to ensure visibility
    private volatile Map<String, String> hadoopProperties;

    public CatalogProperty(String resource, Map<String, String> properties) {
        this.resource = resource; // Keep but not used
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newConcurrentMap();
        }
    }

    public String getOrDefault(String key, String defaultVal) {
        return properties.getOrDefault(key, defaultVal);
    }

    public Map<String, String> getProperties() {
        return Maps.newHashMap(properties);
    }

    /**
     * @return true if varbinary mapping is enabled, false otherwise
     */
    public boolean getEnableMappingVarbinary() {
        return Boolean.parseBoolean(getOrDefault(ENABLE_MAPPING_VARBINARY, "false"));
    }

    /**
     * Set enable mapping varbinary property.
     * @param enable true to enable varbinary mapping, false to disable
     */
    public void setEnableMappingVarbinary(boolean enable) {
        addProperty(ENABLE_MAPPING_VARBINARY, String.valueOf(enable));
    }

    /**
     * @return true if timestamp_tz mapping is enabled, false otherwise
     */
    public boolean getEnableMappingTimestampTz() {
        return Boolean.parseBoolean(getOrDefault(ENABLE_MAPPING_TIMESTAMP_TZ, "false"));
    }

    /**
     * Set enable mapping timestamp_tz property.
     * @param enable true to enable timestamp_tz mapping, false to disable
     */
    public void setEnableMappingTimestampTz(boolean enable) {
        addProperty(ENABLE_MAPPING_TIMESTAMP_TZ, String.valueOf(enable));
    }

    public void modifyCatalogProps(Map<String, String> props) {
        synchronized (this) {
            properties.putAll(props);
            resetAllCaches();
        }
    }

    public void rollBackCatalogProps(Map<String, String> props) {
        synchronized (this) {
            properties = new HashMap<>(props);
            resetAllCaches();
        }
    }

    public void addProperty(String key, String val) {
        synchronized (this) {
            this.properties.put(key, val);
            resetAllCaches();
        }
    }

    public void deleteProperty(String key) {
        synchronized (this) {
            this.properties.remove(key);
            resetAllCaches();
        }
    }

    /**
     * Unified cache reset method to ensure all caches are properly cleared
     */
    private void resetAllCaches() {
        this.storageBindings = null;
        this.metastoreProperties = null;
        this.backendStorageProperties = null;
        this.hadoopProperties = null;
    }

    /**
     * Get storage adapter bindings with lazy loading, using double-check locking to ensure
     * thread safety. Vended-credentials-enabled catalogs skip storage binding entirely,
     * exactly like the legacy typed track did.
     */
    private StorageBindings initStorageAdapters() {
        StorageBindings local = storageBindings;
        if (local == null) {
            synchronized (this) {
                local = storageBindings;
                if (local == null) {
                    boolean checkStorageProperties = true;
                    AbstractVendedCredentialsProvider provider =
                            VendedCredentialsFactory.getProviderType(getMetastoreProperties());
                    if (provider != null) {
                        checkStorageProperties = !provider.isVendedCredentialsEnabled(getMetastoreProperties());
                    }
                    if (checkStorageProperties) {
                        List<StorageAdapter> ordered = StorageAdapter.ofAll(getProperties());
                        // LinkedHashMap in binding order (default HDFS pad first, explicit
                        // providers after): values() iteration drives last-writer-wins merging
                        // in getBackendStorageProperties/getHadoopProperties. The legacy
                        // enum-keyed HashMap iterated in JVM-arbitrary (identity-hash) order;
                        // observed runs matched binding order, and the adapter track pins that
                        // order down deterministically.
                        Map<StorageTypeId, StorageAdapter> byType = ordered.stream()
                                .collect(Collectors.toMap(StorageAdapter::getType, Function.identity(),
                                        (a, b) -> {
                                            throw new IllegalStateException(
                                                    "Duplicate storage type: " + a.getType());
                                        }, LinkedHashMap::new));
                        local = new StorageBindings(ordered, byType);
                    } else {
                        local = new StorageBindings(Lists.newArrayList(), Maps.newHashMap());
                    }
                    this.storageBindings = local;
                }
            }
        }
        return local;
    }

    public Map<StorageTypeId, StorageAdapter> getStorageAdaptersMap() {
        return initStorageAdapters().byType;
    }

    public List<StorageAdapter> getOrderedStorageAdapters() {
        return initStorageAdapters().ordered;
    }

    public void checkMetaStoreAndStorageProperties(Class msClass) {
        MetastoreProperties msProperties;
        try {
            msProperties = MetastoreProperties.create(getProperties());
            initStorageAdapters();
        } catch (UserException e) {
            throw new RuntimeException("Failed to initialize Catalog properties, error: "
                    + ExceptionUtils.getRootCauseMessage(e), e);
        }
        Preconditions.checkNotNull(msProperties, "Metastore properties are not configured properly");
        Preconditions.checkArgument(
                msClass.isInstance(msProperties),
                String.format("Metastore properties type is not correct. Expected %s but got %s",
                        msClass.getName(), msProperties.getClass().getName()));
    }

    /**
     * Get metastore properties with lazy loading, using double-check locking to ensure thread safety
     */
    public MetastoreProperties getMetastoreProperties() {
        if (MapUtils.isEmpty(getProperties())) {
            return null;
        }

        if (metastoreProperties == null) {
            synchronized (this) {
                if (metastoreProperties == null) {
                    try {
                        metastoreProperties = MetastoreProperties.create(getProperties());
                    } catch (UserException e) {
                        LOG.warn("Failed to create metastore properties", e);
                        throw new RuntimeException("Failed to create metastore properties, error: "
                                + ExceptionUtils.getRootCauseMessage(e), e);
                    }
                }
            }
        }
        return metastoreProperties;
    }

    /**
     * Get backend storage properties with lazy loading, using double-check locking to ensure thread safety
     */
    public Map<String, String> getBackendStorageProperties() {
        if (backendStorageProperties == null) {
            synchronized (this) {
                if (backendStorageProperties == null) {
                    Map<String, String> result = new HashMap<>();
                    Map<StorageTypeId, StorageAdapter> storageMap = getStorageAdaptersMap();

                    for (StorageAdapter sp : storageMap.values()) {
                        Map<String, String> backendProps = sp.getBackendConfigProperties();
                        // the backend property's value can not be null, because it will be serialized to thrift,
                        // which does not support null value.
                        backendProps.entrySet().stream().filter(e -> e.getValue() != null)
                                .forEach(e -> result.put(e.getKey(), e.getValue()));
                    }

                    this.backendStorageProperties = result;
                }
            }
        }
        return backendStorageProperties;
    }

    /**
     * Get Hadoop properties with lazy loading, using double-check locking to ensure thread safety
     */
    public Map<String, String> getHadoopProperties() {
        if (hadoopProperties == null) {
            synchronized (this) {
                if (hadoopProperties == null) {
                    hadoopProperties = new HashMap<>();
                    Map<StorageTypeId, StorageAdapter> storageMap = getStorageAdaptersMap();

                    for (StorageAdapter sp : storageMap.values()) {
                        Configuration configuration = sp.getHadoopStorageConfig();
                        if (configuration != null) {
                            configuration.forEach(entry -> {
                                String key = entry.getKey();
                                String value = entry.getValue();
                                if (value != null) {
                                    hadoopProperties.put(key, value);
                                }
                            });
                        }
                    }
                }
            }
        }
        return hadoopProperties;
    }
}
