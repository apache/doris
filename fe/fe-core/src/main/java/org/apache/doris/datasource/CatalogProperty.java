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

import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.credentials.AbstractVendedCredentialsProvider;
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
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
     * An ordered list of all initialized {@link StorageProperties} instances.
     * <p>
     * The order of this list is significant:
     * <ul>
     *   <li>The default HDFSProperties (if auto-created) is always inserted at index 0.</li>
     *   <li>Explicitly configured storage providers follow in the order they are detected.</li>
     *   <li>Callers rely on this deterministic ordering for selecting or iterating through
     *       storage backends.</li>
     * </ul>
     * <p>
     * Declared as {@code volatile} to ensure visibility across threads once initialized.
     */
    private volatile List<StorageProperties> orderedStoragePropertiesList;

    // Lazy-loaded storage properties map, using volatile to ensure visibility
    private volatile Map<StorageProperties.Type, StorageProperties> storagePropertiesMap;

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
        this.storagePropertiesMap = null;
        this.metastoreProperties = null;
        this.backendStorageProperties = null;
        this.hadoopProperties = null;
    }

    /**
     * Get storage properties map with lazy loading, using double-check locking to ensure thread safety
     */
    private void initStorageProperties() {
        if (storagePropertiesMap == null) {
            synchronized (this) {
                if (storagePropertiesMap == null) {
                    try {
                        MetastoreProperties msp = getMetastoreProperties();
                        if (shouldBuildStaticStorage(msp)) {
                            Map<String, String> storageProps = mergeDerivedStorageDefaults(msp);
                            this.orderedStoragePropertiesList = StorageProperties.createAll(storageProps);
                            this.storagePropertiesMap = orderedStoragePropertiesList.stream()
                                    .collect(Collectors.toMap(StorageProperties::getType, Function.identity()));
                        } else {
                            this.orderedStoragePropertiesList = Lists.newArrayList();
                            this.storagePropertiesMap = Maps.newHashMap();
                        }
                    } catch (UserException e) {
                        LOG.warn("Failed to initialize catalog storage properties", e);
                        throw new RuntimeException("Failed to initialize storage properties, error: "
                                + ExceptionUtils.getRootCauseMessage(e), e);
                    }
                }
            }
        }
    }

    /**
     * Whether fe-core should build the static storage map for this catalog: {@code true} unless the
     * connector supplies per-table vended credentials (a REST/vended catalog whose static storage map is
     * empty by design). Provider-backed metastores (iceberg) route through the {@link VendedCredentialsFactory}
     * provider; non-provider backends (e.g. a Paimon REST catalog) signal vended credentials through the
     * metastore-props gate. The null guard preserves the "build the static map" behavior when there is no
     * metastore. Verbatim extraction of the former inline gate — the single source of truth shared by
     * {@link #initStorageProperties} (the fe-core parse) and {@link #getEffectiveRawStorageProperties} (the
     * fe-filesystem bind), so the two paths never diverge.
     */
    private boolean shouldBuildStaticStorage(MetastoreProperties msp) {
        AbstractVendedCredentialsProvider provider = VendedCredentialsFactory.getProviderType(msp);
        if (provider != null) {
            return !provider.isVendedCredentialsEnabled(msp);
        } else if (msp != null) {
            return !msp.isVendedCredentialsEnabled();
        }
        return true;
    }

    /**
     * The catalog's persisted user props merged with derived storage defaults: from the metastore props when
     * present; otherwise (an SPI catalog with no fe-core {@link MetastoreProperties} — e.g. a native iceberg
     * catalog whose property cluster moved to the connector) from the neutral {@code hdfs://} warehouse
     * bridge. Derived props are defaults (an explicit user key wins via {@code putIfAbsent}) and the persisted
     * {@link #getProperties()} map is never mutated. This is the exact map {@link #initStorageProperties}
     * feeds to {@link StorageProperties#createAll}.
     */
    private Map<String, String> mergeDerivedStorageDefaults(MetastoreProperties msp) {
        Map<String, String> storageProps = getProperties();
        Map<String, String> derived = msp != null
                ? msp.getDerivedStorageProperties()
                : deriveHdfsDefaultFsFromWarehouse(storageProps);
        if (MapUtils.isNotEmpty(derived)) {
            storageProps = new HashMap<>(storageProps);
            derived.forEach(storageProps::putIfAbsent);
        }
        return storageProps;
    }

    /**
     * The effective raw storage property map for a plugin catalog to bind directly through fe-filesystem
     * (design S2), letting {@code ConnectorContext.getStorageProperties()} hand the connector typed
     * fe-filesystem storage without the redundant fe-core {@link StorageProperties#createAll} round-trip.
     * Returns the same map {@link #initStorageProperties} would parse — user props plus derived defaults
     * (warehouse -> fs.defaultFS) — honoring the vended gate (empty when the connector supplies vended
     * credentials, so no static storage is bound). Byte-identical to
     * {@code getStoragePropertiesMap().values().iterator().next().getOrigProps()} (createAll passes the map
     * through unmutated), so the bound typed StorageProperties, and the BE {@code location.*} map derived from
     * them, are unchanged. The returned map must be treated as read-only by callers.
     */
    public Map<String, String> getEffectiveRawStorageProperties() {
        // synchronized(this) so the vended-gate metastore read and the persisted-props read form a single
        // consistent snapshot, matching the atomicity of the fe-core parse path (initStorageProperties builds
        // under the same monitor, mutually exclusive with modifyCatalogProps/addProperty/deleteProperty).
        // Without it a concurrent ALTER of a derivation-feeding key (e.g. warehouse) could tear the derived
        // defaults against the user props. The critical section is a small map copy + pure derivation and
        // takes no foreign lock, so it is deadlock-free and contention-free at scan-planning frequency.
        synchronized (this) {
            MetastoreProperties msp = getMetastoreProperties();
            return shouldBuildStaticStorage(msp) ? mergeDerivedStorageDefaults(msp) : Collections.emptyMap();
        }
    }

    /**
     * Derives {@code fs.defaultFS=hdfs://<nameservice>} from an HDFS {@code warehouse=hdfs://<ns>/path} for a
     * catalog that carries no fe-core {@link MetastoreProperties} (an SPI catalog whose metastore properties
     * live connector-side, e.g. a native iceberg catalog after its property cluster moved to the connector).
     * Without it an HA-nameservice catalog configured with only {@code warehouse} (relying on the classpath
     * {@code core-site.xml}/{@code hdfs-site.xml} for the rest) would not bind HDFS storage with the warehouse
     * nameservice, because {@code HdfsProperties} detection never reads {@code warehouse}. Non-hdfs warehouses
     * (and a blank one) derive nothing. Keyed purely on the neutral {@code warehouse} property + hdfs scheme
     * (no engine-specific keys); the parse and empty-nameservice message are a verbatim port of the former
     * {@code IcebergFileSystemMetaStoreProperties.getDerivedStorageProperties} bridge it replaces.
     */
    static Map<String, String> deriveHdfsDefaultFsFromWarehouse(Map<String, String> storageProps) {
        String warehouse = storageProps.get("warehouse");
        if (StringUtils.isBlank(warehouse) || !StringUtils.startsWith(warehouse, HdfsResource.HDFS_PREFIX)) {
            return Collections.emptyMap();
        }
        String nameService = StringUtils.substringBetween(warehouse, HdfsResource.HDFS_FILE_PREFIX, "/");
        if (StringUtils.isEmpty(nameService)) {
            throw new IllegalArgumentException("Unrecognized 'warehouse' location format"
                    + " because name service is required.");
        }
        return Collections.singletonMap(HdfsResource.HADOOP_FS_NAME, HdfsResource.HDFS_FILE_PREFIX + nameService);
    }

    public Map<StorageProperties.Type, StorageProperties> getStoragePropertiesMap() {
        initStorageProperties();
        return storagePropertiesMap;
    }

    public List<StorageProperties> getOrderedStoragePropertiesList() {
        initStorageProperties();
        return orderedStoragePropertiesList;
    }

    public void checkMetaStoreAndStorageProperties(Class msClass) {
        MetastoreProperties msProperties;
        try {
            msProperties = MetastoreProperties.create(getProperties());
            initStorageProperties();
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
                    Map<StorageProperties.Type, StorageProperties> storageMap = getStoragePropertiesMap();

                    for (StorageProperties sp : storageMap.values()) {
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
                    Map<StorageProperties.Type, StorageProperties> storageMap = getStoragePropertiesMap();

                    for (StorageProperties sp : storageMap.values()) {
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
