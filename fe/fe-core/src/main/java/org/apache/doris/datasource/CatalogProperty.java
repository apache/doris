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
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    @Deprecated
    @SerializedName(value = "resource")
    private String resource;

    @SerializedName(value = "properties")
    private Map<String, String> properties;

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

    public void modifyCatalogProps(Map<String, String> props) {
        synchronized (this) {
            properties.putAll(PropertyConverter.convertToMetaProperties(props));
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
    public Map<StorageProperties.Type, StorageProperties> getStoragePropertiesMap() {
        if (storagePropertiesMap == null) {
            synchronized (this) {
                if (storagePropertiesMap == null) {
                    try {
                        List<StorageProperties> storageProperties = StorageProperties.createAll(getProperties());
                        this.storagePropertiesMap = storageProperties.stream()
                                .collect(Collectors.toMap(StorageProperties::getType, Function.identity()));
                    } catch (UserException e) {
                        LOG.warn("Failed to initialize catalog storage properties", e);
                        throw new RuntimeException("Failed to initialize storage properties for catalog", e);
                    }
                }
            }
        }
        return storagePropertiesMap;
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
                        throw new RuntimeException("Failed to create metastore properties", e);
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
                    Map<String, String> result = getProperties();
                    result.putAll(PropertyConverter.convertToHadoopFSProperties(getProperties()));
                    this.hadoopProperties = result;
                }
            }
        }
        return hadoopProperties;
    }
}
