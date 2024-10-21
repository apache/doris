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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.fs.HdfsUtil;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * CatalogProperty to store the properties for catalog.
 * the properties in "properties" will overwrite properties in "resource"
 */
@Data
public class CatalogProperty implements Writable {
    private static final Logger LOG = LogManager.getLogger(CatalogProperty.class);

    @SerializedName(value = "resource")
    private String resource;
    @SerializedName(value = "properties")
    private Map<String, String> properties;

    private volatile Map<String, String> fileCatalogConfigProperties = null;

    private volatile Resource catalogResource = null;

    public CatalogProperty(String resource, Map<String, String> properties) {
        this.resource = Strings.nullToEmpty(resource);
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newConcurrentMap();
        }
    }

    private Resource catalogResource() {
        if (!Strings.isNullOrEmpty(resource) && catalogResource == null) {
            synchronized (this) {
                if (catalogResource == null) {
                    catalogResource = Env.getCurrentEnv().getResourceMgr().getResource(resource);
                }
            }
        }
        return catalogResource;
    }

    public String getOrDefault(String key, String defaultVal) {
        String val = properties.get(key);
        if (val == null) {
            Resource res = catalogResource();
            if (res != null) {
                val = res.getCopiedProperties().getOrDefault(key, defaultVal);
            } else {
                val = defaultVal;
            }
        }
        return val;
    }

    public Map<String, String> getProperties() {
        Map<String, String> mergedProperties = Maps.newHashMap();
        if (!Strings.isNullOrEmpty(resource)) {
            Resource res = catalogResource();
            if (res != null) {
                mergedProperties = res.getCopiedProperties();
            }
        }
        mergedProperties.putAll(properties);
        return mergedProperties;
    }

    public void modifyCatalogProps(Map<String, String> props) {
        properties.putAll(PropertyConverter.convertToMetaProperties(props));
        fileCatalogConfigProperties = null;
    }

    public void rollBackCatalogProps(Map<String, String> props) {
        properties.clear();
        properties = new HashMap<>(props);
        fileCatalogConfigProperties = null;
    }

    public Map<String, String> getHadoopProperties() {
        Map<String, String> hadoopProperties = getFileCatalogProperties();
        hadoopProperties.putAll(getProperties());
        hadoopProperties.putAll(PropertyConverter.convertToHadoopFSProperties(getProperties()));
        return hadoopProperties;
    }

    /**
     * Retrieves the file catalog properties. If the properties have not been loaded yet,
     * it loads them from the Hadoop configuration. This method ensures thread safety
     * using double-checked locking to minimize synchronization overhead.
     *
     * @return a map containing the file catalog properties, or an empty map if the
     * configuration resources are not available.
     */
    private Map<String, String> getFileCatalogProperties() {
        // First check without synchronization to avoid unnecessary locking
        if (fileCatalogConfigProperties == null) {
            synchronized (this) {
                // Double-check inside the synchronized block to ensure safe initialization
                if (fileCatalogConfigProperties == null) {
                    // If the configuration resource is available, load properties from Hadoop configuration
                    if (properties.containsKey(DFSFileSystem.HADOOP_CONFIG_RESOURCES)) {
                        Configuration configuration = HdfsUtil.loadConfigurationFromHadoopConfDir(
                                this.properties.get(DFSFileSystem.HADOOP_CONFIG_RESOURCES));
                        // Load all properties matching the regex and store in fileCatalogConfigProperties
                        fileCatalogConfigProperties = configuration.getValByRegex(".*");
                    } else {
                        // If no configuration resource is available, initialize an empty HashMap
                        fileCatalogConfigProperties = new HashMap<>();
                    }
                }
            }
        }
        // Return the file catalog properties (either loaded or an empty map)
        return fileCatalogConfigProperties;
    }

    public void addProperty(String key, String val) {
        this.properties.put(key, val);
        if (key.equals(DFSFileSystem.HADOOP_CONFIG_RESOURCES)) {
            this.fileCatalogConfigProperties = null;
        }
    }

    public void deleteProperty(String key) {
        this.properties.remove(key);
        if (key.equals(DFSFileSystem.HADOOP_CONFIG_RESOURCES)) {
            this.fileCatalogConfigProperties = new HashMap<>();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static CatalogProperty read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CatalogProperty.class);
    }
}
