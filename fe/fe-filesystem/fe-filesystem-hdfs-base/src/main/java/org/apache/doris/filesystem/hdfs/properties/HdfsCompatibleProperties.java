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

package org.apache.doris.filesystem.hdfs.properties;

import org.apache.doris.filesystem.hdfs.SimpleHadoopAuthenticator;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.HadoopStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.foundation.security.ExecutionAuthenticator;

import com.google.common.base.Strings;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Shared base for HDFS-compatible storage property models in fe-filesystem.
 *
 * <p>Both plain HDFS ({@link HdfsProperties}) and Aliyun OSS-HDFS (JindoFS) bind
 * {@code @ConnectorProperty} fields via fe-foundation, then derive a Hadoop-style backend
 * configuration map that is handed to {@code DFSFileSystem}. This base owns everything that is
 * identical between the two: raw-property retention, {@code @ConnectorProperty} binding, the
 * generic required-field check, xml-resource loading, and the backend-config accessor. The
 * provider-specific normalization (auth translation for HDFS, Jindo wiring for OSS-HDFS) is
 * supplied by subclasses via {@link #doInitNormalizeAndCheckProps()}.</p>
 *
 * <p>Implements the fe-filesystem-api {@link FileSystemProperties} family so HDFS-compatible
 * providers can be bound via {@code FileSystemProvider.bind(Map)} like every other provider.
 * For this family the backend map and the Hadoop configuration map are the same thing —
 * fe-core's parity oracle (HdfsPropertiesParityTest) shows its Hadoop config is built directly
 * from the backend map; the generic user-fs.* passthrough and disable-cache orchestration are
 * the facade's job, not this layer's.</p>
 *
 * <p>Zero fe-core / fe-common dependency — only fe-foundation.</p>
 */
public abstract class HdfsCompatibleProperties
        implements FileSystemProperties, BackendStorageProperties, HadoopStorageProperties {

    public static final String HDFS_DEFAULT_FS_NAME = "fs.defaultFS";

    protected final Map<String, String> origProps;

    protected Map<String, String> backendConfigProperties;

    private Map<String, String> matchedProperties = Collections.emptyMap();

    // Lazily created: Kerberos construction performs the actual JAAS login, which must not
    // happen at bind() time (tests and pure planning paths never touch the KDC).
    private volatile ExecutionAuthenticator executionAuthenticator;

    protected HdfsCompatibleProperties(Map<String, String> origProps) {
        this.origProps = origProps;
    }

    /**
     * Binds the {@code @ConnectorProperty} fields from the raw properties, validates them, then
     * delegates the provider-specific derivation of {@link #backendConfigProperties} to the
     * subclass.
     */
    public void initNormalizeAndCheckProps() {
        ConnectorPropertiesUtils.bindConnectorProperties(this, origProps);
        this.matchedProperties = Collections.unmodifiableMap(collectMatchedProperties());
        checkRequiredProperties();
        doInitNormalizeAndCheckProps();
    }

    private Map<String, String> collectMatchedProperties() {
        Map<String, String> matched = new HashMap<>();
        for (Field field : ConnectorPropertiesUtils.getConnectorProperties(this.getClass())) {
            String matchedName = ConnectorPropertiesUtils.getMatchedPropertyName(field, origProps);
            if (matchedName != null) {
                matched.put(matchedName, origProps.get(matchedName));
            }
        }
        return matched;
    }

    @Override
    public StorageKind kind() {
        return StorageKind.HDFS_COMPATIBLE;
    }

    @Override
    public Map<String, String> rawProperties() {
        return origProps;
    }

    @Override
    public Map<String, String> matchedProperties() {
        return matchedProperties;
    }

    @Override
    public Optional<BackendStorageProperties> toBackendProperties() {
        return Optional.of(this);
    }

    @Override
    public Optional<HadoopStorageProperties> toHadoopProperties() {
        return Optional.of(this);
    }

    @Override
    public BackendStorageKind backendKind() {
        return BackendStorageKind.HDFS;
    }

    @Override
    public Map<String, String> toMap() {
        return getBackendConfigProperties();
    }

    @Override
    public Map<String, String> toHadoopConfigurationMap() {
        return getBackendConfigProperties();
    }

    @Override
    public ExecutionAuthenticator getExecutionAuthenticator() {
        ExecutionAuthenticator local = executionAuthenticator;
        if (local == null) {
            synchronized (this) {
                local = executionAuthenticator;
                if (local == null) {
                    local = createExecutionAuthenticator();
                    executionAuthenticator = local;
                }
            }
        }
        return local;
    }

    /**
     * Creates the authenticator on first use. The default honours {@code hadoop.username}
     * from the derived backend map (fe-core parity: non-Kerberos HDFS-compatible storage
     * uses a simple authenticator). {@link HdfsProperties} overrides for Kerberos.
     */
    protected ExecutionAuthenticator createExecutionAuthenticator() {
        String username = backendConfigProperties == null ? null : backendConfigProperties.get("hadoop.username");
        return new SimpleHadoopAuthenticator(username);
    }

    /**
     * Generic reflection-based validation of {@code required=true} string fields, shared by all
     * HDFS-compatible types. Subclasses may override to add type-specific checks (calling
     * {@code super.checkRequiredProperties()} first).
     */
    protected void checkRequiredProperties() {
        for (Field field : ConnectorPropertiesUtils.getConnectorProperties(this.getClass())) {
            field.setAccessible(true);
            ConnectorProperty anno = field.getAnnotation(ConnectorProperty.class);
            String[] names = anno.names();
            if (anno.required() && field.getType().equals(String.class)) {
                try {
                    String value = (String) field.get(this);
                    if (Strings.isNullOrEmpty(value)) {
                        throw new IllegalArgumentException("Property " + names[0] + " is required.");
                    }
                } catch (IllegalAccessException e) {
                    throw new StoragePropertiesException("Failed to get property " + names[0]
                            + ", " + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Provider-specific normalization that must populate {@link #backendConfigProperties}.
     * Called after binding and {@link #checkRequiredProperties()}.
     */
    protected abstract void doInitNormalizeAndCheckProps();

    // The config directory prefix is taken from the injected `_HADOOP_CONFIG_DIR_` property
    // instead of fe-core's Config.hadoop_config_dir, keeping this module fe-core independent.
    protected Map<String, String> loadConfigFromFile(String resourceConfig) {
        if (Strings.isNullOrEmpty(resourceConfig)) {
            return new HashMap<>();
        }
        String configDir = origProps == null ? null : origProps.get("_HADOOP_CONFIG_DIR_");
        return HdfsConfigFileLoader.load(resourceConfig, configDir);
    }

    public Map<String, String> getBackendConfigProperties() {
        return backendConfigProperties;
    }

    @Override
    public String storageFamilyName() {
        return "HDFS";
    }

    @Override
    public Set<String> legacyCacheSchemes() {
        return Set.of("hdfs");
    }

}
