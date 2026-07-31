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

import org.apache.doris.filesystem.hdfs.HdfsConfigBuilder;
import org.apache.doris.filesystem.hdfs.KerberosHadoopAuthenticator;
import org.apache.doris.filesystem.hdfs.SimpleHadoopAuthenticator;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.FsCacheKeys;
import org.apache.doris.filesystem.properties.HadoopStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.foundation.security.ExecutionAuthenticator;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

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

    // ---------------------------------------------------------------------------------------
    // Shared Kerberos/simple authentication model (single implementation for HDFS/JFS/...).
    //
    // Lifted verbatim from the legacy fe-core HdfsProperties, which served hdfs://, viewfs://
    // AND jfs:// with ONE class: both supported alias families are declared here
    // (hdfs.authentication.* and the hadoop.* Hadoop-native names), alias precedence is the
    // annotation name order (first present in the raw map wins), and the derived backend map
    // carries the translated Hadoop keys. Subclasses opt in to the backend-map translation by
    // calling {@link #applyAuthBackendConfig(Map)} while deriving their backend map; the
    // authenticator selection and kerberos credential validation apply uniformly.
    // ---------------------------------------------------------------------------------------

    @ConnectorProperty(names = {"hdfs.authentication.type", "hadoop.security.authentication"},
            required = false,
            description = "The authentication type of HDFS. The default value is 'none'.")
    protected String hdfsAuthenticationType = "simple";

    @ConnectorProperty(names = {"hdfs.authentication.kerberos.principal", "hadoop.kerberos.principal"},
            required = false,
            description = "The principal of the kerberos authentication.")
    protected String hdfsKerberosPrincipal = "";

    @ConnectorProperty(names = {"hdfs.authentication.kerberos.keytab", "hadoop.kerberos.keytab"},
            required = false,
            description = "The keytab of the kerberos authentication.")
    protected String hdfsKerberosKeytab = "";

    @ConnectorProperty(names = {"hadoop.username"},
            required = false,
            description = "The username of Hadoop. Doris will user this user to access HDFS")
    protected String hadoopUsername = "";

    @ConnectorProperty(names = {"hdfs.impersonation.enabled"},
            required = false,
            supported = false,
            description = "Whether to enable the impersonation of HDFS.")
    protected boolean hdfsImpersonationEnabled = false;

    @ConnectorProperty(names = {"ipc.client.fallback-to-simple-auth-allowed"},
            required = false,
            description = "Whether to allow fallback to simple authentication.")
    protected String allowFallbackToSimpleAuth = "";

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
        return withFsCacheKey(getBackendConfigProperties());
    }

    @Override
    public Map<String, String> toHadoopConfigurationMap() {
        return withFsCacheKey(getBackendConfigProperties());
    }

    /**
     * Adds the fully derived backend map to the identity. The HDFS families resolve two inputs
     * into it that {@link FsCacheKeys#identityProperties} cannot see from the raw properties: the
     * contents of the XML files named by {@code hadoop.config.resources} (only the file <em>path</em>
     * is a bound alias, so editing a file would otherwise keep the old fingerprint), and the auth
     * keys {@code applyAuthBackendConfig} derives. Everything here already went through the raw
     * overlay, so two definitions reaching the same nameservice through different namenode
     * addresses no longer collide.
     *
     * <p>Reads the untagged backend map, never {@code toHadoopConfigurationMap()}: the latter calls
     * back into this fingerprint.
     */
    @Override
    public String fsCacheFingerprint() {
        Map<String, String> identity = new TreeMap<>(FsCacheKeys.identityProperties(this));
        Map<String, String> derived = getBackendConfigProperties();
        if (derived != null) {
            derived.forEach((key, value) -> identity.put(FsCacheKeys.derivedIdentityKey(key), value));
        }
        return FsCacheKeys.fingerprintOf(getClass().getName(), identity);
    }

    /**
     * Tags a derived property map with this binding's per-scheme credential fingerprint so the
     * Doris-patched {@code org.apache.hadoop.fs.FileSystem} keys its cache by it instead of
     * colliding on {@code (scheme, authority, UGI)}; see {@link FsCacheKeys}.
     */
    private Map<String, String> withFsCacheKey(Map<String, String> base) {
        Map<String, String> tagged = new HashMap<>(base);
        FsCacheKeys.putFsCacheKeys(tagged, this);
        return tagged;
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

    @Override
    public boolean isKerberos() {
        return "kerberos".equalsIgnoreCase(hdfsAuthenticationType);
    }

    /**
     * Creates the authenticator on first use. Kerberos configurations (either alias family)
     * yield a {@link KerberosHadoopAuthenticator} built from the derived backend map — the same
     * construction path as {@code DFSFileSystem}, performing the JAAS login here, on first use
     * only. Otherwise the default honours {@code hadoop.username} from the derived backend map
     * (fe-core parity: non-Kerberos HDFS-compatible storage uses a simple authenticator).
     */
    protected ExecutionAuthenticator createExecutionAuthenticator() {
        if (isKerberos()) {
            return new KerberosHadoopAuthenticator(hdfsKerberosPrincipal, hdfsKerberosKeytab,
                    HdfsConfigBuilder.build(getBackendConfigProperties()));
        }
        String username = backendConfigProperties == null ? null : backendConfigProperties.get("hadoop.username");
        return new SimpleHadoopAuthenticator(username);
    }

    /**
     * Translates the bound authentication fields into the Hadoop configuration keys of the
     * derived backend map — byte-identical to the legacy fe-core
     * {@code HdfsProperties.initBackendConfigProperties()} auth block that served hdfs://,
     * viewfs:// and jfs:// alike. Subclasses call this while building their backend map.
     */
    protected void applyAuthBackendConfig(Map<String, String> props) {
        if (StringUtils.isNotBlank(allowFallbackToSimpleAuth)) {
            props.put("ipc.client.fallback-to-simple-auth-allowed", allowFallbackToSimpleAuth);
        } else {
            props.put("ipc.client.fallback-to-simple-auth-allowed", "true");
        }
        props.put("hdfs.security.authentication", hdfsAuthenticationType);
        if (isKerberos()) {
            props.put("hadoop.security.authentication", "kerberos");
            props.put("hadoop.kerberos.principal", hdfsKerberosPrincipal);
            props.put("hadoop.kerberos.keytab", hdfsKerberosKeytab);
        }
        if (StringUtils.isNotBlank(hadoopUsername)) {
            props.put("hadoop.username", hadoopUsername);
        }
    }

    /**
     * Generic reflection-based validation of {@code required=true} string fields, shared by all
     * HDFS-compatible types, plus the shared kerberos credential validation (byte-identical
     * message to the legacy fe-core HdfsProperties). Subclasses may override to add
     * type-specific checks (calling {@code super.checkRequiredProperties()} first).
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
        if ("kerberos".equalsIgnoreCase(hdfsAuthenticationType) && (Strings.isNullOrEmpty(hdfsKerberosPrincipal)
                || Strings.isNullOrEmpty(hdfsKerberosKeytab))) {
            throw new IllegalArgumentException("HDFS authentication type is kerberos, "
                    + "but principal or keytab is not set.");
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
