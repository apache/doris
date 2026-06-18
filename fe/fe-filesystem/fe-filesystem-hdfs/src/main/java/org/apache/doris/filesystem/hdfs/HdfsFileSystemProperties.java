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

package org.apache.doris.filesystem.hdfs;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Provider-owned typed properties for HDFS / HDFS-compatible filesystems (hdfs, viewfs, ofs, jfs, oss-hdfs).
 *
 * <p>This is the typed <b>backend</b> model for HDFS: it implements {@link BackendStorageProperties} so the
 * typed pipeline ({@code ConnectorContext.getStorageProperties().toBackendProperties().toMap()}) can
 * re-produce the HDFS backend key set ({@code fs.defaultFS}, {@code dfs.*} HA, {@code hadoop.security.*}
 * + Kerberos principal/keytab, {@code hadoop.username}, ...) that the BE turns into {@code THdfsParams}.
 * Without it the typed path returns nothing for HDFS-warehouse catalogs (see DV-004 / R-007).
 *
 * <p>The backend key set is a faithful port of the legacy fe-core
 * {@code org.apache.doris.datasource.property.storage.HdfsProperties.getBackendConfigProperties()} so the
 * new typed path and the legacy path stay at parity.
 *
 * <p><b>Scope note:</b> this model deliberately does NOT implement {@code HadoopStorageProperties}. The
 * FE-side Hadoop {@link org.apache.hadoop.conf.Configuration} used to actually open an HDFS file system is
 * still built by {@link HdfsConfigBuilder} on the {@link HdfsFileSystemProvider#create(Map)} path, and the
 * real {@code UGI.doAs} stays in fe-core/ctx. This class emits only the neutral BE key strings; Kerberos
 * here is BE-key emission only, no authenticator is built (K1).
 */
public final class HdfsFileSystemProperties implements FileSystemProperties, BackendStorageProperties {

    public static final String HDFS_DEFAULT_FS_NAME = "fs.defaultFS";

    private static final String AUTH_KERBEROS = "kerberos";
    private static final String DFS_NAME_SERVICES_KEY = "dfs.nameservices";
    private static final String URI_KEY = "uri";

    // URI schemes recognized when deriving fs.defaultFS from a 'uri' property (parity with legacy supportSchema).
    private static final Set<String> URI_SCHEMES = Set.of("hdfs", "viewfs", "jfs");

    // HA keys, inlined from org.apache.hadoop.hdfs.client.HdfsClientConfigKeys to avoid a hadoop-hdfs
    // dependency (these are stable, well-known HDFS HA configuration keys).
    private static final String DFS_HA_NAMENODES_KEY_PREFIX = "dfs.ha.namenodes";
    private static final String DFS_NAMENODE_RPC_ADDRESS_KEY = "dfs.namenode.rpc-address";
    private static final String DFS_HA_FAILOVER_PROXY_PROVIDER_KEY_PREFIX = "dfs.client.failover.proxy.provider";

    @ConnectorProperty(names = {"hdfs.authentication.type", "hadoop.security.authentication"},
            required = false,
            description = "The authentication type of HDFS. The default value is 'simple'.")
    private String hdfsAuthenticationType = "simple";

    @ConnectorProperty(names = {"hdfs.authentication.kerberos.principal", "hadoop.kerberos.principal"},
            required = false,
            description = "The principal of the kerberos authentication.")
    private String hdfsKerberosPrincipal = "";

    @ConnectorProperty(names = {"hdfs.authentication.kerberos.keytab", "hadoop.kerberos.keytab"},
            required = false,
            description = "The keytab of the kerberos authentication.")
    private String hdfsKerberosKeytab = "";

    @ConnectorProperty(names = {"hadoop.username"},
            required = false,
            description = "The username of Hadoop. Doris will use this user to access HDFS.")
    private String hadoopUsername = "";

    @ConnectorProperty(names = {"hdfs.impersonation.enabled"},
            required = false,
            supported = false,
            description = "Whether to enable the impersonation of HDFS.")
    private boolean hdfsImpersonationEnabled = false;

    @ConnectorProperty(names = {"ipc.client.fallback-to-simple-auth-allowed"},
            required = false,
            description = "Whether to allow fallback to simple authentication.")
    private String allowFallbackToSimpleAuth = "";

    @ConnectorProperty(names = {"fs.defaultFS"}, required = false, description = "The default file system URI.")
    private String fsDefaultFS = "";

    @ConnectorProperty(names = {"hadoop.config.resources"},
            required = false,
            description = "The xml files of Hadoop configuration.")
    private String hadoopConfigResources = "";

    private final Map<String, String> rawProperties;
    private final Map<String, String> matchedProperties;
    private final Map<String, String> backendConfigProperties;

    private HdfsFileSystemProperties(Map<String, String> rawProperties) {
        this.rawProperties = Collections.unmodifiableMap(new HashMap<>(rawProperties));
        this.matchedProperties = Collections.unmodifiableMap(collectMatchedProperties(rawProperties));
        ConnectorPropertiesUtils.bindConnectorProperties(this, rawProperties);
        if (StringUtils.isBlank(fsDefaultFS)) {
            this.fsDefaultFS = extractDefaultFsFromUri(rawProperties);
        }
        this.backendConfigProperties =
                Collections.unmodifiableMap(buildBackendConfigProperties(rawProperties));
    }

    /** Binds and validates raw properties. */
    public static HdfsFileSystemProperties of(Map<String, String> properties) {
        HdfsFileSystemProperties props = new HdfsFileSystemProperties(properties);
        props.validate();
        return props;
    }

    @Override
    public void validate() {
        // Parity with legacy HdfsProperties.checkRequiredProperties(): kerberos requires principal + keytab.
        if (isKerberos()
                && (StringUtils.isBlank(hdfsKerberosPrincipal) || StringUtils.isBlank(hdfsKerberosKeytab))) {
            throw new IllegalArgumentException(
                    "HDFS authentication type is kerberos, but principal or keytab is not set.");
        }
        checkHaConfig(backendConfigProperties);
    }

    @Override
    public String providerName() {
        return "HDFS";
    }

    @Override
    public StorageKind kind() {
        return StorageKind.HDFS_COMPATIBLE;
    }

    @Override
    public FileSystemType type() {
        return FileSystemType.HDFS;
    }

    @Override
    public Map<String, String> rawProperties() {
        return rawProperties;
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
    public BackendStorageKind backendKind() {
        return BackendStorageKind.HDFS;
    }

    @Override
    public Map<String, String> toMap() {
        return backendConfigProperties;
    }

    public boolean isKerberos() {
        return AUTH_KERBEROS.equalsIgnoreCase(hdfsAuthenticationType);
    }

    /**
     * Builds the backend configuration key set. Faithful port of legacy
     * {@code HdfsProperties.initBackendConfigProperties()} so the typed BE map stays at parity with fe-core
     * {@code getBackendConfigProperties()}. Overlay order (last-write-wins): config-resource XML files, then
     * the {@code hadoop./dfs./fs./juicefs.} pass-through from the raw map, then the synthesized keys.
     */
    private Map<String, String> buildBackendConfigProperties(Map<String, String> origProps) {
        Map<String, String> props = HdfsConfigFileLoader.loadConfigMap(hadoopConfigResources);
        Map<String, String> userOverridden = extractUserOverriddenHdfsConfig(origProps);
        if (!userOverridden.isEmpty()) {
            props.putAll(userOverridden);
        }
        if (StringUtils.isNotBlank(fsDefaultFS)) {
            props.put(HDFS_DEFAULT_FS_NAME, fsDefaultFS);
        }
        if (StringUtils.isNotBlank(allowFallbackToSimpleAuth)) {
            props.put("ipc.client.fallback-to-simple-auth-allowed", allowFallbackToSimpleAuth);
        } else {
            props.put("ipc.client.fallback-to-simple-auth-allowed", "true");
        }
        props.put("hdfs.security.authentication", hdfsAuthenticationType);
        if (isKerberos()) {
            props.put("hadoop.security.authentication", AUTH_KERBEROS);
            props.put("hadoop.kerberos.principal", hdfsKerberosPrincipal);
            props.put("hadoop.kerberos.keytab", hdfsKerberosKeytab);
        }
        if (StringUtils.isNotBlank(hadoopUsername)) {
            props.put("hadoop.username", hadoopUsername);
        }
        return props;
    }

    private static Map<String, String> extractUserOverriddenHdfsConfig(Map<String, String> origProps) {
        Map<String, String> overridden = new HashMap<>();
        if (origProps == null || origProps.isEmpty()) {
            return overridden;
        }
        origProps.forEach((key, value) -> {
            if (key.startsWith("hadoop.") || key.startsWith("dfs.") || key.startsWith("fs.")
                    || key.startsWith("juicefs.")) {
                overridden.put(key, value);
            }
        });
        return overridden;
    }

    // ---- helpers ported from fe HdfsPropertiesUtils (kept local; single-use) ----

    private static String extractDefaultFsFromUri(Map<String, String> props) {
        String uriStr = getSingleUri(props);
        if (StringUtils.isBlank(uriStr)) {
            return "";
        }
        // Parity with legacy HdfsPropertiesUtils.extractDefaultFsFromUri: URI.create is unguarded, so a
        // malformed uri fails loud at bind/catalog-create rather than silently dropping fs.defaultFS.
        URI uri = URI.create(uriStr);
        String scheme = uri.getScheme();
        if (scheme == null || !URI_SCHEMES.contains(scheme.toLowerCase())) {
            return "";
        }
        return scheme + "://" + uri.getAuthority();
    }

    private static String getSingleUri(Map<String, String> props) {
        String uriValue = props.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase(URI_KEY))
                .map(Map.Entry::getValue)
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);
        if (uriValue == null) {
            return null;
        }
        // HDFS fs.defaultFS only supports a single URI; a comma-separated list is not a usable default.
        if (uriValue.split(",").length > 1) {
            return null;
        }
        return uriValue;
    }

    /**
     * Validates HDFS HA configuration. Port of legacy {@code HdfsPropertiesUtils.checkHaConfig}: when
     * {@code dfs.nameservices} is present, each nameservice must declare at least two namenodes, an
     * rpc-address per namenode, and a failover proxy provider. Validates only; adds no keys.
     */
    private static void checkHaConfig(Map<String, String> hdfsProperties) {
        if (hdfsProperties == null) {
            return;
        }
        String dfsNameservices = hdfsProperties.getOrDefault(DFS_NAME_SERVICES_KEY, "");
        if (StringUtils.isBlank(dfsNameservices)) {
            // No nameservice configured => HA is not enabled, nothing to validate.
            return;
        }
        for (String dfsservice : splitAndTrim(dfsNameservices)) {
            String haNnKey = DFS_HA_NAMENODES_KEY_PREFIX + "." + dfsservice;
            String namenodes = hdfsProperties.getOrDefault(haNnKey, "");
            if (StringUtils.isBlank(namenodes)) {
                throw new IllegalArgumentException("Missing property: " + haNnKey);
            }
            List<String> names = splitAndTrim(namenodes);
            if (names.size() < 2) {
                throw new IllegalArgumentException("HA requires at least 2 namenodes for service: " + dfsservice);
            }
            for (String name : names) {
                String rpcKey = DFS_NAMENODE_RPC_ADDRESS_KEY + "." + dfsservice + "." + name;
                if (StringUtils.isBlank(hdfsProperties.getOrDefault(rpcKey, ""))) {
                    throw new IllegalArgumentException(
                            "Missing property: " + rpcKey + " (expected format: host:port)");
                }
            }
            String failoverKey = DFS_HA_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + dfsservice;
            if (StringUtils.isBlank(hdfsProperties.getOrDefault(failoverKey, ""))) {
                throw new IllegalArgumentException("Missing property: " + failoverKey);
            }
        }
    }

    private static List<String> splitAndTrim(String s) {
        List<String> result = new ArrayList<>();
        if (StringUtils.isBlank(s)) {
            return result;
        }
        for (String token : s.split(",")) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }

    private static Map<String, String> collectMatchedProperties(Map<String, String> rawProperties) {
        Map<String, String> matched = new HashMap<>();
        for (Field field : ConnectorPropertiesUtils.getConnectorProperties(HdfsFileSystemProperties.class)) {
            String matchedName = ConnectorPropertiesUtils.getMatchedPropertyName(field, rawProperties);
            if (StringUtils.isNotBlank(matchedName)) {
                matched.put(matchedName, rawProperties.get(matchedName));
            }
        }
        return matched;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
