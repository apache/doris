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

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.foundation.property.ConnectorProperty;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Aliyun OSS-HDFS (JindoFS) storage properties for fe-filesystem.
 *
 * <p>OSS-HDFS is an HDFS-compatible service over OSS, addressed with {@code oss://} URIs and
 * served by the JindoFS Hadoop FileSystem ({@code com.aliyun.jindodata.oss.JindoOssFileSystem}).
 * It therefore sits on the shared {@link HdfsCompatibleProperties} base alongside plain
 * {@link HdfsProperties} and produces a backend config map for {@code DFSFileSystem}; the only
 * delta is the Jindo {@code fs.oss.*} wiring plus oss-dls endpoint/region handling.</p>
 *
 * <p>Self-contained port of the kernel {@code OSSHdfsProperties}, with zero fe-core / fe-common
 * dependency. The Jindo impl class names are kept as string constants (loaded reflectively by the
 * backend), so no compile dependency on the Jindo SDK is introduced.</p>
 *
 * <p>The normalized {@code fs.oss.*} backend keys are accepted as aliases so the binder is a
 * fixpoint for converter-produced maps: fe-core's StoragePropertiesConverter hands the plugin
 * {@code getBackendConfigProperties()} output (plus the {@code _STORAGE_TYPE_} marker), not the
 * raw user keys, and rebinding that map must yield the same configuration.</p>
 */
public class OssHdfsProperties extends HdfsCompatibleProperties {

    private static final String JINDO_OSS_FILE_SYSTEM_IMPL =
            "com.aliyun.jindodata.oss.JindoOssFileSystem";
    private static final String JINDO_OSS_ABSTRACT_FILE_SYSTEM_IMPL =
            "com.aliyun.jindodata.oss.JindoOSS";

    private static final String OSS_HDFS_PREFIX_KEY = "oss.hdfs.";
    private static final String OSS_HDFS_ENDPOINT_SUFFIX = ".oss-dls.aliyuncs.com";

    private static final Set<String> OSS_ENDPOINT_KEY_NAME = ImmutableSet.of("oss.hdfs.endpoint",
            "oss.endpoint", "dlf.endpoint", "dlf.catalog.endpoint");

    private static final Set<Pattern> ENDPOINT_PATTERN = ImmutableSet.of(
            Pattern.compile("(?:https?://)?([a-z]{2}-[a-z0-9-]+)\\.oss-dls\\.aliyuncs\\.com"),
            Pattern.compile("^(?:https?://)?dlf(?:-vpc)?\\.([a-z0-9-]+)\\.aliyuncs\\.com(?:/.*)?$"));

    private static final Set<String> SUPPORT_SCHEMA = ImmutableSet.of("oss", "hdfs");

    @ConnectorProperty(names = {"oss.hdfs.endpoint", "oss.endpoint", "dlf.endpoint", "dlf.catalog.endpoint",
            "fs.oss.endpoint"},
            description = "The endpoint of OSS.")
    private String endpoint = "";

    @ConnectorProperty(names = {"oss.hdfs.access_key", "oss.access_key", "dlf.access_key", "dlf.catalog.accessKeyId",
            "fs.oss.accessKeyId"},
            sensitive = true,
            description = "The access key of OSS.")
    private String accessKey = "";

    @ConnectorProperty(names = {"oss.hdfs.secret_key", "oss.secret_key", "dlf.secret_key", "dlf.catalog.secret_key",
            "fs.oss.accessKeySecret"},
            sensitive = true,
            description = "The secret key of OSS.")
    private String secretKey = "";

    @ConnectorProperty(names = {"oss.hdfs.region", "oss.region", "dlf.region", "fs.oss.region"},
            required = false,
            description = "The region of OSS.")
    private String region = "";

    @ConnectorProperty(names = {"oss.hdfs.fs.defaultFS", "fs.defaultFS"}, required = false, description = "")
    private String fsDefaultFS = "";

    @ConnectorProperty(names = {"oss.hdfs.hadoop.config.resources"},
            required = false,
            description = "The xml files of Hadoop configuration.")
    private String hadoopConfigResources = "";

    @ConnectorProperty(names = {"oss.hdfs.security_token", "oss.security_token", "fs.oss.securityToken"},
            required = false,
            sensitive = true,
            description = "The security token of OSS.")
    private String securityToken = "";

    public OssHdfsProperties(Map<String, String> origProps) {
        super(origProps);
    }

    @Override
    public String providerName() {
        return "OSS_HDFS";
    }

    @Override
    public FileSystemType type() {
        return FileSystemType.HDFS;
    }

    @Override
    public Set<String> getSupportedSchemes() {
        return SUPPORT_SCHEMA;
    }

    @Override
    public String validateAndNormalizeUri(String uri) {
        // fe-core parity (OSSHdfsProperties.validateUri): only the oss:// scheme is accepted.
        if (StringUtils.isBlank(uri)) {
            throw new IllegalArgumentException("The uri is empty.");
        }
        URI uriObj = URI.create(uri);
        if (uriObj.getScheme() == null) {
            throw new IllegalArgumentException("The uri scheme is empty.");
        }
        if (!uriObj.getScheme().equalsIgnoreCase("oss")) {
            throw new IllegalArgumentException("The uri scheme is not oss.");
        }
        return uriObj.toString();
    }

    /**
     * Cheap, deterministic detection of an OSS-HDFS configuration: an explicit {@code oss.hdfs.}
     * enable flag, or any endpoint key pointing at an {@code *.oss-dls.aliyuncs.com} host.
     */
    public static boolean guessIsMe(Map<String, String> props) {
        boolean enable = props.entrySet().stream()
                .anyMatch(e -> e.getKey().equalsIgnoreCase(OSS_HDFS_PREFIX_KEY)
                        && Boolean.parseBoolean(e.getValue()));
        if (enable) {
            return true;
        }
        return OSS_ENDPOINT_KEY_NAME.stream()
                .map(props::get)
                .anyMatch(ep -> StringUtils.isNotBlank(ep) && ep.endsWith(OSS_HDFS_ENDPOINT_SUFFIX));
    }

    static Optional<String> extractRegion(String endpoint) {
        for (Pattern pattern : ENDPOINT_PATTERN) {
            Matcher matcher = pattern.matcher(endpoint.toLowerCase());
            if (matcher.matches()) {
                return Optional.ofNullable(matcher.group(1));
            }
        }
        return Optional.empty();
    }

    private void convertDlfToOssEndpointIfNeeded() {
        if (this.endpoint.contains("dlf")) {
            this.endpoint = this.region + ".oss-dls.aliyuncs.com";
        }
    }

    @Override
    protected void doInitNormalizeAndCheckProps() {
        // Derive region from the endpoint when not explicitly set, e.g.
        // "cn-shanghai.oss-dls.aliyuncs.com" -> "cn-shanghai".
        if (StringUtils.isBlank(this.region)) {
            Optional<String> regionOptional = extractRegion(endpoint);
            if (!regionOptional.isPresent()) {
                throw new IllegalArgumentException("The region extracted from the endpoint is empty. "
                        + "Please check the endpoint format: " + endpoint + " or set oss.hdfs.region");
            }
            this.region = regionOptional.get();
        }
        convertDlfToOssEndpointIfNeeded();
        if (StringUtils.isBlank(fsDefaultFS)) {
            this.fsDefaultFS = HdfsPropertiesUtils.extractDefaultFsFromUri(origProps, SUPPORT_SCHEMA);
        }
        initConfigurationParams();
    }

    private void initConfigurationParams() {
        Map<String, String> config = loadConfigFromFile(hadoopConfigResources);
        // Converter-produced backend maps carry XML-loaded tuning keys (e.g. fs.oss.* Jindo
        // settings) and fs.defaultFS as plain entries — the config-resources key itself is not
        // part of that map — so pass Hadoop-shaped entries through like HdfsProperties and
        // JfsProperties do. The derived keys below still win.
        origProps.forEach((key, value) -> {
            if (key.startsWith("hadoop.") || key.startsWith("dfs.") || key.startsWith("fs.")) {
                config.put(key, value);
            }
        });
        config.put("fs.oss.endpoint", endpoint);
        config.put("fs.oss.accessKeyId", accessKey);
        config.put("fs.oss.accessKeySecret", secretKey);
        config.put("fs.oss.region", region);
        config.put("fs.oss.impl", JINDO_OSS_FILE_SYSTEM_IMPL);
        config.put("fs.AbstractFileSystem.oss.impl", JINDO_OSS_ABSTRACT_FILE_SYSTEM_IMPL);
        if (StringUtils.isNotBlank(securityToken)) {
            config.put("fs.oss.securityToken", securityToken);
        }
        if (StringUtils.isNotBlank(fsDefaultFS)) {
            config.put(HDFS_DEFAULT_FS_NAME, fsDefaultFS);
        }
        this.backendConfigProperties = config;
    }

    @Override
    public String storageFamilyName() {
        return "OSSHDFS";
    }

}
