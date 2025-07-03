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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.ConnectorProperty;

import com.google.common.collect.ImmutableSet;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * todo
 * Should consider using the same class as DLF Properties.
 * Configuration properties for OSS-HDFS.
 *
 * <p><strong>Important:</strong> It is recommended to use the "oss.hdfs" prefix for all OSS-related
 * configuration properties instead of the standalone "oss" prefix.
 * This is because when both "oss" and "oss.hdfs" prefixed parameters are provided simultaneously,
 * the system cannot distinguish which parameter belongs to which prefix, leading to ambiguity and confusion.
 * To prevent such conflicts, the standalone "oss" prefix is planned to be fully deprecated in the future.
 *
 * <p>Users should migrate their configurations to use the "oss.hdfs" prefix to ensure clarity
 * and future compatibility.
 */
public class OSSHdfsProperties extends HdfsCompatibleProperties {

    @Setter
    @ConnectorProperty(names = {"oss.hdfs.endpoint",
            "oss.endpoint"},
            description = "The endpoint of OSS.")
    protected String endpoint = "";

    @ConnectorProperty(names = {"oss.hdfs.access_key", "oss.access_key"}, description = "The access key of OSS.")
    protected String accessKey = "";

    @ConnectorProperty(names = {"oss.hdfs.secret_key", "oss.secret_key"}, description = "The secret key of OSS.")
    protected String secretKey = "";

    @ConnectorProperty(names = {"oss.hdfs.region", "oss.region"},
            required = false,
            description = "The region of OSS.")
    protected String region;

    @ConnectorProperty(names = {"oss.hdfs.fs.defaultFS"}, required = false, description = "")
    protected String fsDefaultFS = "";

    @ConnectorProperty(names = {"oss.hdfs.hadoop.config.resources"},
            required = false,
            description = "The xml files of Hadoop configuration.")
    protected String hadoopConfigResources = "";

    /**
     * TODO: Do not expose to users for now.
     * Mutual exclusivity between parameters should be validated at the framework level
     * to prevent messy, repetitive checks in application code.
     */
    @ConnectorProperty(names = {"oss.hdfs.security_token", "oss.security_token"}, required = false,
            description = "The security token of OSS.")
    protected String securityToken = "";

    private static final String OSS_ENDPOINT_KEY_NAME = "oss.endpoint";

    private Map<String, String> backendConfigProperties;

    private static final Pattern ENDPOINT_PATTERN = Pattern
            .compile("(?:https?://)?([a-z]{2}-[a-z0-9-]+)\\.oss-dls\\.aliyuncs\\.com");

    private static final Set<String> supportSchema = ImmutableSet.of("oss", "hdfs");

    protected OSSHdfsProperties(Map<String, String> origProps) {
        super(Type.HDFS, origProps);
    }

    private static final String OSS_HDFS_PREFIX_KEY = "oss.hdfs.";

    public static boolean guessIsMe(Map<String, String> props) {
        boolean enable = props.entrySet().stream()
                .anyMatch(e -> e.getKey().equalsIgnoreCase(OSS_HDFS_PREFIX_KEY) && Boolean.parseBoolean(e.getValue()));
        if (enable) {
            return true;
        }
        String endpoint = props.get(OSS_ENDPOINT_KEY_NAME);
        if (StringUtils.isBlank(endpoint)) {
            return false;
        }
        return endpoint.endsWith(OSS_HDFS_ENDPOINT_SUFFIX);
    }

    @Override
    protected void checkRequiredProperties() {
        super.checkRequiredProperties();
        if (!endpointIsValid(endpoint)) {
            throw new IllegalArgumentException("Property oss.endpoint is required and must be a valid OSS endpoint.");
        }
    }

    @Override
    protected void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        Matcher matcher = ENDPOINT_PATTERN.matcher(endpoint.toLowerCase());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("The endpoint is not a valid OSS HDFS endpoint: " + endpoint
                    + ". It should match the pattern: <region>.oss-dls.aliyuncs.com");
        }
        // Extract region from the endpoint, e.g., "cn-shanghai.oss-dls.aliyuncs.com" -> "cn-shanghai"
        if (StringUtils.isBlank(this.region)) {
            this.region = matcher.group(1);
            if (StringUtils.isBlank(this.region)) {
                throw new IllegalArgumentException("The region extracted from the endpoint is empty. "
                        + "Please check the endpoint format: {} or set oss.region" + endpoint);
            }
        }
        if (StringUtils.isBlank(fsDefaultFS)) {
            this.fsDefaultFS = HdfsPropertiesUtils.extractDefaultFsFromUri(origProps, supportSchema);
        }
        initConfigurationParams();
    }

    private static final String OSS_HDFS_ENDPOINT_SUFFIX = ".oss-dls.aliyuncs.com";

    private boolean endpointIsValid(String endpoint) {
        // example: cn-shanghai.oss-dls.aliyuncs.com contains the "oss-dls.aliyuncs".
        // https://www.alibabacloud.com/help/en/e-mapreduce/latest/oss-kusisurumen
        return StringUtils.isNotBlank(endpoint) && endpoint.endsWith(OSS_HDFS_ENDPOINT_SUFFIX);
    }

    @Override
    public Map<String, String> getBackendConfigProperties() {
        return backendConfigProperties;
    }

    private void initConfigurationParams() {
        Configuration conf = new Configuration();
        // TODO: Currently we load all config parameters and pass them to the BE directly.
        // In the future, we should pass the path to the configuration directory instead,
        // and let the BE load the config file on its own.
        Map<String, String> config = loadConfigFromFile(hadoopConfigResources);
        config.put("fs.oss.endpoint", endpoint);
        config.put("fs.oss.accessKeyId", accessKey);
        config.put("fs.oss.accessKeySecret", secretKey);
        config.put("fs.oss.region", region);
        config.put("fs.oss.impl", "com.aliyun.jindodata.oss.JindoOssFileSystem");
        config.put("fs.AbstractFileSystem.oss.impl", "com.aliyun.jindodata.oss.JindoOSS");
        if (StringUtils.isNotBlank(fsDefaultFS)) {
            config.put(HDFS_DEFAULT_FS_NAME, fsDefaultFS);
        }
        config.forEach(conf::set);
        this.backendConfigProperties = config;
        this.configuration = conf;
    }

    @Override
    public String validateAndNormalizeUri(String url) throws UserException {
        return validateUri(url);
    }

    @Override
    public String validateAndGetUri(Map<String, String> loadProps) throws UserException {
        String uri = loadProps.get("uri");
        return validateUri(uri);
    }

    private String validateUri(String uri) throws UserException {
        if (StringUtils.isBlank(uri)) {
            throw new UserException("The uri is empty.");
        }
        URI uriObj = URI.create(uri);
        if (uriObj.getScheme() == null) {
            throw new UserException("The uri scheme is empty.");
        }
        if (!uriObj.getScheme().equalsIgnoreCase("oss")) {
            throw new UserException("The uri scheme is not oss.");
        }
        return uriObj.toString();
    }

    @Override
    public String getStorageName() {
        return "HDFS";
    }
}
