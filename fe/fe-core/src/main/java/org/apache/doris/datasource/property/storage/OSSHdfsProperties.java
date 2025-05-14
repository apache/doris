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

import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.Map;

/**
 * todo
 * Should consider using the same class as DLF Properties.
 */
public class OSSHdfsProperties extends HdfsCompatibleProperties {

    @Setter
    @ConnectorProperty(names = {"oss.endpoint"},
            description = "The endpoint of OSS.")
    protected String endpoint = "";

    @ConnectorProperty(names = {"oss.access_key"}, description = "The access key of OSS.")
    protected String accessKey = "";

    @ConnectorProperty(names = {"oss.secret_key"}, description = "The secret key of OSS.")
    protected String secretKey = "";

    @ConnectorProperty(names = {"oss.region"},
            description = "The region of OSS.")
    protected String region;

    /**
     * TODO: Do not expose to users for now.
     * Mutual exclusivity between parameters should be validated at the framework level
     * to prevent messy, repetitive checks in application code.
     */
    @ConnectorProperty(names = {"oss.security_token"}, required = false,
            description = "The security token of OSS.")
    protected String securityToken = "";

    private static final String OSS_ENDPOINT_KEY_NAME = "oss.endpoint";

    private Map<String, String> backendConfigProperties;

    protected OSSHdfsProperties(Map<String, String> origProps) {
        super(Type.HDFS, origProps);
    }

    public static boolean guessIsMe(Map<String, String> props) {
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
    protected void initNormalizeAndCheckProps() throws UserException {
        super.initNormalizeAndCheckProps();
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
        Configuration conf = new Configuration(true);
        // TODO: Currently we load all config parameters and pass them to the BE directly.
        // In the future, we should pass the path to the configuration directory instead,
        // and let the BE load the config file on its own.
        Map<String, String> config = loadConfigFromFile(getResourceConfigPropName());
        config.put("fs.oss.endpoint", endpoint);
        config.put("fs.oss.accessKeyId", accessKey);
        config.put("fs.oss.accessKeySecret", secretKey);
        config.put("fs.oss.region", region);
        config.put("fs.oss.impl", "com.aliyun.jindodata.oss.JindoOssFileSystem");
        config.put("fs.AbstractFileSystem.oss.impl", "com.aliyun.jindodata.oss.JindoOSS");
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
    public Configuration getHadoopConfiguration() {
        return configuration;
    }

    @Override
    public String getStorageName() {
        return "HDFS";
    }
}
