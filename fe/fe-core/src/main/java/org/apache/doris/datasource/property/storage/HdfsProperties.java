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

import com.google.common.base.Strings;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HdfsProperties extends HdfsCompatibleProperties {

    @ConnectorProperty(names = {"hdfs.authentication.type", "hadoop.security.authentication"},
            required = false,
            description = "The authentication type of HDFS. The default value is 'none'.")
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
            description = "The username of Hadoop. Doris will user this user to access HDFS")
    private String hadoopUsername = "";

    @ConnectorProperty(names = {"hdfs.impersonation.enabled"},
            required = false,
            supported = false,
            description = "Whether to enable the impersonation of HDFS.")
    private boolean hdfsImpersonationEnabled = false;

    private Map<String, String> backendConfigProperties;

    /**
     * The final HDFS configuration map that determines the effective settings.
     * Priority rules:
     * 1. If a key exists in `overrideConfig` (user-provided settings), its value takes precedence.
     * 2. If a key is not present in `overrideConfig`, the value from `hdfs-site.xml` or `core-site.xml` is used.
     * 3. This map should be used to read the resolved HDFS configuration, ensuring the correct precedence is applied.
     */
    private Map<String, String> userOverriddenHdfsConfig;

    private static final List<String> HDFS_PROPERTIES_KEYS = Arrays.asList("hdfs.authentication.type",
            "hadoop.security.authentication", "hadoop.username",
            "hdfs.authentication.kerberos.principal", "hadoop.kerberos.principal", "dfs.nameservices");

    public HdfsProperties(Map<String, String> origProps) {
        super(Type.HDFS, origProps);
    }

    public static boolean guessIsMe(Map<String, String> props) {
        if (MapUtils.isEmpty(props)) {
            return false;
        }
        if (HDFS_PROPERTIES_KEYS.stream().anyMatch(props::containsKey)) {
            return true;
        }
        // This logic is somewhat hacky due to the shared usage of base parameters
        // between native HDFS and HDFS-compatible implementations (such as OSS_HDFS).
        // Since both may contain keys defined in HDFS_COMPATIBLE_PROPERTIES_KEYS,
        // we cannot reliably determine whether the configuration belongs to native HDFS
        // based on the presence of those keys alone.
        // To work around this, we explicitly exclude OSS_HDFS by checking
        // !OSSHdfsProperties.guessIsMe(props).
        // This is currently the most practical way to differentiate native HDFS
        // from HDFS-compatible systems using shared configuration.
        return HDFS_COMPATIBLE_PROPERTIES_KEYS.stream().anyMatch(props::containsKey)
                && (!OSSHdfsProperties.guessIsMe(props));
    }

    @Override
    protected void initNormalizeAndCheckProps() throws UserException {
        super.initNormalizeAndCheckProps();
        extractUserOverriddenHdfsConfig(origProps);
        initHadoopConfiguration();
        initBackendConfigProperties();
    }

    private void extractUserOverriddenHdfsConfig(Map<String, String> origProps) {
        if (MapUtils.isEmpty(origProps)) {
            return;
        }
        userOverriddenHdfsConfig = new HashMap<>();
        origProps.forEach((key, value) -> {
            if (key.startsWith("hadoop.") || key.startsWith("dfs.") || key.equals("fs.defaultFS")) {
                userOverriddenHdfsConfig.put(key, value);
            }
        });

    }

    protected void checkRequiredProperties() {
        super.checkRequiredProperties();
        if ("kerberos".equalsIgnoreCase(hdfsAuthenticationType) && (Strings.isNullOrEmpty(hdfsKerberosPrincipal)
                || Strings.isNullOrEmpty(hdfsKerberosKeytab))) {
            throw new IllegalArgumentException("HDFS authentication type is kerberos, "
                    + "but principal or keytab is not set.");
        }
        // If fsDefaultFS is not explicitly provided, we attempt to infer it from the 'uri' field.
        // However, the 'uri' is not a dedicated HDFS-specific property and may be present
        // even when the user is configuring multiple storage backends.
        // Additionally, since we are not using FileSystem.get(Configuration conf),
        // fsDefaultFS is not strictly required here.
        // This is a best-effort fallback to populate fsDefaultFS when possible.
        if (StringUtils.isBlank(fsDefaultFS)) {
            try {
                this.fsDefaultFS = HdfsPropertiesUtils.validateAndGetUri(origProps);
            } catch (UserException e) {
                //ignore
            }
        }
    }

    private void initHadoopConfiguration() {
        Configuration conf = new Configuration(true);
        Map<String, String> allProps = loadConfigFromFile(getResourceConfigPropName());
        allProps.forEach(conf::set);
        if (MapUtils.isNotEmpty(userOverriddenHdfsConfig)) {
            userOverriddenHdfsConfig.forEach(conf::set);
        }
        if (StringUtils.isNotBlank(fsDefaultFS)) {
            conf.set("fs.defaultFS", fsDefaultFS);
        }
        conf.set("hdfs.security.authentication", hdfsAuthenticationType);
        if ("kerberos".equalsIgnoreCase(hdfsAuthenticationType)) {
            conf.set("hadoop.kerberos.principal", hdfsKerberosPrincipal);
            conf.set("hadoop.kerberos.keytab", hdfsKerberosKeytab);
        }
        if (StringUtils.isNotBlank(hadoopUsername)) {
            conf.set("hadoop.username", hadoopUsername);
        }
        this.configuration = conf;
    }

    private void initBackendConfigProperties() {
        Map<String, String> backendConfigProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : configuration) {
            backendConfigProperties.put(entry.getKey(), entry.getValue());
        }

        this.backendConfigProperties = backendConfigProperties;
    }

    public Configuration getHadoopConfiguration() {
        return this.configuration;
    }

    //fixme be should send use input params
    @Override
    public Map<String, String> getBackendConfigProperties() {
        return backendConfigProperties;
    }

    @Override
    public String validateAndNormalizeUri(String url) throws UserException {
        return HdfsPropertiesUtils.convertUrlToFilePath(url);
    }

    @Override
    public String validateAndGetUri(Map<String, String> loadProps) throws UserException {
        return HdfsPropertiesUtils.validateAndGetUri(loadProps);
    }

    @Override
    public String getStorageName() {
        return "HDFS";
    }
}
