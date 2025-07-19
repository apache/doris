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
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.datasource.property.ConnectorProperty;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    @ConnectorProperty(names = {"ipc.client.fallback-to-simple-auth-allowed"},
            required = false,
            description = "Whether to allow fallback to simple authentication.")
    private String allowFallbackToSimpleAuth = "";


    @ConnectorProperty(names = {"fs.defaultFS"}, required = false, description = "")
    protected String fsDefaultFS = "";

    @ConnectorProperty(names = {"hadoop.config.resources"},
            required = false,
            description = "The xml files of Hadoop configuration.")
    protected String hadoopConfigResources = "";

    private String dfsNameServices;

    private static final String DFS_NAME_SERVICES_KEY = "dfs.nameservices";

    private Map<String, String> backendConfigProperties;

    private static final Set<String> supportSchema = ImmutableSet.of("hdfs", "viewfs");

    /**
     * The final HDFS configuration map that determines the effective settings.
     * Priority rules:
     * 1. If a key exists in `overrideConfig` (user-provided settings), its value takes precedence.
     * 2. If a key is not present in `overrideConfig`, the value from `hdfs-site.xml` or `core-site.xml` is used.
     * 3. This map should be used to read the resolved HDFS configuration, ensuring the correct precedence is applied.
     */
    private Map<String, String> userOverriddenHdfsConfig;

    private static final List<String> HDFS_PROPERTIES_KEYS = Arrays.asList("hdfs.authentication.type",
            "hadoop.security.authentication", "hadoop.username", "fs.defaultFS",
            "hdfs.authentication.kerberos.principal", "hadoop.kerberos.principal", DFS_NAME_SERVICES_KEY,
            "hdfs.config.resources");

    public HdfsProperties(Map<String, String> origProps) {
        super(Type.HDFS, origProps);
    }

    public static boolean guessIsMe(Map<String, String> props) {
        if (MapUtils.isEmpty(props)) {
            return false;
        }
        if (HdfsPropertiesUtils.validateUriIsHdfsUri(props, supportSchema)) {
            return true;
        }
        return HDFS_PROPERTIES_KEYS.stream().anyMatch(props::containsKey);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        if (StringUtils.isBlank(fsDefaultFS)) {
            this.fsDefaultFS = HdfsPropertiesUtils.extractDefaultFsFromUri(origProps, supportSchema);
        }
        extractUserOverriddenHdfsConfig(origProps);
        initHadoopConfiguration();
        initBackendConfigProperties();
        hadoopAuthenticator = HadoopAuthenticator.getHadoopAuthenticator(configuration);
    }

    private void extractUserOverriddenHdfsConfig(Map<String, String> origProps) {
        if (MapUtils.isEmpty(origProps)) {
            return;
        }
        userOverriddenHdfsConfig = new HashMap<>();
        origProps.forEach((key, value) -> {
            if (key.startsWith("hadoop.") || key.startsWith("dfs.") || key.startsWith("fs.")) {
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
    }

    private void initHadoopConfiguration() {
        Configuration conf = new Configuration(true);
        Map<String, String> allProps = loadConfigFromFile(hadoopConfigResources);
        allProps.forEach(conf::set);
        if (MapUtils.isNotEmpty(userOverriddenHdfsConfig)) {
            userOverriddenHdfsConfig.forEach(conf::set);
        }
        if (StringUtils.isNotBlank(fsDefaultFS)) {
            conf.set(HDFS_DEFAULT_FS_NAME, fsDefaultFS);
        }
        if (StringUtils.isNotBlank(allowFallbackToSimpleAuth)) {
            conf.set("ipc.client.fallback-to-simple-auth-allowed", allowFallbackToSimpleAuth);
        } else {
            conf.set("ipc.client.fallback-to-simple-auth-allowed", "true");
        }
        conf.set("hdfs.security.authentication", hdfsAuthenticationType);
        if ("kerberos".equalsIgnoreCase(hdfsAuthenticationType)) {
            conf.set("hadoop.kerberos.principal", hdfsKerberosPrincipal);
            conf.set("hadoop.kerberos.keytab", hdfsKerberosKeytab);
        }
        if (StringUtils.isNotBlank(hadoopUsername)) {
            conf.set("hadoop.username", hadoopUsername);
        }
        this.dfsNameServices = conf.get(DFS_NAME_SERVICES_KEY, "");
        if (StringUtils.isBlank(fsDefaultFS)) {
            this.fsDefaultFS = conf.get(HDFS_DEFAULT_FS_NAME, "");
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

    public boolean isKerberos() {
        return "kerberos".equalsIgnoreCase(hdfsAuthenticationType);
    }

    //fixme be should send use input params
    @Override
    public Map<String, String> getBackendConfigProperties() {
        return backendConfigProperties;
    }

    @Override
    public String validateAndNormalizeUri(String url) throws UserException {
        return HdfsPropertiesUtils.convertUrlToFilePath(url, this.dfsNameServices, this.fsDefaultFS, supportSchema);

    }

    @Override
    public String validateAndGetUri(Map<String, String> loadProps) throws UserException {
        return HdfsPropertiesUtils.validateAndGetUri(loadProps, this.dfsNameServices, this.fsDefaultFS, supportSchema);
    }

    @Override
    public String getStorageName() {
        return "HDFS";
    }

    @Override
    public void initializeHadoopStorageConfig() {
        hadoopStorageConfig = configuration;
    }
}
