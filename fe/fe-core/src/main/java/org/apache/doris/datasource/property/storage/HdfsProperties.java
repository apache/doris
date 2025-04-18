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

import java.util.HashMap;
import java.util.Map;

public class HdfsProperties extends StorageProperties {

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

    @ConnectorProperty(names = {"hadoop.config.resources"},
            required = false,
            description = "The xml files of Hadoop configuration.")
    private String hadoopConfigResources = "";

    @ConnectorProperty(names = {"hdfs.impersonation.enabled"},
            required = false,
            supported = false,
            description = "Whether to enable the impersonation of HDFS.")
    private boolean hdfsImpersonationEnabled = false;

    @ConnectorProperty(names = {"fs.defaultFS"}, required = false, description = "")
    private String fsDefaultFS = "";

    private Configuration configuration;

    private Map<String, String> backendConfigProperties;

    /**
     * The final HDFS configuration map that determines the effective settings.
     * Priority rules:
     * 1. If a key exists in `overrideConfig` (user-provided settings), its value takes precedence.
     * 2. If a key is not present in `overrideConfig`, the value from `hdfs-site.xml` or `core-site.xml` is used.
     * 3. This map should be used to read the resolved HDFS configuration, ensuring the correct precedence is applied.
     */
    private Map<String, String> userOverriddenHdfsConfig;

    public HdfsProperties(Map<String, String> origProps) {
        super(Type.HDFS, origProps);
    }

    public static boolean guessIsMe(Map<String, String> props) {
        if (MapUtils.isEmpty(props)) {
            return false;
        }
        if (props.containsKey("hadoop.config.resources") || props.containsKey("hadoop.security.authentication")
                || props.containsKey("dfs.nameservices") || props.containsKey("fs.defaultFS")) {
            return true;
        }
        return false;
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

    @Override
    protected String getResourceConfigPropName() {
        return "hadoop.config.resources";
    }

    protected void checkRequiredProperties() {
        super.checkRequiredProperties();
        if ("kerberos".equalsIgnoreCase(hdfsAuthenticationType) && (Strings.isNullOrEmpty(hdfsKerberosPrincipal)
                || Strings.isNullOrEmpty(hdfsKerberosKeytab))) {
            throw new IllegalArgumentException("HDFS authentication type is kerberos, "
                    + "but principal or keytab is not set.");
        }

        if (StringUtils.isBlank(fsDefaultFS)) {
            this.fsDefaultFS = HdfsPropertiesUtils.constructDefaultFsFromUri(origProps);
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
