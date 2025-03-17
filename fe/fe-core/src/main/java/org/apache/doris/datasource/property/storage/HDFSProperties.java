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

import org.apache.doris.datasource.property.ConnectorProperty;

import com.google.common.base.Strings;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public class HDFSProperties extends StorageProperties {

    @ConnectorProperty(names = {"hdfs.authentication.type", "hadoop.security.authentication"},
            required = false,
            description = "The authentication type of HDFS. The default value is 'none'.")
    private String hdfsAuthenticationType = "none";

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

    /**
     * The final HDFS configuration map that determines the effective settings.
     * Priority rules:
     * 1. If a key exists in `overrideConfig` (user-provided settings), its value takes precedence.
     * 2. If a key is not present in `overrideConfig`, the value from `hdfs-site.xml` or `core-site.xml` is used.
     * 3. This map should be used to read the resolved HDFS configuration, ensuring the correct precedence is applied.
     */
    Map<String, String> finalHdfsConfig;

    public HDFSProperties(Map<String, String> origProps) {
        super(Type.HDFS, origProps);
        // to be care     setOrigProps(matchParams);
        loadFinalHdfsConfig(origProps);
    }

    private void loadFinalHdfsConfig(Map<String, String> origProps) {
        if (MapUtils.isEmpty(origProps)) {
            return;
        }
        finalHdfsConfig = new HashMap<>();
        Configuration configuration = new Configuration();
        origProps.forEach((k, v) -> {
            if (null != configuration.getTrimmed(k)) {
                finalHdfsConfig.put(k, v);
            }
        });

    }

    @Override
    protected String getResourceConfigPropName() {
        return "hadoop.config.resources";
    }

    protected void checkRequiredProperties() {
        super.checkRequiredProperties();
        checkConfigFileIsValid(hadoopConfigResources);
        if ("kerberos".equalsIgnoreCase(hdfsAuthenticationType)) {
            if (Strings.isNullOrEmpty(hdfsKerberosPrincipal)
                    || Strings.isNullOrEmpty(hdfsKerberosKeytab)) {
                throw new IllegalArgumentException("HDFS authentication type is kerberos, "
                        + "but principal or keytab is not set.");
            }
        }
    }

    private void checkConfigFileIsValid(String configFile) {
        if (Strings.isNullOrEmpty(configFile)) {
            return;
        }
        loadConfigFromFile(getResourceConfigPropName());
    }

    public void toHadoopConfiguration(Configuration conf) {
        Map<String, String> allProps = loadConfigFromFile(getResourceConfigPropName());
        allProps.forEach(conf::set);
        if (MapUtils.isNotEmpty(finalHdfsConfig)) {
            finalHdfsConfig.forEach(conf::set);
        }
        //todo waiting be support should use new params
        conf.set("hdfs.security.authentication", hdfsAuthenticationType);
        if ("kerberos".equalsIgnoreCase(hdfsAuthenticationType)) {
            conf.set("hadoop.kerberos.principal", hdfsKerberosPrincipal);
            conf.set("hadoop.kerberos.keytab", hdfsKerberosKeytab);
        }
        if (!Strings.isNullOrEmpty(hadoopUsername)) {
            conf.set("hadoop.username", hadoopUsername);
        }
    }

    public Configuration getHadoopConfiguration() {
        Configuration conf = new Configuration(false);
        Map<String, String> allProps = loadConfigFromFile(getResourceConfigPropName());
        allProps.forEach(conf::set);
        if (MapUtils.isNotEmpty(finalHdfsConfig)) {
            finalHdfsConfig.forEach(conf::set);
        }
        conf.set("hdfs.security.authentication", hdfsAuthenticationType);
        if ("kerberos".equalsIgnoreCase(hdfsAuthenticationType)) {
            conf.set("hadoop.kerberos.principal", hdfsKerberosPrincipal);
            conf.set("hadoop.kerberos.keytab", hdfsKerberosKeytab);
        }
        if (!Strings.isNullOrEmpty(hadoopUsername)) {
            conf.set("hadoop.username", hadoopUsername);
        }

        return conf;
    }

    //fixme be should send use input params
    @Override
    public Map<String, String> getBackendConfigProperties() {
        Configuration configuration = getHadoopConfiguration();
        Map<String, String> backendConfigProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : configuration) {
            backendConfigProperties.put(entry.getKey(), entry.getValue());
        }

        return backendConfigProperties;
    }
}
