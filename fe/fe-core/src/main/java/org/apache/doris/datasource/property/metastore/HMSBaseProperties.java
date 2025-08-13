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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.common.CatalogConfigFileUtils;
import org.apache.doris.common.Config;
import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.common.security.authentication.KerberosAuthenticationConfig;
import org.apache.doris.datasource.property.ConnectorPropertiesUtils;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.ParamRules;

import com.google.common.base.Strings;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.HashMap;
import java.util.Map;

public class HMSBaseProperties {

    @Getter
    @ConnectorProperty(names = {"hive.metastore.uris", "uri"},
            description = "The uri of the hive metastore.")
    private String hiveMetastoreUri = "";

    @ConnectorProperty(names = {"hive.metastore.authentication.type"},
            required = false,
            description = "The authentication type of the hive metastore.")
    private String hiveMetastoreAuthenticationType = "none";

    @ConnectorProperty(names = {"hive.conf.resources"},
            required = false,
            description = "The conf resources of the hive metastore.")
    private String hiveConfResourcesConfig = "";

    @ConnectorProperty(names = {"hive.metastore.service.principal", "hive.metastore.kerberos.principal"},
            required = false,
            description = "The service principal of the hive metastore.")
    private String hiveMetastoreServicePrincipal = "";

    @ConnectorProperty(names = {"hive.metastore.client.principal"},
            required = false,
            description = "The client principal of the hive metastore.")
    private String hiveMetastoreClientPrincipal = "";

    @ConnectorProperty(names = {"hive.metastore.client.keytab"},
            required = false,
            description = "The client keytab of the hive metastore.")
    private String hiveMetastoreClientKeytab = "";

    @ConnectorProperty(names = {"hadoop.security.authentication"},
            required = false,
            description = "The authentication type of HDFS. The default value is 'none'.")
    private String hdfsAuthenticationType = "";

    @ConnectorProperty(names = {"hadoop.kerberos.principal"},
            required = false,
            description = "The principal of the kerberos authentication.")
    private String hdfsKerberosPrincipal = "";

    @ConnectorProperty(names = {"hadoop.kerberos.keytab"},
            required = false,
            description = "The keytab of the kerberos authentication.")
    private String hdfsKerberosKeytab = "";

    @Getter
    private HiveConf hiveConf;

    @Getter
    private HadoopAuthenticator hmsAuthenticator;

    private Map<String, String> userOverriddenHiveConfig = new HashMap<>();

    private Map<String, String> origProps;

    public HMSBaseProperties(Map<String, String> origProps) {
        this.origProps = origProps;
    }

    public static HMSBaseProperties of(Map<String, String> properties) {
        HMSBaseProperties propertiesObj = new HMSBaseProperties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(propertiesObj, properties);
        return propertiesObj;
    }

    private ParamRules buildRules() {
        return new ParamRules()
                .require(hiveMetastoreUri, "hive.metastore.uris or uri is required")
                .forbidIf(hiveMetastoreAuthenticationType, "simple", new String[]{
                        hiveMetastoreClientPrincipal, hiveMetastoreClientKeytab},
                        "hive.metastore.client.principal and hive.metastore.client.keytab cannot be set when "
                                + "hive.metastore.authentication.type is simple"
                )
                .requireIf(hiveMetastoreAuthenticationType, "kerberos", new String[]{
                        hiveMetastoreClientPrincipal, hiveMetastoreClientKeytab},
                        "hive.metastore.client.principal and hive.metastore.client.keytab are required when "
                                + "hive.metastore.authentication.type is kerberos");
    }

    /**
     * Helper class for initializing the Hadoop authenticator (HadoopAuthenticator).
     * <p>
     * Authentication initialization logic:
     * 1. First, check the Hive Metastore authentication type (hiveMetastoreAuthenticationType):
     * - If set to "kerberos", use the Hive Metastore principal and keytab for Kerberos authentication;
     * - If set to "simple", use the simple authentication method;
     * 2. If Hive Metastore configuration does not match, fallback to checking HDFS Kerberos
     * configuration (hdfsAuthenticationType):
     * - If set to "kerberos", use the HDFS principal and keytab for Kerberos authentication;
     * - Note: This branch exists purely for backward compatibility — using HDFS keytab is a
     * workaround, not the preferred approach;
     * 3. If none of the above conditions are met, fall back to simple authentication as the default.
     * <p>
     * The overall design prioritizes Hive Metastore's authentication settings.
     * HDFS Kerberos usage is retained for legacy compatibility, but unification under Hive configuration is
     * strongly recommended.
     */
    private void initHadoopAuthenticator() {
        if (StringUtils.isNotBlank(hiveMetastoreServicePrincipal)) {
            hiveConf.set("hive.metastore.kerberos.principal", hiveMetastoreServicePrincipal);
        }
        if (this.hiveMetastoreAuthenticationType.equalsIgnoreCase("kerberos")) {
            hiveConf.set("hadoop.security.authentication", "kerberos");
            KerberosAuthenticationConfig authenticationConfig = new KerberosAuthenticationConfig(
                    this.hiveMetastoreClientPrincipal, this.hiveMetastoreClientKeytab, hiveConf);
            this.hmsAuthenticator = HadoopAuthenticator.getHadoopAuthenticator(authenticationConfig);
            return;
        }
        if (this.hiveMetastoreAuthenticationType.equalsIgnoreCase("simple")) {
            AuthenticationConfig authenticationConfig = AuthenticationConfig.getSimpleAuthenticationConfig(hiveConf);
            this.hmsAuthenticator = HadoopAuthenticator.getHadoopAuthenticator(authenticationConfig);
            return;
        }

        if (StringUtils.isNotBlank(this.hdfsAuthenticationType)
                && this.hdfsAuthenticationType.equalsIgnoreCase("kerberos")) {
            KerberosAuthenticationConfig authenticationConfig = new KerberosAuthenticationConfig(
                    this.hdfsKerberosPrincipal, this.hdfsKerberosKeytab, hiveConf);
            this.hmsAuthenticator = HadoopAuthenticator.getHadoopAuthenticator(authenticationConfig);
            return;
        }
        AuthenticationConfig simpleAuthenticationConfig = AuthenticationConfig.getSimpleAuthenticationConfig(hiveConf);
        this.hmsAuthenticator = HadoopAuthenticator.getHadoopAuthenticator(simpleAuthenticationConfig);
    }


    private HiveConf loadHiveConfFromFile(String resourceConfig) {
        if (Strings.isNullOrEmpty(resourceConfig)) {
            return new HiveConf();
        }
        return CatalogConfigFileUtils.loadHiveConfFromHiveConfDir(resourceConfig);
    }

    public void initAndCheckParams() {
        buildRules().validate();
        this.hiveConf = loadHiveConfFromFile(hiveConfResourcesConfig);
        initUserHiveConfig(origProps);
        userOverriddenHiveConfig.forEach(hiveConf::set);
        hiveConf.set("hive.metastore.uris", hiveMetastoreUri);
        HiveConf.setVar(hiveConf, HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT,
                String.valueOf(Config.hive_metastore_client_timeout_second));
        initHadoopAuthenticator();
    }

    private void initUserHiveConfig(Map<String, String> origProps) {
        if (origProps == null || origProps.isEmpty()) {
            return;
        }
        origProps.forEach((key, value) -> {
            if (key.startsWith("hive.")) {
                userOverriddenHiveConfig.put(key, value);
            }
        });
    }

}
