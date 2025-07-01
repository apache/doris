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
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.ParamRules;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.paimon.options.Options;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class HMSProperties extends AbstractHMSProperties{

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

    @ConnectorProperty(names = {"hive.metastore.service.principal"},
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

    @ConnectorProperty(names = {"hive.enable_hms_events_incremental_sync"},
            required = false,
            description = "Whether to enable incremental sync of hms events.")
    private String hmsEventsIncrementalSyncEnabledStr;

    @ConnectorProperty(names = {"hive.hms_events_batch_size_per_rpc"},
            required = false,
            description = "The batch size of hms events per rpc.")
    private String hmsEventisBatchSizePerRpcString;



    private Map<String, String> userOverriddenHiveConfig = new HashMap<>();

    public HMSProperties(Map<String, String> origProps) {
        super(Type.HMS, origProps);
    }

    @Override
    protected String getResourceConfigPropName() {
        return "hive.conf.resources";
    }

    @Override
    protected void checkRequiredProperties() {
        super.checkRequiredProperties();
        if (!Strings.isNullOrEmpty(hiveConfResourcesConfig)) {
            checkHiveConfResourcesConfig();
        }
        buildRules().validate();
    }

    @Override
    protected void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        initHiveConf();
        initHadoopAuthenticator();
        initRefreshParams();
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


    private void initRefreshParams() {
        if (StringUtils.isNotBlank(hmsEventsIncrementalSyncEnabledStr)) {
            this.hmsEventsIncrementalSyncEnabled = BooleanUtils.toBoolean(hmsEventsIncrementalSyncEnabledStr);
        }
        if (StringUtils.isNotBlank(hmsEventisBatchSizePerRpcString)) {
            this.hmsEventsBatchSizePerRpc = Integer.parseInt(hmsEventisBatchSizePerRpcString);
        }
    }

    private void initHiveConf() {
        hiveConf = loadHiveConfFromFile(hiveConfResourcesConfig);
        initUserHiveConfig(origProps);
        userOverriddenHiveConfig.forEach(hiveConf::set);
        hiveConf.set("hive.metastore.uris", hiveMetastoreUri);
        HiveConf.setVar(hiveConf, HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT,
                String.valueOf(Config.hive_metastore_client_timeout_second));
    }

    private void checkHiveConfResourcesConfig() {
        loadConfigFromFile(hiveConfResourcesConfig);
    }

    public void toPaimonOptionsAndConf(Options options) {
        //hmsConnectionProperties.forEach(options::set);
    }

    public void toIcebergHiveCatalogProperties(Map<String, String> catalogProps) {
        // hmsConnectionProperties.forEach(catalogProps::put);
    }

    protected HiveConf loadHiveConfFromFile(String resourceConfig) {
        if (Strings.isNullOrEmpty(resourceConfig)) {
            return new HiveConf();
        }
        return CatalogConfigFileUtils.loadHiveConfFromHiveConfDir(resourceConfig);
    }

    private ParamRules buildRules() {

        return new ParamRules()
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

    private void initHadoopAuthenticator() {
        if (this.hiveMetastoreAuthenticationType.equalsIgnoreCase("kerberos")) {
            KerberosAuthenticationConfig authenticationConfig = new KerberosAuthenticationConfig(
                    this.hiveMetastoreClientPrincipal, this.hiveMetastoreClientKeytab, hiveConf);
            this.hdfsAuthenticator = HadoopAuthenticator.getHadoopAuthenticator(authenticationConfig);
            return;
        }
        if (this.hiveMetastoreAuthenticationType.equalsIgnoreCase("simple")) {
            AuthenticationConfig authenticationConfig = AuthenticationConfig.getSimpleAuthenticationConfig(hiveConf);
            this.hdfsAuthenticator = HadoopAuthenticator.getHadoopAuthenticator(authenticationConfig);
            return;
        }

        if (StringUtils.isNotBlank(this.hdfsAuthenticationType)
                && this.hdfsAuthenticationType.equalsIgnoreCase("kerberos")) {
            KerberosAuthenticationConfig authenticationConfig = new KerberosAuthenticationConfig(
                    this.hdfsKerberosPrincipal, this.hdfsKerberosKeytab, hiveConf);
            this.hdfsAuthenticator = HadoopAuthenticator.getHadoopAuthenticator(authenticationConfig);
            return;
        }
        AuthenticationConfig simpleAuthenticationConfig = AuthenticationConfig.getSimpleAuthenticationConfig(hiveConf);
        this.hdfsAuthenticator = HadoopAuthenticator.getHadoopAuthenticator(simpleAuthenticationConfig);
    }

}
