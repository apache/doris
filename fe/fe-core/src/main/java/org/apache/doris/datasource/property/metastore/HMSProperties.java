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
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.ConnectorProperty;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.paimon.options.Options;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class HMSProperties extends MetastoreProperties {

    @ConnectorProperty(names = {"hive.metastore.uris"},
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

    private Map<String, String> hiveConfParams;

    private Map<String, String> hmsConnectionProperties;

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
        if ("kerberos".equalsIgnoreCase(hiveMetastoreAuthenticationType)) {
            if (Strings.isNullOrEmpty(hiveMetastoreServicePrincipal)
                    || Strings.isNullOrEmpty(hiveMetastoreClientPrincipal)
                    || Strings.isNullOrEmpty(hiveMetastoreClientKeytab)) {
                throw new IllegalArgumentException("Hive metastore authentication type is kerberos, "
                        + "but service principal, client principal or client keytab is not set.");
            }
        }
        if (Strings.isNullOrEmpty(hiveMetastoreUri)) {
            throw new IllegalArgumentException("Hive metastore uri is required.");
        }
    }

    @Override
    protected void initNormalizeAndCheckProps() throws UserException {
        super.initNormalizeAndCheckProps();
        hiveConfParams = loadConfigFromFile(getResourceConfigPropName());
        initHmsConnectionProperties();
    }

    private void initHmsConnectionProperties() {
        hmsConnectionProperties = new HashMap<>();
        hmsConnectionProperties.putAll(hiveConfParams);
        hmsConnectionProperties.put("hive.metastore.authentication.type", hiveMetastoreAuthenticationType);
        if ("kerberos".equalsIgnoreCase(hiveMetastoreAuthenticationType)) {
            hmsConnectionProperties.put("hive.metastore.service.principal", hiveMetastoreServicePrincipal);
            hmsConnectionProperties.put("hive.metastore.client.principal", hiveMetastoreClientPrincipal);
            hmsConnectionProperties.put("hive.metastore.client.keytab", hiveMetastoreClientKeytab);
        }
        hmsConnectionProperties.put("uri", hiveMetastoreUri);
    }

    private void checkHiveConfResourcesConfig() {
        loadConfigFromFile(getResourceConfigPropName());
    }

    public void toPaimonOptionsAndConf(Options options) {
        hmsConnectionProperties.forEach(options::set);
    }

    public void toIcebergHiveCatalogProperties(Map<String, String> catalogProps) {
        hmsConnectionProperties.forEach(catalogProps::put);
    }

    protected Map<String, String> loadConfigFromFile(String resourceConfig) {
        if (Strings.isNullOrEmpty(origProps.get(resourceConfig))) {
            return Maps.newHashMap();
        }
        HiveConf conf = CatalogConfigFileUtils.loadHiveConfFromHiveConfDir(origProps.get(resourceConfig));
        Map<String, String> confMap = Maps.newHashMap();
        for (Map.Entry<String, String> entry : conf) {
            confMap.put(entry.getKey(), entry.getValue());
        }
        return confMap;
    }

}
