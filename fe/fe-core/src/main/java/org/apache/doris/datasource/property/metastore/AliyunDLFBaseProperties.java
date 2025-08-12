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

import org.apache.doris.datasource.property.ConnectorPropertiesUtils;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.ParamRules;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class AliyunDLFBaseProperties {
    @ConnectorProperty(names = {"dlf.access_key", "dlf.catalog.accessKeyId"},
            description = "The access key of the Aliyun DLF.")
    protected String dlfAccessKey = "";

    @ConnectorProperty(names = {"dlf.secret_key", "dlf.catalog.accessKeySecret"},
            description = "The secret key of the Aliyun DLF.")
    protected String dlfSecretKey = "";

    @ConnectorProperty(names = {"dlf.session_token", "dlf.catalog.sessionToken"},
            required = false,
            description = "The session token of the Aliyun DLF.")
    protected String dlfSessionToken = "";

    @ConnectorProperty(names = {"dlf.region"},
            description = "The region of the Aliyun DLF.")
    protected String dlfRegion = "";

    @ConnectorProperty(names = {"dlf.endpoint", "dlf.catalog.endpoint"},
            required = false,
            description = "The region of the Aliyun DLF.")
    protected String dlfEndpoint = "";

    @ConnectorProperty(names = {"dlf.catalog.uid", "dlf.uid"},
            description = "The uid of the Aliyun DLF.")
    protected String dlfUid = "";

    @ConnectorProperty(names = {"dlf.catalog.id", "dlf.catalog_id"},
            required = false,
            description = "The catalog id of the Aliyun DLF. If not set, it will be the same as dlf.uid.")
    protected String dlfCatalogId = "";

    @ConnectorProperty(names = {"dlf.access.public", "dlf.catalog.accessPublic"},
            required = false,
            description = "Enable public access to Aliyun DLF.")
    protected String dlfAccessPublic = "false";

    @ConnectorProperty(names = {DataLakeConfig.CATALOG_PROXY_MODE, "dlf.proxy.mode"},
            required = false,
            description = "The proxy mode of the Aliyun DLF. Default is DLF_ONLY.")
    protected String dlfProxyMode = "DLF_ONLY";

    public static AliyunDLFBaseProperties of(Map<String, String> properties) {
        AliyunDLFBaseProperties propertiesObj = new AliyunDLFBaseProperties();
        ConnectorPropertiesUtils.bindConnectorProperties(propertiesObj, properties);
        propertiesObj.checkAndInit();
        return propertiesObj;
    }

    private ParamRules buildRules() {

        return new ParamRules()
                .require(dlfAccessKey, "dlf.access_key is required")
                .require(dlfSecretKey, "dlf.secret_key is required");
    }

    public void checkAndInit() {
        buildRules().validate();
        if (StringUtils.isBlank(dlfEndpoint) && StringUtils.isNotBlank(dlfRegion)) {
            if (BooleanUtils.toBoolean(dlfAccessPublic)) {
                dlfEndpoint = "dlf." + dlfRegion + ".aliyuncs.com";
            } else {
                dlfEndpoint = "dlf-vpc." + dlfRegion + ".aliyuncs.com";
            }
        }
        if (StringUtils.isBlank(dlfEndpoint)) {
            throw new StoragePropertiesException("dlf.endpoint is required.");
        }
        if (StringUtils.isBlank(dlfCatalogId)) {
            this.dlfCatalogId = dlfUid;
        }
    }

}
