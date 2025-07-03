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

import org.apache.doris.datasource.property.ConnectorProperty;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.paimon.options.Options;

import java.util.Map;

public class AliyunDLFProperties extends MetastoreProperties {

    @ConnectorProperty(names = {"dlf.access_key", "dlf.catalog.accessKeyId"},
            description = "The access key of the Aliyun DLF.")
    public String dlfAccessKey = "";

    @ConnectorProperty(names = {"dlf.secret_key", "dlf.catalog.accessKeySecret"},
            description = "The secret key of the Aliyun DLF.")
    private String dlfSecretKey = "";

    @ConnectorProperty(names = {"dlf.region"},
            description = "The region of the Aliyun DLF.")
    private String dlfRegion = "";

    @ConnectorProperty(names = {"dlf.endpoint", "dlf.catalog.endpoint"},
            required = false,
            description = "The region of the Aliyun DLF.")
    private String dlfEndpoint = "";

    @ConnectorProperty(names = {"dlf.uid", "dlf.catalog.uid"},
            description = "The uid of the Aliyun DLF.")
    private String dlfUid = "";

    @ConnectorProperty(names = {"dlf.access.public", "dlf.catalog.accessPublic"},
            required = false,
            description = "Enable public access to Aliyun DLF.")
    private String dlfAccessPublic = "false";

    private static final String DLF_PREFIX = "dlf.";

    @Getter
    private final Map<String, String> otherDlfProps = Maps.newHashMap();

    private Map<String, String> dlfConnectProps = Maps.newHashMap();

    public AliyunDLFProperties(Map<String, String> origProps) {
        super(Type.DLF, origProps);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        // Other properties that start with "dlf." will be saved in otherDlfProps,
        // and passed to the DLF client.
        for (Map.Entry<String, String> entry : origProps.entrySet()) {
            if (entry.getKey().startsWith(DLF_PREFIX) && !matchedProperties.containsKey(entry.getKey())) {
                otherDlfProps.put(entry.getKey(), entry.getValue());
            }
        }
        initDlfConnectProps();
    }

    private void initDlfConnectProps() {
        dlfConnectProps.put("dlf.catalog.region", dlfRegion);
        dlfConnectProps.put("dlf.catalog.endpoint", getEndpointOrFromRegion(dlfEndpoint, dlfRegion, dlfAccessPublic));
        dlfConnectProps.put("dlf.catalog.proxyMode", "DLF_ONLY");
        dlfConnectProps.put("dlf.catalog.accessKeyId", dlfAccessKey);
        dlfConnectProps.put("dlf.catalog.accessKeySecret", dlfSecretKey);
        dlfConnectProps.put("dlf.catalog.accessPublic", dlfAccessPublic);
        dlfConnectProps.put("dlf.catalog.uid", dlfUid);
        dlfConnectProps.put("dlf.catalog.createDefaultDBIfNotExist", "false");
        otherDlfProps.forEach((dlfConnectProps::put));
    }

    public void toPaimonOptions(Options options) {
        // See DataLakeConfig.java for property keys
        dlfConnectProps.forEach(options::set);
    }

    private String getEndpointOrFromRegion(String endpoint, String region, String dlfAccessPublic) {
        if (!Strings.isNullOrEmpty(endpoint)) {
            return endpoint;
        } else {
            // https://www.alibabacloud.com/help/en/dlf/dlf-1-0/regions-and-endpoints
            if ("true".equalsIgnoreCase(dlfAccessPublic)) {
                return "dlf." + region + ".aliyuncs.com";
            } else {
                return "dlf-vpc." + region + ".aliyuncs.com";
            }
        }
    }

    @Override
    protected String getResourceConfigPropName() {
        return "dlf.resource_config";
    }
}
