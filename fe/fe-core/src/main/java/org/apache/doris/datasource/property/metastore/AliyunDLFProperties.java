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
    private String dlfAccessKey = "";

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
            description = "Enable public access to Aliyun DLF.")
    private String dlfAccessPublic = "false";

    private static final String DLF_PREFIX = "dlf.";

    private Map<String, String> otherDlfProps = Maps.newHashMap();

    public AliyunDLFProperties(Map<String, String> origProps) {
        super(Type.DLF, origProps);
        // Other properties that start with "dlf." will be saved in otherDlfProps,
        // and passed to the DLF client.
        for (Map.Entry<String, String> entry : origProps.entrySet()) {
            if (entry.getKey().startsWith(DLF_PREFIX)) {
                otherDlfProps.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public void toPaimonOptions(Options options) {
        // See DataLakeConfig.java for property keys
        options.set("dlf.catalog.region", dlfRegion);
        options.set("dlf.catalog.endpoint", getEndpointOrFromRegion(dlfEndpoint, dlfRegion, dlfAccessPublic));
        options.set("dlf.catalog.proxyMode", "DLF_ONLY");
        options.set("dlf.catalog.accessKeyId", dlfAccessKey);
        options.set("dlf.catalog.accessKeySecret", dlfSecretKey);
        options.set("dlf.catalog.accessPublic", dlfAccessPublic);
        options.set("dlf.catalog.uid", dlfUid);
        options.set("dlf.catalog.createDefaultDBIfNotExist", "false");

        for (Map.Entry<String, String> entry : otherDlfProps.entrySet()) {
            options.set(entry.getKey(), entry.getValue());
        }
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
    protected String getResouceConfigPropName() {
        return "dlf.resouce_config";
    }

    @Getter
    public static class OSSConfiguration {
        private Map<String, String> conf = Maps.newHashMap();

        public OSSConfiguration(AliyunDLFProperties props) {
            conf.put("oss.region", getOssRegionFromDlfRegion(props.dlfRegion));
            conf.put("oss.endpoint", getOssEndpointFromDlfRegion(props.dlfRegion, props.dlfAccessPublic));
            conf.put("oss.access_key", props.dlfAccessKey);
            conf.put("oss.secret_key", props.dlfSecretKey);
        }

        private String getOssRegionFromDlfRegion(String dlfRegion) {
            return "oss-" + dlfRegion;
        }

        private String getOssEndpointFromDlfRegion(String dlfRegion, String dlfAccessPublic) {
            if ("true".equalsIgnoreCase(dlfAccessPublic)) {
                return "oss-" + dlfRegion + ".aliyuncs.com";
            } else {
                return "oss-" + dlfRegion + "-internal.aliyuncs.com";
            }
        }

        private String getEndpointOrFromRegion(String endpoint, String region) {
            if (!Strings.isNullOrEmpty(endpoint)) {
                return endpoint;
            }
            return "dlf-vpc." + region + ".aliyuncs.com";
        }
    }
}
