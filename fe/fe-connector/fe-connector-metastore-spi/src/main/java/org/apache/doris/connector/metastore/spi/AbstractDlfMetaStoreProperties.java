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

package org.apache.doris.connector.metastore.spi;

import org.apache.doris.connector.metastore.DlfMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;

/** Shared Aliyun DLF property binding and neutral catalog configuration. */
public abstract class AbstractDlfMetaStoreProperties extends AbstractMetaStoreProperties
        implements DlfMetaStoreProperties {

    // These aliases must stay aligned with both OSS binders so one credential set reaches metadata and storage.
    @ConnectorProperty(names = {"dlf.access_key", "dlf.catalog.accessKeyId"}, required = false, sensitive = true,
            description = "DLF access key id.")
    private String accessKey = "";

    @ConnectorProperty(names = {"dlf.secret_key", "dlf.catalog.secret_key", "dlf.catalog.accessKeySecret"},
            required = false, sensitive = true,
            description = "DLF access key secret.")
    private String secretKey = "";

    @ConnectorProperty(names = {"dlf.session_token", "dlf.catalog.sessionToken", "dlf.catalog.securityToken"},
            required = false, sensitive = true,
            description = "DLF session/security token.")
    private String sessionToken = "";

    @ConnectorProperty(names = {"dlf.region"}, required = false,
            description = "DLF region used to derive the endpoint when it is not set.")
    private String region = "";

    @ConnectorProperty(names = {"dlf.endpoint", "dlf.catalog.endpoint"}, required = false,
            description = "DLF endpoint.")
    private String endpoint = "";

    @ConnectorProperty(names = {"dlf.catalog.uid", "dlf.uid"}, required = false,
            description = "DLF account uid.")
    private String uid = "";

    @ConnectorProperty(names = {"dlf.catalog.id", "dlf.catalog_id"}, required = false,
            description = "DLF catalog id, defaulting to the uid.")
    private String catalogId = "";

    @ConnectorProperty(names = {"dlf.access.public", "dlf.catalog.accessPublic"}, required = false,
            description = "Whether to use the public DLF endpoint instead of the VPC endpoint.")
    private String accessPublic = "false";

    @ConnectorProperty(names = {"dlf.catalog.proxyMode", "dlf.proxy.mode"}, required = false,
            description = "DLF proxy mode.")
    private String proxyMode = "DLF_ONLY";

    private final Map<String, String> storageHadoopConfig;

    protected AbstractDlfMetaStoreProperties(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        super(raw);
        this.storageHadoopConfig = storageHadoopConfig;
    }

    @Override
    public String providerName() {
        return "DLF";
    }

    @Override
    public boolean needsStorage() {
        return true;
    }

    protected void validateConnection() {
        if (StringUtils.isBlank(accessKey)) {
            throw new IllegalArgumentException("dlf.access_key is required");
        }
        if (StringUtils.isBlank(secretKey)) {
            throw new IllegalArgumentException("dlf.secret_key is required");
        }
        if (StringUtils.isBlank(endpoint) && StringUtils.isBlank(region)) {
            throw new IllegalArgumentException("dlf.endpoint is required.");
        }
    }

    @Override
    public Map<String, String> toDlfCatalogConf() {
        String resolvedEndpoint = endpoint;
        if (StringUtils.isBlank(resolvedEndpoint) && StringUtils.isNotBlank(region)) {
            resolvedEndpoint = BooleanUtils.toBoolean(accessPublic)
                    ? "dlf." + region + ".aliyuncs.com"
                    : "dlf-vpc." + region + ".aliyuncs.com";
        }
        if (StringUtils.isBlank(resolvedEndpoint)) {
            throw new IllegalStateException("dlf.endpoint is required.");
        }
        String resolvedCatalogId = StringUtils.isBlank(catalogId) ? uid : catalogId;

        Map<String, String> conf = new LinkedHashMap<>();
        conf.put("dlf.catalog.accessKeyId", MetaStoreParseUtils.nullToEmpty(accessKey));
        conf.put("dlf.catalog.accessKeySecret", MetaStoreParseUtils.nullToEmpty(secretKey));
        conf.put("dlf.catalog.endpoint", resolvedEndpoint);
        conf.put("dlf.catalog.region", MetaStoreParseUtils.nullToEmpty(region));
        conf.put("dlf.catalog.securityToken", MetaStoreParseUtils.nullToEmpty(sessionToken));
        conf.put("dlf.catalog.uid", MetaStoreParseUtils.nullToEmpty(uid));
        conf.put("dlf.catalog.id", MetaStoreParseUtils.nullToEmpty(resolvedCatalogId));
        conf.put("dlf.catalog.proxyMode", proxyMode);
        MetaStoreParseUtils.applyStorageConfig(storageHadoopConfig, raw, conf::put);
        return conf;
    }
}
