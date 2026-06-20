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

package org.apache.doris.connector.metastore.spi.dlf;

import org.apache.doris.connector.metastore.DlfMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.AbstractMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.MetaStoreParseUtils;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Aliyun DLF metastore backend facts: the 8 {@code dlf.catalog.*} keys with endpoint-from-region
 * derivation and {@code catalogId}←{@code uid} fallback (legacy
 * {@code AliyunDLFBaseProperties}/{@code PaimonAliyunDLFMetaStoreProperties}), overlaid with the OSS
 * storage config. {@link #needsStorage()} is true and an OSS storage key is required.
 */
public final class DlfMetaStorePropertiesImpl extends AbstractMetaStoreProperties
        implements DlfMetaStoreProperties {

    @ConnectorProperty(names = {"dlf.access_key", "dlf.catalog.accessKeyId"}, required = false, sensitive = true,
            description = "DLF access key id.")
    private String accessKey = "";

    @ConnectorProperty(names = {"dlf.secret_key", "dlf.catalog.accessKeySecret"}, required = false, sensitive = true,
            description = "DLF access key secret.")
    private String secretKey = "";

    @ConnectorProperty(names = {"dlf.session_token", "dlf.catalog.sessionToken"}, required = false, sensitive = true,
            description = "DLF session/security token.")
    private String sessionToken = "";

    @ConnectorProperty(names = {"dlf.region"}, required = false,
            description = "DLF region (used to derive the endpoint when the endpoint is not set).")
    private String region = "";

    @ConnectorProperty(names = {"dlf.endpoint", "dlf.catalog.endpoint"}, required = false,
            description = "DLF endpoint.")
    private String endpoint = "";

    @ConnectorProperty(names = {"dlf.catalog.uid", "dlf.uid"}, required = false,
            description = "DLF account uid.")
    private String uid = "";

    @ConnectorProperty(names = {"dlf.catalog.id", "dlf.catalog_id"}, required = false,
            description = "DLF catalog id (defaults to the uid).")
    private String catalogId = "";

    @ConnectorProperty(names = {"dlf.access.public", "dlf.catalog.accessPublic"}, required = false,
            description = "Whether to use the public DLF endpoint (vs the VPC endpoint).")
    private String accessPublic = "false";

    @ConnectorProperty(names = {"dlf.catalog.proxyMode", "dlf.proxy.mode"}, required = false,
            description = "DLF proxy mode.")
    private String proxyMode = "DLF_ONLY";

    private final Map<String, String> storageHadoopConfig;

    private DlfMetaStorePropertiesImpl(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        super(raw);
        this.storageHadoopConfig = storageHadoopConfig;
    }

    public static DlfMetaStorePropertiesImpl of(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        DlfMetaStorePropertiesImpl props = new DlfMetaStorePropertiesImpl(raw, storageHadoopConfig);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public String providerName() {
        return "DLF";
    }

    @Override
    public boolean needsStorage() {
        return true;
    }

    @Override
    public void validate() {
        requireWarehouse();
        if (StringUtils.isBlank(accessKey)) {
            throw new IllegalArgumentException("dlf.access_key is required");
        }
        if (StringUtils.isBlank(secretKey)) {
            throw new IllegalArgumentException("dlf.secret_key is required");
        }
        // Fail-fast mirror of the endpoint-from-region derivation: if both are blank we cannot derive.
        if (StringUtils.isBlank(endpoint) && StringUtils.isBlank(region)) {
            throw new IllegalArgumentException("dlf.endpoint is required.");
        }
        // OSS storage is required for a DLF catalog (legacy selected StorageProperties of OSS/OSS_HDFS at
        // init; here we key off the user's OSS prefixes). Outcome-equivalent rejection + same message.
        requireOssStorage();
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
        // Overlay the OSS storage config (legacy ossProps.getHadoopStorageConfig + appendUserHadoopConfig).
        MetaStoreParseUtils.applyStorageConfig(storageHadoopConfig, raw, conf::put);
        return conf;
    }

    private void requireOssStorage() {
        for (String key : raw.keySet()) {
            if (key.startsWith("oss.") || key.startsWith("fs.oss.") || key.startsWith("paimon.fs.oss.")) {
                return;
            }
        }
        // IllegalArgumentException (not legacy's IllegalStateException) to keep validate() uniform with the
        // other fail-fast rules; the message is byte-identical to legacy and the framework wraps both the
        // same way. (toDlfCatalogConf's blank-endpoint guard keeps ISE to match buildDlfHiveConf.)
        throw new IllegalArgumentException("Paimon DLF metastore requires OSS storage properties.");
    }
}
