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

package org.apache.doris.connector.metastore.spi.hms;

import org.apache.doris.connector.metastore.HmsMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.AbstractMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.MetaStoreParseUtils;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.kerberos.AuthType;
import org.apache.doris.kerberos.KerberosAuthSpec;

import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Hive Metastore (HMS) backend facts. {@link #toHiveConfOverrides()} produces the neutral key map the
 * connector layers onto its own {@code HiveConf} (the connector seeds {@code new HiveConf()} +
 * {@code hive.conf.resources} first, then applies these overrides). Ported faithfully from the paimon
 * connector's {@code buildHmsHiveConf} (the up-move source), whose ordering is load-bearing: the
 * storage overlay runs BEFORE the kerberos block so a raw {@code hadoop.security.authentication=simple}
 * passthrough cannot clobber the forced {@code kerberos}.
 *
 * <p>The real {@code UGI.doAs} is performed FE-side via {@code ConnectorContext.executeAuthenticated};
 * this impl only carries facts ({@link #getAuthType()}, {@link #kerberos()}, neutral string keys), so
 * no hadoop authenticator code is needed here (DV-006).
 */
public final class HmsMetaStorePropertiesImpl extends AbstractMetaStoreProperties
        implements HmsMetaStoreProperties {

    @ConnectorProperty(names = {"hive.metastore.uris", "uri"}, required = false,
            description = "The hive metastore thrift URI.")
    private String uri = "";

    // Default "" (NOT "none"): emit-when-present parity (legacy copyIfPresent only sets it when the raw
    // key is present); the kerberos branch below treats blank as "none" via getOrDefault semantics.
    @ConnectorProperty(names = {"hive.metastore.authentication.type"}, required = false,
            description = "The hive metastore authentication type.")
    private String authType = "";

    @ConnectorProperty(names = {"hive.metastore.client.principal"}, required = false,
            description = "The client principal of the hive metastore.")
    private String clientPrincipal = "";

    @ConnectorProperty(names = {"hive.metastore.client.keytab"}, required = false, sensitive = true,
            description = "The client keytab of the hive metastore.")
    private String clientKeytab = "";

    @ConnectorProperty(names = {"hadoop.security.authentication"}, required = false,
            description = "The HDFS authentication type (kerberos fallback).")
    private String hdfsAuthType = "";

    @ConnectorProperty(names = {"hadoop.kerberos.principal"}, required = false,
            description = "The HDFS kerberos principal (kerberos fallback).")
    private String hdfsKerberosPrincipal = "";

    @ConnectorProperty(names = {"hadoop.kerberos.keytab"}, required = false, sensitive = true,
            description = "The HDFS kerberos keytab (kerberos fallback).")
    private String hdfsKerberosKeytab = "";

    @ConnectorProperty(names = {"hive.metastore.service.principal", "hive.metastore.kerberos.principal"},
            required = false, description = "The hive metastore service principal.")
    private String servicePrincipal = "";

    @ConnectorProperty(names = {"hive.metastore.username", "hadoop.username"}, required = false,
            description = "The user name for the hive metastore service.")
    private String userName = "";

    private final Map<String, String> storageHadoopConfig;

    private HmsMetaStorePropertiesImpl(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        super(raw);
        this.storageHadoopConfig = storageHadoopConfig;
    }

    public static HmsMetaStorePropertiesImpl of(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        HmsMetaStorePropertiesImpl props = new HmsMetaStorePropertiesImpl(raw, storageHadoopConfig);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public String providerName() {
        return "HMS";
    }

    @Override
    public boolean needsStorage() {
        return true;
    }

    @Override
    public void validate() {
        requireWarehouse();
        if (StringUtils.isBlank(uri)) {
            throw new IllegalArgumentException("hive.metastore.uris or uri is required");
        }
        // forbidIf(authType, "simple", {clientPrincipal, clientKeytab}) — legacy HMSBaseProperties.buildRules
        // uses CASE-SENSITIVE Objects.equals (the paimon hand-copy omits this rule; restored here — D-4).
        if ("simple".equals(authType)
                && (StringUtils.isNotBlank(clientPrincipal) || StringUtils.isNotBlank(clientKeytab))) {
            throw new IllegalArgumentException(
                    "hive.metastore.client.principal and hive.metastore.client.keytab cannot be set when "
                            + "hive.metastore.authentication.type is simple");
        }
        // requireIf(authType, "kerberos", {clientPrincipal, clientKeytab}) — also CASE-SENSITIVE.
        if ("kerberos".equals(authType)
                && (StringUtils.isBlank(clientPrincipal) || StringUtils.isBlank(clientKeytab))) {
            throw new IllegalArgumentException(
                    "hive.metastore.client.principal and hive.metastore.client.keytab are required when "
                            + "hive.metastore.authentication.type is kerberos");
        }
    }

    @Override
    public String getUri() {
        return uri;
    }

    @Override
    public AuthType getAuthType() {
        return AuthType.fromString(authType);
    }

    @Override
    public Optional<KerberosAuthSpec> kerberos() {
        // Mirrors HMSBaseProperties.initHadoopAuthenticator: kerberos HMS -> client creds; else the
        // legacy HDFS-kerberos fallback -> hdfs creds (case-insensitive, matching the conf-build branch).
        String effectiveAuthType = StringUtils.isNotBlank(authType) ? authType : "none";
        if ("kerberos".equalsIgnoreCase(effectiveAuthType)) {
            return Optional.of(new KerberosAuthSpec(clientPrincipal, clientKeytab));
        }
        if (!"simple".equalsIgnoreCase(effectiveAuthType) && "kerberos".equalsIgnoreCase(hdfsAuthType)) {
            return Optional.of(new KerberosAuthSpec(hdfsKerberosPrincipal, hdfsKerberosKeytab));
        }
        return Optional.empty();
    }

    @Override
    public Map<String, String> toHiveConfOverrides() {
        Map<String, String> conf = new LinkedHashMap<>();
        // 1. All user hive.* keys verbatim (legacy initUserHiveConfig).
        raw.forEach((k, v) -> {
            if (k.startsWith("hive.")) {
                conf.put(k, v);
            }
        });
        // 2. Metastore uri (legacy checkAndInit: hiveConf.set("hive.metastore.uris", uri)).
        if (StringUtils.isNotBlank(uri)) {
            conf.put("hive.metastore.uris", uri);
        }
        // 3. Present auth keys, in legacy copyIfPresent order. Single-alias fields == raw values; the
        //    hadoop.* ones are not covered by step 1's hive.* passthrough.
        putIfNotBlank(conf, "hive.metastore.authentication.type", authType);
        putIfNotBlank(conf, "hive.metastore.client.principal", clientPrincipal);
        putIfNotBlank(conf, "hive.metastore.client.keytab", clientKeytab);
        putIfNotBlank(conf, "hadoop.security.authentication", hdfsAuthType);
        putIfNotBlank(conf, "hadoop.kerberos.principal", hdfsKerberosPrincipal);
        putIfNotBlank(conf, "hadoop.kerberos.keytab", hdfsKerberosKeytab);
        // 4. Metastore client socket-timeout default (legacy checkAndInit: default 10s when unset).
        if (StringUtils.isBlank(raw.get("hive.metastore.client.socket.timeout"))) {
            conf.put("hive.metastore.client.socket.timeout", "10");
        }
        // 5. Storage overlay (legacy buildHiveConfiguration + appendUserHadoopConfig). BEFORE kerberos.
        MetaStoreParseUtils.applyStorageConfig(storageHadoopConfig, raw, conf::put);
        // 6. Kerberos-conditional metastore block (legacy initHadoopAuthenticator), LAST.
        if (StringUtils.isNotBlank(servicePrincipal)) {
            conf.put("hive.metastore.kerberos.principal", servicePrincipal);
        }
        MetaStoreParseUtils.copyIfPresent(raw, "hadoop.security.auth_to_local", conf::put);
        String hmsAuthType = StringUtils.isNotBlank(authType) ? authType : "none";
        boolean hmsKerberos = "kerberos".equalsIgnoreCase(hmsAuthType);
        boolean hdfsFallbackKerberos = !"simple".equalsIgnoreCase(hmsAuthType)
                && !hmsKerberos
                && "kerberos".equalsIgnoreCase(hdfsAuthType);
        if (hmsKerberos || hdfsFallbackKerberos) {
            conf.put("hadoop.security.authentication", "kerberos");
            conf.put("hive.metastore.sasl.enabled", "true");
        }
        // 7. Username alias resolved to hadoop.username, after the storage overlay.
        if (StringUtils.isNotBlank(userName)) {
            conf.put("hadoop.username", userName);
        }
        return conf;
    }

    private static void putIfNotBlank(Map<String, String> conf, String key, String value) {
        if (StringUtils.isNotBlank(value)) {
            conf.put(key, value);
        }
    }
}
