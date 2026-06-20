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

package org.apache.doris.connector.metastore.spi.rest;

import org.apache.doris.connector.metastore.RestMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.AbstractMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * REST catalog metastore backend facts. Options-only (the REST server owns storage, so
 * {@link #needsStorage()} stays false): the {@code uri} plus every {@code paimon.rest.*} key
 * re-keyed with the prefix stripped (legacy {@code appendRestOptions}).
 */
public final class RestMetaStorePropertiesImpl extends AbstractMetaStoreProperties
        implements RestMetaStoreProperties {

    private static final String PAIMON_REST_PREFIX = "paimon.rest.";

    @ConnectorProperty(names = {"paimon.rest.uri", "uri"}, required = false,
            description = "The REST catalog service URI.")
    private String uri = "";

    @ConnectorProperty(names = {"paimon.rest.token.provider"}, required = false,
            description = "The REST catalog token provider (e.g. dlf).")
    private String tokenProvider = "";

    @ConnectorProperty(names = {"paimon.rest.dlf.access-key-id"}, required = false, sensitive = true,
            description = "DLF access key id for the REST DLF token provider.")
    private String dlfAccessKeyId = "";

    @ConnectorProperty(names = {"paimon.rest.dlf.access-key-secret"}, required = false, sensitive = true,
            description = "DLF access key secret for the REST DLF token provider.")
    private String dlfAccessKeySecret = "";

    private RestMetaStorePropertiesImpl(Map<String, String> raw) {
        super(raw);
    }

    public static RestMetaStorePropertiesImpl of(Map<String, String> raw) {
        RestMetaStorePropertiesImpl props = new RestMetaStorePropertiesImpl(raw);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public String providerName() {
        return "REST";
    }

    @Override
    public void validate() {
        // Shared warehouse check first (legacy parity: AbstractPaimonProperties requires warehouse for
        // REST too — PaimonRestMetaStoreProperties does not override it).
        requireWarehouse();
        if (StringUtils.isBlank(uri)) {
            throw new IllegalArgumentException("paimon.rest.uri or uri is required");
        }
        // CASE-SENSITIVE match: the authoritative legacy contract is ParamRules.requireIf, which uses
        // Objects.equals("dlf", tokenProvider) (PaimonRestMetaStoreProperties). The paimon hand-copy's
        // equalsIgnoreCase is a latent divergence we do NOT carry forward (T2 parity = legacy).
        if ("dlf".equals(tokenProvider)
                && (StringUtils.isBlank(dlfAccessKeyId) || StringUtils.isBlank(dlfAccessKeySecret))) {
            throw new IllegalArgumentException(
                    "DLF token provider requires 'paimon.rest.dlf.access-key-id' "
                            + "and 'paimon.rest.dlf.access-key-secret'");
        }
    }

    @Override
    public String getUri() {
        return uri;
    }

    @Override
    public Map<String, String> toRestOptions() {
        // Mirrors legacy appendRestOptions: set "uri" then re-key every paimon.rest.* (prefix stripped).
        // Legacy sets "uri" unconditionally; we guard null so the neutral map carries no null value (the
        // no-uri case is already rejected by validate()).
        Map<String, String> options = new LinkedHashMap<>();
        if (StringUtils.isNotBlank(uri)) {
            options.put("uri", uri);
        }
        raw.forEach((k, v) -> {
            if (k.startsWith(PAIMON_REST_PREFIX)) {
                options.put(k.substring(PAIMON_REST_PREFIX.length()), v);
            }
        });
        return options;
    }
}
