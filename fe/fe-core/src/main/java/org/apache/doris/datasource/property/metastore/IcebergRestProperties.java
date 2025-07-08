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

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.logging.log4j.util.Strings;

import java.util.HashMap;
import java.util.Map;

public class IcebergRestProperties extends MetastoreProperties {

    private Map<String, String> icebergRestCatalogProperties;

    @ConnectorProperty(names = {"iceberg.rest.uri", "uri"},
            description = "The uri of the iceberg rest catalog service.")
    private String icebergRestUri = "";

    @ConnectorProperty(names = {"iceberg.rest.prefix"},
            required = false,
            description = "The prefix of the iceberg rest catalog service.")
    private String icebergRestPrefix = "";

    @ConnectorProperty(names = {"iceberg.rest.warehouse", "warehouse"},
            required = false,
            description = "The warehouse of the iceberg rest catalog service.")
    private String icebergRestWarehouse = "";

    @ConnectorProperty(names = {"iceberg.rest.security.type"},
            required = false,
            description = "The security type of the iceberg rest catalog service,"
                    + "optional: (none, oauth2), default: none.")
    private String icebergRestSecurityType = "none";

    @ConnectorProperty(names = {"iceberg.rest.session"},
            required = false,
            supported = false,
            description = "The session type of the iceberg rest catalog service,"
                    + "optional: (none, user), default: none.")
    private String icebergRestSession = "none";

    @ConnectorProperty(names = {"iceberg.rest.session-timeout"},
            required = false,
            supported = false,
            description = "The session timeout of the iceberg rest catalog service.")
    private String icebergRestSessionTimeout = "0";

    @ConnectorProperty(names = {"iceberg.rest.oauth2.token"},
            required = false,
            description = "The oauth2 token for the iceberg rest catalog service.")
    private String icebergRestOauth2Token;

    @ConnectorProperty(names = {"iceberg.rest.oauth2.credential"},
            required = false,
            description = "The oauth2 credential for the iceberg rest catalog service.")
    private String icebergRestOauth2Credential;

    @ConnectorProperty(names = {"iceberg.rest.oauth2.scope"},
            required = false,
            description = "The oauth2 scope for the iceberg rest catalog service.")
    private String icebergRestOauth2Scope;

    @ConnectorProperty(names = {"iceberg.rest.oauth2.server-uri"},
            required = false,
            description = "The oauth2 server uri for fetching token.")
    private String icebergRestOauth2ServerUri;

    @ConnectorProperty(names = {"iceberg.rest.oauth2.token-refresh-enabled"},
            required = false,
            description = "Enable oauth2 token refresh for the iceberg rest catalog service.")
    private String icebergRestOauth2TokenRefreshEnabled = "false";

    @ConnectorProperty(names = {"iceberg.rest.vended-credentials-enabled"},
            required = false,
            description = "Enable vended credentials for the iceberg rest catalog service.")
    private String icebergRestVendedCredentialsEnabled = "false";

    @ConnectorProperty(names = {"iceberg.rest.nested-namespace-enabled"},
            required = false,
            supported = false,
            description = "Enable nested namespace for the iceberg rest catalog service.")
    private String icebergRestNestedNamespaceEnabled = "true";

    @ConnectorProperty(names = {"iceberg.rest.case-insensitive-name-matching"},
            required = false,
            supported = false,
            description = "Enable case insensitive name matching for the iceberg rest catalog service.")
    private String icebergRestCaseInsensitiveNameMatching = "false";

    @ConnectorProperty(names = {"iceberg.rest.case-insensitive-name-matching.cache-ttl"},
            required = false,
            supported = false,
            description = "The cache TTL for case insensitive name matching in ms.")
    private String icebergRestCaseInsensitiveNameMatchingCacheTtlMs = "0";

    public IcebergRestProperties(Map<String, String> origProps) {
        super(Type.ICEBERG_REST, origProps);
    }

    @Override
    protected void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        Security security;
        try {
            security = Security.valueOf(icebergRestSecurityType.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid security type: " + icebergRestSecurityType
                    + ". Supported values are: none, oauth2");
        }

        if (security == Security.OAUTH2) {
            // Validate OAuth2 configuration
            boolean hasCredential = Strings.isNotBlank(icebergRestOauth2Credential);
            boolean hasToken = Strings.isNotBlank(icebergRestOauth2Token);
            if (!hasCredential && !hasToken) {
                throw new IllegalArgumentException("OAuth2 requires either credential or token");
            }
            if (hasCredential && hasToken) {
                throw new IllegalArgumentException("OAuth2 cannot have both credential and token configured");
            }
            if (hasCredential) {
                // Client Credentials Flow
                // Server URI is required for credential flow
                if (Strings.isBlank(icebergRestOauth2ServerUri)) {
                    throw new IllegalArgumentException("OAuth2 credential flow requires server-uri");
                }
            } else {
                // Pre-configured Token Flow
                // Validate that scope is not used with token
                if (Strings.isNotBlank(icebergRestOauth2Scope)) {
                    throw new IllegalArgumentException(
                            "OAuth2 scope is only applicable when using credential, not token");
                }
            }
        }
        initIcebergRestCatalogProperties();
    }

    @Override
    protected void checkRequiredProperties() {
    }

    private void initIcebergRestCatalogProperties() {
        icebergRestCatalogProperties = new HashMap<>();
        // See CatalogUtil.java
        icebergRestCatalogProperties.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
        // See CatalogProperties.java
        icebergRestCatalogProperties.put(CatalogProperties.URI, icebergRestUri);

        if (Strings.isNotBlank(icebergRestPrefix)) {
            icebergRestCatalogProperties.put("prefix", icebergRestPrefix);
        }

        if (Strings.isNotBlank(icebergRestWarehouse)) {
            icebergRestCatalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, icebergRestWarehouse);
        }

        if (isIcebergRestVendedCredentialsEnabled()) {
            icebergRestCatalogProperties.put("header.X-Iceberg-Access-Delegation", "vended-credentials");
        }

        Security security = Security.valueOf(icebergRestSecurityType.toUpperCase());
        if (security == Security.OAUTH2) {
            handleOauth2Properties(icebergRestCatalogProperties);
        }
    }

    public Map<String, String> getIcebergRestCatalogProperties() {
        return icebergRestCatalogProperties;
    }

    private void handleOauth2Properties(Map<String, String> catalogProps) {
        if (Strings.isNotBlank(icebergRestOauth2Credential)) {
            // Client Credentials Flow
            catalogProps.put(OAuth2Properties.CREDENTIAL, icebergRestOauth2Credential);
            catalogProps.put(OAuth2Properties.OAUTH2_SERVER_URI, icebergRestOauth2ServerUri);
            if (Strings.isNotBlank(icebergRestOauth2Scope)) {
                catalogProps.put(OAuth2Properties.SCOPE, icebergRestOauth2Scope);
            }
            catalogProps.put(OAuth2Properties.TOKEN_REFRESH_ENABLED,
                    icebergRestOauth2TokenRefreshEnabled);
        } else {
            // Pre-configured Token Flow
            catalogProps.put(OAuth2Properties.TOKEN, icebergRestOauth2Token);
        }
    }

    public boolean isIcebergRestVendedCredentialsEnabled() {
        return Boolean.parseBoolean(icebergRestVendedCredentialsEnabled);
    }

    public enum Security {
        NONE,
        OAUTH2,
    }

    public enum SessionType {
        NONE,
        USER
    }
}
