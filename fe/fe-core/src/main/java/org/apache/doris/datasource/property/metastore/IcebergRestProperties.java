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

import org.apache.doris.datasource.iceberg.IcebergCatalogConstants;
import org.apache.doris.datasource.property.common.AwsCredentialsProviderMode;
import org.apache.doris.datasource.property.common.IcebergAwsClientCredentialsProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.logging.log4j.util.Strings;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergRestProperties extends AbstractIcebergProperties {

    // REST catalog property constants
    private static final String PREFIX_PROPERTY = "prefix";
    private static final String VENDED_CREDENTIALS_HEADER = "header.X-Iceberg-Access-Delegation";
    private static final String VENDED_CREDENTIALS_VALUE = "vended-credentials";
    private static final String ICEBERG_REST_ROLE_ARN = "iceberg.rest.role_arn";
    private static final String ICEBERG_REST_EXTERNAL_ID = "iceberg.rest.external-id";

    private Map<String, String> icebergRestCatalogProperties;
    private S3Properties s3Properties;

    @Getter
    @ConnectorProperty(names = {"iceberg.rest.uri", "uri"},
            description = "The uri of the iceberg rest catalog service.")
    private String icebergRestUri = "";

    @ConnectorProperty(names = {"iceberg.rest.prefix"},
            required = false,
            description = "The prefix of the iceberg rest catalog service.")
    private String icebergRestPrefix = "";

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
            sensitive = true,
            description = "The oauth2 token for the iceberg rest catalog service.")
    private String icebergRestOauth2Token;

    @ConnectorProperty(names = {"iceberg.rest.oauth2.credential"},
            required = false,
            sensitive = true,
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
    private String icebergRestOauth2TokenRefreshEnabled = String.valueOf(
            OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT);

    @ConnectorProperty(names = {"iceberg.rest.vended-credentials-enabled"},
            required = false,
            description = "Enable vended credentials for the iceberg rest catalog service.")
    private String icebergRestVendedCredentialsEnabled = "false";

    @ConnectorProperty(names = {"iceberg.rest.nested-namespace-enabled"},
            required = false,
            description = "Enable nested namespace for the iceberg rest catalog service.")
    private String icebergRestNestedNamespaceEnabled = "false";

    @ConnectorProperty(names = {"iceberg.rest.view-enabled"},
            required = false,
            description = "Enable view operations for the iceberg rest catalog service.")
    private String icebergRestViewEnabled = "true";

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

    // The following properties are specific to AWS Glue Rest Catalog
    @ConnectorProperty(names = {"iceberg.rest.sigv4-enabled"},
            required = false,
            description = "True for Glue Rest Catalog")
    private String icebergRestSigV4Enabled = "";

    @ConnectorProperty(names = {"iceberg.rest.signing-name"},
            required = false,
            description = "The signing name for the iceberg rest catalog service.")
    private String icebergRestSigningName = "";

    @ConnectorProperty(names = {"iceberg.rest.signing-region"},
            required = false,
            description = "The signing region for the iceberg rest catalog service.")
    private String icebergRestSigningRegion = "";

    @ConnectorProperty(names = {"iceberg.rest.access-key-id"},
            required = false,
            description = "The access key ID for the iceberg rest catalog service.")
    private String icebergRestAccessKeyId = "";

    @ConnectorProperty(names = {"iceberg.rest.secret-access-key"},
            required = false,
            sensitive = true,
            description = "The secret access key for the iceberg rest catalog service.")
    private String icebergRestSecretAccessKey = "";

    @ConnectorProperty(names = {"iceberg.rest.session-token"},
            required = false,
            sensitive = true,
            description = "The session-token for the iceberg rest catalog service.")
    private String icebergRestSessionToken = "";

    @ConnectorProperty(names = {"iceberg.rest.credentials_provider_type"},
            required = false,
            description = "The credentials provider type for AWS authentication. "
                    + "Options are: DEFAULT, INSTANCE_PROFILE, ENV, SYSTEM_PROPERTIES, "
                    + "WEB_IDENTITY, CONTAINER. "
                    + "If not set, defaults to DEFAULT (provider chain).")
    private String icebergRestCredentialsProviderType = AwsCredentialsProviderMode.DEFAULT.name();

    private AwsCredentialsProviderMode icebergRestCredentialsProviderMode;

    @ConnectorProperty(names = {"iceberg.rest.connection-timeout-ms"},
            required = false,
            description = "Connection timeout in milliseconds for the REST catalog HTTP client. Default: 10000 (10s).")
    private String icebergRestConnectionTimeoutMs = "10000";

    @ConnectorProperty(names = {"iceberg.rest.socket-timeout-ms"},
            required = false,
            description = "Socket timeout in milliseconds for the REST catalog HTTP client. Default: 60000 (60s).")
    private String icebergRestSocketTimeoutMs = "60000";

    protected IcebergRestProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public String getIcebergCatalogType() {
        return IcebergCatalogConstants.ICEBERG_REST;
    }

    @Override
    public Catalog initCatalog(String catalogName, Map<String, String> catalogProps,
            List<StorageProperties> storagePropertiesList) {
        catalogProps.putAll(getIcebergRestCatalogProperties());
        Configuration configuration = new Configuration();
        toFileIOProperties(storagePropertiesList, catalogProps, configuration);
        // 4. Build iceberg catalog
        return buildIcebergCatalog(catalogName, catalogProps, configuration);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        validateSecurityType();
        icebergRestCredentialsProviderMode =
                AwsCredentialsProviderMode.fromString(icebergRestCredentialsProviderType);
        buildRules().validate();
        if (shouldUseS3PropertiesForRestCredentials()) {
            s3Properties = S3Properties.of(origProps);
        }
        initIcebergRestCatalogProperties();
    }

    @Override
    protected void checkRequiredProperties() {
    }

    private void validateSecurityType() {
        try {
            Security.valueOf(icebergRestSecurityType.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid security type: " + icebergRestSecurityType
                    + ". Supported values are: none, oauth2");
        }
    }

    private ParamRules buildRules() {
        ParamRules rules = new ParamRules()
                // OAuth2 requires either credential or token, but not both
                .mutuallyExclusive(icebergRestOauth2Credential, icebergRestOauth2Token,
                        "OAuth2 cannot have both credential and token configured");

        // Custom validation: OAuth2 scope should not be used with token
        if (Strings.isNotBlank(icebergRestOauth2Token) && Strings.isNotBlank(icebergRestOauth2Scope)) {
            throw new IllegalArgumentException("OAuth2 scope is only applicable when using credential, not token");
        }
        // Custom validation: If OAuth2 is enabled, require either credential or token
        if ("oauth2".equalsIgnoreCase(icebergRestSecurityType)) {
            boolean hasCredential = Strings.isNotBlank(icebergRestOauth2Credential);
            boolean hasToken = Strings.isNotBlank(icebergRestOauth2Token);
            if (!hasCredential && !hasToken) {
                throw new IllegalArgumentException("OAuth2 requires either credential or token");
            }
        }

        // When signing-name is glue or s3tables: require signing-region and sigv4-enabled
        rules.requireIf(icebergRestSigningName, "glue",
                new String[] {icebergRestSigningRegion, icebergRestSigV4Enabled},
                "Rest Catalog requires signing-region and sigv4-enabled set to true when signing-name is glue");
        rules.requireIf(icebergRestSigningName, "s3tables",
                new String[] {icebergRestSigningRegion, icebergRestSigV4Enabled},
                "Rest Catalog requires signing-region and sigv4-enabled set to true when signing-name is s3tables");

        rejectUnsupportedAwsAssumeRoleProperty(ICEBERG_REST_ROLE_ARN);
        rejectUnsupportedAwsAssumeRoleProperty(ICEBERG_REST_EXTERNAL_ID);

        // access-key-id and secret-access-key must be set together when either is set
        rules.requireTogether(new String[] {icebergRestAccessKeyId, icebergRestSecretAccessKey},
                "iceberg.rest.access-key-id and iceberg.rest.secret-access-key must be set together");

        return rules;
    }

    private void rejectUnsupportedAwsAssumeRoleProperty(String propertyName) {
        if (Strings.isNotBlank(origProps.get(propertyName))) {
            throw new IllegalArgumentException(propertyName + " is not supported for Iceberg REST catalog. "
                    + "Use iceberg.rest.access-key-id and iceberg.rest.secret-access-key, "
                    + "or iceberg.rest.credentials_provider_type instead");
        }
    }

    private void initIcebergRestCatalogProperties() {
        icebergRestCatalogProperties = new HashMap<>();
        // Core catalog properties
        addCoreCatalogProperties();
        // Optional properties
        addOptionalProperties();
        // Authentication properties
        addAuthenticationProperties();
        // Glue Rest Catalog specific properties
        addGlueRestCatalogProperties();
    }

    private void addCoreCatalogProperties() {
        // See CatalogUtil.java
        icebergRestCatalogProperties.put(CatalogProperties.CATALOG_IMPL, CatalogUtil.ICEBERG_CATALOG_REST);
        // See CatalogProperties.java
        icebergRestCatalogProperties.put(CatalogProperties.URI, icebergRestUri);
    }

    private void addOptionalProperties() {
        if (Strings.isNotBlank(icebergRestPrefix)) {
            icebergRestCatalogProperties.put(PREFIX_PROPERTY, icebergRestPrefix);
        }

        if (Strings.isNotBlank(warehouse)) {
            icebergRestCatalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        }

        if (isIcebergRestVendedCredentialsEnabled()) {
            icebergRestCatalogProperties.put(VENDED_CREDENTIALS_HEADER, VENDED_CREDENTIALS_VALUE);
        }

        if (Strings.isNotBlank(icebergRestConnectionTimeoutMs)) {
            icebergRestCatalogProperties.put("rest.client.connection-timeout-ms", icebergRestConnectionTimeoutMs);
        }
        if (Strings.isNotBlank(icebergRestSocketTimeoutMs)) {
            icebergRestCatalogProperties.put("rest.client.socket-timeout-ms", icebergRestSocketTimeoutMs);
        }
    }

    private void addAuthenticationProperties() {
        Security security = Security.valueOf(icebergRestSecurityType.toUpperCase());
        if (security == Security.OAUTH2) {
            addOAuth2Properties();
        }
    }

    private void addOAuth2Properties() {
        if (Strings.isNotBlank(icebergRestOauth2Credential)) {
            // Client Credentials Flow
            icebergRestCatalogProperties.put(OAuth2Properties.CREDENTIAL, icebergRestOauth2Credential);
            if (Strings.isNotBlank(icebergRestOauth2ServerUri)) {
                icebergRestCatalogProperties.put(OAuth2Properties.OAUTH2_SERVER_URI, icebergRestOauth2ServerUri);
            }
            if (Strings.isNotBlank(icebergRestOauth2Scope)) {
                icebergRestCatalogProperties.put(OAuth2Properties.SCOPE, icebergRestOauth2Scope);
            }
            icebergRestCatalogProperties.put(OAuth2Properties.TOKEN_REFRESH_ENABLED,
                    icebergRestOauth2TokenRefreshEnabled);
        } else {
            // Pre-configured Token Flow
            icebergRestCatalogProperties.put(OAuth2Properties.TOKEN, icebergRestOauth2Token);
        }
    }

    private void addGlueRestCatalogProperties() {
        if (Strings.isNotBlank(icebergRestSigningName)) {
            // signing-name is case sensible, do not use lowercase()
            icebergRestCatalogProperties.put("rest.signing-name", icebergRestSigningName);
            icebergRestCatalogProperties.put("rest.sigv4-enabled", icebergRestSigV4Enabled);
            icebergRestCatalogProperties.put("rest.signing-region", icebergRestSigningRegion);

            if (shouldUseS3PropertiesForRestCredentials()) {
                IcebergAwsClientCredentialsProperties.putCredentialProviderProperties(
                        icebergRestCatalogProperties, s3Properties);
            } else {
                IcebergAwsClientCredentialsProperties.putCredentialProviderProperties(
                        icebergRestCatalogProperties, icebergRestAccessKeyId,
                        icebergRestSecretAccessKey, icebergRestSessionToken, icebergRestCredentialsProviderMode);
            }
        }
    }

    private boolean shouldUseS3PropertiesForRestCredentials() {
        return "glue".equals(icebergRestSigningName)
                || "s3tables".equals(icebergRestSigningName);
    }

    public Map<String, String> getIcebergRestCatalogProperties() {
        return Collections.unmodifiableMap(icebergRestCatalogProperties);
    }

    public boolean isIcebergRestVendedCredentialsEnabled() {
        return Boolean.parseBoolean(icebergRestVendedCredentialsEnabled);
    }

    public boolean isIcebergRestNestedNamespaceEnabled() {
        return Boolean.parseBoolean(icebergRestNestedNamespaceEnabled);
    }

    public boolean isIcebergRestViewEnabled() {
        return Boolean.parseBoolean(icebergRestViewEnabled);
    }

    public enum Security {
        NONE,
        OAUTH2,
    }
}
