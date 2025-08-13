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

import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.ParamRules;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.HdfsCompatibleProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
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

    private Map<String, String> icebergRestCatalogProperties;

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
            description = "The secret access key for the iceberg rest catalog service.")
    private String icebergRestSecretAccessKey = "";

    protected IcebergRestProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public String getIcebergCatalogType() {
        return IcebergExternalCatalog.ICEBERG_REST;
    }

    @Override
    public Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList) {
        Map<String, String> fileIOProperties = Maps.newHashMap();
        Configuration conf = new Configuration();
        toFileIOProperties(storagePropertiesList, fileIOProperties, conf);

        // 3. Merge properties for REST catalog service.
        Map<String, String> options = Maps.newHashMap(getIcebergRestCatalogProperties());
        options.putAll(fileIOProperties);

        // 4. Build iceberg catalog
        return CatalogUtil.buildIcebergCatalog(catalogName, options, conf);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        validateSecurityType();
        buildRules().validate();
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
                        "OAuth2 cannot have both credential and token configured")
                // If using credential flow, server URI is required
                .requireAllIfPresent(icebergRestOauth2Credential,
                        new String[] {icebergRestOauth2ServerUri},
                        "OAuth2 credential flow requires server-uri");

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

        // Check for glue rest catalog specific properties
        rules.requireIf(icebergRestSigningName, "glue",
                new String[] {icebergRestSigningRegion,
                        icebergRestAccessKeyId,
                        icebergRestSecretAccessKey,
                        icebergRestSigV4Enabled},
                "Rest Catalog requires signing-region, access-key-id, secret-access-key "
                        + "and sigv4-enabled set to true when signing-name is glue");
        return rules;
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
        icebergRestCatalogProperties.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
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
            icebergRestCatalogProperties.put(OAuth2Properties.OAUTH2_SERVER_URI, icebergRestOauth2ServerUri);
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
        if (Strings.isNotBlank(icebergRestSigningName) && icebergRestSigningName.equalsIgnoreCase("glue")) {
            icebergRestCatalogProperties.put("rest.signing-name", "glue");
            icebergRestCatalogProperties.put("rest.sigv4-enabled", icebergRestSigV4Enabled);
            icebergRestCatalogProperties.put("rest.access-key-id", icebergRestAccessKeyId);
            icebergRestCatalogProperties.put("rest.secret-access-key", icebergRestSecretAccessKey);
            icebergRestCatalogProperties.put("rest.signing-region", icebergRestSigningRegion);
        }
    }


    public Map<String, String> getIcebergRestCatalogProperties() {
        return Collections.unmodifiableMap(icebergRestCatalogProperties);
    }

    public boolean isIcebergRestVendedCredentialsEnabled() {
        return Boolean.parseBoolean(icebergRestVendedCredentialsEnabled);
    }

    /**
     * Unified method to configure FileIO properties for Iceberg catalog.
     * This method handles all storage types (HDFS, S3, MinIO, etc.) and populates
     * the fileIOProperties map and Configuration object accordingly.
     *
     * @param storagePropertiesList Map of storage properties
     * @param fileIOProperties Options map to be populated
     * @param conf Configuration object to be populated (for HDFS), will be created if null and HDFS is used
     */
    public void toFileIOProperties(List<StorageProperties> storagePropertiesList,
            Map<String, String> fileIOProperties, Configuration conf) {

        for (StorageProperties storageProperties : storagePropertiesList) {
            if (storageProperties instanceof HdfsCompatibleProperties) {
                storageProperties.getBackendConfigProperties().forEach(conf::set);
            } else if (storageProperties instanceof AbstractS3CompatibleProperties) {
                // For all S3-compatible storage types, put properties in fileIOProperties map
                toS3FileIOProperties((AbstractS3CompatibleProperties) storageProperties, fileIOProperties);
            } else {
                // For other storage types, just use fileIOProperties map
                fileIOProperties.putAll(storageProperties.getBackendConfigProperties());
            }
        }

    }

    /**
     * Configure S3 FileIO properties for all S3-compatible storage types (S3, MinIO, etc.)
     * This method provides a unified way to convert S3-compatible properties to Iceberg S3FileIO format.
     *
     * @param s3Properties S3-compatible properties
     * @param options Options map to be populated with S3 FileIO properties
     */
    public void toS3FileIOProperties(AbstractS3CompatibleProperties s3Properties, Map<String, String> options) {
        // Common properties - only set if not blank
        if (StringUtils.isNotBlank(s3Properties.getEndpoint())) {
            options.put(S3FileIOProperties.ENDPOINT, s3Properties.getEndpoint());
        }
        if (StringUtils.isNotBlank(s3Properties.getUsePathStyle())) {
            options.put(S3FileIOProperties.PATH_STYLE_ACCESS, s3Properties.getUsePathStyle());
        }
        if (StringUtils.isNotBlank(s3Properties.getRegion())) {
            options.put(AwsClientProperties.CLIENT_REGION, s3Properties.getRegion());
        }
        if (StringUtils.isNotBlank(s3Properties.getAccessKey())) {
            options.put(S3FileIOProperties.ACCESS_KEY_ID, s3Properties.getAccessKey());
        }
        if (StringUtils.isNotBlank(s3Properties.getSecretKey())) {
            options.put(S3FileIOProperties.SECRET_ACCESS_KEY, s3Properties.getSecretKey());
        }
        if (StringUtils.isNotBlank(s3Properties.getSessionToken())) {
            options.put(S3FileIOProperties.SESSION_TOKEN, s3Properties.getSessionToken());
        }
    }


    public enum Security {
        NONE,
        OAUTH2,
    }
}
