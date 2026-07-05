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

package org.apache.doris.connector.metastore.iceberg.rest;

import org.apache.doris.connector.metastore.spi.AbstractMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import org.apache.commons.lang3.StringUtils;

import java.util.Locale;
import java.util.Map;

/**
 * Iceberg REST catalog metastore backend — validation only (the REST catalog conf is connector-side in
 * {@code IcebergCatalogFactory}). Ports the legacy {@code IcebergRestProperties.initNormalizeAndCheckProps}
 * validation verbatim (§4 of the P6-T10 design), in observable fire order:
 * <ol>
 *   <li>security-type enum (none/oauth2)</li>
 *   <li>AWS credentials-provider mode enum</li>
 *   <li>OAuth2 scope-only-with-credential (eager)</li>
 *   <li>OAuth2 requires credential-or-token (eager)</li>
 *   <li>iceberg.rest.role_arn rejected (eager)</li>
 *   <li>iceberg.rest.external-id rejected (eager)</li>
 *   <li>OAuth2 credential/token mutually exclusive (ParamRules)</li>
 *   <li>signing-name=glue requires signing-region + sigv4-enabled (ParamRules)</li>
 *   <li>signing-name=s3tables requires signing-region + sigv4-enabled (ParamRules)</li>
 *   <li>access-key-id + secret-access-key set together (ParamRules)</li>
 * </ol>
 * No uri/warehouse requirement. The {@code Security}/{@code AwsCredentialsProviderMode} enum checks are
 * reproduced inline (the fe-core enums cannot be imported into a connector module).
 */
public final class IcebergRestMetaStoreProperties extends AbstractMetaStoreProperties {

    private static final String ICEBERG_REST_ROLE_ARN = "iceberg.rest.role_arn";
    private static final String ICEBERG_REST_EXTERNAL_ID = "iceberg.rest.external-id";

    @ConnectorProperty(names = {"iceberg.rest.security.type"}, required = false,
            description = "The security type of the iceberg rest catalog service, optional: (none, oauth2).")
    private String securityType = "none";

    @ConnectorProperty(names = {"iceberg.rest.credentials_provider_type"}, required = false,
            description = "The credentials provider type for AWS authentication.")
    private String credentialsProviderType = "DEFAULT";

    @ConnectorProperty(names = {"iceberg.rest.oauth2.token"}, required = false, sensitive = true,
            description = "The oauth2 token for the iceberg rest catalog service.")
    private String oauth2Token;

    @ConnectorProperty(names = {"iceberg.rest.oauth2.credential"}, required = false, sensitive = true,
            description = "The oauth2 credential for the iceberg rest catalog service.")
    private String oauth2Credential;

    @ConnectorProperty(names = {"iceberg.rest.oauth2.scope"}, required = false,
            description = "The oauth2 scope for the iceberg rest catalog service.")
    private String oauth2Scope;

    @ConnectorProperty(names = {"iceberg.rest.signing-name"}, required = false,
            description = "The signing name for the iceberg rest catalog service.")
    private String signingName = "";

    @ConnectorProperty(names = {"iceberg.rest.signing-region"}, required = false,
            description = "The signing region for the iceberg rest catalog service.")
    private String signingRegion = "";

    @ConnectorProperty(names = {"iceberg.rest.sigv4-enabled"}, required = false,
            description = "True for Glue/S3Tables Rest Catalog.")
    private String sigV4Enabled = "";

    @ConnectorProperty(names = {"iceberg.rest.access-key-id"}, required = false,
            description = "The access key ID for the iceberg rest catalog service.")
    private String accessKeyId = "";

    @ConnectorProperty(names = {"iceberg.rest.secret-access-key"}, required = false, sensitive = true,
            description = "The secret access key for the iceberg rest catalog service.")
    private String secretAccessKey = "";

    private IcebergRestMetaStoreProperties(Map<String, String> raw) {
        super(raw);
    }

    public static IcebergRestMetaStoreProperties of(Map<String, String> raw) {
        IcebergRestMetaStoreProperties props = new IcebergRestMetaStoreProperties(raw);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public String providerName() {
        return "REST";
    }

    @Override
    public void validate() {
        // 1. security type (legacy validateSecurityType: Security.valueOf(securityType.toUpperCase())).
        if (!"none".equalsIgnoreCase(securityType) && !"oauth2".equalsIgnoreCase(securityType)) {
            throw new IllegalArgumentException("Invalid security type: " + securityType
                    + ". Supported values are: none, oauth2");
        }
        // 2. AWS credentials-provider mode (legacy AwsCredentialsProviderMode.fromString).
        validateCredentialsProviderMode();
        // 3-10. Legacy buildRules() structure: eager throws interleaved with ParamRules registration, then
        // validate() runs the registered rules in registration order. Statement order is preserved verbatim
        // so the observable fire order matches §4.
        ParamRules rules = new ParamRules()
                // OAuth2 credential/token mutually exclusive (registered; fires at validate()).
                .mutuallyExclusive(oauth2Credential, oauth2Token,
                        "OAuth2 cannot have both credential and token configured");
        // OAuth2 scope must not be used with token (eager).
        if (StringUtils.isNotBlank(oauth2Token) && StringUtils.isNotBlank(oauth2Scope)) {
            throw new IllegalArgumentException("OAuth2 scope is only applicable when using credential, not token");
        }
        // If OAuth2 is enabled, require either credential or token (eager).
        if ("oauth2".equalsIgnoreCase(securityType)) {
            boolean hasCredential = StringUtils.isNotBlank(oauth2Credential);
            boolean hasToken = StringUtils.isNotBlank(oauth2Token);
            if (!hasCredential && !hasToken) {
                throw new IllegalArgumentException("OAuth2 requires either credential or token");
            }
        }
        // When signing-name is glue or s3tables: require signing-region and sigv4-enabled (registered).
        rules.requireIf(signingName, "glue", new String[] {signingRegion, sigV4Enabled},
                "Rest Catalog requires signing-region and sigv4-enabled set to true when signing-name is glue");
        rules.requireIf(signingName, "s3tables", new String[] {signingRegion, sigV4Enabled},
                "Rest Catalog requires signing-region and sigv4-enabled set to true when signing-name is s3tables");
        // AWS assume-role properties are not supported for the Iceberg REST catalog (eager).
        rejectUnsupportedAwsAssumeRoleProperty(ICEBERG_REST_ROLE_ARN);
        rejectUnsupportedAwsAssumeRoleProperty(ICEBERG_REST_EXTERNAL_ID);
        // access-key-id and secret-access-key must be set together (registered).
        rules.requireTogether(new String[] {accessKeyId, secretAccessKey},
                "iceberg.rest.access-key-id and iceberg.rest.secret-access-key must be set together");
        rules.validate();
    }

    /**
     * Reproduces fe-core {@code AwsCredentialsProviderMode.fromString}: blank ⇒ DEFAULT (no throw); the 7
     * known modes accepted; unknown ⇒ throw with the ORIGINAL value. Deliberate nit-deviation: legacy
     * upper-cases with the JVM default locale, here {@code Locale.ROOT} — byte-identical for the ASCII mode
     * names; under a non-ASCII default locale (Turkish 'i') ROOT is strictly more correct (legacy would
     * wrongly reject {@code web-identity}/{@code instance-profile}). Unreachable for real ASCII inputs.
     */
    private void validateCredentialsProviderMode() {
        if (credentialsProviderType == null || credentialsProviderType.isEmpty()) {
            return;
        }
        String normalized = credentialsProviderType.trim().toUpperCase(Locale.ROOT).replace('-', '_');
        switch (normalized) {
            case "ENV":
            case "SYSTEM_PROPERTIES":
            case "WEB_IDENTITY":
            case "CONTAINER":
            case "INSTANCE_PROFILE":
            case "ANONYMOUS":
            case "DEFAULT":
                return;
            default:
                throw new IllegalArgumentException(
                        "Unsupported AWS credentials provider mode: " + credentialsProviderType);
        }
    }

    private void rejectUnsupportedAwsAssumeRoleProperty(String propertyName) {
        if (StringUtils.isNotBlank(raw.get(propertyName))) {
            throw new IllegalArgumentException(propertyName + " is not supported for Iceberg REST catalog. "
                    + "Use iceberg.rest.access-key-id and iceberg.rest.secret-access-key, "
                    + "or iceberg.rest.credentials_provider_type instead");
        }
    }
}
