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
 *   <li>signing-name=osstables requires signing-region + sigv4-enabled (ParamRules)</li>
 *   <li>managed signing names require sigv4-enabled=true (ParamRules)</li>
 *   <li>access-key-id + secret-access-key set together (ParamRules)</li>
 * </ol>
 * No uri/warehouse requirement. The {@code Security}/{@code AwsCredentialsProviderMode} enum checks are
 * reproduced inline (the fe-core enums cannot be imported into a connector module).
 */
public final class IcebergRestMetaStoreProperties extends AbstractMetaStoreProperties {

    private static final String ICEBERG_REST_ROLE_ARN = "iceberg.rest.role_arn";
    private static final String ICEBERG_REST_EXTERNAL_ID = "iceberg.rest.external-id";

    // Per-user session (#63068 re-migration). Local literal copies (this metastore module does not depend on
    // fe-connector-iceberg, so IcebergCatalogProperties' constants are not importable — same rationale as the
    // "none"/"oauth2" security-type literals already inlined below).
    private static final String SESSION_NONE = "none";
    private static final String SESSION_USER = "user";
    private static final String TOKEN_MODE_ACCESS_TOKEN = "access_token";
    private static final String TOKEN_MODE_TOKEN_EXCHANGE = "token_exchange";

    @ConnectorProperty(names = {"iceberg.rest.uri", "uri"}, required = false,
            description = "The endpoint of the iceberg rest catalog service.")
    private String uri = "";

    @ConnectorProperty(names = {"iceberg.rest.prefix"}, required = false,
            description = "The resource path prefix the rest catalog service is served under.")
    private String prefix = "";

    @ConnectorProperty(names = {"iceberg.rest.vended-credentials-enabled"}, required = false,
            description = "Ask the rest catalog service to vend per-table storage credentials.")
    private boolean vendedCredentialsEnabled;

    // Kept as Strings, not ints: the values are handed to the iceberg SDK verbatim, and a catalog that was
    // created with an unparseable one is live today -- binding them as numbers would make it unbuildable.
    @ConnectorProperty(names = {"iceberg.rest.connection-timeout-ms"}, required = false,
            description = "Connection timeout of the rest client, in milliseconds.")
    private String connectionTimeoutMs = "10000";

    @ConnectorProperty(names = {"iceberg.rest.socket-timeout-ms"}, required = false,
            description = "Socket timeout of the rest client, in milliseconds.")
    private String socketTimeoutMs = "60000";

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

    @ConnectorProperty(names = {"iceberg.rest.oauth2.server-uri"}, required = false,
            description = "The oauth2 token endpoint, when it is not the rest catalog's own.")
    private String oauth2ServerUri = "";

    // Blank rather than "true": the emitted default is the iceberg SDK's own
    // OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT, which only the connector (where the SDK is on the
    // classpath) may name. This module stays SDK-free, so it reports "unset" and the connector defaults.
    @ConnectorProperty(names = {"iceberg.rest.oauth2.token-refresh-enabled"}, required = false,
            description = "Whether the rest client refreshes the oauth2 token before it expires.")
    private String oauth2TokenRefreshEnabled = "";

    @ConnectorProperty(names = {"iceberg.rest.signing-name"}, required = false,
            description = "The signing name for the iceberg rest catalog service.")
    private String signingName = "";

    @ConnectorProperty(names = {"iceberg.rest.signing-region"}, required = false,
            description = "The signing region for the iceberg rest catalog service.")
    private String signingRegion = "";

    @ConnectorProperty(names = {"iceberg.rest.sigv4-enabled"}, required = false,
            description = "True for Glue/S3Tables/OSS Tables Rest Catalog.")
    private String sigV4Enabled = "";

    @ConnectorProperty(names = {"iceberg.rest.access-key-id"}, required = false,
            description = "The access key ID for the iceberg rest catalog service.")
    private String accessKeyId = "";

    @ConnectorProperty(names = {"iceberg.rest.secret-access-key"}, required = false, sensitive = true,
            description = "The secret access key for the iceberg rest catalog service.")
    private String secretAccessKey = "";

    @ConnectorProperty(names = {"iceberg.rest.session-token"}, required = false, sensitive = true,
            description = "The session token accompanying the iceberg rest access key pair.")
    private String sessionToken = "";

    // Listing behavior of a REST catalog. Both are inert for every other flavor (IcebergCatalogOps gates them
    // on restFlavor), which is why they can live here rather than on the connector-level properties.
    @ConnectorProperty(names = {"iceberg.rest.nested-namespace-enabled"}, required = false,
            description = "Recurse into nested namespaces when listing databases.")
    private boolean nestedNamespaceEnabled;

    @ConnectorProperty(names = {"iceberg.rest.view-enabled"}, required = false,
            description = "Expose the rest catalog's views alongside its tables.")
    private boolean viewEnabled = true;

    @ConnectorProperty(names = {"iceberg.rest.session-timeout"}, required = false,
            description = "Lifetime of an oauth2 AuthSession, in milliseconds.")
    private String sessionTimeout = "";

    @ConnectorProperty(names = {"iceberg.rest.session"}, required = false,
            description = "Per-user session mode of the iceberg rest catalog, optional: (none, user). "
                    + "user requires iceberg.rest.security.type=oauth2.")
    private String session = "none";

    @ConnectorProperty(names = {"iceberg.rest.oauth2.delegated-token-mode"}, required = false,
            description = "How the user's delegated credential is attached in session=user mode, optional: "
                    + "(access_token, token_exchange).")
    private String delegatedTokenMode = "access_token";

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

    // ---------------------------------------------------------------------
    // Assembly surface: the connector builds the catalog options from these, so the alias set declared above
    // is the single place a REST key name lives. Before this existed the connector re-scanned the raw map
    // with its own copy of the alias arrays, and the two could disagree about which alias wins.
    // ---------------------------------------------------------------------

    public String getUri() {
        return uri;
    }

    public String getPrefix() {
        return prefix;
    }

    public boolean isVendedCredentialsEnabled() {
        return vendedCredentialsEnabled;
    }

    public String getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public String getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public String getSecurityType() {
        return securityType;
    }

    public String getCredentialsProviderType() {
        return credentialsProviderType;
    }

    public String getOauth2Token() {
        return oauth2Token;
    }

    public String getOauth2Credential() {
        return oauth2Credential;
    }

    public String getOauth2Scope() {
        return oauth2Scope;
    }

    public String getOauth2ServerUri() {
        return oauth2ServerUri;
    }

    /** Blank when the catalog does not set it — the connector then applies the iceberg SDK default. */
    public String getOauth2TokenRefreshEnabled() {
        return oauth2TokenRefreshEnabled;
    }

    public String getSigningName() {
        return signingName;
    }

    public String getSigningRegion() {
        return signingRegion;
    }

    public String getSigV4Enabled() {
        return sigV4Enabled;
    }

    /** Whether REST signing reuses the selected S3-compatible storage credentials. */
    public boolean usesS3CredentialsForRestSigning() {
        return "glue".equals(signingName)
                || "s3tables".equals(signingName)
                || "osstables".equals(signingName);
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public String getSessionToken() {
        return sessionToken;
    }

    public boolean isNestedNamespaceEnabled() {
        return nestedNamespaceEnabled;
    }

    public boolean isViewEnabled() {
        return viewEnabled;
    }

    public String getSessionTimeout() {
        return sessionTimeout;
    }

    /** Whether this catalog projects the querying user's delegated credential ({@code session=user}). */
    public boolean isUserSession() {
        return SESSION_USER.equalsIgnoreCase(session);
    }

    public String getDelegatedTokenMode() {
        return delegatedTokenMode;
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
        // 2b. Per-user session (#63068): session enum, delegated-token-mode enum, and session=user⇒oauth2.
        validateUserSession();
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
        // If OAuth2 is enabled, require either credential or token (eager) — EXCEPT for a user-session catalog,
        // which has no static bootstrap credential (the per-request user token supplies identity), so the
        // requirement is relaxed for session=user (#63068 parity).
        if ("oauth2".equalsIgnoreCase(securityType)) {
            boolean hasCredential = StringUtils.isNotBlank(oauth2Credential);
            boolean hasToken = StringUtils.isNotBlank(oauth2Token);
            if (!hasCredential && !hasToken && !isUserSession()) {
                throw new IllegalArgumentException("OAuth2 requires either credential or token");
            }
        }
        // SigV4-backed REST catalogs require a signing region and SigV4 to be enabled (registered).
        rules.requireIf(signingName, "glue", new String[] {signingRegion, sigV4Enabled},
                "Rest Catalog requires signing-region and sigv4-enabled set to true when signing-name is glue");
        rules.requireIf(signingName, "s3tables", new String[] {signingRegion, sigV4Enabled},
                "Rest Catalog requires signing-region and sigv4-enabled set to true when signing-name is s3tables");
        rules.requireIf(signingName, "osstables", new String[] {signingRegion, sigV4Enabled},
                "Rest Catalog requires signing-region and sigv4-enabled set to true when signing-name is osstables");
        rules.check(() -> usesS3CredentialsForRestSigning() && !"true".equalsIgnoreCase(sigV4Enabled),
                "Rest Catalog requires sigv4-enabled set to true when signing-name is " + signingName);
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

    /**
     * Validates the per-user session config (#63068): the {@code iceberg.rest.session} enum (none/user), the
     * {@code iceberg.rest.oauth2.delegated-token-mode} enum (access_token/token_exchange), and that a
     * {@code session=user} catalog uses {@code security.type=oauth2} (user-session requires OAuth2 — it has no
     * bootstrap identity of its own). Case-insensitive to match the security-type check above.
     */
    private void validateUserSession() {
        if (!SESSION_NONE.equalsIgnoreCase(session) && !SESSION_USER.equalsIgnoreCase(session)) {
            throw new IllegalArgumentException("Invalid iceberg.rest.session: " + session
                    + ". Supported values are: none, user");
        }
        if (!TOKEN_MODE_ACCESS_TOKEN.equalsIgnoreCase(delegatedTokenMode)
                && !TOKEN_MODE_TOKEN_EXCHANGE.equalsIgnoreCase(delegatedTokenMode)) {
            throw new IllegalArgumentException("Invalid iceberg.rest.oauth2.delegated-token-mode: "
                    + delegatedTokenMode + ". Supported values are: access_token, token_exchange");
        }
        if (isUserSession() && !"oauth2".equalsIgnoreCase(securityType)) {
            throw new IllegalArgumentException(
                    "iceberg.rest.session=user requires iceberg.rest.security.type=oauth2");
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
