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

package org.apache.doris.filesystem.azure;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.FsCacheKeys;
import org.apache.doris.filesystem.properties.HadoopStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;
import org.apache.doris.foundation.property.StoragePropertiesException;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Provider-owned Azure Blob Storage properties.
 *
 * <p>The public aliases, endpoint formatting, and shared-key validation retain fe-core Azure
 * compatibility. Backend maps use provider-owned {@code AZURE_*} keys; legacy uppercase input
 * aliases remain accepted so existing FE filesystem callers can migrate through
 * {@link AzureFileSystemProvider#bind(Map)}.
 */
public final class AzureFileSystemProperties
        implements FileSystemProperties, BackendStorageProperties, HadoopStorageProperties {

    public static final String ENDPOINT = "azure.endpoint";
    public static final String ACCOUNT_NAME = "azure.account_name";
    public static final String ACCOUNT_KEY = "azure.account_key";
    public static final String CLIENT_ID = "azure.oauth2_client_id";
    public static final String CLIENT_SECRET = "azure.oauth2_client_secret";
    public static final String OAUTH_SERVER_URI = "azure.oauth2_server_uri";
    public static final String OAUTH_ACCOUNT_HOST = "azure.oauth2_account_host";
    public static final String TENANT_ID = "azure.oauth2_client_tenant_id";
    public static final String AUTH_TYPE = "azure.auth_type";
    public static final String SAS_TOKEN = "azure.sas_token";
    public static final String SAS_EXPIRY_MS = "azure.sas_expiry_ms";
    public static final String CONTAINER = "container";
    public static final String USE_PATH_STYLE = "use_path_style";
    public static final String FORCE_PARSING_BY_STANDARD_URI = "force_parsing_by_standard_uri";

    public static final String SHARED_KEY_AUTH = "SharedKey";
    public static final String SAS_AUTH = "SAS";
    public static final String OAUTH2_AUTH = "OAuth2";
    // Provider-owned backend keys. Azure keeps the existing FILE_S3 wire slot for compatibility,
    // while these names prevent Azure credentials from inheriting AWS/S3 parameter semantics.
    public static final String BACKEND_AUTH_TYPE = "AZURE_AUTH_TYPE";
    public static final String BACKEND_ENDPOINT = "AZURE_ENDPOINT";
    public static final String BACKEND_ACCOUNT_NAME = "AZURE_ACCOUNT_NAME";
    public static final String BACKEND_ACCOUNT_KEY = "AZURE_ACCOUNT_KEY";
    public static final String BACKEND_CONTAINER = "AZURE_CONTAINER";
    public static final String BACKEND_SAS_TOKEN = "AZURE_SAS_TOKEN";
    public static final String BACKEND_SAS_EXPIRY_MS = "AZURE_SAS_EXPIRY_MS";
    public static final String BACKEND_CLIENT_ID = "AZURE_CLIENT_ID";
    public static final String BACKEND_CLIENT_SECRET = "AZURE_CLIENT_SECRET";
    public static final String BACKEND_TENANT_ID = "AZURE_TENANT_ID";
    public static final String BACKEND_OAUTH_SERVER_URI = "AZURE_OAUTH_SERVER_URI";
    public static final String BACKEND_OAUTH_ACCOUNT_HOST = "AZURE_OAUTH_ACCOUNT_HOST";

    private static final String[] AZURE_BLOB_HOST_SUFFIXES = {
            "blob.core.windows.net",
            "blob.core.chinacloudapi.cn",
            "blob.core.usgovcloudapi.net",
            "blob.core.cloudapi.de"
    };

    // In each @ConnectorProperty below, the first name is the canonical key, kept as a
    // constant because other code references it. The remaining literal names are legacy
    // aliases accepted for compatibility only and referenced nowhere else, so they are not
    // promoted to constants.
    @ConnectorProperty(names = {ENDPOINT, "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT",
            "AZURE_ENDPOINT"},
            required = false,
            description = "The endpoint of Azure Blob Storage.")
    private String endpoint = "";

    @ConnectorProperty(names = {ACCOUNT_NAME, "azure.access_key", "s3.access_key",
            "AWS_ACCESS_KEY", "ACCESS_KEY", "access_key", "AZURE_ACCOUNT_NAME"},
            required = false,
            description = "The Azure storage account name.")
    private String accountName = "";

    @ConnectorProperty(names = {ACCOUNT_KEY, "azure.secret_key", "s3.secret_key",
            "AWS_SECRET_KEY", "secret_key", "SECRET_KEY", "AZURE_ACCOUNT_KEY"},
            required = false,
            sensitive = true,
            description = "The Azure storage account key.")
    private String accountKey = "";

    @ConnectorProperty(names = {CLIENT_ID, "AZURE_CLIENT_ID"},
            required = false,
            description = "The client id of Azure AD application.")
    private String clientId = "";

    @ConnectorProperty(names = {CLIENT_SECRET, "AZURE_CLIENT_SECRET"},
            required = false,
            sensitive = true,
            description = "The client secret of Azure AD application.")
    private String clientSecret = "";

    @ConnectorProperty(names = {OAUTH_SERVER_URI, BACKEND_OAUTH_SERVER_URI},
            required = false,
            description = "The Azure OAuth2 token endpoint.")
    private String oauthServerUri = "";

    @ConnectorProperty(names = {OAUTH_ACCOUNT_HOST, BACKEND_OAUTH_ACCOUNT_HOST},
            required = false,
            description = "The Azure account host used by Hadoop OAuth2 config.")
    private String oauthAccountHost = "";

    @ConnectorProperty(names = {TENANT_ID, "AZURE_TENANT_ID"},
            required = false,
            description = "The Azure AD tenant id used by the native Azure SDK.")
    private String tenantId = "";

    @ConnectorProperty(names = {AUTH_TYPE, "AZURE_AUTH_TYPE"},
            required = false,
            description = "The auth type of Azure Blob Storage.")
    private String azureAuthType = "";

    @ConnectorProperty(names = {SAS_TOKEN, "azure.sas-token", "AZURE_SAS_TOKEN"},
            required = false,
            sensitive = true,
            description = "A provider-issued Azure SAS token.")
    private String sasToken = "";

    @ConnectorProperty(names = {SAS_EXPIRY_MS, "azure.sas-token-expires-at-ms", "AZURE_SAS_EXPIRY_MS"},
            required = false,
            description = "The expiry time of the Azure SAS token in Unix milliseconds.")
    private String sasExpiryMs = "";

    @ConnectorProperty(names = {CONTAINER, "azure.bucket", "azure.container", "s3.bucket",
            "AZURE_CONTAINER", "AZURE_BUCKET", "AWS_BUCKET"},
            required = false,
            description = "The Azure container name.")
    private String container = "";

    @ConnectorProperty(names = {USE_PATH_STYLE, "s3.path-style-access"},
            required = false,
            description = "Whether to use path style URL for the storage.")
    private String usePathStyle = "false";

    @ConnectorProperty(names = {FORCE_PARSING_BY_STANDARD_URI},
            required = false,
            description = "Whether to force standard URI parsing.")
    private String forceParsingByStandardUrl = "false";

    private final Map<String, String> rawProperties;
    private final Map<String, String> matchedProperties;
    private final AzureAuthType authType;
    private final AzureAccountHost accountHost;
    private final AzureSasToken sasCredential;

    private AzureFileSystemProperties(Map<String, String> rawProperties) {
        // Defensive copy before wrapping: unmodifiableMap alone is only a read-only view,
        // so without the copy later mutations of the caller's map would leak through.
        this.rawProperties = Collections.unmodifiableMap(new HashMap<>(rawProperties));
        this.matchedProperties = Collections.unmodifiableMap(collectMatchedProperties(rawProperties));
        ConnectorPropertiesUtils.bindConnectorProperties(this, rawProperties);
        this.authType = resolveAuthType();
        azureAuthType = authType.propertyValue();
        this.accountHost = resolveAccountHostModel();
        endpoint = formatAzureEndpoint(endpoint, accountHost);
        this.sasCredential = authType == AzureAuthType.SAS && StringUtils.isNotBlank(sasToken)
                ? AzureSasToken.of(sasToken, parseSasExpiry(sasExpiryMs)) : null;
        if (sasCredential != null) {
            sasExpiryMs = sasCredential.expiresAt()
                    .map(expiry -> Long.toString(expiry.toEpochMilli())).orElse("");
        }
    }

    public static AzureFileSystemProperties of(Map<String, String> properties) {
        AzureFileSystemProperties props = new AzureFileSystemProperties(properties);
        props.validate();
        return props;
    }

    @Override
    public void validate() {
        ParamRules rules = new ParamRules();
        switch (authType) {
            case SHARED_KEY:
                rules.check(() -> StringUtils.isBlank(accountName) || StringUtils.isBlank(accountKey),
                                "When auth_type is SharedKey, account_name and account_key are required.")
                        .check(() -> StringUtils.isNotBlank(sasToken),
                                "When auth_type is SharedKey, sas_token must not be set.");
                break;
            case SAS:
                rules.check(() -> StringUtils.isBlank(sasToken),
                        "When auth_type is SAS, sas_token is required.")
                        .check(() -> StringUtils.isNotBlank(accountKey),
                                "When auth_type is SAS, account_key must not be set.")
                        .check(() -> hasOauth2CredentialMaterial(),
                                "When auth_type is SAS, OAuth2 credential material must not be set.");
                break;
            case OAUTH2:
                rules.check(() -> StringUtils.isBlank(oauthAccountHost)
                                || StringUtils.isBlank(clientId)
                                || StringUtils.isBlank(clientSecret)
                                || StringUtils.isBlank(oauthServerUri),
                        "When auth_type is OAuth2, oauth2_account_host, oauth2_client_id, "
                                + "oauth2_client_secret, and oauth2_server_uri are required.");
                rules.check(() -> StringUtils.isNotBlank(accountKey),
                                "When auth_type is OAuth2, account_key must not be set.")
                        .check(() -> StringUtils.isNotBlank(sasToken),
                                "When auth_type is OAuth2, sas_token must not be set.");
                break;
            default:
                throw new IllegalStateException("Unhandled Azure auth type: " + authType);
        }
        rules.validate("Invalid Azure filesystem properties");
    }

    @Override
    public String providerName() {
        return "AZURE";
    }

    @Override
    public java.util.Set<String> getSupportedSchemes() {
        // fe-core parity (AzureProperties.schemas()): wasb/wasbs/abfs/abfss.
        return java.util.Set.of("wasb", "wasbs", "abfs", "abfss");
    }

    @Override
    public StorageKind kind() {
        return StorageKind.OBJECT_STORAGE;
    }

    @Override
    public FileSystemType type() {
        return FileSystemType.AZURE;
    }

    @Override
    public Map<String, String> rawProperties() {
        return rawProperties;
    }

    @Override
    public Map<String, String> matchedProperties() {
        return matchedProperties;
    }

    @Override
    public Optional<BackendStorageProperties> toBackendProperties() {
        return Optional.of(this);
    }

    @Override
    public Optional<HadoopStorageProperties> toHadoopProperties() {
        return Optional.of(this);
    }

    @Override
    public BackendStorageKind backendKind() {
        // Azure keeps FILE_S3 for the existing Thrift contract. provider=azure and the
        // provider-owned AZURE_* map make the BE select its native Azure SDK client.
        return BackendStorageKind.S3_COMPATIBLE;
    }

    @Override
    public Map<String, String> toMap() {
        validateSasExpiry(Clock.systemUTC());
        // Keep Azure's native credential vocabulary at the FE→BE boundary. The BE still receives
        // FILE_S3, but its provider marker dispatches this map to the Azure SDK. In particular,
        // an Azure SAS is not an AWS session token.
        Map<String, String> azureProps = new HashMap<>();
        azureProps.put("provider", "azure");
        azureProps.put(BACKEND_AUTH_TYPE, authType.name());
        switch (authType) {
            case SHARED_KEY:
                azureProps.put(BACKEND_ACCOUNT_KEY, accountKey);
                break;
            case SAS:
                azureProps.put(BACKEND_SAS_TOKEN, sasCredential.value());
                if (StringUtils.isNotBlank(sasExpiryMs)) {
                    azureProps.put(BACKEND_SAS_EXPIRY_MS, sasExpiryMs);
                }
                break;
            case OAUTH2:
                // Preserve the OneLake Hadoop view until routing selects it separately from the
                // native credentials. Both paths currently consume this binding's backend map.
                azureProps.putAll(oauth2BackendProperties());
                azureProps.put(BACKEND_CLIENT_ID, clientId);
                azureProps.put(BACKEND_CLIENT_SECRET, clientSecret);
                resolveTenantId().ifPresent(value -> azureProps.put(BACKEND_TENANT_ID, value));
                azureProps.put(BACKEND_OAUTH_SERVER_URI, oauthServerUri);
                azureProps.put(BACKEND_OAUTH_ACCOUNT_HOST, oauthAccountHost);
                break;
            default:
                throw new IllegalStateException("Unhandled Azure auth type: " + authType);
        }
        if (StringUtils.isNotBlank(endpoint)) {
            azureProps.put(BACKEND_ENDPOINT, endpoint);
        }
        String backendAccountName = resolveBackendAccountName();
        if (StringUtils.isNotBlank(backendAccountName)) {
            azureProps.put(BACKEND_ACCOUNT_NAME, backendAccountName);
        }
        if (StringUtils.isNotBlank(container)) {
            azureProps.put(BACKEND_CONTAINER, container);
        }
        // Keep this generic option for existing Azure callers that persist it. Native Azure does
        // not use path-style addressing, but removing the key would change old maps.
        azureProps.put(USE_PATH_STYLE, usePathStyle);
        return Collections.unmodifiableMap(azureProps);
    }

    @Override
    public Map<String, String> toHadoopConfigurationMap() {
        validateSasExpiry(Clock.systemUTC());
        Map<String, String> cfg = new HashMap<>();
        // No blanket ABFS/WASB cache disabling: the Doris-patched FileSystem keys its cache by the
        // per-scheme credential fingerprint below, so different credentials never share an
        // instance and merging this map with another storage's loses neither.
        FsCacheKeys.putFsCacheKeys(cfg, this);
        rawProperties.forEach((key, value) -> {
            if (key.startsWith("fs.azure.")) {
                cfg.put(key, value);
            }
        });
        switch (authType) {
            case SHARED_KEY:
                for (String suffix : normalizedAzureBlobHostSuffixes()) {
                    cfg.put("fs.azure.account.key." + accountName + "." + suffix, accountKey);
                }
                cfg.put("fs.azure.account.key", accountKey);
                break;
            case SAS:
                String accountHost = resolveAccountHost();
                if (StringUtils.isNotBlank(accountHost)) {
                    cfg.put("fs.azure.account.auth.type." + accountHost, SAS_AUTH);
                    cfg.put("fs.azure.sas.fixed.token." + accountHost, sasCredential.value());
                }
                break;
            case OAUTH2:
                cfg.put("fs.azure.account.auth.type." + oauthAccountHost, "OAuth");
                cfg.put("fs.azure.account.oauth.provider.type." + oauthAccountHost,
                        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
                cfg.put("fs.azure.account.oauth2.client.id." + oauthAccountHost, clientId);
                cfg.put("fs.azure.account.oauth2.client.secret." + oauthAccountHost, clientSecret);
                cfg.put("fs.azure.account.oauth2.client.endpoint." + oauthAccountHost, oauthServerUri);
                break;
            default:
                throw new IllegalStateException("Unhandled Azure auth type: " + authType);
        }
        return Collections.unmodifiableMap(cfg);
    }

    /**
     * Legacy Hadoop configuration view retained for genuine Fabric OneLake locations. OneLake
     * remains explicitly FILE_HDFS-routed; ordinary Azure ABFS paths use the native fields in
     * {@link #toMap()}.
     */
    private Map<String, String> oauth2BackendProperties() {
        Configuration conf = new Configuration();
        toHadoopConfigurationMap().forEach(conf::set);
        rawProperties.forEach((key, value) -> {
            if (key.startsWith("fs.") && StringUtils.isNotBlank(value)) {
                conf.set(key, value);
            }
        });
        for (String scheme : legacyCacheSchemes()) {
            String key = "fs." + scheme + ".impl.disable.cache";
            String userValue = rawProperties.get(key);
            if (StringUtils.isNotBlank(userValue)) {
                conf.setBoolean(key, BooleanUtils.toBoolean(userValue));
            }
        }
        Map<String, String> dump = new HashMap<>();
        conf.forEach(entry -> dump.put(entry.getKey(), entry.getValue()));
        return Collections.unmodifiableMap(dump);
    }

    /**
     * Keeps Azure's account authority and object path intact for the BE native reader. The old
     * fe-core adapter rewrote these locations to {@code s3://container/path}, which discarded the
     * account host and forced Azure data through an HDFS/S3 compatibility interpretation.
     */
    @Override
    public String validateAndNormalizeUri(String path) {
        if (StringUtils.isBlank(path)) {
            throw new StoragePropertiesException("Path cannot be null or empty");
        }
        int delimiter = path.indexOf("://");
        if (delimiter <= 0) {
            throw new StoragePropertiesException("Azure URI must contain a scheme: " + path);
        }
        String scheme = path.substring(0, delimiter).toLowerCase(Locale.ROOT);
        if (!(scheme.equals("wasb") || scheme.equals("wasbs")
                || scheme.equals("abfs") || scheme.equals("abfss")
                || scheme.equals("http") || scheme.equals("https")
                || scheme.equals("s3"))) {
            throw new StoragePropertiesException("Unsupported Azure URI scheme: " + path);
        }
        // Parse account/container/path now so malformed locations fail before a scan reaches BE;
        // return the original path (apart from a case-insensitive scheme) to preserve object keys.
        try {
            AzureUri parsed = AzureUri.parse(path);
            validateLocationBinding(parsed);
        } catch (IOException e) {
            throw new StoragePropertiesException("Invalid Azure URI: " + path, e);
        }
        return path.substring(0, delimiter).equals(scheme)
                ? path : scheme + path.substring(delimiter);
    }

    private void validateLocationBinding(AzureUri uri) {
        if (StringUtils.isNotBlank(accountName) && StringUtils.isNotBlank(uri.accountName())
                && !StringUtils.equalsIgnoreCase(accountName, uri.accountName())) {
            throw new StoragePropertiesException(
                    "Azure URI account does not match configured account_name");
        }
        if (StringUtils.isNotBlank(container) && !StringUtils.equals(container, uri.container())) {
            throw new StoragePropertiesException(
                    "Azure URI container does not match configured container");
        }
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getAccountName() {
        return accountName;
    }

    public String getAccountKey() {
        return accountKey;
    }

    public String getSasToken() {
        return sasCredential == null ? sasToken : sasCredential.value();
    }

    public String getSasExpiryMs() {
        return sasExpiryMs;
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public String getOauthServerUri() {
        return oauthServerUri;
    }

    public String getOauthAccountHost() {
        return oauthAccountHost;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getAzureAuthType() {
        return azureAuthType;
    }

    public AzureAuthType authType() {
        return authType;
    }

    public String getContainer() {
        return container;
    }

    public String getUsePathStyle() {
        return usePathStyle;
    }

    public String getForceParsingByStandardUrl() {
        return forceParsingByStandardUrl;
    }

    public boolean isSharedKeyAuth() {
        return authType == AzureAuthType.SHARED_KEY;
    }

    public boolean isSasAuth() {
        return authType == AzureAuthType.SAS;
    }

    public boolean isOauth2Auth() {
        return authType == AzureAuthType.OAUTH2;
    }

    public Optional<String> resolveTenantId() {
        if (StringUtils.isNotBlank(tenantId)) {
            return Optional.of(tenantId);
        }
        if (StringUtils.isBlank(oauthServerUri)) {
            return Optional.empty();
        }
        try {
            String path = new URI(oauthServerUri).getPath();
            if (StringUtils.isBlank(path)) {
                return Optional.empty();
            }
            String[] parts = path.split("/");
            for (String part : parts) {
                if (StringUtils.isNotBlank(part)
                        && !"oauth2".equalsIgnoreCase(part)
                        && !"v2.0".equalsIgnoreCase(part)
                        && !"token".equalsIgnoreCase(part)) {
                    return Optional.of(part);
                }
            }
            return Optional.empty();
        } catch (URISyntaxException e) {
            return Optional.empty();
        }
    }

    private AzureAuthType resolveAuthType() {
        if (StringUtils.isNotBlank(azureAuthType)) {
            return AzureAuthType.parse(azureAuthType);
        }
        // Infer only when the property binder found no nonblank auth type, respecting its alias
        // precedence and case-sensitive key matching. OAuth2 continues to require an explicit type.
        boolean hasSas = StringUtils.isNotBlank(sasToken);
        boolean hasSharedKey = StringUtils.isNotBlank(accountKey);
        if (hasSas && hasSharedKey) {
            throw new StoragePropertiesException(
                    "Azure auth_type is required when SharedKey and SAS credential materials are both set; "
                            + "cannot infer an authentication mode");
        }
        return hasSas ? AzureAuthType.SAS : AzureAuthType.SHARED_KEY;
    }

    private boolean hasOauth2CredentialMaterial() {
        return StringUtils.isNotBlank(clientId) || StringUtils.isNotBlank(clientSecret)
                || StringUtils.isNotBlank(oauthServerUri) || StringUtils.isNotBlank(oauthAccountHost)
                || StringUtils.isNotBlank(tenantId);
    }

    private AzureAccountHost resolveAccountHostModel() {
        if (StringUtils.isNotBlank(oauthAccountHost)) {
            return AzureAccountHost.parse(oauthAccountHost);
        }
        if (StringUtils.isNotBlank(endpoint)) {
            return AzureAccountHost.parse(endpoint);
        }
        if (StringUtils.isNotBlank(accountName)) {
            return AzureAccountHost.fromAccountName(accountName);
        }
        return null;
    }

    private String resolveBackendAccountName() {
        if (StringUtils.isNotBlank(accountName)) {
            return accountName;
        }
        return accountHost == null ? "" : accountHost.accountName();
    }

    private String resolveAccountHost() {
        return accountHost == null ? "" : accountHost.dfsHost();
    }

    void validateSasExpiry(Clock clock) {
        if (isSasAuth()) {
            sasCredential.validateNotExpired(clock);
        }
    }

    private static Long parseSasExpiry(String expiry) {
        if (StringUtils.isBlank(expiry)) {
            return null;
        }
        try {
            return Long.parseLong(expiry.trim());
        } catch (NumberFormatException e) {
            throw new StoragePropertiesException("Invalid Azure SAS expiry value");
        }
    }

    private static String formatAzureEndpoint(String endpoint, AzureAccountHost accountHost) {
        if (StringUtils.isBlank(endpoint)) {
            return accountHost == null ? "" : accountHost.blobEndpoint();
        }
        return AzureAccountHost.parse(endpoint).blobEndpoint();
    }

    private static Set<String> normalizedAzureBlobHostSuffixes() {
        Set<String> endpoints = new LinkedHashSet<>();
        for (String suffix : AZURE_BLOB_HOST_SUFFIXES) {
            String normalizedEndpoint = suffix.trim().toLowerCase(Locale.ROOT);
            if (normalizedEndpoint.startsWith(".")) {
                normalizedEndpoint = normalizedEndpoint.substring(1);
            }
            if (!normalizedEndpoint.isEmpty()) {
                endpoints.add(normalizedEndpoint);
            }
        }
        return endpoints;
    }

    private static Map<String, String> collectMatchedProperties(Map<String, String> rawProperties) {
        Map<String, String> matched = new HashMap<>();
        for (Field field : ConnectorPropertiesUtils.getConnectorProperties(AzureFileSystemProperties.class)) {
            String matchedName = ConnectorPropertiesUtils.getMatchedPropertyName(field, rawProperties);
            if (StringUtils.isNotBlank(matchedName)) {
                matched.put(matchedName, rawProperties.get(matchedName));
            }
        }
        return matched;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }

    @Override
    public Set<String> legacyCacheSchemes() {
        return Set.of("wasb", "wasbs", "abfs", "abfss");
    }

}
