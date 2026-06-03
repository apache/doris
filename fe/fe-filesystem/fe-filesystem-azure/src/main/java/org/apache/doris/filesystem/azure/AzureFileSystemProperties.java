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
import org.apache.doris.filesystem.properties.HadoopStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
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
 * <p>The public aliases, endpoint formatting, shared-key validation, and backend
 * map match fe-core AzureProperties. Legacy uppercase aliases remain accepted so
 * existing FE filesystem callers can migrate through {@link AzureFileSystemProvider#bind(Map)}.
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
    public static final String CONTAINER = "container";
    public static final String USE_PATH_STYLE = "use_path_style";
    public static final String FORCE_PARSING_BY_STANDARD_URI = "force_parsing_by_standard_uri";

    public static final String SHARED_KEY_AUTH = "SharedKey";
    public static final String OAUTH2_AUTH = "OAuth2";
    public static final String AZURE_ENDPOINT_TEMPLATE = "https://%s.blob.core.windows.net";

    private static final String[] AZURE_BLOB_HOST_SUFFIXES = {
            "blob.core.windows.net",
            "blob.core.chinacloudapi.cn",
            "blob.core.usgovcloudapi.net",
            "blob.core.cloudapi.de"
    };

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

    @ConnectorProperty(names = {OAUTH_SERVER_URI},
            required = false,
            description = "The Azure OAuth2 token endpoint.")
    private String oauthServerUri = "";

    @ConnectorProperty(names = {OAUTH_ACCOUNT_HOST},
            required = false,
            description = "The Azure account host used by Hadoop OAuth2 config.")
    private String oauthAccountHost = "";

    @ConnectorProperty(names = {TENANT_ID, "AZURE_TENANT_ID"},
            required = false,
            description = "The Azure AD tenant id used by the native Azure SDK.")
    private String tenantId = "";

    @ConnectorProperty(names = {AUTH_TYPE},
            required = false,
            description = "The auth type of Azure Blob Storage.")
    private String azureAuthType = SHARED_KEY_AUTH;

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

    private AzureFileSystemProperties(Map<String, String> rawProperties) {
        this.rawProperties = Collections.unmodifiableMap(new HashMap<>(rawProperties));
        this.matchedProperties = Collections.unmodifiableMap(collectMatchedProperties(rawProperties));
        ConnectorPropertiesUtils.bindConnectorProperties(this, rawProperties);
        normalize();
    }

    public static AzureFileSystemProperties of(Map<String, String> properties) {
        AzureFileSystemProperties props = new AzureFileSystemProperties(properties);
        props.validate();
        return props;
    }

    @Override
    public void validate() {
        new ParamRules()
                .check(() -> !isSharedKeyAuth() && !isOauth2Auth(),
                        "Unsupported Azure auth_type: " + azureAuthType)
                .check(() -> isSharedKeyAuth()
                                && (StringUtils.isBlank(accountName) || StringUtils.isBlank(accountKey)),
                        "When auth_type is SharedKey, account_name and account_key are required.")
                .check(() -> isOauth2Auth()
                                && (StringUtils.isBlank(oauthAccountHost)
                                || StringUtils.isBlank(clientId)
                                || StringUtils.isBlank(clientSecret)
                                || StringUtils.isBlank(oauthServerUri)),
                        "When auth_type is OAuth2, oauth2_account_host, oauth2_client_id, "
                                + "oauth2_client_secret, and oauth2_server_uri are required.")
                .validate("Invalid Azure filesystem properties");
    }

    @Override
    public String providerName() {
        return "AZURE";
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
        return BackendStorageKind.S3_COMPATIBLE;
    }

    @Override
    public Map<String, String> toMap() {
        if (isOauth2Auth()) {
            return toHadoopConfigurationMap();
        }
        Map<String, String> s3Props = new HashMap<>();
        s3Props.put("AWS_ENDPOINT", endpoint);
        s3Props.put("AWS_REGION", "dummy_region");
        s3Props.put("AWS_ACCESS_KEY", accountName);
        s3Props.put("AWS_SECRET_KEY", accountKey);
        s3Props.put("AWS_NEED_OVERRIDE_ENDPOINT", "true");
        s3Props.put("provider", "azure");
        s3Props.put("use_path_style", usePathStyle);
        return Collections.unmodifiableMap(s3Props);
    }

    @Override
    public Map<String, String> toHadoopConfigurationMap() {
        Map<String, String> cfg = new HashMap<>();
        for (String scheme : Arrays.asList("abfs", "abfss", "wasb", "wasbs")) {
            cfg.put("fs." + scheme + ".impl.disable.cache", "true");
        }
        rawProperties.forEach((key, value) -> {
            if (key.startsWith("fs.azure.")) {
                cfg.put(key, value);
            }
        });
        if (isOauth2Auth()) {
            cfg.put("fs.azure.account.auth.type." + oauthAccountHost, "OAuth");
            cfg.put("fs.azure.account.oauth.provider.type." + oauthAccountHost,
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
            cfg.put("fs.azure.account.oauth2.client.id." + oauthAccountHost, clientId);
            cfg.put("fs.azure.account.oauth2.client.secret." + oauthAccountHost, clientSecret);
            cfg.put("fs.azure.account.oauth2.client.endpoint." + oauthAccountHost, oauthServerUri);
        } else {
            for (String suffix : normalizedAzureBlobHostSuffixes()) {
                cfg.put("fs.azure.account.key." + accountName + "." + suffix, accountKey);
            }
            cfg.put("fs.azure.account.key", accountKey);
        }
        return Collections.unmodifiableMap(cfg);
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
        return SHARED_KEY_AUTH.equalsIgnoreCase(azureAuthType);
    }

    public boolean isOauth2Auth() {
        return OAUTH2_AUTH.equalsIgnoreCase(azureAuthType);
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

    private void normalize() {
        endpoint = formatAzureEndpoint(endpoint, accountName, oauthAccountHost);
        if (StringUtils.isNotBlank(azureAuthType)) {
            if (SHARED_KEY_AUTH.equalsIgnoreCase(azureAuthType)) {
                azureAuthType = SHARED_KEY_AUTH;
            } else if (OAUTH2_AUTH.equalsIgnoreCase(azureAuthType)) {
                azureAuthType = OAUTH2_AUTH;
            }
        }
    }

    private static String formatAzureEndpoint(String endpoint, String accountName, String accountHost) {
        if (StringUtils.isBlank(endpoint)) {
            if (StringUtils.isNotBlank(accountName)) {
                return String.format(AZURE_ENDPOINT_TEMPLATE, accountName);
            }
            return addHttpsScheme(accountHost);
        }
        return addHttpsScheme(endpoint);
    }

    private static String addHttpsScheme(String endpoint) {
        if (StringUtils.isBlank(endpoint)) {
            return "";
        }
        if (endpoint.contains("://")) {
            return endpoint;
        }
        return "https://" + endpoint;
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
}
