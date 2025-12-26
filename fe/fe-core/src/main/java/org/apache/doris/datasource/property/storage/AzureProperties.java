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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.ParamRules;
import org.apache.doris.datasource.property.storage.exception.AzureAuthType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * AzureProperties is a specialized configuration class for accessing Azure Blob Storage
 * using an S3-compatible interface.
 *
 * <p>This class extends {@link StorageProperties} and adapts Azure-specific properties
 * to a format that is compatible with the backend engine (BE), which expects configurations
 * similar to Amazon S3. This is necessary because the backend is designed to work with
 * S3-style parameters regardless of the actual cloud provider.
 *
 * <p>Although Azure Blob Storage does not use all of the S3 parameters (e.g., region),
 * this class maps and provides dummy or compatible values to satisfy the expected format.
 * It also tags the provider as "azure" in the final configuration map.
 *
 * <p>The class supports common parameters like access key, secret key, endpoint, and
 * path style access, while also ensuring compatibility with existing S3 processing
 * logic by delegating some functionality to {@code S3PropertyUtils}.
 *
 * <p>Typical usage includes validation of required parameters, transformation to a
 * backend-compatible configuration map, and conversion of URLs to storage paths.
 *
 * <p>Note: This class may evolve as the backend introduces native Azure support
 * or adopts a more flexible configuration model.
 *
 * @see StorageProperties
 * @see S3PropertyUtils
 */
public class AzureProperties extends StorageProperties {
    @Getter
    @ConnectorProperty(names = {"azure.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            description = "The endpoint of S3.")
    protected String endpoint = "";


    @Getter
    @ConnectorProperty(names = {"azure.account_name", "azure.access_key", "s3.access_key",
            "AWS_ACCESS_KEY", "ACCESS_KEY", "access_key"},
            required = false,
            sensitive = true,
            description = "The access key of S3.")
    protected String accountName = "";

    @Getter
    @ConnectorProperty(names = {"azure.account_key", "azure.secret_key", "s3.secret_key",
            "AWS_SECRET_KEY", "secret_key"},
            sensitive = true,
            required = false,
            description = "The secret key of S3.")
    protected String accountKey = "";

    @ConnectorProperty(names = {"azure.oauth2_client_id"},
            required = false,
            description = "The client id of Azure AD application.")
    private String clientId;

    @ConnectorProperty(names = {"azure.oauth2_client_secret"},
            required = false,
            sensitive = true,
            description = "The client secret of Azure AD application.")
    private String clientSecret;


    @ConnectorProperty(names = {"azure.oauth2_server_uri"},
            required = false,
            description = "The account host of Azure blob.")
    private String oauthServerUri;

    @ConnectorProperty(names = {"azure.oauth2_account_host"},
            required = false,
            description = "The account host of Azure blob.")
    private String accountHost;

    @ConnectorProperty(names = {"azure.auth_type"},
            required = false,
            description = "The auth type of Azure blob.")
    private String azureAuthType = AzureAuthType.SharedKey.name();

    @Getter
    @ConnectorProperty(names = {"container", "azure.bucket", "s3.bucket"},
            required = false,
            description = "The container of Azure blob.")
    protected String container = "";

    /**
     * Flag indicating whether to use path-style URLs for the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Setter
    @Getter
    @ConnectorProperty(names = {"use_path_style", "s3.path-style-access"}, required = false,
            description = "Whether to use path style URL for the storage.")
    protected String usePathStyle = "false";
    @ConnectorProperty(names = {"force_parsing_by_standard_uri"}, required = false,
            description = "Whether to use path style URL for the storage.")
    @Getter
    protected String forceParsingByStandardUrl = "false";

    public AzureProperties(Map<String, String> origProps) {
        super(Type.AZURE, origProps);
    }

    private static final String AZURE_ENDPOINT_SUFFIX = ".blob.core.windows.net";

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        //check endpoint
        this.endpoint = formatAzureEndpoint(endpoint, accountName);
        buildRules().validate();
        if (AzureAuthType.OAuth2.name().equals(azureAuthType) && (!isIcebergRestCatalog())) {
            throw new UnsupportedOperationException("OAuth2 auth type is only supported for iceberg rest catalog");
        }
    }

    public static boolean guessIsMe(Map<String, String> origProps) {
        boolean enable = origProps.containsKey(FS_PROVIDER_KEY)
                && origProps.get(FS_PROVIDER_KEY).equalsIgnoreCase("azure");
        if (enable) {
            return true;
        }
        String value = Stream.of("azure.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT")
                .map(origProps::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
        if (!Strings.isNullOrEmpty(value)) {
            return value.endsWith(AZURE_ENDPOINT_SUFFIX);
        }
        return false;
    }

    @Override
    public Map<String, String> getBackendConfigProperties() {
        if (!azureAuthType.equalsIgnoreCase("OAuth2")) {
            Map<String, String> s3Props = new HashMap<>();
            s3Props.put("AWS_ENDPOINT", endpoint);
            s3Props.put("AWS_REGION", "dummy_region");
            s3Props.put("AWS_ACCESS_KEY", accountName);
            s3Props.put("AWS_SECRET_KEY", accountKey);
            s3Props.put("AWS_NEED_OVERRIDE_ENDPOINT", "true");
            s3Props.put("provider", "azure");
            s3Props.put("use_path_style", usePathStyle);
            return s3Props;
        }
        // oauth2 use hadoop config
        Map<String, String> s3Props = new HashMap<>();
        hadoopStorageConfig.forEach(entry -> {
            String key = entry.getKey();

            s3Props.put(key, entry.getValue());

        });
        return s3Props;
    }

    public static final String AZURE_ENDPOINT_TEMPLATE = "https://%s.blob.core.windows.net";

    public static String formatAzureEndpoint(String endpoint, String accessKey) {
        if (Config.force_azure_blob_global_endpoint) {
            return String.format(AZURE_ENDPOINT_TEMPLATE, accessKey);
        }
        if (endpoint.contains("://")) {
            return endpoint;
        }
        return "https://" + endpoint;
    }

    @Override
    public String validateAndNormalizeUri(String url) throws UserException {
        return AzurePropertyUtils.validateAndNormalizeUri(url);

    }

    @Override
    public String validateAndGetUri(Map<String, String> loadProps) throws UserException {
        return AzurePropertyUtils.validateAndGetUri(loadProps);
    }

    @Override
    public String getStorageName() {
        return "AZURE";
    }

    @Override
    public void initializeHadoopStorageConfig() {
        hadoopStorageConfig = new Configuration();
        //disable azure cache
        // Disable all Azure ABFS/WASB FileSystem caching to ensure fresh instances per configuration
        for (String scheme : new String[]{"abfs", "abfss", "wasb", "wasbs"}) {
            hadoopStorageConfig.set("fs." + scheme + ".impl.disable.cache", "true");
        }
        origProps.forEach((k, v) -> {
            if (k.startsWith("fs.azure.")) {
                hadoopStorageConfig.set(k, v);
            }
        });
        if (azureAuthType != null && azureAuthType.equalsIgnoreCase("OAuth2")) {
            setHDFSAzureOauth2Config(hadoopStorageConfig);
        } else {
            setHDFSAzureAccountKeys(hadoopStorageConfig, accountName, accountKey);
        }
    }

    @Override
    protected Set<String> schemas() {
        return ImmutableSet.of("wasb", "wasbs", "abfs", "abfss");
    }

    private static void setHDFSAzureAccountKeys(Configuration conf, String accountName, String accountKey) {
        String[] endpoints = {
                "dfs.core.windows.net",
                "blob.core.windows.net"
        };
        for (String endpoint : endpoints) {
            String key = String.format("fs.azure.account.key.%s.%s", accountName, endpoint);
            conf.set(key, accountKey);
        }
        conf.set("fs.azure.account.key", accountKey);
    }

    private void setHDFSAzureOauth2Config(Configuration conf) {
        conf.set(String.format("fs.azure.account.auth.type.%s", accountHost), "OAuth");
        conf.set(String.format("fs.azure.account.oauth.provider.type.%s", accountHost),
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
        conf.set(String.format("fs.azure.account.oauth2.client.id.%s", accountHost), clientId);
        conf.set(String.format("fs.azure.account.oauth2.client.secret.%s", accountHost), clientSecret);
        conf.set(String.format("fs.azure.account.oauth2.client.endpoint.%s", accountHost), oauthServerUri);
    }

    private ParamRules buildRules() {
        return new ParamRules()
                // OAuth2 requires either credential or token, but not both
                .requireIf(azureAuthType, AzureAuthType.OAuth2.name(), new String[]{accountHost,
                        clientId,
                        clientSecret,
                        oauthServerUri}, "When auth_type is OAuth2, oauth2_account_host, oauth2_client_id"
                        + ", oauth2_client_secret, and oauth2_server_uri are required.")
                .requireIf(azureAuthType, AzureAuthType.SharedKey.name(), new String[]{accountName, accountKey},
                        "When auth_type is SharedKey, account_name and account_key are required.");
    }

    // NB:Temporary check:
    // Temporary check: Currently using OAuth2 for accessing Onalake storage via HDFS.
    // In the future, OAuth2 will be supported via native SDK to reduce maintenance.
    // For now, OAuth2 authentication is only allowed for Iceberg REST.
    // TODO: Remove this temporary check later
    private static final String ICEBERG_CATALOG_TYPE_KEY = "iceberg.catalog.type";
    private static final String ICEBERG_CATALOG_TYPE_REST = "rest";
    private static final String TYPE_KEY = "type";
    private static final String ICEBERG_VALUE = "iceberg";

    private boolean isIcebergRestCatalog() {
        // check iceberg type
        boolean hasIcebergType = origProps.entrySet().stream()
                .anyMatch(entry -> TYPE_KEY.equalsIgnoreCase(entry.getKey())
                        && ICEBERG_VALUE.equalsIgnoreCase(entry.getValue()));
        if (!hasIcebergType && origProps.keySet().stream().anyMatch(TYPE_KEY::equalsIgnoreCase)) {
            return false;
        }
        return origProps.entrySet().stream()
                .anyMatch(entry -> ICEBERG_CATALOG_TYPE_KEY.equalsIgnoreCase(entry.getKey())
                        && ICEBERG_CATALOG_TYPE_REST.equalsIgnoreCase(entry.getValue()));
    }

}
