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

import org.apache.doris.common.util.S3Util;
import org.apache.doris.datasource.property.storage.auth.GcpAuth;
import org.apache.doris.datasource.property.storage.auth.GcpCredential;
import org.apache.doris.datasource.property.storage.auth.GcpCredentialProviderType;
import org.apache.doris.datasource.property.storage.auth.GcsAuth;
import org.apache.doris.datasource.property.storage.auth.GcsAuthResolver;
import org.apache.doris.foundation.property.ConnectorProperty;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Google Cloud Storage (GCS) properties based on the S3-compatible protocol.
 *
 * <p>
 * Key differences and considerations:
 * <ul>
 *   <li>The default endpoint is {@code https://storage.googleapis.com}, which usually does not need
 *       to be configured unless a custom domain is required.</li>
 *   <li>The region is typically not relevant for GCS since it is mapped internally by bucket,
 *       but may still be required when using the S3-compatible API.</li>
 *   <li>Access Key and Secret Key are not native GCS concepts. They exist here only for compatibility
 *       with the S3 protocol. Google recommends using OAuth2.0, Service Accounts, or other native
 *       authentication methods instead.</li>
 *   <li>Compatibility with older versions:
 *       <ul>
 *         <li>Previously, the endpoint was required. For example,
 *             {@code gs.endpoint=https://storage.googleapis.com} is valid and backward-compatible.</li>
 *         <li>If a custom endpoint is used (e.g., {@code https://my-custom-endpoint.com}),
 *             the user must explicitly declare that this is GCS storage and configure the mapping.</li>
 *       </ul>
 *   </li>
 *   <li>Without HMAC keys, authentication defaults to ADC, including the VM's attached service account.
 *       Set {@code gs.credential_provider_type=ANONYMOUS} explicitly for public objects.</li>
 *   <li>Native OAuth2 credentials are supported through ADC, Compute Engine metadata credentials,
 *       and Compute Engine credentials with service account impersonation.</li>
 * </ul>
 * </p>
 */
public class GCSProperties extends AbstractS3CompatibleProperties {

    public static final String ENDPOINT = "gs.endpoint";
    public static final String CREDENTIAL_PROVIDER_TYPE = GcpCredential.CREDENTIAL_PROVIDER_TYPE;
    public static final String IMPERSONATION_SERVICE_ACCOUNT = GcpCredential.IMPERSONATION_SERVICE_ACCOUNT;

    private GcsAuth auth;

    @Setter
    @Getter
    @ConnectorProperty(names = {ENDPOINT, "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            required = false,
            description = "The endpoint of GCS.")
    protected String endpoint = "https://storage.googleapis.com";

    @Getter
    protected String region = "us-east1";

    @Getter
    @ConnectorProperty(names = {"gs.access_key", "s3.access_key", "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY"},
            required = false,
            sensitive = true,
            description = "The access key of GCS.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"gs.secret_key", "s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY"},
            required = false,
            sensitive = true,
            description = "The secret key of GCS.")
    protected String secretKey = "";

    @Getter
    @ConnectorProperty(names = {"gs.session_token", "s3.session_token", "session_token", S3Properties.Env.TOKEN},
            required = false,
            sensitive = true,
            description = "The session token of GCS.")
    protected String sessionToken = "";

    /**
     * The maximum number of concurrent connections that can be made to the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Getter
    @ConnectorProperty(names = {"gs.connection.maximum", "s3.connection.maximum"}, required = false,
            description = "Maximum number of connections.")
    protected String maxConnections = "100";

    /**
     * The timeout (in milliseconds) for requests made to the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Getter
    @ConnectorProperty(names = {"gs.connection.request.timeout", "s3.connection.request.timeout"}, required = false,
            description = "Request timeout in seconds.")
    protected String requestTimeoutS = "10000";

    /**
     * The timeout (in milliseconds) for establishing a connection to the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Getter
    @ConnectorProperty(names = {"gs.connection.timeout", "s3.connection.timeout"}, required = false,
            description = "Connection timeout in seconds.")
    protected String connectionTimeoutS = "10000";

    /**
     * Flag indicating whether to use path-style URLs for the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Getter
    @ConnectorProperty(names = {"gs.use_path_style", "use_path_style", "s3.path-style-access"}, required = false,
            description = "Whether to use path style URL for the storage.")
    protected String usePathStyle = "false";

    @ConnectorProperty(names = {"gs.force_parsing_by_standard_uri", "force_parsing_by_standard_uri"}, required = false,
            description = "Whether to use path style URL for the storage.")
    @Getter
    protected String forceParsingByStandardUrl = "false";

    /** Resolved GCP credential provider; empty when authentication uses HMAC keys. */
    @Getter
    @ConnectorProperty(names = {CREDENTIAL_PROVIDER_TYPE}, required = false,
            description = "GCP credential source: DEFAULT (ADC), COMPUTE_ENGINE or ANONYMOUS. "
                    + "Defaults to ADC without HMAC keys.")
    protected String credentialProviderType = "";

    @Getter
    @ConnectorProperty(names = {IMPERSONATION_SERVICE_ACCOUNT}, required = false,
            description = "Target service account email used for GCP service account impersonation.")
    protected String impersonationServiceAccount = "";

    /**
     * Constructor to initialize the object storage properties with the provided type and original properties map.
     *
     * @param origProps the original properties map.
     */
    protected GCSProperties(Map<String, String> origProps) {
        super(Type.GCS, origProps);
    }

    public static boolean guessIsMe(Map<String, String> props) {
        return GcsAuthResolver.guessIsGcs(props);
    }

    @Override
    protected Set<Pattern> endpointPatterns() {
        return new HashSet<>();
    }


    @Override
    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public void initializeHadoopStorageConfig() {
        if (hasGcpCredential()) {
            // HadoopCatalog namespace operations bypass Iceberg FileIO. Native GCP credentials
            // must use the GCS connector here, rather than the S3A filesystem's AWS credential chain.
            GcpCredential credential = getGcpCredential();
            hadoopStorageConfig = new Configuration();
            hadoopStorageConfig.set("fs.gs.impl", GoogleHadoopFileSystem.class.getName());
            hadoopStorageConfig.set("fs.gs.auth.type",
                    credential.getCredentialProviderType() == GcpCredentialProviderType.DEFAULT
                            ? "APPLICATION_DEFAULT" : "COMPUTE_ENGINE");
            hadoopStorageConfig.set("fs.gs.auth.impersonation.service.account",
                    credential.getImpersonationServiceAccount());
        } else {
            super.initializeHadoopStorageConfig();
            hadoopStorageConfig.set("fs.gs.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            if (auth.isAnonymous()) {
                hadoopStorageConfig.set("fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");
            }
        }
    }

    protected Map<String, String> doGetBackendConfigProperties() {
        Map<String, String> backendProperties = generateBackendS3Configuration();
        backendProperties.put("provider", "GCP");
        if (hasGcpCredential()) {
            GcpCredential credential = getGcpCredential();
            backendProperties.put(CREDENTIAL_PROVIDER_TYPE, credential.getCredentialProviderType().name());
            if (StringUtils.isNotBlank(credential.getImpersonationServiceAccount())) {
                backendProperties.put(IMPERSONATION_SERVICE_ACCOUNT,
                        credential.getImpersonationServiceAccount());
            }
        }
        return backendProperties;
    }

    @Override
    public void initNormalizeAndCheckProps() {
        // Resolve raw aliases before binding, then share the immutable choice across all consumers.
        auth = GcsAuthResolver.resolve(origProps)
                .orElseThrow(() -> new IllegalArgumentException("GCS storage properties are required."));
        super.initNormalizeAndCheckProps();
        if (hasGcpCredential()) {
            GcpCredential credential = getGcpCredential();
            credentialProviderType = credential.getCredentialProviderType().name();
            impersonationServiceAccount = credential.getImpersonationServiceAccount();
        } else if (auth.isAnonymous()) {
            credentialProviderType = GcpCredentialProviderType.ANONYMOUS.name();
            impersonationServiceAccount = "";
        } else {
            credentialProviderType = "";
            impersonationServiceAccount = "";
        }
        if (auth.getMode() != GcsAuth.Mode.HMAC) {
            // Cache identity follows resolved authentication, including implicit ADC and aliases.
            matchedProperties.put(CREDENTIAL_PROVIDER_TYPE, credentialProviderType);
            matchedProperties.put(IMPERSONATION_SERVICE_ACCOUNT, impersonationServiceAccount);
        }
    }

    public static boolean hasNativeCredentialProperties(Map<String, String> properties) {
        return GcsAuthResolver.hasNativeCredentialProperties(properties);
    }

    public static void validateNativeCredentialCompatibility(Map<String, String> properties) {
        GcsAuthResolver.resolve(properties);
    }

    public boolean hasGcpCredential() {
        return auth.getNativeCredential().isPresent();
    }

    public boolean isAnonymous() {
        return auth.isAnonymous();
    }

    public GcpCredentialProviderType getGcpCredentialProviderType() {
        return getGcpCredential().getCredentialProviderType();
    }

    public GcpCredential getGcpCredential() {
        return auth.getNativeCredential()
                .orElseThrow(() -> new IllegalArgumentException(CREDENTIAL_PROVIDER_TYPE
                        + " does not select native GCP authentication."));
    }

    public S3Client createS3Client() {
        return createS3Client(endpoint, Boolean.parseBoolean(usePathStyle));
    }

    @Override
    public S3Client createS3Client(String endpoint, boolean usePathStyle) {
        if (!hasGcpCredential()) {
            return super.createS3Client(endpoint, usePathStyle);
        }
        try {
            return S3Util.buildBearerTokenS3Client(endpoint, getRegion(), usePathStyle,
                    GcpAuth.bearerTokenInterceptor(GcpAuth.createCredentials(getGcpCredential())));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize native GCP authentication", e);
        }
    }

    public S3AsyncClient createS3AsyncClient() {
        if (!hasGcpCredential()) {
            throw new IllegalStateException("Native GCP authentication is required for the Iceberg client");
        }
        try {
            return S3Util.buildBearerTokenS3AsyncClient(endpoint, region, Boolean.parseBoolean(usePathStyle),
                    GcpAuth.bearerTokenInterceptor(GcpAuth.createCredentials(getGcpCredential())));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize native GCP authentication", e);
        }
    }

    @Override
    protected Set<String> schemas() {
        return ImmutableSet.of("gs");
    }

    @Override
    protected String getAwsCredentialsProviderTypeForBackend() {
        return auth.isAnonymous() ? GcpCredentialProviderType.ANONYMOUS.name() : null;
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialsProvider() {
        if (auth.isAnonymous()) {
            return AnonymousCredentialsProvider.create();
        }
        if (auth.getMode() == GcsAuth.Mode.HMAC) {
            return super.getAwsCredentialsProvider();
        }
        throw new IllegalStateException("Native GCP authentication requires the bearer-token client");
    }
}
