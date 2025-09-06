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

import org.apache.doris.datasource.property.ConnectorProperty;

import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

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
 *   <li>Additional authentication methods (e.g., OAuth2, Service Account) may be supported in the future.</li>
 * </ul>
 * </p>
 */
public class GCSProperties extends AbstractS3CompatibleProperties {

    private static final Set<String> GS_ENDPOINT_ALIAS = ImmutableSet.of(
            "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT");

    private static final String GCS_ENDPOINT_KEY_NAME = "gs.endpoint";


    @Setter
    @Getter
    @ConnectorProperty(names = {"gs.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            required = false,
            description = "The endpoint of GCS.")
    protected String endpoint = "https://storage.googleapis.com";

    @Getter
    protected String region = "us-east1";

    @Getter
    @ConnectorProperty(names = {"gs.access_key", "s3.access_key", "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY"},
            required = false,
            description = "The access key of GCS.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"gs.secret_key", "s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY"},
            required = false,
            sensitive = true,
            description = "The secret key of GCS.")
    protected String secretKey = "";

    @Getter
    @ConnectorProperty(names = {"gs.session_token", "s3.session_token", "session_token"},
            required = false,
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
    @Setter
    @Getter
    @ConnectorProperty(names = {"gs.use_path_style", "use_path_style", "s3.path-style-access"}, required = false,
            description = "Whether to use path style URL for the storage.")
    protected String usePathStyle = "false";

    @ConnectorProperty(names = {"gs.force_parsing_by_standard_uri", "force_parsing_by_standard_uri"}, required = false,
            description = "Whether to use path style URL for the storage.")
    @Setter
    @Getter
    protected String forceParsingByStandardUrl = "false";

    /**
     * Constructor to initialize the object storage properties with the provided type and original properties map.
     *
     * @param origProps the original properties map.
     */
    protected GCSProperties(Map<String, String> origProps) {
        super(Type.GCS, origProps);
    }

    public static boolean guessIsMe(Map<String, String> props) {
        // check has gcs specific keys,ignore case
        if (props.containsKey(GCS_ENDPOINT_KEY_NAME) && StringUtils.isNotBlank(props.get(GCS_ENDPOINT_KEY_NAME))) {
            return true;
        }
        String endpoint;
        for (String key : props.keySet()) {
            if (GS_ENDPOINT_ALIAS.contains(key.toLowerCase())) {
                endpoint = props.get(key);
                if (StringUtils.isNotBlank(endpoint) && endpoint.toLowerCase().endsWith("storage.googleapis.com")) {
                    return true;
                }
            }
        }
        return false;
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
        super.initializeHadoopStorageConfig();
        hadoopStorageConfig.set("fs.gs.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    }

    public Map<String, String> getBackendConfigProperties() {
        Map<String, String> backendProperties = generateBackendS3Configuration();
        backendProperties.put("provider", "GCP");
        return backendProperties;
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialsProvider() {
        AwsCredentialsProvider credentialsProvider = super.getAwsCredentialsProvider();
        if (credentialsProvider != null) {
            return credentialsProvider;
        }
        if (StringUtils.isBlank(accessKey) && StringUtils.isBlank(secretKey)) {
            // For anonymous access (no credentials required)
            return AnonymousCredentialsProvider.create();
        }
        return null;
    }
}
