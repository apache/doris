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

package org.apache.doris.property.storage;

import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.property.common.AwsCredentialsProviderMode;

import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class S3Properties extends AbstractS3CompatibleProperties {

    public static final String USE_PATH_STYLE = "use_path_style";
    public static final String ENDPOINT = "s3.endpoint";
    public static final String REGION = "s3.region";
    public static final String ROLE_ARN = "s3.role_arn";
    public static final String EXTERNAL_ID = "s3.external_id";
    public static final String CREDENTIALS_PROVIDER_TYPE = "s3.credentials_provider_type";

    private static final String[] ENDPOINT_NAMES_FOR_GUESSING = {
            "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT", "aws.endpoint", "glue.endpoint",
            "aws.glue.endpoint"
    };

    private static final String[] REGION_NAMES_FOR_GUESSING = {
            "s3.region", "glue.region", "aws.glue.region", "iceberg.rest.signing-region",
            "rest.signing-region", "client.region"
    };

    @Setter
    @Getter
    @ConnectorProperty(names = {ENDPOINT, "AWS_ENDPOINT", "endpoint", "ENDPOINT", "aws.endpoint", "glue.endpoint",
            "aws.glue.endpoint"},
            required = false,
            description = "The endpoint of S3.")
    protected String endpoint = "";

    @Setter
    @Getter
    @ConnectorProperty(names = {REGION, "AWS_REGION", "region", "REGION", "aws.region", "glue.region",
            "aws.glue.region", "iceberg.rest.signing-region", "rest.signing-region", "client.region"},
            required = false,
            isRegionField = true,
            description = "The region of S3.")
    protected String region = "";

    @Getter
    @ConnectorProperty(names = {"s3.access_key", "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY", "glue.access_key",
            "aws.glue.access-key", "client.credentials-provider.glue.access_key", "iceberg.rest.access-key-id",
            "s3.access-key-id"},
            required = false,
            sensitive = true,
            description = "The access key of S3. Optional for anonymous access to public datasets.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY", "glue.secret_key",
            "aws.glue.secret-key", "client.credentials-provider.glue.secret_key", "iceberg.rest.secret-access-key",
            "s3.secret-access-key"},
            required = false,
            sensitive = true,
            description = "The secret key of S3. Optional for anonymous access to public datasets.")
    protected String secretKey = "";

    @Getter
    @ConnectorProperty(names = {"s3.session_token", "session_token", "s3.session-token", "AWS_TOKEN",
            "iceberg.rest.session-token"},
            required = false,
            description = "The session token of S3.")
    protected String sessionToken = "";

    @Getter
    @ConnectorProperty(names = {"s3.session-token-token-expires-at-ms"},
            required = false,
            description = "The session token expiration time in milliseconds since epoch.")
    protected String sessionTokenExpiresAtMs = "";

    @Getter
    @ConnectorProperty(names = {"s3.connection.maximum",
            "AWS_MAX_CONNECTIONS"},
            required = false,
            description = "The maximum number of connections to S3.")
    protected String maxConnections = "50";

    @Getter
    @ConnectorProperty(names = {"s3.connection.request.timeout",
            "AWS_REQUEST_TIMEOUT_MS"},
            required = false,
            description = "The request timeout of S3 in milliseconds,")
    protected String requestTimeoutS = "3000";

    @Getter
    @ConnectorProperty(names = {"s3.connection.timeout",
            "AWS_CONNECTION_TIMEOUT_MS"},
            required = false,
            description = "The connection timeout of S3 in milliseconds,")
    protected String connectionTimeoutS = "1000";

    @Setter
    @Getter
    @ConnectorProperty(names = {USE_PATH_STYLE, "s3.path-style-access"}, required = false,
            description = "Whether to use path style URL for the storage.")
    protected String usePathStyle = "false";

    @ConnectorProperty(names = {"force_parsing_by_standard_uri"}, required = false,
            description = "Whether to use path style URL for the storage.")
    @Setter
    @Getter
    protected String forceParsingByStandardUrl = "false";

    @ConnectorProperty(names = {"s3.sts_endpoint"},
            supported = false,
            required = false,
            description = "The sts endpoint of S3.")
    protected String s3StsEndpoint = "";

    @ConnectorProperty(names = {"s3.sts_region"},
            supported = false,
            required = false,
            description = "The sts region of S3.")
    protected String s3StsRegion = "";

    @Getter
    @ConnectorProperty(names = {ROLE_ARN, "AWS_ROLE_ARN", "glue.role_arn"},
            required = false,
            description = "The iam role of S3.")
    protected String s3IAMRole = "";

    @Getter
    @ConnectorProperty(names = {EXTERNAL_ID, "AWS_EXTERNAL_ID", "glue.external_id"},
            required = false,
            description = "The external id of S3.")
    protected String s3ExternalId = "";

    @ConnectorProperty(names = {CREDENTIALS_PROVIDER_TYPE, "glue.credentials_provider_type",
            "iceberg.rest.credentials_provider_type"},
            required = false,
            description = "The credentials provider type of S3. "
                    + "Options are: DEFAULT, ASSUME_ROLE, ENVIRONMENT, SYSTEM_PROPERTIES, "
                    + "WEB_IDENTITY_TOKEN_FILE, INSTANCE_PROFILE. "
                    + "If not set, it will use the default provider chain of AWS SDK.")
    protected String awsCredentialsProviderType = AwsCredentialsProviderMode.DEFAULT.name();

    @Getter
    private AwsCredentialsProviderMode awsCredentialsProviderMode;

    public static S3Properties of(Map<String, String> properties) {
        S3Properties propertiesObj = new S3Properties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(propertiesObj, properties);
        propertiesObj.initNormalizeAndCheckProps();
        return propertiesObj;
    }

    /**
     * Pattern to match various AWS S3 endpoint formats and extract the region part.
     * <p>
     * Supported formats:
     * - s3.us-west-2.amazonaws.com                => region = us-west-2
     * - s3.dualstack.us-east-1.amazonaws.com      => region = us-east-1
     * - s3-fips.us-east-2.amazonaws.com           => region = us-east-2
     * - s3-fips.dualstack.us-east-2.amazonaws.com => region = us-east-2
     * - s3express-control.us-west-2.amazonaws.com => region = us-west-2 (S3 Directory Bucket Regional)
     * - s3express-usw2-az1.us-west-2.amazonaws.com => region = us-west-2 (S3 Directory Bucket Zonal)
     * <p>
     * Group(1), Group(2), or Group(3) in the pattern captures the region part if available.
     * <p>
     * For Glue https://docs.aws.amazon.com/general/latest/gr/glue.html
     */
    private static final Set<Pattern> ENDPOINT_PATTERN = ImmutableSet.of(
            Pattern.compile(
                    "^(?:https?://)?(?:"
                            + "s3(?:[-.]fips)?(?:[-.]dualstack)?[-.]([a-z0-9-]+)|" // Standard S3 endpoints
                            + "s3express-control\\.([a-z0-9-]+)|"                  // Directory bucket regional
                            + "s3express-[a-z0-9-]+\\.([a-z0-9-]+)"                // Directory bucket zonal
                            + ")\\.amazonaws\\.com(?:/.*)?$",
                    Pattern.CASE_INSENSITIVE),
            Pattern.compile(
                    "^(?:https?://)?glue(?:-fips)?\\.([a-z0-9-]+)\\.(amazonaws\\.com(?:\\.cn)?|api\\.aws)$",
                    Pattern.CASE_INSENSITIVE));

    public S3Properties(Map<String, String> origProps) {
        super(Type.S3, origProps);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        if (StringUtils.isNotBlank(s3ExternalId) && StringUtils.isBlank(s3IAMRole)) {
            throw new IllegalArgumentException("s3.external_id must be used with s3.role_arn");
        }
        convertGlueToS3EndpointIfNeeded();
        awsCredentialsProviderMode = AwsCredentialsProviderMode.fromString(awsCredentialsProviderType);
    }

    /**
     * Guess if the storage properties is for this storage type.
     * Subclass should override this method to provide the correct implementation.
     *
     * @return
     */
    protected static boolean guessIsMe(Map<String, String> origProps) {
        String endpoint = Stream.of(ENDPOINT_NAMES_FOR_GUESSING)
                .map(origProps::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
        /**
         * Check if the endpoint contains "amazonaws.com" to determine if it's an S3-compatible storage.
         * Note: This check should not be overly strict, as a malformed or misconfigured endpoint may
         * cause the type detection to fail, leading to missed recognition of valid S3 properties.
         * A more robust approach would allow further validation downstream rather than failing early here.
         */
        if (StringUtils.isNotBlank(endpoint)) {
            return endpoint.contains("amazonaws.com");
        }

        // guess from URI
        Optional<String> uriValue = origProps.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase("uri"))
                .map(Map.Entry::getValue)
                .findFirst();
        if (uriValue.isPresent()) {
            return uriValue.get().contains("amazonaws.com");
        }

        // guess from region
        String region = Stream.of(REGION_NAMES_FOR_GUESSING)
                .map(origProps::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
        if (StringUtils.isNotBlank(region)) {
            return true;
        }
        return false;
    }

    @Override
    protected Set<Pattern> endpointPatterns() {
        return ENDPOINT_PATTERN;
    }

    @Override
    protected Set<String> schemas() {
        return ImmutableSet.of("s3", "s3a", "s3n");
    }

    @Override
    public Map<String, String> getBackendConfigProperties() {
        Map<String, String> backendProperties = generateBackendS3Configuration();

        if (StringUtils.isNotBlank(s3IAMRole)) {
            backendProperties.put("AWS_ROLE_ARN", s3IAMRole);
        }
        if (StringUtils.isNotBlank(s3ExternalId)) {
            backendProperties.put("AWS_EXTERNAL_ID", s3ExternalId);
        }
        return backendProperties;
    }

    @Override
    protected String getAwsCredentialsProviderTypeForBackend() {
        return awsCredentialsProviderMode == null ? null : awsCredentialsProviderMode.getMode();
    }

    private void convertGlueToS3EndpointIfNeeded() {
        if (this.endpoint.contains("glue")) {
            this.endpoint = "https://s3." + this.region + ".amazonaws.com";
        }
    }

    /**
     * Builds the {@code fs.s3a.*} Hadoop config keys for assumed-role (IAM role) access when no static access key is
     * configured.
     *
     * <p>NOTE (fe-property simplification): the legacy fe-core class branched on the FE-global
     * {@code Config.aws_credentials_provider_version} ("v1"/"v2") and consulted {@code AwsCredentialsProviderFactory}.
     * fe-property is a connector-facing parsing module with no fe-core {@code Config} and no AWS SDK dependency, so it
     * emits the default (v1) assumed-role provider wiring only, referencing provider classes by their fully-qualified
     * name string. Consumers needing the v2 wiring set the credential-provider keys themselves.
     */
    @Override
    public void initializeHadoopStorageConfig() {
        super.initializeHadoopStorageConfig();
        if (StringUtils.isNotBlank(accessKey)) {
            return;
        }
        // Set assumed_roles
        // @See https://hadoop.apache.org/docs/r3.4.1/hadoop-aws/tools/hadoop-aws/assumed_roles.html
        if (StringUtils.isNotBlank(s3IAMRole)) {
            // @See org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider
            hadoopConfigMap.put("fs.s3a.assumed.role.arn", s3IAMRole);
            hadoopConfigMap.put("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider");
            hadoopConfigMap.put("fs.s3a.assumed.role.credentials.provider",
                    "software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider");
            if (StringUtils.isNotBlank(s3ExternalId)) {
                hadoopConfigMap.put("fs.s3a.assumed.role.external.id", s3ExternalId);
            }
        }
    }

    @Override
    protected String getEndpointFromRegion() {
        if (!StringUtils.isBlank(endpoint)) {
            return endpoint;
        }
        if (StringUtils.isBlank(region)) {
            return "";
        }
        return "https://s3." + region + ".amazonaws.com";
    }

    private static final Pattern IPV4_PORT_PATTERN = Pattern.compile("((?:\\d{1,3}\\.){3}\\d{1,3}:\\d{1,5})");

    public static String getRegionOfEndpoint(String endpoint) {
        if (IPV4_PORT_PATTERN.matcher(endpoint).find()) {
            // if endpoint contains '192.168.0.1:8999', return null region
            return null;
        }
        String[] endpointSplit = endpoint.replace("http://", "")
                .replace("https://", "")
                .split("\\.");
        if (endpointSplit.length < 2) {
            return null;
        }
        if (endpointSplit[0].contains("oss-")) {
            // compatible with the endpoint: oss-cn-bejing.aliyuncs.com
            return endpointSplit[0];
        }
        return endpointSplit[1];
    }
}
