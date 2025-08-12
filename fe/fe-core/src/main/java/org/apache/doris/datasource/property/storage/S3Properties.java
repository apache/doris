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

import org.apache.doris.datasource.property.ConnectorPropertiesUtils;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class S3Properties extends AbstractS3CompatibleProperties {

    private static final String[] ENDPOINT_NAMES_FOR_GUESSING = {
            "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT", "aws.endpoint", "glue.endpoint",
            "aws.glue.endpoint"
    };

    private static final String[] REGION_NAMES_FOR_GUESSING = {
            "s3.region", "glue.region", "aws.glue.region", "iceberg.rest.signing-region"
    };

    @Setter
    @Getter
    @ConnectorProperty(names = {"s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT", "aws.endpoint", "glue.endpoint",
            "aws.glue.endpoint"},
            required = false,
            description = "The endpoint of S3.")
    protected String endpoint = "";

    @Setter
    @Getter
    @ConnectorProperty(names = {"s3.region", "AWS_REGION", "region", "REGION", "aws.region", "glue.region",
            "aws.glue.region", "iceberg.rest.signing-region"},
            required = false,
            description = "The region of S3.")
    protected String region = "";

    @Getter
    @ConnectorProperty(names = {"s3.access_key", "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY", "glue.access_key",
            "aws.glue.access-key", "client.credentials-provider.glue.access_key", "iceberg.rest.access-key-id"},
            required = false,
            description = "The access key of S3. Optional for anonymous access to public datasets.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY", "glue.secret_key",
            "aws.glue.secret-key", "client.credentials-provider.glue.secret_key", "iceberg.rest.secret-access-key"},
            required = false,
            description = "The secret key of S3. Optional for anonymous access to public datasets.")
    protected String secretKey = "";


    @ConnectorProperty(names = {"s3.connection.maximum",
            "AWS_MAX_CONNECTIONS"},
            required = false,
            description = "The maximum number of connections to S3.")
    protected String s3ConnectionMaximum = "50";

    @ConnectorProperty(names = {"s3.connection.request.timeout",
            "AWS_REQUEST_TIMEOUT_MS"},
            required = false,
            description = "The request timeout of S3 in milliseconds,")
    protected String s3ConnectionRequestTimeoutS = "3000";

    @ConnectorProperty(names = {"s3.connection.timeout",
            "AWS_CONNECTION_TIMEOUT_MS"},
            required = false,
            description = "The connection timeout of S3 in milliseconds,")
    protected String s3ConnectionTimeoutS = "1000";

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

    @ConnectorProperty(names = {"s3.role_arn", "AWS_ROLE_ARN"},
            required = false,
            description = "The iam role of S3.")
    protected String s3IAMRole = "";

    @ConnectorProperty(names = {"s3.external_id", "AWS_EXTERNAL_ID"},
            required = false,
            description = "The external id of S3.")
    protected String s3ExternalId = "";

    public static S3Properties of(Map<String, String> properties) {
        S3Properties propertiesObj = new S3Properties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(propertiesObj, properties);
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
        convertGlueToS3EndpointIfNeeded();
        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
            return;
        }
        if (StringUtils.isNotBlank(s3ExternalId) && StringUtils.isNotBlank(s3IAMRole)) {
            return;
        }
        // When using vended credentials with a REST catalog, AK/SK are not provided directly.
        // The credentials will be fetched from the REST service later.
        // So we skip the credential check in this case.
        if (Boolean.parseBoolean(origProps.getOrDefault("iceberg.rest.vended-credentials-enabled", "false"))) {
            return;
        }
        // Allow anonymous access if both access_key and secret_key are empty
        if (StringUtils.isBlank(accessKey) && StringUtils.isBlank(secretKey)) {
            return;
        }
        throw new StoragePropertiesException("Please set s3.access_key and s3.secret_key or s3.role_arn and "
                + "s3.external_id or omit all for anonymous access to public bucket.");
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
        if (!Strings.isNullOrEmpty(endpoint)) {
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
        if (!Strings.isNullOrEmpty(region)) {
            return true;
        }
        return false;
    }

    @Override
    protected Set<Pattern> endpointPatterns() {
        return ENDPOINT_PATTERN;
    }

    @Override
    public Map<String, String> getBackendConfigProperties() {
        Map<String, String> backendProperties = generateBackendS3Configuration(s3ConnectionMaximum,
                s3ConnectionRequestTimeoutS, s3ConnectionTimeoutS, String.valueOf(usePathStyle));

        if (StringUtils.isNotBlank(s3ExternalId)
                && StringUtils.isNotBlank(s3IAMRole)) {
            backendProperties.put("AWS_ROLE_ARN", s3IAMRole);
            backendProperties.put("AWS_EXTERNAL_ID", s3ExternalId);
        }
        return backendProperties;
    }

    private void convertGlueToS3EndpointIfNeeded() {
        if (this.endpoint.contains("glue")) {
            this.endpoint = "https://s3." + this.region + ".amazonaws.com";
        }
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialsProvider() {
        AwsCredentialsProvider credentialsProvider = super.getAwsCredentialsProvider();
        if (credentialsProvider != null) {
            return credentialsProvider;
        }
        if (StringUtils.isNotBlank(s3IAMRole)) {
            StsClient stsClient = StsClient.builder()
                    .credentialsProvider(InstanceProfileCredentialsProvider.create())
                    .build();

            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(builder -> {
                        builder.roleArn(s3IAMRole).roleSessionName("aws-sdk-java-v2-fe");
                        if (!Strings.isNullOrEmpty(s3ExternalId)) {
                            builder.externalId(s3ExternalId);
                        }
                    }).build();
        }
        // For anonymous access (no credentials required)
        if (StringUtils.isBlank(accessKey) && StringUtils.isBlank(secretKey)) {
            return AnonymousCredentialsProvider.create();
        }
        return AwsCredentialsProviderChain.of(SystemPropertyCredentialsProvider.create(),
                EnvironmentVariableCredentialsProvider.create(),
                WebIdentityTokenFileCredentialsProvider.create(),
                ProfileCredentialsProvider.create(),
                InstanceProfileCredentialsProvider.create());
    }

    @Override
    public void initializeHadoopStorageConfig() {
        super.initializeHadoopStorageConfig();
        //Set assumed_roles
        //@See https://hadoop.apache.org/docs/r3.4.1/hadoop-aws/tools/hadoop-aws/assumed_roles.html
        if (StringUtils.isNotBlank(s3ExternalId) && StringUtils.isNotBlank(s3IAMRole)) {
            //@See org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider
            hadoopStorageConfig.set("fs.s3a.assumed.role.external.id", s3ExternalId);
            hadoopStorageConfig.set("fs.s3a.assumed.role.arn", s3IAMRole);
            hadoopStorageConfig.set("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider");
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
}

