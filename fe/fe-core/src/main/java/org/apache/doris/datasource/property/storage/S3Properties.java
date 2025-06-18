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
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class S3Properties extends AbstractS3CompatibleProperties {


    @Setter
    @Getter
    @ConnectorProperty(names = {"s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            required = false,
            description = "The endpoint of S3.")
    protected String endpoint = "";

    @Setter
    @Getter
    @ConnectorProperty(names = {"s3.region", "AWS_REGION", "region", "REGION"},
            required = false,
            description = "The region of S3.")
    protected String region = "";

    @Getter
    @ConnectorProperty(names = {"s3.access_key", "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY"},
            required = false,
            description = "The access key of S3.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY"},
            required = false,
            description = "The secret key of S3.")
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


    /**
     * Pattern to match various AWS S3 endpoint formats and extract the region part.
     * <p>
     * Supported formats:
     * - s3.us-west-2.amazonaws.com                => region = us-west-2
     * - s3.dualstack.us-east-1.amazonaws.com      => region = us-east-1
     * - s3-fips.us-east-2.amazonaws.com           => region = us-east-2
     * - s3-fips.dualstack.us-east-2.amazonaws.com => region = us-east-2
     * <p>
     * Group(1) in the pattern captures the region part if available.
     */
    private static final Pattern ENDPOINT_PATTERN = Pattern.compile(
            "^(?:https?://)?s3(?:[-.]fips)?(?:[-.]dualstack)?(?:[-.]([a-z0-9-]+))?\\.amazonaws\\.com$"
    );

    public S3Properties(Map<String, String> origProps) {
        super(Type.S3, origProps);
    }

    @Override
    protected void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
            return;
        }
        if (StringUtils.isNotBlank(s3ExternalId) && StringUtils.isNotBlank(s3IAMRole)) {
            return;
        }
        throw new StoragePropertiesException("Please set s3.access_key and s3.secret_key or s3.role_arn and "
                + "s3.external_id");
    }

    /**
     * Guess if the storage properties is for this storage type.
     * Subclass should override this method to provide the correct implementation.
     *
     * @return
     */
    protected static boolean guessIsMe(Map<String, String> origProps) {
        String endpoint = Stream.of("s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT")
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
        Optional<String> uriValue = origProps.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase("uri"))
                .map(Map.Entry::getValue)
                .findFirst();
        return uriValue.isPresent() && uriValue.get().contains("amazonaws.com");
    }

    @Override
    protected Pattern endpointPattern() {
        return ENDPOINT_PATTERN;
    }

    private static List<Field> getIdentifyFields() {
        List<Field> fields = Lists.newArrayList();
        try {
            //todo AliyunDlfProperties should in OSS storage type.
            fields.add(S3Properties.class.getDeclaredField("s3AccessKey"));
            // fixme Add it when MS done
            //fields.add(AliyunDLFProperties.class.getDeclaredField("dlfAccessKey"));
            //fields.add(AWSGlueProperties.class.getDeclaredField("glueAccessKey"));
            return fields;
        } catch (NoSuchFieldException e) {
            // should not happen
            throw new RuntimeException("Failed to get field: " + e.getMessage(), e);
        }
    }

    /*
    public void toPaimonOSSFileIOProperties(Options options) {
        options.set("fs.oss.endpoint", s3Endpoint);
        options.set("fs.oss.accessKeyId", s3AccessKey);
        options.set("fs.oss.accessKeySecret", s3SecretKey);
    }

    public void toPaimonS3FileIOProperties(Options options) {
        options.set("s3.endpoint", s3Endpoint);
        options.set("s3.access-key", s3AccessKey);
        options.set("s3.secret-key", s3SecretKey);
    }*/

    public void toIcebergS3FileIOProperties(Map<String, String> catalogProps) {
        // See S3FileIOProperties.java
        catalogProps.put("s3.endpoint", endpoint);
        catalogProps.put("s3.access-key-id", accessKey);
        catalogProps.put("s3.secret-access-key", secretKey);
        catalogProps.put("client.region", region);
        catalogProps.put("s3.path-style-access", usePathStyle);
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
        return AwsCredentialsProviderChain.of(SystemPropertyCredentialsProvider.create(),
                EnvironmentVariableCredentialsProvider.create(),
                WebIdentityTokenFileCredentialsProvider.create(),
                ProfileCredentialsProvider.create(),
                InstanceProfileCredentialsProvider.create());
    }

}
