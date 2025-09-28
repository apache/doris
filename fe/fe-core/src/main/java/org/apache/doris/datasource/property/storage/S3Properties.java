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

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.CredProviderTypePB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.property.ConnectorPropertiesUtils;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.thrift.TCredProviderType;
import org.apache.doris.thrift.TS3StorageParam;

import com.google.common.base.Preconditions;
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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class S3Properties extends AbstractS3CompatibleProperties {

    public static final String USE_PATH_STYLE = "use_path_style";

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
            "aws.glue.access-key", "client.credentials-provider.glue.access_key", "iceberg.rest.access-key-id",
            "s3.access-key-id"},
            required = false,
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
    @ConnectorProperty(names = {"s3.session_token", "session_token", "s3.session-token"},
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

    @ConnectorProperty(names = {"s3.role_arn", "AWS_ROLE_ARN", "glue.role_arn"},
            required = false,
            description = "The iam role of S3.")
    protected String s3IAMRole = "";

    @ConnectorProperty(names = {"s3.external_id", "AWS_EXTERNAL_ID", "glue.external_id"},
            required = false,
            description = "The external id of S3.")
    protected String s3ExternalId = "";

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
                    .region(Region.of(region))
                    .credentialsProvider(InstanceProfileCredentialsProvider.create())
                    .build();

            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(builder -> {
                        builder.roleArn(s3IAMRole).roleSessionName("aws-sdk-java-v2-fe");
                        if (StringUtils.isNotBlank(s3ExternalId)) {
                            builder.externalId(s3ExternalId);
                        }
                    }).build();
        }
        // For anonymous access (no credentials required)
        //fixme: should return AwsCredentialsProviderChain
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
        if (StringUtils.isNotBlank(s3IAMRole)) {
            //@See org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider
            hadoopStorageConfig.set("fs.s3a.assumed.role.arn", s3IAMRole);
            hadoopStorageConfig.set("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider");
            hadoopStorageConfig.set("fs.s3a.assumed.role.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,com.amazonaws.auth.EnvironmentVar"
                            + "iableCredentialsProvider,com.amazonaws.auth.InstanceProfileCredentialsProvider");
            if (StringUtils.isNotBlank(s3ExternalId)) {
                hadoopStorageConfig.set("fs.s3a.assumed.role.external.id", s3ExternalId);
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

    /**
     * ===========================================
     * NOTICE:
     * This parameter is still used for Cloud-related features,
     * although it is no longer recommended.
     * <p>
     * Reason:
     * - Cloud may access S3-compatible object storage via the S3 protocol.
     * - The exact behavior has not yet been fully clarified.
     * <p>
     * Therefore:
     * - We cannot directly replace it with the new parameter.
     * - This redundant parameter is temporarily kept for compatibility.
     * ===========================================
     */

    public static final String S3_PREFIX = "s3.";

    public static final String ENDPOINT = "s3.endpoint";
    public static final String EXTERNAL_ENDPOINT = "s3.external_endpoint";
    public static final String REGION = "s3.region";
    public static final String ACCESS_KEY = "s3.access_key";
    public static final String SECRET_KEY = "s3.secret_key";
    public static final String SESSION_TOKEN = "s3.session_token";
    public static final String MAX_CONNECTIONS = "s3.connection.maximum";
    public static final String REQUEST_TIMEOUT_MS = "s3.connection.request.timeout";
    public static final String CONNECTION_TIMEOUT_MS = "s3.connection.timeout";

    public static final String ROLE_ARN = "s3.role_arn";
    public static final String EXTERNAL_ID = "s3.external_id";
    public static final String ROOT_PATH = "s3.root.path";
    public static final String BUCKET = "s3.bucket";
    public static final String VALIDITY_CHECK = "s3_validity_check";

    public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT);

    public static class Env {
        public static final String PROPERTIES_PREFIX = "AWS";
        // required
        public static final String ENDPOINT = "AWS_ENDPOINT";
        public static final String REGION = "AWS_REGION";
        public static final String ACCESS_KEY = "AWS_ACCESS_KEY";
        public static final String SECRET_KEY = "AWS_SECRET_KEY";
        public static final String TOKEN = "AWS_TOKEN";
        // required by storage policy
        public static final String ROOT_PATH = "AWS_ROOT_PATH";
        public static final String BUCKET = "AWS_BUCKET";
        // optional
        public static final String MAX_CONNECTIONS = "AWS_MAX_CONNECTIONS";
        public static final String REQUEST_TIMEOUT_MS = "AWS_REQUEST_TIMEOUT_MS";
        public static final String CONNECTION_TIMEOUT_MS = "AWS_CONNECTION_TIMEOUT_MS";
        public static final String DEFAULT_MAX_CONNECTIONS = "50";
        public static final String DEFAULT_REQUEST_TIMEOUT_MS = "3000";
        public static final String DEFAULT_CONNECTION_TIMEOUT_MS = "1000";

        public static final String ROLE_ARN = "AWS_ROLE_ARN";
        public static final String EXTERNAL_ID = "AWS_EXTERNAL_ID";

        public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT);
        public static final List<String> FS_KEYS = Arrays.asList(ENDPOINT, REGION, ACCESS_KEY, SECRET_KEY, TOKEN,
                ROOT_PATH, BUCKET, MAX_CONNECTIONS, REQUEST_TIMEOUT_MS, CONNECTION_TIMEOUT_MS);
    }

    public static void requiredS3Properties(Map<String, String> properties) throws DdlException {
        // Try to convert env properties to uniform properties
        // compatible with old version
        convertToStdProperties(properties);
        if (properties.containsKey(Env.ENDPOINT)
                && !properties.containsKey(ENDPOINT)) {
            for (String field : Env.REQUIRED_FIELDS) {
                checkRequiredProperty(properties, field);
            }
        } else {
            for (String field : REQUIRED_FIELDS) {
                checkRequiredProperty(properties, field);
            }
        }
        if (StringUtils.isNotBlank(properties.get(StorageProperties.FS_PROVIDER_KEY))) {
            // S3 Provider properties should be case insensitive.
            if (!PROVIDERS.stream().anyMatch(s -> s.equals(properties.get(FS_PROVIDER_KEY).toUpperCase()))) {
                throw new DdlException("Provider must be one of OSS, OBS, AZURE, BOS, COS, S3, GCP");
            }
        }

    }

    public static final List<String> PROVIDERS = Arrays.asList("COS", "OSS", "S3", "OBS", "BOS", "AZURE", "GCP", "TOS");

    public static void checkRequiredProperty(Map<String, String> properties, String propertyKey)
            throws DdlException {
        String value = properties.get(propertyKey);
        if (StringUtils.isBlank(value)) {
            throw new DdlException("Missing [" + propertyKey + "] in properties.");
        }
    }

    public static void requiredS3PingProperties(Map<String, String> properties) throws DdlException {
        requiredS3Properties(properties);
        checkRequiredProperty(properties, BUCKET);
    }

    public static void convertToStdProperties(Map<String, String> properties) {
        if (properties.containsKey(Env.ENDPOINT)) {
            properties.putIfAbsent(ENDPOINT, properties.get(Env.ENDPOINT));
        }
        if (properties.containsKey(Env.REGION)) {
            properties.putIfAbsent(REGION, properties.get(Env.REGION));
        }
        if (properties.containsKey(Env.ACCESS_KEY)) {
            properties.putIfAbsent(ACCESS_KEY, properties.get(Env.ACCESS_KEY));
        }
        if (properties.containsKey(Env.SECRET_KEY)) {
            properties.putIfAbsent(SECRET_KEY, properties.get(Env.SECRET_KEY));
        }
        if (properties.containsKey(Env.TOKEN)) {
            properties.putIfAbsent(SESSION_TOKEN, properties.get(Env.TOKEN));
        }
        if (properties.containsKey(Env.MAX_CONNECTIONS)) {
            properties.putIfAbsent(MAX_CONNECTIONS, properties.get(Env.MAX_CONNECTIONS));
        }
        if (properties.containsKey(Env.REQUEST_TIMEOUT_MS)) {
            properties.putIfAbsent(REQUEST_TIMEOUT_MS,
                    properties.get(Env.REQUEST_TIMEOUT_MS));

        }
        if (properties.containsKey(Env.CONNECTION_TIMEOUT_MS)) {
            properties.putIfAbsent(CONNECTION_TIMEOUT_MS,
                    properties.get(Env.CONNECTION_TIMEOUT_MS));
        }
        if (properties.containsKey(Env.ROOT_PATH)) {
            properties.putIfAbsent(ROOT_PATH, properties.get(Env.ROOT_PATH));
        }
        if (properties.containsKey(Env.BUCKET)) {
            properties.putIfAbsent(BUCKET, properties.get(Env.BUCKET));
        }
        if (properties.containsKey(USE_PATH_STYLE)) {
            properties.putIfAbsent(USE_PATH_STYLE, properties.get(USE_PATH_STYLE));
        }

        if (properties.containsKey(Env.ROLE_ARN)) {
            properties.putIfAbsent(ROLE_ARN, properties.get(Env.ROLE_ARN));
        }

        if (properties.containsKey(Env.EXTERNAL_ID)) {
            properties.putIfAbsent(EXTERNAL_ID, properties.get(Env.EXTERNAL_ID));
        }
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

    public static void optionalS3Property(Map<String, String> properties) {
        properties.putIfAbsent(MAX_CONNECTIONS, Env.DEFAULT_MAX_CONNECTIONS);
        properties.putIfAbsent(REQUEST_TIMEOUT_MS, Env.DEFAULT_REQUEST_TIMEOUT_MS);
        properties.putIfAbsent(CONNECTION_TIMEOUT_MS, Env.DEFAULT_CONNECTION_TIMEOUT_MS);
        // compatible with old version
        properties.putIfAbsent(Env.MAX_CONNECTIONS, Env.DEFAULT_MAX_CONNECTIONS);
        properties.putIfAbsent(Env.REQUEST_TIMEOUT_MS, Env.DEFAULT_REQUEST_TIMEOUT_MS);
        properties.putIfAbsent(Env.CONNECTION_TIMEOUT_MS, Env.DEFAULT_CONNECTION_TIMEOUT_MS);
    }

    public static Cloud.ObjectStoreInfoPB.Builder getObjStoreInfoPB(Map<String, String> properties) {
        Cloud.ObjectStoreInfoPB.Builder builder = Cloud.ObjectStoreInfoPB.newBuilder();
        if (properties.containsKey(S3Properties.ENDPOINT)) {
            builder.setEndpoint(properties.get(S3Properties.ENDPOINT));
        }
        if (properties.containsKey(S3Properties.REGION)) {
            builder.setRegion(properties.get(S3Properties.REGION));
        }
        if (properties.containsKey(S3Properties.ACCESS_KEY)) {
            builder.setAk(properties.get(S3Properties.ACCESS_KEY));
        }
        if (properties.containsKey(S3Properties.SECRET_KEY)) {
            builder.setSk(properties.get(S3Properties.SECRET_KEY));
        }
        if (properties.containsKey(S3Properties.ROOT_PATH)) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.get(S3Properties.ROOT_PATH)),
                    "%s cannot be empty", S3Properties.ROOT_PATH);
            builder.setPrefix(properties.get(S3Properties.ROOT_PATH));
        }
        if (properties.containsKey(S3Properties.BUCKET)) {
            builder.setBucket(properties.get(S3Properties.BUCKET));
        }
        if (properties.containsKey(S3Properties.EXTERNAL_ENDPOINT)) {
            builder.setExternalEndpoint(properties.get(S3Properties.EXTERNAL_ENDPOINT));
        }
        if (properties.containsKey(StorageProperties.FS_PROVIDER_KEY)) {
            // S3 Provider properties should be case insensitive.
            builder.setProvider(Provider.valueOf(properties.get(StorageProperties.FS_PROVIDER_KEY).toUpperCase()));
        }

        if (properties.containsKey(S3Properties.USE_PATH_STYLE)) {
            String value = properties.get(S3Properties.USE_PATH_STYLE);
            Preconditions.checkArgument(!Strings.isNullOrEmpty(value), "use_path_style cannot be empty");
            Preconditions.checkArgument(value.equalsIgnoreCase("true")
                            || value.equalsIgnoreCase("false"),
                    "Invalid use_path_style value: %s only 'true' or 'false' is acceptable", value);
            builder.setUsePathStyle(value.equalsIgnoreCase("true"));
        }

        if (properties.containsKey(S3Properties.ROLE_ARN)) {
            builder.setRoleArn(properties.get(S3Properties.ROLE_ARN));
            if (properties.containsKey(S3Properties.EXTERNAL_ID)) {
                builder.setExternalId(properties.get(S3Properties.EXTERNAL_ID));
            }
            builder.setCredProviderType(CredProviderTypePB.INSTANCE_PROFILE);
        }

        return builder;
    }

    public static TS3StorageParam getS3TStorageParam(Map<String, String> properties) {
        TS3StorageParam s3Info = new TS3StorageParam();

        if (properties.containsKey(S3Properties.ROLE_ARN)) {
            s3Info.setRoleArn(properties.get(S3Properties.ROLE_ARN));
            if (properties.containsKey(S3Properties.EXTERNAL_ID)) {
                s3Info.setExternalId(properties.get(S3Properties.EXTERNAL_ID));
            }
            s3Info.setCredProviderType(TCredProviderType.INSTANCE_PROFILE);
        }

        s3Info.setEndpoint(properties.get(S3Properties.ENDPOINT));
        s3Info.setRegion(properties.get(S3Properties.REGION));
        s3Info.setAk(properties.get(S3Properties.ACCESS_KEY));
        s3Info.setSk(properties.get(S3Properties.SECRET_KEY));
        s3Info.setToken(properties.get(S3Properties.SESSION_TOKEN));

        s3Info.setRootPath(properties.get(S3Properties.ROOT_PATH));
        s3Info.setBucket(properties.get(S3Properties.BUCKET));
        String maxConnections = properties.get(S3Properties.MAX_CONNECTIONS);
        s3Info.setMaxConn(Integer.parseInt(maxConnections == null
                ? S3Properties.Env.DEFAULT_MAX_CONNECTIONS : maxConnections));
        String requestTimeoutMs = properties.get(S3Properties.REQUEST_TIMEOUT_MS);
        s3Info.setRequestTimeoutMs(Integer.parseInt(requestTimeoutMs == null
                ? S3Properties.Env.DEFAULT_REQUEST_TIMEOUT_MS : requestTimeoutMs));
        String connTimeoutMs = properties.get(S3Properties.CONNECTION_TIMEOUT_MS);
        s3Info.setConnTimeoutMs(Integer.parseInt(connTimeoutMs == null
                ? S3Properties.Env.DEFAULT_CONNECTION_TIMEOUT_MS : connTimeoutMs));
        String usePathStyle = properties.getOrDefault(S3Properties.USE_PATH_STYLE, "false");
        s3Info.setUsePathStyle(Boolean.parseBoolean(usePathStyle));
        return s3Info;
    }

}

