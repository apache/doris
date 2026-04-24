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
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.ConnectorPropertiesUtils;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.thrift.TObjStorageType;
import org.apache.doris.thrift.TS3StorageParam;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class OSSProperties extends AbstractS3CompatibleProperties {
    private static final Logger LOG = LogManager.getLogger(OSSProperties.class);

    @Setter
    @Getter
    @ConnectorProperty(names = {"oss.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT", "dlf.endpoint",
            "dlf.catalog.endpoint", "fs.oss.endpoint"},
            required = false,
            description = "The endpoint of OSS.")
    protected String endpoint = "";

    @Getter
    @ConnectorProperty(names = {"oss.access_key", "s3.access_key", "s3.access-key-id", "AWS_ACCESS_KEY", "access_key",
        "ACCESS_KEY", "dlf.access_key", "dlf.catalog.accessKeyId", "fs.oss.accessKeyId"},
            required = false,
            sensitive = true,
            description = "The access key of OSS.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"oss.secret_key", "s3.secret_key", "s3.secret-access-key", "AWS_SECRET_KEY",
        "secret_key", "SECRET_KEY",
            "dlf.secret_key", "dlf.catalog.secret_key", "fs.oss.accessKeySecret"},
            required = false,
            sensitive = true,
            description = "The secret key of OSS.")
    protected String secretKey = "";

    @Getter
    @Setter
    @ConnectorProperty(names = {"oss.region", "s3.region", "AWS_REGION", "region", "REGION", "dlf.region",
        "iceberg.rest.signing-region"},
            required = false,
            isRegionField = true,
            description = "The region of OSS.")
    protected String region;

    @ConnectorProperty(names = {"dlf.access.public", "dlf.catalog.accessPublic"},
            required = false,
            description = "Enable public access to Aliyun DLF.")
    protected String dlfAccessPublic = "false";

    @Getter
    @ConnectorProperty(names = {"oss.session_token", "s3.session_token", "s3.session-token", "session_token",
            "fs.oss.securityToken", "AWS_TOKEN"},
            required = false,
            sensitive = true,
            description = "The session token of OSS.")
    protected String sessionToken = "";

    @Getter
    @ConnectorProperty(names = {"oss.role_arn", "s3.role_arn", "AWS_ROLE_ARN"},
            required = false,
            description = "Alibaba Cloud RAM role ARN for STS AssumeRole.")
    protected String ossRoleArn = "";

    @Getter
    @ConnectorProperty(names = {"oss.external_id", "s3.external_id", "AWS_EXTERNAL_ID"},
            required = false,
            description = "External ID for cross-account STS AssumeRole.")
    protected String ossExternalId = "";

    @Getter
    @ConnectorProperty(names = {"oss.connection.maximum", "s3.connection.maximum"}, required = false,
            description = "Maximum number of connections.")
    protected String maxConnections = "100";

    @Getter
    @ConnectorProperty(names = {"oss.connection.request.timeout", "s3.connection.request.timeout"}, required = false,
            description = "Request timeout in milliseconds.")
    protected String requestTimeoutS = "10000";

    @Getter
    @ConnectorProperty(names = {"oss.connection.timeout", "s3.connection.timeout"}, required = false,
            description = "Connection timeout in milliseconds.")
    protected String connectionTimeoutS = "10000";

    @Setter
    @Getter
    @ConnectorProperty(names = {"oss.use_path_style", "use_path_style", "s3.path-style-access"}, required = false,
            description = "Whether to use path style URL for the storage.")
    protected String usePathStyle = "false";

    @ConnectorProperty(names = {"oss.force_parsing_by_standard_uri", "force_parsing_by_standard_uri"}, required = false,
            description = "Whether to use path style URL for the storage.")
    @Setter
    @Getter
    protected String forceParsingByStandardUrl = "false";

    private static final Pattern STANDARD_ENDPOINT_PATTERN = Pattern
            .compile("^(?:https?://)?(?:s3\\.)?oss-([a-z0-9-]+?)(?:-internal)?\\.aliyuncs\\.com$");

    /**
     * Pattern to extract the region from an Alibaba Cloud OSS endpoint.
     * <p>
     * Supported formats: <a href="https://help.aliyun.com/zh/oss/user-guide/regions-and-endpoints">aliyun oss</a>?
     * - oss-cn-hangzhou.aliyuncs.com              => region = cn-hangzhou
     * - <a href="https://oss-cn-shanghai.aliyuncs.com">...</a>      => region = cn-shanghai
     * - oss-cn-beijing-internal.aliyuncs.com      => region = cn-beijing (internal endpoint)
     * - <a href="http://oss-cn-shenzhen-internal.aliyuncs.com">...</a> => region = cn-shenzhen
     * <p>
     * Group(1) captures the region name (e.g., cn-hangzhou).
     * <p>
     * Support S3 compatible endpoints:<a href="https://help.aliyun.com/zh/oss/developer-reference/
     * use-amazon-s3-sdks-to-access-oss">...</a>
     * - s3.cn-hangzhou.aliyuncs.com              => region = cn-hangzhou
     * <p>
     * https://help.aliyun.com/zh/dlf/dlf-1-0/developer-reference/api-datalake-2020-07-10-endpoint
     * - datalake.cn-hangzhou.aliyuncs.com          => region = cn-hangzhou
     */
    public static final Set<Pattern> ENDPOINT_PATTERN = ImmutableSet.of(STANDARD_ENDPOINT_PATTERN,
            Pattern.compile("^(?:https?://)?dlf(?:-vpc)?\\.([a-z0-9-]+)\\.aliyuncs\\.com(?:/.*)?$"),
            Pattern.compile("^(?:https?://)?datalake(?:-vpc)?\\.([a-z0-9-]+)\\.aliyuncs\\.com(?:/.*)?$"));

    private static final List<String> URI_KEYWORDS = Arrays.asList("uri", "warehouse");

    private static final List<String> DLF_TYPE_KEYWORDS = Arrays.asList("hive.metastore.type",
            "iceberg.catalog.type", "paimon.catalog.type");

    static final String JINDO_OSS_FILE_SYSTEM_IMPL = "com.aliyun.jindodata.oss.JindoOssFileSystem";
    static final String JINDO_OSS_ABSTRACT_FILE_SYSTEM_IMPL = "com.aliyun.jindodata.oss.JindoOSS";

    private static final String DLS_URI_KEYWORDS = "oss-dls.aliyuncs";

    public static final String OSS_PREFIX = "oss.";
    public static final String ENDPOINT_KEY = "oss.endpoint";
    public static final String REGION_KEY = "oss.region";
    public static final String ACCESS_KEY_KEY = "oss.access_key";
    public static final String SECRET_KEY_KEY = "oss.secret_key";
    public static final String SESSION_TOKEN_KEY = "oss.session_token";
    public static final String ROOT_PATH_KEY = "oss.root.path";
    public static final String BUCKET_KEY = "oss.bucket";
    public static final String ROLE_ARN_KEY = "oss.role_arn";
    public static final String EXTERNAL_ID_KEY = "oss.external_id";

    protected OSSProperties(Map<String, String> origProps) {
        super(Type.OSS, origProps);
    }

    public static OSSProperties of(Map<String, String> properties) {
        OSSProperties propertiesObj = new OSSProperties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(propertiesObj, properties);
        propertiesObj.initNormalizeAndCheckProps();
        propertiesObj.initializeHadoopStorageConfig();
        return propertiesObj;
    }

    public static boolean guessIsMe(Map<String, String> origProps) {
        String value = Stream.of("oss.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT",
                        "dlf.endpoint", "dlf.catalog.endpoint", "fs.oss.endpoint", "fs.oss.accessKeyId")
                .map(origProps::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
        if (StringUtils.isNotBlank(value)) {
            if (value.contains(DLS_URI_KEYWORDS)) {
                return false;
            }
            return (value.contains("aliyuncs.com"));
        }

        value = Stream.of("oss.region")
                .map(origProps::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
        if (StringUtils.isNotBlank(value)) {
            return true;
        }
        if (isDlfMSType(origProps)) {
            return true;
        }
        Optional<String> uriValue = origProps.entrySet().stream()
                .filter(e -> URI_KEYWORDS.stream()
                        .anyMatch(key -> key.equalsIgnoreCase(e.getKey())))
                .map(Map.Entry::getValue)
                .filter(Objects::nonNull)
                .filter(OSSProperties::isKnownObjectStorage)
                .findFirst();
        return uriValue.isPresent();
    }

    private static boolean isKnownObjectStorage(String value) {
        if (value == null) {
            return false;
        }
        boolean isDls = value.contains(DLS_URI_KEYWORDS);
        if (isDls) {
            return false;
        }
        if (value.startsWith("oss://")) {
            return true;
        }
        if (!value.contains("aliyuncs.com")) {
            return false;
        }
        boolean isAliyunOss = (value.contains("oss-"));
        boolean isAmazonS3 = value.contains("s3.");
        return isAliyunOss || isAmazonS3;
    }

    private static boolean isDlfMSType(Map<String, String> params) {
        return DLF_TYPE_KEYWORDS.stream()
                .anyMatch(key -> params.containsKey(key) && StringUtils.isNotBlank(params.get(key))
                        && StringUtils.equalsIgnoreCase("dlf", params.get(key)));
    }

    @Override
    protected void setEndpointIfPossible() {
        if (StringUtils.isBlank(this.endpoint) && StringUtils.isNotBlank(this.region)) {
            if (isDlfMSType(origProps)) {
                this.endpoint = getOssEndpoint(region, BooleanUtils.toBoolean(dlfAccessPublic));
            } else {
                Optional<String> uriValueOpt = origProps.entrySet().stream()
                        .filter(e -> URI_KEYWORDS.stream()
                                .anyMatch(key -> key.equalsIgnoreCase(e.getKey())))
                        .map(Map.Entry::getValue)
                        .filter(Objects::nonNull)
                        .filter(OSSProperties::isKnownObjectStorage)
                        .findFirst();
                if (uriValueOpt.isPresent()) {
                    String uri = uriValueOpt.get();
                    if (!uri.startsWith("http://") && !uri.startsWith("https://")) {
                        this.endpoint = getOssEndpoint(region, BooleanUtils.toBoolean(dlfAccessPublic));
                    }
                }
            }
        }
        super.setEndpointIfPossible();
    }

    @Override
    public String validateAndNormalizeUri(String uri) throws UserException {
        return super.validateAndNormalizeUri(rewriteOssBucketIfNecessary(uri));
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        if (StringUtils.isNotBlank(ossExternalId) && StringUtils.isBlank(ossRoleArn)) {
            throw new IllegalArgumentException("oss.external_id must be used with oss.role_arn");
        }
        if (StringUtils.isBlank(endpoint) || !STANDARD_ENDPOINT_PATTERN.matcher(endpoint).matches()) {
            Preconditions.checkArgument(StringUtils.isNotBlank(region),
                    "OSS region is not set. Either provide a standard endpoint "
                    + "(e.g. oss-cn-hangzhou.aliyuncs.com) or specify oss.region explicitly.");
            this.endpoint = getOssEndpoint(region, BooleanUtils.toBoolean(dlfAccessPublic));
        }
    }

    private static String getOssEndpoint(String region, boolean publicAccess) {
        String prefix = "oss-";
        String suffix = ".aliyuncs.com";
        if (!publicAccess) {
            suffix = "-internal" + suffix;
        }
        return prefix + region + suffix;
    }

    @Override
    protected Set<Pattern> endpointPatterns() {
        return ENDPOINT_PATTERN;
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialsProvider() {
        // When role_arn is set, the FE cannot assume an Alibaba Cloud RAM role via
        // the AWS SDK (which calls AWS STS endpoints, not Alibaba STS). Credential
        // resolution is delegated to the BE via OSSSTSCredentialProvider (Alibaba
        // STS v2 SDK). Return null here; role_arn/external_id are forwarded to the
        // BE through getBackendConfigProperties().
        if (StringUtils.isNotBlank(ossRoleArn)) {
            return null;
        }
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

    @Override
    public Map<String, String> getBackendConfigProperties() {
        Map<String, String> backendProperties = super.getBackendConfigProperties();
        backendProperties.put("provider", "OSS");
        if (StringUtils.isNotBlank(ossRoleArn)) {
            backendProperties.put("AWS_ROLE_ARN", ossRoleArn);
            // OSS native SDK requires INSTANCE_PROFILE to activate STS AssumeRole.
            backendProperties.put("AWS_CREDENTIALS_PROVIDER_TYPE", "INSTANCE_PROFILE");
        }
        if (StringUtils.isNotBlank(ossExternalId)) {
            backendProperties.put("AWS_EXTERNAL_ID", ossExternalId);
        }
        return backendProperties;
    }

    @Override
    protected Set<String> schemas() {
        return ImmutableSet.of("oss");
    }

    @Override
    public void initializeHadoopStorageConfig() {
        super.initializeHadoopStorageConfig();
        hadoopStorageConfig.set("fs.oss.impl", JINDO_OSS_FILE_SYSTEM_IMPL);
        hadoopStorageConfig.set("fs.AbstractFileSystem.oss.impl", JINDO_OSS_ABSTRACT_FILE_SYSTEM_IMPL);
        hadoopStorageConfig.set("fs.oss.accessKeyId", accessKey);
        hadoopStorageConfig.set("fs.oss.accessKeySecret", secretKey);
        if (StringUtils.isNotBlank(sessionToken)) {
            hadoopStorageConfig.set("fs.oss.securityToken", sessionToken);
        }
        hadoopStorageConfig.set("fs.oss.endpoint", endpoint);
        hadoopStorageConfig.set("fs.oss.region", region);
    }

    /**
     * Rewrites the bucket part of an OSS URI if the bucket is specified
     * in the form of bucket.endpoint. https://help.aliyun.com/zh/oss/user-guide/access-oss-via-bucket-domain-name
     *
     * <p>This method is designed for OSS usage, but it also supports
     * the {@code s3://} scheme since OSS URIs are sometimes written
     * using the S3-style scheme.</p>
     *
     * <p>HTTP and HTTPS URIs are returned unchanged.</p>
     *
     * <p>Examples:
     * <pre>
     *   oss://bucket.endpoint/path  -> oss://bucket/path
     *   s3://bucket.endpoint        -> s3://bucket
     *   https://bucket.endpoint     -> unchanged
     * </pre>
     *
     * @param uri the original URI string
     * @return the rewritten URI string, or the original URI if no rewrite is needed
     */
    @VisibleForTesting
    protected static String rewriteOssBucketIfNecessary(String uri) {
        if (uri == null || uri.isEmpty()) {
            return uri;
        }

        URI parsed;
        try {
            parsed = URI.create(uri);
        } catch (IllegalArgumentException e) {
            return uri;
        }

        String scheme = parsed.getScheme();
        if ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)) {
            return uri;
        }

        // For non-standard schemes (oss / s3), authority is more reliable than host
        String authority = parsed.getAuthority();
        if (authority == null || authority.isEmpty()) {
            return uri;
        }

        int dotIndex = authority.indexOf('.');
        if (dotIndex <= 0) {
            return uri;
        }

        String bucket = authority.substring(0, dotIndex);

        try {
            URI rewritten = new URI(
                    scheme,
                    bucket,
                    parsed.getPath(),
                    parsed.getQuery(),
                    parsed.getFragment()
            );
            return rewritten.toString();
        } catch (URISyntaxException e) {
            return uri;
        }
    }

    /**
     * Builds an {@link Cloud.ObjectStoreInfoPB} from the bound (normalized) field values of
     * this instance. Unlike the static overload which re-reads {@code origProps}, this method
     * uses the fields already resolved by {@link #initNormalizeAndCheckProps()} — notably
     * the endpoint that may have been derived from the region when not explicitly provided.
     */
    public Cloud.ObjectStoreInfoPB.Builder getObjStoreInfoPB() {
        Cloud.ObjectStoreInfoPB.Builder builder = Cloud.ObjectStoreInfoPB.newBuilder();
        builder.setProvider(Cloud.ObjectStoreInfoPB.Provider.OSS);
        if (StringUtils.isNotBlank(endpoint)) {
            // Endpoint is forwarded without scheme; BE normalises it in OSSConf::get_oss_conf.
            builder.setEndpoint(endpoint);
        }
        if (StringUtils.isNotBlank(region)) {
            builder.setRegion(region);
        }
        if (StringUtils.isNotBlank(accessKey)) {
            builder.setAk(accessKey);
        }
        if (StringUtils.isNotBlank(secretKey)) {
            builder.setSk(secretKey);
        }
        // bucket and rootPath are not bound fields; re-read from origProps.
        String rootPath = Stream.of(ROOT_PATH_KEY, "s3.root.path", "root.path")
                .map(origProps::get).filter(StringUtils::isNotBlank).findFirst().orElse(null);
        if (rootPath != null) {
            builder.setPrefix(rootPath);
        }
        String bucket = Stream.of(BUCKET_KEY, "s3.bucket", "bucket")
                .map(origProps::get).filter(StringUtils::isNotBlank).findFirst().orElse(null);
        if (bucket != null) {
            builder.setBucket(bucket);
        }
        if (StringUtils.isNotBlank(ossRoleArn)) {
            builder.setRoleArn(ossRoleArn);
            if (StringUtils.isNotBlank(ossExternalId)) {
                builder.setExternalId(ossExternalId);
            }
            builder.setCredProviderType(Cloud.CredProviderTypePB.INSTANCE_PROFILE);
        } else {
            builder.setCredProviderType(
                    (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey))
                            ? Cloud.CredProviderTypePB.DEFAULT
                            : Cloud.CredProviderTypePB.SIMPLE);
        }
        return builder;
    }

    /**
     * Builds an {@link Cloud.ObjectStoreInfoPB} builder from an OSS property map.
     * Always sets {@code provider=OSS} and handles role_arn / external_id for AssumeRole.
     */
    public static Cloud.ObjectStoreInfoPB.Builder getObjStoreInfoPB(Map<String, String> properties) {
        Cloud.ObjectStoreInfoPB.Builder builder = Cloud.ObjectStoreInfoPB.newBuilder();
        builder.setProvider(Cloud.ObjectStoreInfoPB.Provider.OSS);

        String endpoint = Stream.of(ENDPOINT_KEY, "s3.endpoint", "endpoint")
                .map(properties::get).filter(StringUtils::isNotBlank).findFirst().orElse(null);
        if (endpoint != null) {
            builder.setEndpoint(endpoint);
        }

        String region = Stream.of(REGION_KEY, "s3.region", "region")
                .map(properties::get).filter(StringUtils::isNotBlank).findFirst().orElse(null);
        if (region != null) {
            builder.setRegion(region);
        }

        String accessKey = Stream.of(ACCESS_KEY_KEY, "s3.access_key", "access_key")
                .map(properties::get).filter(StringUtils::isNotBlank).findFirst().orElse(null);
        if (accessKey != null) {
            builder.setAk(accessKey);
        }

        String secretKey = Stream.of(SECRET_KEY_KEY, "s3.secret_key", "secret_key")
                .map(properties::get).filter(StringUtils::isNotBlank).findFirst().orElse(null);
        if (secretKey != null) {
            builder.setSk(secretKey);
        }

        String rootPath = Stream.of(ROOT_PATH_KEY, "s3.root.path", "root.path")
                .map(properties::get).filter(StringUtils::isNotBlank).findFirst().orElse(null);
        if (rootPath != null) {
            builder.setPrefix(rootPath);
        }

        String bucket = Stream.of(BUCKET_KEY, "s3.bucket", "bucket")
                .map(properties::get).filter(StringUtils::isNotBlank).findFirst().orElse(null);
        if (bucket != null) {
            builder.setBucket(bucket);
        }

        String roleArn = Stream.of(ROLE_ARN_KEY, "s3.role_arn")
                .map(properties::get).filter(StringUtils::isNotBlank).findFirst().orElse(null);
        if (roleArn != null) {
            builder.setRoleArn(roleArn);
            String externalId = Stream.of(EXTERNAL_ID_KEY, "s3.external_id")
                    .map(properties::get).filter(StringUtils::isNotBlank).findFirst().orElse(null);
            if (externalId != null) {
                builder.setExternalId(externalId);
            }
            builder.setCredProviderType(Cloud.CredProviderTypePB.INSTANCE_PROFILE);
        } else {
            builder.setCredProviderType(
                    (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey))
                            ? Cloud.CredProviderTypePB.DEFAULT
                            : Cloud.CredProviderTypePB.SIMPLE);
        }

        return builder;
    }

    /**
     * Builds a {@link TS3StorageParam} from an OSS property map for the Thrift/storage-policy path.
     * Always sets {@code provider=OSS} so the BE routes to the correct SDK.
     * Properties must already be normalized to s3.* keys (e.g. via OSSResource.getCopiedProperties()).
     */
    public static TS3StorageParam getS3TStorageParam(Map<String, String> properties) {
        TS3StorageParam param = S3Properties.getS3TStorageParam(properties);
        param.setProvider(TObjStorageType.OSS);
        return param;
    }
}
