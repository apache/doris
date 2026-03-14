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
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
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

    /**
     * The maximum number of concurrent connections that can be made to the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Getter
    @ConnectorProperty(names = {"oss.connection.maximum", "s3.connection.maximum"}, required = false,
            description = "Maximum number of connections.")
    protected String maxConnections = "100";

    /**
     * The timeout (in milliseconds) for requests made to the object storage system.
     * This value is optional and can be configured by the user.
     * Default: 10000ms (10 seconds)
     */
    @Getter
    @ConnectorProperty(names = {"oss.connection.request.timeout", "s3.connection.request.timeout"}, required = false,
            description = "Request timeout in milliseconds. Default: 10000 (10 seconds)")
    protected String requestTimeoutS = "10000";

    /**
     * The timeout (in milliseconds) for establishing a connection to the object storage system.
     * This value is optional and can be configured by the user.
     * Default: 10000ms (10 seconds)
     */
    @Getter
    @ConnectorProperty(names = {"oss.connection.timeout", "s3.connection.timeout"}, required = false,
            description = "Connection timeout in milliseconds. Default: 10000 (10 seconds)")
    protected String connectionTimeoutS = "10000";

    /**
     * Flag indicating whether to use path-style URLs for the object storage system.
     * This value is optional and can be configured by the user.
     */
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

    private static List<String> DLF_TYPE_KEYWORDS = Arrays.asList("hive.metastore.type",
            "iceberg.catalog.type", "paimon.catalog.type");

    private static final String DLS_URI_KEYWORDS = "oss-dls.aliyuncs";

    // OSS Property Constants for Storage Vault Integration
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
        return uriValue.filter(OSSProperties::isKnownObjectStorage).isPresent();
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
                    // If the URI does not start with http(s), derive endpoint from region
                    // (http(s) URIs are handled by separate logic elsewhere)
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
        if (StringUtils.isBlank(endpoint) || !STANDARD_ENDPOINT_PATTERN.matcher(endpoint).matches()) {
            Preconditions.checkArgument(StringUtils.isNotBlank(region),
                    "Region is not set. Either set a standard endpoint or specify oss.region explicitly.");
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
    protected Set<String> schemas() {
        return ImmutableSet.of("oss");
    }

    @Override
    public void initializeHadoopStorageConfig() {
        super.initializeHadoopStorageConfig();
        hadoopStorageConfig.set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
        hadoopStorageConfig.set("fs.oss.accessKeyId", accessKey);
        hadoopStorageConfig.set("fs.oss.accessKeySecret", secretKey);
        hadoopStorageConfig.set("fs.oss.endpoint", endpoint);
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
            // Invalid URI, do not rewrite
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

        // Handle bucket.endpoint format
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
            // Be conservative: fallback to original URI
            return uri;
        }
    }

    /** Delegates to {@link #getObjStoreInfoPB(Map)} using this instance's origProps. */
    public Cloud.ObjectStoreInfoPB.Builder getObjStoreInfoPB() {
        return getObjStoreInfoPB(this.origProps);
    }

    /**
     * Build ObjectStoreInfoPB from OSS properties for storage vault creation/alteration.
     * This method specifically handles OSS properties and sets the provider to OSS.
     *
     * @param properties Map containing OSS configuration properties
     * @return Builder for ObjectStoreInfoPB with OSS configuration
     */
    public static Cloud.ObjectStoreInfoPB.Builder getObjStoreInfoPB(Map<String, String> properties) {
        Cloud.ObjectStoreInfoPB.Builder builder = Cloud.ObjectStoreInfoPB.newBuilder();

        // Set OSS provider explicitly
        builder.setProvider(Cloud.ObjectStoreInfoPB.Provider.OSS);

        // Endpoint - try multiple property keys for compatibility
        String endpoint = Stream.of(ENDPOINT_KEY, "s3.endpoint", "endpoint")
                .map(properties::get)
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);
        if (endpoint != null) {
            builder.setEndpoint(endpoint);
        }

        // Region - try multiple property keys for compatibility
        String region = Stream.of(REGION_KEY, "s3.region", "region")
                .map(properties::get)
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);
        if (region != null) {
            builder.setRegion(region);
        }

        // Access Key - try multiple property keys for compatibility
        String accessKey = Stream.of(ACCESS_KEY_KEY, "s3.access_key", "access_key")
                .map(properties::get)
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);
        if (accessKey != null) {
            builder.setAk(accessKey);
        }

        // Secret Key - try multiple property keys for compatibility
        String secretKey = Stream.of(SECRET_KEY_KEY, "s3.secret_key", "secret_key")
                .map(properties::get)
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);
        if (secretKey != null) {
            builder.setSk(secretKey);
        }

        // Session token managed at runtime by ECSMetadataCredentialsProvider, not stored in protobuf

        // Root Path (prefix)
        String rootPath = Stream.of(ROOT_PATH_KEY, "s3.root.path", "root.path")
                .map(properties::get)
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);
        if (rootPath != null) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(rootPath),
                    "OSS root path cannot be empty");
            builder.setPrefix(rootPath);
        }

        // Bucket
        String bucket = Stream.of(BUCKET_KEY, "bucket")
                .map(properties::get)
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);
        if (bucket != null) {
            builder.setBucket(bucket);
        }

        // AssumeRole configuration - matches S3 pattern (lines 628-634 of S3Properties.java)
        // When role_arn is present, automatically set INSTANCE_PROFILE as base credential provider
        String roleArn = Stream.of(ROLE_ARN_KEY, "oss.role_arn", "s3.role_arn")
                .map(properties::get)
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);

        if (roleArn != null) {
            builder.setRoleArn(roleArn);

            // Set external_id if provided (for cross-account AssumeRole security)
            String externalId = Stream.of(EXTERNAL_ID_KEY, "oss.external_id", "s3.external_id")
                    .map(properties::get)
                    .filter(StringUtils::isNotBlank)
                    .findFirst()
                    .orElse(null);
            if (externalId != null) {
                builder.setExternalId(externalId);
            }

            // When role_arn is present, use INSTANCE_PROFILE as base credentials for AssumeRole
            // This matches S3 behavior: instance profile credentials → AssumeRole API → temporary credentials
            builder.setCredProviderType(Cloud.CredProviderTypePB.INSTANCE_PROFILE);
        } else {
            // No role_arn: determine credential provider type based on whether AK/SK are provided
            if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)) {
                builder.setCredProviderType(Cloud.CredProviderTypePB.INSTANCE_PROFILE);
            } else {
                builder.setCredProviderType(Cloud.CredProviderTypePB.SIMPLE);
            }
        }

        return builder;
    }
}
