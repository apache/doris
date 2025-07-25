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
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

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
            "dlf.catalog.endpoint"},
            required = false,
            description = "The endpoint of OSS.")
    protected String endpoint = "";

    @Getter
    @ConnectorProperty(names = {"oss.access_key", "s3.access_key", "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY",
            "dlf.access_key", "dlf.catalog.accessKeyId"},
            required = false,
            description = "The access key of OSS.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"oss.secret_key", "s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY",
            "dlf.secret_key", "dlf.catalog.secret_key"},
            required = false,
            description = "The secret key of OSS.")
    protected String secretKey = "";

    @Getter
    @Setter
    @ConnectorProperty(names = {"oss.region", "s3.region", "AWS_REGION", "region", "REGION", "dlf.region"},
            required = false,
            description = "The region of OSS.")
    protected String region;

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
     */
    private static final Set<Pattern> ENDPOINT_PATTERN = ImmutableSet.of(Pattern
                    .compile("^(?:https?://)?(?:s3\\.)?oss-([a-z0-9-]+?)(?:-internal)?\\.aliyuncs\\.com$"),
            Pattern.compile("(?:https?://)?([a-z]{2}-[a-z0-9-]+)\\.oss-dls\\.aliyuncs\\.com"),
            Pattern.compile("^(?:https?://)?dlf(?:-vpc)?\\.([a-z0-9-]+)\\.aliyuncs\\.com(?:/.*)?$"));

    protected OSSProperties(Map<String, String> origProps) {
        super(Type.OSS, origProps);
    }

    protected static boolean guessIsMe(Map<String, String> origProps) {
        String value = Stream.of("oss.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT",
                        "dlf.endpoint", "dlf.catalog.endpoint")
                .map(origProps::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
        if (!Strings.isNullOrEmpty(value)) {
            return (value.contains("aliyuncs.com"));
        }
        Optional<String> uriValue = origProps.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase("uri"))
                .map(Map.Entry::getValue)
                .findFirst();
        return uriValue.filter(OSSProperties::isKnownObjectStorage).isPresent();
    }

    private static boolean isKnownObjectStorage(String value) {
        if (value == null) {
            return false;
        }
        if (!value.contains("aliyuncs.com")) {
            return false;
        }
        boolean isAliyunOss = (value.contains("oss-") || value.contains("dlf."));
        boolean isAmazonS3 = value.contains("s3.");
        boolean isDls = value.contains("dls");
        return isAliyunOss || isAmazonS3 || isDls;
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        if (endpoint.contains("dlf") || endpoint.contains("oss-dls")) {
            String publicAccess = origProps.getOrDefault("dlf.catalog.accessPublic", "false");
            this.endpoint = getOssEndpoint(region, Boolean.parseBoolean(publicAccess));
        }
        // Check if credentials are provided properly - either both or neither
        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
            return;
        }
        // Allow anonymous access if both access_key and secret_key are empty
        if (StringUtils.isBlank(accessKey) && StringUtils.isBlank(secretKey)) {
            return;
        }
        // If only one is provided, it's an error
        throw new StoragePropertiesException(
                "Please set access_key and secret_key or omit both for anonymous access to public bucket.");

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
}
