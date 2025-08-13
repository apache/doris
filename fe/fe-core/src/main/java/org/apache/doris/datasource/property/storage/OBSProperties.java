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

public class OBSProperties extends AbstractS3CompatibleProperties {

    @Setter
    @Getter
    @ConnectorProperty(names = {"obs.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            required = false,
            description = "The endpoint of OBS.")
    protected String endpoint = "";

    @Getter
    @ConnectorProperty(names = {"obs.access_key", "s3.access_key", "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY"},
            required = false,
            description = "The access key of OBS.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"obs.secret_key", "s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY"},
            required = false,
            description = "The secret key of OBS.")
    protected String secretKey = "";

    @Getter
    @Setter
    @ConnectorProperty(names = {"obs.region", "s3.region", "AWS_REGION", "region", "REGION"}, required = false,
            description = "The region of OBS.")
    protected String region;

    /**
     * Pattern to extract the region from a Huawei Cloud OBS endpoint.
     * <p>
     * Supported formats:
     * - obs-cn-hangzhou.myhuaweicloud.com          => region = cn-hangzhou
     * - https://obs-cn-shanghai.myhuaweicloud.com  => region = cn-shanghai
     * <p>
     * Group(1) captures the region name (e.g., cn-hangzhou).
     * FYI: https://console-intl.huaweicloud.com/apiexplorer/#/endpoint/OBS
     */
    private static final Set<Pattern> ENDPOINT_PATTERN = ImmutableSet.of(Pattern
            .compile("^(?:https?://)?obs\\.([a-z0-9-]+)\\.myhuaweicloud\\.com$"));


    public OBSProperties(Map<String, String> origProps) {
        super(Type.OBS, origProps);
        // Initialize fields from origProps
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
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

    protected static boolean guessIsMe(Map<String, String> origProps) {
        String value = Stream.of("obs.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT")
                .map(origProps::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

        if (!Strings.isNullOrEmpty(value)) {
            return value.contains("myhuaweicloud.com");
        }
        Optional<String> uriValue = origProps.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase("uri"))
                .map(Map.Entry::getValue)
                .findFirst();
        return uriValue.isPresent() && uriValue.get().contains("myhuaweicloud.com");
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
    public void initializeHadoopStorageConfig() {
        super.initializeHadoopStorageConfig();
        hadoopStorageConfig.set("fs.obs.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopStorageConfig.set("fs.obs.access.key", accessKey);
        hadoopStorageConfig.set("fs.obs.secret.key", secretKey);
        hadoopStorageConfig.set("fs.obs.endpoint", endpoint);
    }

    protected void setEndpointIfPossible() {
        super.setEndpointIfPossible();
        if (StringUtils.isBlank(getEndpoint())) {
            throw new IllegalArgumentException("Property obs.endpoint is required.");
        }
    }
}
