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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class OSSProperties extends AbstractS3CompatibleProperties {

    @Setter
    @Getter
    @ConnectorProperty(names = {"oss.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            required = false,
            description = "The endpoint of OSS.")
    protected String endpoint = "";

    @Getter
    @ConnectorProperty(names = {"oss.access_key", "s3.access_key", "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY"},
            description = "The access key of OSS.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"oss.secret_key", "s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY"},
            description = "The secret key of OSS.")
    protected String secretKey = "";

    @Getter
    @Setter
    @ConnectorProperty(names = {"oss.region", "s3.region", "AWS_REGION", "region", "REGION"}, required = false,
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
            .compile("^(?:https?://)?(?:s3\\.)?oss-([a-z0-9-]+?)(?:-internal)?\\.aliyuncs\\.com$"));

    protected OSSProperties(Map<String, String> origProps) {
        super(Type.OSS, origProps);
    }

    protected static boolean guessIsMe(Map<String, String> origProps) {
        String value = Stream.of("oss.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT")
                .map(origProps::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
        if (!Strings.isNullOrEmpty(value)) {
            return (value.contains("aliyuncs.com") && !value.contains("oss-dls.aliyuncs.com"));
        }
        Optional<String> uriValue = origProps.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase("uri"))
                .map(Map.Entry::getValue)
                .findFirst();
        if (!uriValue.isPresent()) {
            return false;
        }
        String uri = uriValue.get();
        return uri.contains("aliyuncs.com") && (!uri.contains("oss-dls.aliyuncs.com"));
    }

    @Override
    protected Set<Pattern> endpointPatterns() {
        return ENDPOINT_PATTERN;
    }

    @Override
    public void initializeHadoopStorageConfig() {
        hadoopStorageConfig = new Configuration();
        hadoopStorageConfig = new Configuration();
        hadoopStorageConfig.set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
        hadoopStorageConfig.set("fs.oss.endpoint", endpoint);
        hadoopStorageConfig.set("fs.oss.region", region);
        hadoopStorageConfig.set("fs.oss.accessKeyId", accessKey);
        hadoopStorageConfig.set("fs.oss.accessKeySecret", secretKey);
        hadoopStorageConfig.set("fs.oss.connection.timeout", connectionTimeoutS);
        hadoopStorageConfig.set("fs.oss.connection.max", maxConnections);
        hadoopStorageConfig.set("fs.oss.connection.request.timeout", requestTimeoutS);
        hadoopStorageConfig.set("fs.oss.use.path.style.access", usePathStyle);
    }
}
