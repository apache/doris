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

public class OBSProperties extends AbstractS3CompatibleProperties {

    @Setter
    @Getter
    @ConnectorProperty(names = {"obs.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            required = false,
            description = "The endpoint of OBS.")
    protected String endpoint = "";

    @Getter
    @ConnectorProperty(names = {"obs.access_key", "s3.access_key", "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY"},
            description = "The access key of OBS.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"obs.secret_key", "s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY"},
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
    public void initializeHadoopStorageConfig() {
        hadoopStorageConfig = new Configuration();
        hadoopStorageConfig = new Configuration();
        hadoopStorageConfig.set("fs.obs.impl", "com.obs.services.hadoop.fs.OBSFileSystem");
        hadoopStorageConfig.set("fs.obs.access.key", accessKey);
        hadoopStorageConfig.set("fs.obs.secret.key", secretKey);
        hadoopStorageConfig.set("fs.obs.endpoint", endpoint);
        hadoopStorageConfig.set("fs.obs.connection.timeout", connectionTimeoutS);
        hadoopStorageConfig.set("fs.obs.request.timeout", requestTimeoutS);
        hadoopStorageConfig.set("fs.obs.connection.max", maxConnections);
    }
}
