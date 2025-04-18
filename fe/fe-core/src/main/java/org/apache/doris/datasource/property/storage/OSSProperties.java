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

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.ConnectorProperty;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class OSSProperties extends AbstractObjectStorageProperties {

    @Setter
    @Getter
    @ConnectorProperty(names = {"oss.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            required = false,
            description = "The endpoint of OSS.")
    protected String endpoint = "";

    @Getter
    @ConnectorProperty(names = {"oss.access_key", "s3.access_key", "AWS_ACCESS_KEY", "ACCESS_KEY", "access_key"},
            description = "The access key of OSS.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"oss.secret_key", "s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY"},
            description = "The secret key of OSS.")
    protected String secretKey = "";

    @Getter
    @ConnectorProperty(names = {"oss.region", "s3.region", "AWS_REGION", "region", "REGION"}, required = false,
            description = "The region of OSS.")
    protected String region;

    private static Pattern ENDPOINT_PATTERN = Pattern.compile("^oss-[a-z0-9-]+\\.aliyuncs\\.com(\\.internal)?$");

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
            return value.contains("aliyuncs.com");
        }
        if (!origProps.containsKey("uri")) {
            return false;
        }
        return origProps.get("uri").contains("aliyuncs.com");
    }

    @Override
    protected void initNormalizeAndCheckProps() throws UserException {
        super.initNormalizeAndCheckProps();
        initRegionIfNecessary();
    }

    @Override
    protected Pattern endpointPattern() {
        return ENDPOINT_PATTERN;
    }

    /**
     * Initializes the region field based on the endpoint if it's not already set.
     * <p>
     * This method attempts to extract the region name from the OSS endpoint string.
     * It supports both external and internal Alibaba Cloud OSS endpoint formats.
     * <p>
     * Examples:
     * - External endpoint: "oss-cn-hangzhou.aliyuncs.com" → region = "cn-hangzhou"
     * - Internal endpoint: "oss-cn-shanghai.intranet.aliyuncs.com" → region = "cn-shanghai"
     */
    public void initRegionIfNecessary() {
        // Return the region if it is already set
        if (!Strings.isNullOrEmpty(this.region)) {
            return;
        }
        // Check for external endpoint and extract region
        if (endpoint.contains("aliyuncs.com")) {
            // Regex pattern for external endpoint (e.g., oss-<region>.aliyuncs.com)
            Pattern ossPattern = Pattern.compile("oss-([a-z0-9-]+)\\.aliyuncs\\.com");
            Matcher matcher = ossPattern.matcher(endpoint);
            if (matcher.find()) {
                this.region = matcher.group(1);
                return;
            }
        }
        // Check for internal endpoint and extract region
        if (endpoint.contains("intranet.aliyuncs.com")) {
            // Regex pattern for internal endpoint (e.g., oss-<region>.intranet.aliyuncs.com)
            Pattern ossIntranetPattern = Pattern.compile("oss-([a-z0-9-]+)\\.intranet\\.aliyuncs\\.com");
            Matcher matcher = ossIntranetPattern.matcher(endpoint);
            if (matcher.find()) {
                this.region = matcher.group(1);
            }
        }
    }

}
