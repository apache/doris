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
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class OBSProperties extends AbstractObjectStorageProperties {

    @Setter
    @Getter
    @ConnectorProperty(names = {"obs.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            required = false,
            description = "The endpoint of OBS.")
    protected String endpoint = "";

    @Getter
    @ConnectorProperty(names = {"obs.access_key", "AWS_ACCESS_KEY", "ACCESS_KEY", "access_key"},
            description = "The access key of OBS.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"obs.secret_key", "secret_key", "s3.secret_key"},
            description = "The secret key of OBS.")
    protected String secretKey = "";

    @Getter
    @ConnectorProperty(names = {"obs.region", "s3.region", "AWS_REGION", "region", "REGION"}, required = false,
            description = "The region of OBS.")
    protected String region;

    private static Pattern ENDPOINT_PATTERN = Pattern
            .compile("^obs\\.[a-z0-9-]+\\.myhuaweicloud\\.com(\\.internal)?$");

    public OBSProperties(Map<String, String> origProps) {
        super(Type.OBS, origProps);
        // Initialize fields from origProps
    }

    protected static boolean guessIsMe(Map<String, String> origProps) {
        String value = Stream.of("obs.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT", "uri")
                .map(origProps::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

        if (!Strings.isNullOrEmpty(value)) {
            return value.contains("myhuaweicloud.com");
        }
        if (!origProps.containsKey("uri")) {
            return false;
        }
        // Check if the uri property contains "myhuaweicloud.com"
        return origProps.get("uri").contains("myhuaweicloud.com");
    }

    @Override
    protected Pattern endpointPattern() {
        return ENDPOINT_PATTERN;
    }

    /**
     * Initializes the region field based on the OBS endpoint if it's not already set.
     * <p>
     * This method extracts the region from Huawei Cloud OBS endpoints.
     * It supports typical OBS endpoint formats like:
     * <p>
     * Example:
     * - "obs.cn-north-4.myhuaweicloud.com" → region = "cn-north-4"
     */
    @Override
    protected void initRegionIfNecessary() {
        if (Strings.isNullOrEmpty(this.region)) {
            Pattern obsPattern = Pattern.compile("obs\\.([a-z0-9-]+)\\.myhuaweicloud\\.com");
            Matcher matcher = obsPattern.matcher(endpoint);
            if (matcher.find()) {
                this.region = matcher.group(1);
            }
        }
    }

}
