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

public class COSProperties extends AbstractObjectStorageProperties {

    @Setter
    @Getter
    @ConnectorProperty(names = {"cos.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            required = false,
            description = "The endpoint of COS.")
    protected String endpoint = "";

    @Getter
    @ConnectorProperty(names = {"cos.region", "s3.region", "AWS_REGION", "region", "REGION"},
            required = false,
            description = "The region of COS.")
    protected String region = "";

    @Getter
    @ConnectorProperty(names = {"cos.access_key", "AWS_ACCESS_KEY", "ACCESS_KEY", "access_key"},
            description = "The access key of COS.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"cos.secret_key", "s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY"},
            description = "The secret key of COS.")
    protected String secretKey = "";

    private static final Pattern COS_ENDPOINT_PATTERN = Pattern
            .compile("^cos\\.[a-z0-9-]+\\.myqcloud\\.com(\\.internal)?$");

    protected COSProperties(Map<String, String> origProps) {
        super(Type.COS, origProps);
    }

    protected static boolean guessIsMe(Map<String, String> origProps) {
        String value = Stream.of("cos.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT")
                .map(origProps::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
        if (!Strings.isNullOrEmpty(value)) {
            return value.contains("myqcloud.com");
        }
        if (!origProps.containsKey("uri")) {
            return false;
        }
        return origProps.get("uri").contains("myqcloud.com");
    }

    @Override
    protected Pattern endpointPattern() {
        return COS_ENDPOINT_PATTERN;
    }

    /**
     * Initializes the cosRegion field based on the COS endpoint if it's not already set.
     * <p>
     * This method extracts the region from Tencent Cloud COS endpoints.
     * It supports typical COS endpoint formats like:
     * <p>
     * Example:
     * - "cos.ap-guangzhou.myqcloud.com" → cosRegion = "ap-guangzhou"
     */
    @Override
    protected void initRegionIfNecessary() {
        if (Strings.isNullOrEmpty(this.region)) {
            Pattern cosPattern = Pattern.compile("cos\\.([a-z0-9-]+)\\.myqcloud\\.com");
            Matcher matcher = cosPattern.matcher(endpoint);
            if (matcher.find()) {
                this.region = matcher.group(1);
            }
        }
    }

}
