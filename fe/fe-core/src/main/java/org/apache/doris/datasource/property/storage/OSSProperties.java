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
import lombok.Setter;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OSSProperties extends AbstractObjectStorageProperties {

    @Setter
    @ConnectorProperty(names = {"oss.endpoint", "endpoint", "s3.endpoint"}, required = false,
            description = "The endpoint of OSS.")
    protected String endpoint = "";

    @ConnectorProperty(names = {"oss.access_key"}, description = "The access key of OSS.")
    protected String accessKey = "";

    @ConnectorProperty(names = {"oss.secret_key"}, description = "The secret key of OSS.")
    protected String secretKey = "";

    @ConnectorProperty(names = {"oss.region", "region", "s3.region"}, required = false,
            description = "The region of OSS.")
    protected String region;


    protected OSSProperties(Map<String, String> origProps) {
        super(Type.OSS, origProps);
    }

    protected static boolean guessIsMe(Map<String, String> origProps) {
        return origProps.containsKey("oss.access_key");
    }

    @Override
    public void toNativeS3Configuration(Map<String, String> config) {
        config.putAll(generateAWSS3Properties(endpoint, getRegion(), accessKey, secretKey));
    }

    public String getRegion() {
        // Return the region if it is already set
        if (!Strings.isNullOrEmpty(this.region)) {
            return region;
        }

        // Check for external endpoint and extract region
        if (endpoint.contains("aliyuncs.com")) {
            // Regex pattern for external endpoint (e.g., oss-<region>.aliyuncs.com)
            Pattern ossPattern = Pattern.compile("oss-([a-z0-9-]+)\\.aliyuncs\\.com");
            Matcher matcher = ossPattern.matcher(endpoint);
            if (matcher.find()) {
                this.region = matcher.group(1);
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

        return this.region;
    }

    @Override
    public String getEndpoint() {
        return endpoint;
    }

    @Override
    public String getAccessKey() {
        return accessKey;
    }

    @Override
    public String getSecretKey() {
        return secretKey;
    }
}
