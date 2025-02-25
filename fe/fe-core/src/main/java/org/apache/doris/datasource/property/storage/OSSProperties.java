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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OSSProperties extends AbstractObjectStorageProperties {
    @ConnectorProperty(names = {"oss.endpoint"}, required = false, description = "The endpoint of OSS.")
    protected String endpoint = "oss-cn-hangzhou.aliyuncs.com";

    @ConnectorProperty(names = {"oss.access_key"}, description = "The access key of OSS.")
    protected String accessKey = "";

    @ConnectorProperty(names = {"oss.secret_key"}, description = "The secret key of OSS.")
    protected String secretKey = "";

    protected String region;


    protected OSSProperties(Map<String, String> origProps) {
        super(Type.OSS, origProps);
    }

    @Override
    public void toHadoopConfiguration(Map<String, String> config) {
        config.put("fs.oss.endpoint", endpoint);
        config.put("fs.oss.accessKeyId", accessKey);
        config.put("fs.oss.accessKeySecret", secretKey);
        config.put("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
    }

    @Override
    public void toNativeS3Configuration(Map<String, String> config) {
        config.putAll(generateAWSS3Properties(endpoint, getRegion(), accessKey, secretKey));
    }

    private String getRegion() {
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


}
