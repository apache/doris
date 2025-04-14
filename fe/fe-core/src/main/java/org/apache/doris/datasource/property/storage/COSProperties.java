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

public class COSProperties extends AbstractObjectStorageProperties {

    @Setter
    @ConnectorProperty(names = {"cos.endpoint", "endpoint", "s3.endpoint"},
            required = false,
            description = "The endpoint of COS.")
    protected String cosEndpoint = "";

    @ConnectorProperty(names = {"cos.region", "region", "s3.region"},
            required = false,
            description = "The region of COS.")
    protected String cosRegion = "";

    @ConnectorProperty(names = {"cos.access_key"},
            description = "The access key of S3.")
    protected String cosAccessKey = "";

    @ConnectorProperty(names = {"cos.secret_key"},
            description = "The secret key of S3.")
    protected String cosSecretKey = "";


    protected COSProperties(Map<String, String> origProps) {
        super(Type.COS, origProps);
    }

    protected static boolean guessIsMe(Map<String, String> origProps) {
        return origProps.containsKey("cos.access_key");
    }


    @Override
    public void toNativeS3Configuration(Map<String, String> config) {
        config.putAll(generateAWSS3Properties(cosEndpoint, getRegion(), cosAccessKey, cosSecretKey));
    }

    public String getRegion() {
        if (Strings.isNullOrEmpty(this.cosRegion)) {
            if (cosEndpoint.contains("myqcloud.com")) {
                Pattern cosPattern = Pattern.compile("cos\\.([a-z0-9-]+)\\.myqcloud\\.com");
                Matcher matcher = cosPattern.matcher(cosEndpoint);
                if (matcher.find()) {
                    this.cosRegion = matcher.group(1);
                }
            }
        }
        return this.cosRegion;
    }

    @Override
    public String getEndpoint() {
        return cosEndpoint;
    }

    @Override
    public String getAccessKey() {
        return cosAccessKey;
    }

    @Override
    public String getSecretKey() {
        return cosSecretKey;
    }

    @Override
    public void setEndpoint(String endpoint) {
        this.cosEndpoint = endpoint;
    }
}
