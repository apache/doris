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

public class COSProperties extends AbstractObjectStorageProperties {

    @ConnectorProperty(names = {"cos.endpoint"},
            required = false,
            description = "The endpoint of COS.")
    protected String cosEndpoint = "cos.ap-guangzhou.myqcloud.com";

    @ConnectorProperty(names = {"cos.region"},
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

    @Override
    public void toHadoopConfiguration(Map<String, String> config) {
        config.put("fs.cosn.bucket.region", getRegion());
        config.put("fs.cos.endpoint", cosEndpoint);
        config.put("fs.cosn.userinfo.secretId", cosAccessKey);
        config.put("fs.cosn.userinfo.secretKey", cosSecretKey);
        config.put("fs.cosn.impl", "org.apache.hadoop.fs.CosFileSystem");
    }

    @Override
    public void toNativeS3Configuration(Map<String, String> config) {
        config.putAll(generateAWSS3Properties(cosEndpoint, getRegion(), cosAccessKey, cosSecretKey));
    }

    private String getRegion() {
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
}
