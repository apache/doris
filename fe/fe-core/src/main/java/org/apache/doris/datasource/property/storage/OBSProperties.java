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

public class OBSProperties extends AbstractObjectStorageProperties {

    @ConnectorProperty(names = {"obs.endpoint"}, required = false, description = "The endpoint of OBS.")
    protected String obsEndpoint = "obs.cn-east-3.myhuaweicloud.com";

    @ConnectorProperty(names = {"obs.access_key"}, description = "The access key of OBS.")
    protected String obsAccessKey = "";

    @ConnectorProperty(names = {"obs.secret_key"}, description = "The secret key of OBS.")
    protected String obsSecretKey = "";


    private String region;

    public OBSProperties(Map<String, String> origProps) {
        super(Type.OBS, origProps);
        // Initialize fields from origProps
    }

    @Override
    public void toHadoopConfiguration(Map<String, String> config) {
        config.put("fs.obs.endpoint", obsEndpoint);
        config.put("fs.obs.access.key", obsAccessKey);
        config.put("fs.obs.secret.key", obsSecretKey);
        config.put("fs.obs.impl", "org.apache.hadoop.fs.obs.OBSFileSystem");
        //set other k v if nessesary
    }

    @Override
    public void toNativeS3Configuration(Map<String, String> config) {
        config.putAll(generateAWSS3Properties(obsEndpoint, getRegion(), obsAccessKey, obsSecretKey));
    }

    private String getRegion() {
        if (Strings.isNullOrEmpty(this.region) && obsEndpoint.contains("myhuaweicloud.com")) {
            Pattern obsPattern = Pattern.compile("obs\\.([a-z0-9-]+)\\.myhuaweicloud\\.com");
            Matcher matcher = obsPattern.matcher(obsEndpoint);
            if (matcher.find()) {
                this.region = matcher.group(1);
            }
        }
        return this.region;
    }
}
