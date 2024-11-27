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

package org.apache.doris.datasource.property.constants;

import org.apache.doris.common.credentials.CloudCredential;

import java.util.Map;

public class BaseProperties {
    public static CloudCredential getCloudCredential(Map<String, String> props,
                                                     String accessKeyName,
                                                     String secretKeyName,
                                                     String sessionTokenName) {
        CloudCredential credential = new CloudCredential();
        credential.setAccessKey(props.getOrDefault(accessKeyName, ""));
        credential.setSecretKey(props.getOrDefault(secretKeyName, ""));
        credential.setSessionToken(props.getOrDefault(sessionTokenName, ""));
        return credential;
    }

    public static CloudCredential getCompatibleCredential(Map<String, String> props) {
        // Compatible with older versions.
        return getCloudCredential(props, S3Properties.Env.ACCESS_KEY, S3Properties.Env.SECRET_KEY,
                S3Properties.Env.TOKEN);
    }
}
