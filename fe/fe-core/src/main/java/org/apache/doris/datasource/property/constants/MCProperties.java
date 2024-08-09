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

/**
 * properties for aliyun max compute
 */
public class MCProperties extends BaseProperties {
    public static final String REGION = "mc.region";
    public static final String PROJECT = "mc.default.project";
    public static final String ACCESS_KEY = "mc.access_key";
    public static final String SECRET_KEY = "mc.secret_key";
    public static final String SESSION_TOKEN = "mc.session_token";
    public static final String PUBLIC_ACCESS = "mc.public_access";
    public static final String ODPS_ENDPOINT = "mc.odps_endpoint";
    public static final String TUNNEL_SDK_ENDPOINT = "mc.tunnel_endpoint";

    public static CloudCredential getCredential(Map<String, String> props) {
        return getCloudCredential(props, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN);
    }
}
