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
    public static final String SESSION_TOKEN = "mc.session_token";
    public static final String PUBLIC_ACCESS = "mc.public_access";
    public static final String DEFAULT_PUBLIC_ACCESS = "false";

    public static final String ACCESS_KEY = "mc.access_key";
    public static final String SECRET_KEY = "mc.secret_key";
    public static final String ENDPOINT = "mc.endpoint";

    public static final String QUOTA = "mc.quota";
    public static final String DEFAULT_QUOTA = "pay-as-you-go";


    public static final String SPLIT_STRATEGY = "mc.split_strategy";
    public static final String SPLIT_BY_BYTE_SIZE_STRATEGY = "byte_size";
    public static final String SPLIT_BY_ROW_COUNT_STRATEGY = "row_count";
    public static final String DEFAULT_SPLIT_STRATEGY = SPLIT_BY_BYTE_SIZE_STRATEGY;


    public static final String SPLIT_BYTE_SIZE = "mc.split_byte_size";
    public static final String DEFAULT_SPLIT_BYTE_SIZE = "268435456"; //256 * 1024L * 1024L = 256MB
    public static final String SPLIT_ROW_COUNT = "mc.split_row_count";
    public static final String DEFAULT_SPLIT_ROW_COUNT = "1048576"; // 256 * 4096

    public static CloudCredential getCredential(Map<String, String> props) {
        return getCloudCredential(props, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN);
    }
}
