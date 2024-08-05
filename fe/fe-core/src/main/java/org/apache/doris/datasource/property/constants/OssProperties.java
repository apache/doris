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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class OssProperties extends BaseProperties {

    public static final String OSS_PREFIX = "oss.";
    public static final String OSS_REGION_PREFIX = "oss-";
    public static final String OSS_FS_PREFIX = "fs.oss";

    public static final String ENDPOINT = "oss.endpoint";
    public static final String REGION = "oss.region";
    public static final String ACCESS_KEY = "oss.access_key";
    public static final String SECRET_KEY = "oss.secret_key";
    public static final String SESSION_TOKEN = "oss.session_token";
    public static final String OSS_HDFS_ENABLED = "oss.hdfs.enabled";
    public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT, ACCESS_KEY, SECRET_KEY);

    public static CloudCredential getCredential(Map<String, String> props) {
        return getCloudCredential(props, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN);
    }
}
