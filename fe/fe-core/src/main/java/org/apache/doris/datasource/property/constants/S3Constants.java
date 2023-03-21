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

import java.util.Arrays;
import java.util.List;

public class S3Constants {
    public static final String S3_FS_PREFIX = "fs.s3";
    public static final String S3_MAX_CONNECTIONS = "s3.connection.maximum";
    public static final String S3_REQUEST_TIMEOUT_MS = "s3.connection.request.timeout";
    public static final String S3_CONNECTION_TIMEOUT_MS = "s3.connection.timeout";

    public static class Custom {
        private static final String S3_VALIDITY_CHECK = "s3_validity_check";
    }

    public static class Environment {
        public static final String S3_PROPERTIES_PREFIX = "AWS";
        // required
        public static final String S3_ENDPOINT = "AWS_ENDPOINT";
        public static final String S3_REGION = "AWS_REGION";
        public static final String S3_ACCESS_KEY = "AWS_ACCESS_KEY";
        public static final String S3_SECRET_KEY = "AWS_SECRET_KEY";
        public static final List<String> REQUIRED_FIELDS =
                Arrays.asList(S3_ENDPOINT, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY);
        // required by storage policy
        public static final String S3_ROOT_PATH = "AWS_ROOT_PATH";
        public static final String S3_BUCKET = "AWS_BUCKET";
        // optional
        public static final String S3_TOKEN = "AWS_TOKEN";
        public static final String S3_MAX_CONNECTIONS = "AWS_MAX_CONNECTIONS";
        public static final String S3_REQUEST_TIMEOUT_MS = "AWS_REQUEST_TIMEOUT_MS";
        public static final String S3_CONNECTION_TIMEOUT_MS = "AWS_CONNECTION_TIMEOUT_MS";
        public static final String DEFAULT_S3_MAX_CONNECTIONS = "50";
        public static final String DEFAULT_S3_REQUEST_TIMEOUT_MS = "3000";
        public static final String DEFAULT_S3_CONNECTION_TIMEOUT_MS = "1000";
    }
}
