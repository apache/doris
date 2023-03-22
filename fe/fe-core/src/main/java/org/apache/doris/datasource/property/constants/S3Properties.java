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

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.credentials.CloudCredential;
import org.apache.doris.datasource.credentials.CloudCredentialWithEndpoint;
import org.apache.doris.datasource.credentials.DataLakeAWSCredentialsProvider;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class S3Properties extends BaseProperties {

    public static final String S3_PREFIX = "s3";
    public static final String S3_FS_PREFIX = "fs.s3";

    public static final String ENDPOINT = "s3.endpoint";
    public static final String REGION = "s3.region";
    public static final String ACCESS_KEY = "s3.access_key";
    public static final String SECRET_KEY = "s3.secret_key";
    public static final String SESSION_TOKEN = "s3.session_token";
    public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT, ACCESS_KEY, SECRET_KEY);

    public static final String MAX_CONNECTIONS = "s3.connection.maximum";
    public static final String REQUEST_TIMEOUT_MS = "s3.connection.request.timeout";
    public static final String CONNECTION_TIMEOUT_MS = "s3.connection.timeout";

    // required by storage policy
    public static final String ROOT_PATH = "s3.root.path";
    public static final String BUCKET = "s3.bucket";
    public static final String VALIDITY_CHECK = "s3_validity_check";

    public static final List<String> AWS_CREDENTIALS_PROVIDERS = Arrays.asList(
            DataLakeAWSCredentialsProvider.class.getName(),
            TemporaryAWSCredentialsProvider.class.getName(),
            SimpleAWSCredentialsProvider.class.getName(),
            EnvironmentVariableCredentialsProvider.class.getName(),
            IAMInstanceCredentialsProvider.class.getName());

    public static class Environment {
        public static final String PROPERTIES_PREFIX = "AWS";
        // required
        public static final String ENDPOINT = "AWS_ENDPOINT";
        public static final String REGION = "AWS_REGION";
        public static final String ACCESS_KEY = "AWS_ACCESS_KEY";
        public static final String SECRET_KEY = "AWS_SECRET_KEY";
        public static final String TOKEN = "AWS_TOKEN";
        public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT, REGION, ACCESS_KEY, SECRET_KEY);

        // required by storage policy
        public static final String ROOT_PATH = "AWS_ROOT_PATH";
        public static final String BUCKET = "AWS_BUCKET";
        // optional
        public static final String MAX_CONNECTIONS = "AWS_MAX_CONNECTIONS";
        public static final String REQUEST_TIMEOUT_MS = "AWS_REQUEST_TIMEOUT_MS";
        public static final String CONNECTION_TIMEOUT_MS = "AWS_CONNECTION_TIMEOUT_MS";
        public static final String DEFAULT_MAX_CONNECTIONS = "50";
        public static final String DEFAULT_REQUEST_TIMEOUT_MS = "3000";
        public static final String DEFAULT_CONNECTION_TIMEOUT_MS = "1000";
    }

    public static CloudCredential getCredential(Map<String, String> props) {
        return getCloudCredential(props, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN);
    }

    public static CloudCredentialWithEndpoint getEnvironmentCredentialWithEndpoint(Map<String, String> props) {
        CloudCredential credential = getCloudCredential(props, Environment.ACCESS_KEY, Environment.SECRET_KEY,
                Environment.TOKEN);
        if (!props.containsKey(S3Properties.Environment.ENDPOINT)) {
            throw new IllegalArgumentException("Missing 'endpoint' property. ");
        }
        String endpoint = props.get(S3Properties.Environment.ENDPOINT);
        String[] endpointSplit = endpoint.split("\\.");
        if (endpointSplit.length < 2) {
            throw new IllegalArgumentException("Need endpoint with region: " + endpoint);
        }
        String region = props.getOrDefault(S3Properties.REGION, endpointSplit[1]);
        return new CloudCredentialWithEndpoint(endpoint, region, credential);
    }

    public static void requiredS3Properties(Map<String, String> properties) throws UserException {
        if (properties.containsKey(S3Properties.Environment.ENDPOINT)) {
            // compatible with older versions
            for (String field : S3Properties.Environment.REQUIRED_FIELDS) {
                if (!properties.containsKey(field)) {
                    throw new UserException(field + " not found.");
                }
            }
        } else {
            for (String field : S3Properties.REQUIRED_FIELDS) {
                if (!properties.containsKey(field)) {
                    throw new UserException(field + " not found.");
                }
            }
        }
    }
}
