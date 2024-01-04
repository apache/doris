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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.credentials.CloudCredential;
import org.apache.doris.datasource.credentials.CloudCredentialWithEndpoint;
import org.apache.doris.datasource.credentials.DataLakeAWSCredentialsProvider;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.thrift.TS3StorageParam;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3Properties extends BaseProperties {

    public static final String S3_PREFIX = "s3.";
    public static final String S3_FS_PREFIX = "fs.s3";

    public static final String CREDENTIALS_PROVIDER = "s3.credentials.provider";
    public static final String ENDPOINT = "s3.endpoint";
    public static final String REGION = "s3.region";
    public static final String ACCESS_KEY = "s3.access_key";
    public static final String SECRET_KEY = "s3.secret_key";
    public static final String SESSION_TOKEN = "s3.session_token";
    public static final String MAX_CONNECTIONS = "s3.connection.maximum";
    public static final String REQUEST_TIMEOUT_MS = "s3.connection.request.timeout";
    public static final String CONNECTION_TIMEOUT_MS = "s3.connection.timeout";

    // required by storage policy
    public static final String ROOT_PATH = "s3.root.path";
    public static final String BUCKET = "s3.bucket";
    public static final String VALIDITY_CHECK = "s3_validity_check";
    public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT, ACCESS_KEY, SECRET_KEY);
    public static final List<String> TVF_REQUIRED_FIELDS = Arrays.asList(ACCESS_KEY, SECRET_KEY);
    public static final List<String> FS_KEYS = Arrays.asList(ENDPOINT, REGION, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN,
            ROOT_PATH, BUCKET, MAX_CONNECTIONS, REQUEST_TIMEOUT_MS, CONNECTION_TIMEOUT_MS);

    public static final List<String> AWS_CREDENTIALS_PROVIDERS = Arrays.asList(
            DataLakeAWSCredentialsProvider.class.getName(),
            TemporaryAWSCredentialsProvider.class.getName(),
            SimpleAWSCredentialsProvider.class.getName(),
            EnvironmentVariableCredentialsProvider.class.getName(),
            IAMInstanceCredentialsProvider.class.getName());

    public static Map<String, String> credentialToMap(CloudCredentialWithEndpoint credential) {
        Map<String, String> resMap = new HashMap<>();
        resMap.put(S3Properties.ENDPOINT, credential.getEndpoint());
        resMap.put(S3Properties.REGION, credential.getRegion());
        if (credential.isWhole()) {
            resMap.put(S3Properties.ACCESS_KEY, credential.getAccessKey());
            resMap.put(S3Properties.SECRET_KEY, credential.getSecretKey());
        }
        if (credential.isTemporary()) {
            resMap.put(S3Properties.SESSION_TOKEN, credential.getSessionToken());
        }
        return resMap;
    }

    public static class Env {
        public static final String PROPERTIES_PREFIX = "AWS";
        // required
        public static final String ENDPOINT = "AWS_ENDPOINT";
        public static final String REGION = "AWS_REGION";
        public static final String ACCESS_KEY = "AWS_ACCESS_KEY";
        public static final String SECRET_KEY = "AWS_SECRET_KEY";
        public static final String TOKEN = "AWS_TOKEN";
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
        public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT, ACCESS_KEY, SECRET_KEY);
        public static final List<String> FS_KEYS = Arrays.asList(ENDPOINT, REGION, ACCESS_KEY, SECRET_KEY, TOKEN,
                ROOT_PATH, BUCKET, MAX_CONNECTIONS, REQUEST_TIMEOUT_MS, CONNECTION_TIMEOUT_MS);
    }

    public static CloudCredential getCredential(Map<String, String> props) {
        return getCloudCredential(props, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN);
    }

    public static CloudCredentialWithEndpoint getEnvironmentCredentialWithEndpoint(Map<String, String> props) {
        CloudCredential credential = getCloudCredential(props, Env.ACCESS_KEY, Env.SECRET_KEY,
                Env.TOKEN);
        if (!props.containsKey(Env.ENDPOINT)) {
            throw new IllegalArgumentException("Missing 'AWS_ENDPOINT' property. ");
        }
        String endpoint = props.get(Env.ENDPOINT);
        String region = props.getOrDefault(Env.REGION, S3Properties.getRegionOfEndpoint(endpoint));
        return new CloudCredentialWithEndpoint(endpoint, region, credential);
    }

    public static String getRegionOfEndpoint(String endpoint) {
        String[] endpointSplit = endpoint.split("\\.");
        if (endpointSplit.length < 2) {
            return null;
        }
        if (endpointSplit[0].contains("oss-")) {
            // compatible with the endpoint: oss-cn-bejing.aliyuncs.com
            return endpointSplit[0];
        }
        return endpointSplit[1];
    }

    public static Map<String, String> prefixToS3(Map<String, String> properties) {
        Map<String, String> s3Properties = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(OssProperties.OSS_PREFIX)) {
                String s3Key = entry.getKey().replace(OssProperties.OSS_PREFIX, S3Properties.S3_PREFIX);
                s3Properties.put(s3Key, entry.getValue());
            } else if (entry.getKey().startsWith(GCSProperties.GCS_PREFIX)) {
                String s3Key = entry.getKey().replace(GCSProperties.GCS_PREFIX, S3Properties.S3_PREFIX);
                s3Properties.put(s3Key, entry.getValue());
            }  else if (entry.getKey().startsWith(CosProperties.COS_PREFIX)) {
                String s3Key = entry.getKey().replace(CosProperties.COS_PREFIX, S3Properties.S3_PREFIX);
                s3Properties.put(s3Key, entry.getValue());
            } else if (entry.getKey().startsWith(ObsProperties.OBS_PREFIX)) {
                String s3Key = entry.getKey().replace(ObsProperties.OBS_PREFIX, S3Properties.S3_PREFIX);
                s3Properties.put(s3Key, entry.getValue());
            } else if (entry.getKey().startsWith(MinioProperties.MINIO_PREFIX)) {
                String s3Key = entry.getKey().replace(MinioProperties.MINIO_PREFIX, S3Properties.S3_PREFIX);
                s3Properties.put(s3Key, entry.getValue());
            } else {
                s3Properties.put(entry.getKey(), entry.getValue());
            }
        }
        return s3Properties;
    }

    public static Map<String, String> requiredS3TVFProperties(Map<String, String> properties)
            throws AnalysisException {
        try {
            for (String field : S3Properties.TVF_REQUIRED_FIELDS) {
                checkRequiredProperty(properties, field);
            }
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        return properties;
    }

    public static void requiredS3Properties(Map<String, String> properties) throws DdlException {
        // Try to convert env properties to uniform properties
        // compatible with old version
        S3Properties.convertToStdProperties(properties);
        if (properties.containsKey(S3Properties.Env.ENDPOINT)
                    && !properties.containsKey(S3Properties.ENDPOINT)) {
            for (String field : S3Properties.Env.REQUIRED_FIELDS) {
                checkRequiredProperty(properties, field);
            }
        } else {
            for (String field : S3Properties.REQUIRED_FIELDS) {
                checkRequiredProperty(properties, field);
            }
        }
    }

    public static void requiredS3PingProperties(Map<String, String> properties) throws DdlException {
        requiredS3Properties(properties);
        checkRequiredProperty(properties, S3Properties.BUCKET);
    }

    public static void checkRequiredProperty(Map<String, String> properties, String propertyKey)
            throws DdlException {
        String value = properties.get(propertyKey);
        if (Strings.isNullOrEmpty(value)) {
            throw new DdlException("Missing [" + propertyKey + "] in properties.");
        }
    }

    public static void optionalS3Property(Map<String, String> properties) {
        properties.putIfAbsent(S3Properties.MAX_CONNECTIONS, S3Properties.Env.DEFAULT_MAX_CONNECTIONS);
        properties.putIfAbsent(S3Properties.REQUEST_TIMEOUT_MS, S3Properties.Env.DEFAULT_REQUEST_TIMEOUT_MS);
        properties.putIfAbsent(S3Properties.CONNECTION_TIMEOUT_MS, S3Properties.Env.DEFAULT_CONNECTION_TIMEOUT_MS);
        // compatible with old version
        properties.putIfAbsent(S3Properties.Env.MAX_CONNECTIONS, S3Properties.Env.DEFAULT_MAX_CONNECTIONS);
        properties.putIfAbsent(S3Properties.Env.REQUEST_TIMEOUT_MS, S3Properties.Env.DEFAULT_REQUEST_TIMEOUT_MS);
        properties.putIfAbsent(S3Properties.Env.CONNECTION_TIMEOUT_MS, S3Properties.Env.DEFAULT_CONNECTION_TIMEOUT_MS);
    }

    public static void convertToStdProperties(Map<String, String> properties) {
        if (properties.containsKey(S3Properties.Env.ENDPOINT)) {
            properties.putIfAbsent(S3Properties.ENDPOINT, properties.get(S3Properties.Env.ENDPOINT));
        }
        if (properties.containsKey(S3Properties.Env.REGION)) {
            properties.putIfAbsent(S3Properties.REGION, properties.get(S3Properties.Env.REGION));
        }
        if (properties.containsKey(S3Properties.Env.ACCESS_KEY)) {
            properties.putIfAbsent(S3Properties.ACCESS_KEY, properties.get(S3Properties.Env.ACCESS_KEY));
        }
        if (properties.containsKey(S3Properties.Env.SECRET_KEY)) {
            properties.putIfAbsent(S3Properties.SECRET_KEY, properties.get(S3Properties.Env.SECRET_KEY));
        }
        if (properties.containsKey(S3Properties.Env.TOKEN)) {
            properties.putIfAbsent(S3Properties.SESSION_TOKEN, properties.get(S3Properties.Env.TOKEN));
        }
        if (properties.containsKey(S3Properties.Env.MAX_CONNECTIONS)) {
            properties.putIfAbsent(S3Properties.MAX_CONNECTIONS, properties.get(S3Properties.Env.MAX_CONNECTIONS));
        }
        if (properties.containsKey(S3Properties.Env.REQUEST_TIMEOUT_MS)) {
            properties.putIfAbsent(S3Properties.REQUEST_TIMEOUT_MS,
                    properties.get(S3Properties.Env.REQUEST_TIMEOUT_MS));

        }
        if (properties.containsKey(S3Properties.Env.CONNECTION_TIMEOUT_MS)) {
            properties.putIfAbsent(S3Properties.CONNECTION_TIMEOUT_MS,
                    properties.get(S3Properties.Env.CONNECTION_TIMEOUT_MS));
        }
        if (properties.containsKey(S3Properties.Env.ROOT_PATH)) {
            properties.putIfAbsent(S3Properties.ROOT_PATH, properties.get(S3Properties.Env.ROOT_PATH));
        }
        if (properties.containsKey(S3Properties.Env.BUCKET)) {
            properties.putIfAbsent(S3Properties.BUCKET, properties.get(S3Properties.Env.BUCKET));
        }
        if (properties.containsKey(PropertyConverter.USE_PATH_STYLE)) {
            properties.putIfAbsent(PropertyConverter.USE_PATH_STYLE, properties.get(PropertyConverter.USE_PATH_STYLE));
        }
    }

    public static TS3StorageParam getS3TStorageParam(Map<String, String> properties) {
        TS3StorageParam s3Info = new TS3StorageParam();
        s3Info.setEndpoint(properties.get(S3Properties.ENDPOINT));
        s3Info.setRegion(properties.get(S3Properties.REGION));
        s3Info.setAk(properties.get(S3Properties.ACCESS_KEY));
        s3Info.setSk(properties.get(S3Properties.SECRET_KEY));

        s3Info.setRootPath(properties.get(S3Properties.ROOT_PATH));
        s3Info.setBucket(properties.get(S3Properties.BUCKET));
        String maxConnections = properties.get(S3Properties.MAX_CONNECTIONS);
        s3Info.setMaxConn(Integer.parseInt(maxConnections == null
                ? S3Properties.Env.DEFAULT_MAX_CONNECTIONS : maxConnections));
        String requestTimeoutMs = properties.get(S3Properties.REQUEST_TIMEOUT_MS);
        s3Info.setMaxConn(Integer.parseInt(requestTimeoutMs == null
                ? S3Properties.Env.DEFAULT_REQUEST_TIMEOUT_MS : requestTimeoutMs));
        String connTimeoutMs = properties.get(S3Properties.CONNECTION_TIMEOUT_MS);
        s3Info.setMaxConn(Integer.parseInt(connTimeoutMs == null
                ? S3Properties.Env.DEFAULT_CONNECTION_TIMEOUT_MS : connTimeoutMs));
        String usePathStyle = properties.getOrDefault(PropertyConverter.USE_PATH_STYLE, "false");
        s3Info.setUsePathStyle(Boolean.parseBoolean(usePathStyle));
        return s3Info;
    }
}
