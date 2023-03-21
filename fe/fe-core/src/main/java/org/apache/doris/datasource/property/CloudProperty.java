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

package org.apache.doris.datasource.property;

import org.apache.doris.datasource.credentials.DataLakeAWSCredentialsProvider;
import org.apache.doris.datasource.property.constants.GenericConstants;
import org.apache.doris.datasource.property.constants.MetadataConstants;
import org.apache.doris.datasource.property.constants.S3Constants;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.google.common.collect.Maps;
import org.apache.doris.datasource.property.constants.StorageConstants;
import org.apache.hadoop.fs.obs.OBSConstants;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CloudProperty {

    public static final String OBS_FS_PREFIX = "fs.obs";

    public static final List<String> AWS_CREDENTIALS_PROVIDERS = Arrays.asList(
            DataLakeAWSCredentialsProvider.class.getName(),
            TemporaryAWSCredentialsProvider.class.getName(),
            SimpleAWSCredentialsProvider.class.getName(),
            EnvironmentVariableCredentialsProvider.class.getName(),
            IAMInstanceCredentialsProvider.class.getName());

    public static final String USE_PATH_STYLE = "use_path_style";

    public enum PropertyType {
        META,
        STORAGE
    }

    public enum StorageType {
        S3,
        OBS,
        OSS,
        BOS,
        COS
    }

    public enum MetadataType {
        GLUE,
        DLF,
    }

    public static Map<String, String> convert(Map<String, String> props) {
        if (props.containsKey(StorageConstants.STORAGE_ENDPOINT)) {
            String endpoint = props.get(StorageConstants.STORAGE_ENDPOINT);
            CloudCredential credential = parseCredential(PropertyType.STORAGE, props);
            StorageType type = StorageType.valueOf(getServerPrefix(endpoint));
            if (type == StorageType.S3) {
                return convertToS3Properties(props, credential);
            } else if (type == StorageType.OBS) {
                return convertToOBSProperties(props, credential);
            }
        }
        if (props.containsKey(MetadataConstants.META_ENDPOINT)) {
            String endpoint = props.get(MetadataConstants.META_ENDPOINT);
            CloudCredential credential = parseCredential(PropertyType.META, props);
            MetadataType type = MetadataType.valueOf(getServerPrefix(endpoint));
            if (type == MetadataType.GLUE) {
                return convertToGlueProperties(props, credential);
            } else if (type == MetadataType.DLF) {
                return convertToDLFProperties(props, credential);
            }
        }
        return props;
    }

    private static String getServerPrefix(String endpoint) {
        String[] endpointStr = endpoint.split("\\.");
        if (endpointStr.length < 2) {
            throw new IllegalArgumentException("Fail to parse server type for " + endpoint);
        }
        return endpointStr[0].toUpperCase();
    }

    private static CloudCredential parseCredential(PropertyType type, Map<String, String> props) {
        CloudCredential credential = new CloudCredential();
        if (type == PropertyType.STORAGE) {
            credential.setAccessKey(props.getOrDefault(props.get(StorageConstants.STORAGE_ACCESS_KEY), ""));
            credential.setSecretKey(props.getOrDefault(props.get(StorageConstants.STORAGE_SECRET_KEY), ""));
            credential.setSessionToken(props.getOrDefault(props.get(StorageConstants.STORAGE_SESSION_TOKEN), ""));
            return credential;
        } else if (type == PropertyType.META) {
            credential.setAccessKey(props.getOrDefault(props.get(MetadataConstants.META_ACCESS_KEY), ""));
            credential.setSecretKey(props.getOrDefault(props.get(MetadataConstants.META_SECRET_KEY), ""));
            credential.setSessionToken(props.getOrDefault(props.get(MetadataConstants.META_SESSION_TOKEN), ""));
            return credential;
        }
        credential.setAccessKey(props.getOrDefault(props.get(GenericConstants.ROLE_ACCESS_KEY), ""));
        credential.setSecretKey(props.getOrDefault(props.get(GenericConstants.ROLE_SECRET_KEY), ""));
        credential.setSessionToken(props.getOrDefault(props.get(GenericConstants.ROLE_SESSION_TOKEN), ""));
        return credential;
    }

    private static Map<String, String> convertToDLFProperties(Map<String, String> props, CloudCredential credential) {
        return null;
    }

    private static Map<String, String> convertToGlueProperties(Map<String, String> props, CloudCredential credential) {
        return null;
    }

    private static Map<String, String> convertToOBSProperties(Map<String, String> props,
                                                              CloudCredential credential) {
        Map<String, String> obsProperties = Maps.newHashMap();
        obsProperties.put(OBSConstants.ENDPOINT, props.get(StorageConstants.STORAGE_ENDPOINT));
        obsProperties.put("fs.obs.impl.disable.cache", "true");
        if (credential.isWhole()) {
            obsProperties.put(OBSConstants.ACCESS_KEY, credential.getAccessKey());
            obsProperties.put(OBSConstants.SECRET_KEY, credential.getSecretKey());
        }
        if (credential.isTemporary()) {
            obsProperties.put("fs.obs.session.token", credential.getSessionToken());
        }
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (entry.getKey().startsWith(OBS_FS_PREFIX)) {
                obsProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return obsProperties;
    }

    private static Map<String, String> convertToS3Properties(Map<String, String> properties,
                                                             CloudCredential credential) {
        Map<String, String> s3Properties = Maps.newHashMap();
        if (properties.containsKey(StorageConstants.STORAGE_ENDPOINT)) {
            s3Properties.put(Constants.ENDPOINT, properties.get(StorageConstants.STORAGE_ENDPOINT));
        }
        if (properties.containsKey(StorageConstants.STORAGE_REGION)) {
            s3Properties.put(Constants.AWS_REGION, properties.get(StorageConstants.STORAGE_REGION));
        }
        if (properties.containsKey(S3Constants.S3_MAX_CONNECTIONS)) {
            s3Properties.put(Constants.MAXIMUM_CONNECTIONS, properties.get(S3Constants.S3_MAX_CONNECTIONS));
        }
        if (properties.containsKey(S3Constants.S3_REQUEST_TIMEOUT_MS)) {
            s3Properties.put(Constants.REQUEST_TIMEOUT, properties.get(S3Constants.S3_REQUEST_TIMEOUT_MS));
        }
        if (properties.containsKey(S3Constants.S3_CONNECTION_TIMEOUT_MS)) {
            s3Properties.put(Constants.SOCKET_TIMEOUT, properties.get(S3Constants.S3_CONNECTION_TIMEOUT_MS));
        }
        s3Properties.put(Constants.MAX_ERROR_RETRIES, "2");
        s3Properties.put("fs.s3.impl.disable.cache", "true");
        s3Properties.put("fs.s3.impl", S3AFileSystem.class.getName());

        String defaultProviderList = String.join(",", AWS_CREDENTIALS_PROVIDERS);
        String credentialsProviders = s3Properties
                .getOrDefault(Constants.AWS_CREDENTIALS_PROVIDER, defaultProviderList);
        s3Properties.put(Constants.AWS_CREDENTIALS_PROVIDER, credentialsProviders);
        if (credential.isWhole()) {
            s3Properties.put(Constants.ACCESS_KEY, credential.getAccessKey());
            s3Properties.put(Constants.SECRET_KEY, properties.get(credential.getSecretKey()));
        }
        if (credential.isTemporary()) {
            s3Properties.put(Constants.SESSION_TOKEN, properties.get(StorageConstants.STORAGE_SESSION_TOKEN));
            s3Properties.put(Constants.AWS_CREDENTIALS_PROVIDER, TemporaryAWSCredentialsProvider.class.getName());
            s3Properties.put("fs.s3.impl.disable.cache", "true");
            s3Properties.put("fs.s3a.impl.disable.cache", "true");
        }

        s3Properties.put(Constants.PATH_STYLE_ACCESS, properties.getOrDefault(USE_PATH_STYLE, "false"));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(S3Constants.S3_FS_PREFIX)) {
                s3Properties.put(entry.getKey(), entry.getValue());
            }
        }
        return s3Properties;
    }
}
