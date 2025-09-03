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

import org.apache.doris.common.credentials.CloudCredential;
import org.apache.doris.common.credentials.CloudCredentialWithEndpoint;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InitCatalogLog.Type;
import org.apache.doris.datasource.property.constants.AzureProperties;
import org.apache.doris.datasource.property.constants.CosProperties;
import org.apache.doris.datasource.property.constants.GCSProperties;
import org.apache.doris.datasource.property.constants.MinioProperties;
import org.apache.doris.datasource.property.constants.ObsProperties;
import org.apache.doris.datasource.property.constants.OssProperties;
import org.apache.doris.datasource.property.constants.PaimonProperties;
import org.apache.doris.datasource.property.constants.S3Properties;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.CosFileSystem;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.obs.OBSConstants;
import org.apache.hadoop.fs.obs.OBSFileSystem;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class PropertyConverter {

    private static final Logger LOG = LogManager.getLogger(PropertyConverter.class);
    public static final String USE_PATH_STYLE = "use_path_style";

    /**
     * Convert properties defined at doris to FE S3 client properties
     * Support other cloud client here.
     */
    public static Map<String, String> convertToHadoopFSProperties(Map<String, String> props) {
        if (props.containsKey(ObsProperties.ENDPOINT)) {
            return convertToOBSProperties(props, ObsProperties.getCredential(props));
        } else if (props.containsKey(GCSProperties.ENDPOINT)) {
            return convertToGCSProperties(props, GCSProperties.getCredential(props));
        } else if (props.containsKey(OssProperties.ENDPOINT)) {
            return convertToOSSProperties(props, OssProperties.getCredential(props));
        } else if (props.containsKey(CosProperties.ENDPOINT)) {
            return convertToCOSProperties(props, CosProperties.getCredential(props));
        } else if (props.containsKey(MinioProperties.ENDPOINT)) {
            return convertToMinioProperties(props, MinioProperties.getCredential(props));
        } else if (props.containsKey(AzureProperties.ENDPOINT)) {
            return convertToAzureProperties(props, AzureProperties.getCredential(props));
        } else if (props.containsKey(S3Properties.ENDPOINT)) {
            CloudCredential s3Credential = S3Properties.getCredential(props);
            Map<String, String> s3Properties = convertToS3Properties(props, s3Credential);
            String s3CliEndpoint = props.get(S3Properties.ENDPOINT);
            return convertToCompatibleS3Properties(props, s3CliEndpoint, s3Credential, s3Properties);
        } else if (props.containsKey(S3Properties.Env.ENDPOINT)) {
            // checkout env in the end
            // compatible with the s3,obs,oss,cos when they use aws client.
            CloudCredentialWithEndpoint envCredentials = S3Properties.getEnvironmentCredentialWithEndpoint(props);
            Map<String, String> s3Properties = convertToS3EnvProperties(props, envCredentials, false);
            String s3CliEndpoint = envCredentials.getEndpoint();
            return convertToCompatibleS3Properties(props, s3CliEndpoint, envCredentials, s3Properties);
        }
        return props;
    }

    private static Map<String, String> convertToAzureProperties(Map<String, String> props, CloudCredential credential) {
        return null;
    }

    private static Map<String, String> convertToCompatibleS3Properties(Map<String, String> props,
                                                                       String s3CliEndpoint,
                                                                       CloudCredential credential,
                                                                       Map<String, String> s3Properties) {
        Map<String, String> heteroProps = new HashMap<>(s3Properties);
        Map<String, String> copiedProps = new HashMap<>(props);
        if (s3CliEndpoint.contains(CosProperties.COS_PREFIX)) {
            copiedProps.putIfAbsent(CosProperties.ENDPOINT, s3CliEndpoint);
            // CosN is not compatible with S3, when use s3 properties, will convert to cosn properties.
            heteroProps.putAll(convertToCOSProperties(copiedProps, credential));
        } else if (s3CliEndpoint.contains(ObsProperties.OBS_PREFIX)) {
            copiedProps.putIfAbsent(ObsProperties.ENDPOINT, s3CliEndpoint);
            heteroProps.putAll(convertToOBSProperties(copiedProps, credential));
        } else if (s3CliEndpoint.contains(OssProperties.OSS_REGION_PREFIX)) {
            copiedProps.putIfAbsent(OssProperties.ENDPOINT, s3CliEndpoint);
            heteroProps.putAll(convertToOSSProperties(copiedProps, credential));
        }
        return heteroProps;
    }


    private static Map<String, String> convertToOBSProperties(Map<String, String> props,
                                                              CloudCredential credential) {
        Map<String, String> obsProperties = Maps.newHashMap();
        obsProperties.put(OBSConstants.ENDPOINT, props.get(ObsProperties.ENDPOINT));
        obsProperties.put("fs.obs.impl", getHadoopFSImplByScheme("obs"));
        if (credential.isWhole()) {
            obsProperties.put(OBSConstants.ACCESS_KEY, credential.getAccessKey());
            obsProperties.put(OBSConstants.SECRET_KEY, credential.getSecretKey());
        }
        if (credential.isTemporary()) {
            obsProperties.put(ObsProperties.FS.SESSION_TOKEN, credential.getSessionToken());
        }
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (entry.getKey().startsWith(ObsProperties.OBS_FS_PREFIX)) {
                obsProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return obsProperties;
    }

    public static String getHadoopFSImplByScheme(String fsScheme) {
        if (fsScheme.equalsIgnoreCase("obs")) {
            return OBSFileSystem.class.getName();
        } else if (fsScheme.equalsIgnoreCase("file")) {
            return LocalFileSystem.class.getName();
        } else if (fsScheme.equalsIgnoreCase("oss")) {
            return AliyunOSSFileSystem.class.getName();
        } else if (fsScheme.equalsIgnoreCase("cosn") || fsScheme.equalsIgnoreCase("lakefs")) {
            return CosFileSystem.class.getName();
        } else {
            return S3AFileSystem.class.getName();
        }
    }

    private static Map<String, String> convertToS3EnvProperties(Map<String, String> properties,
                                                                CloudCredentialWithEndpoint credential,
                                                                boolean isMeta) {
        // Old properties to new properties
        properties.put(S3Properties.ENDPOINT, credential.getEndpoint());
        properties.put(S3Properties.REGION,
                    checkRegion(credential.getEndpoint(), credential.getRegion(), S3Properties.Env.REGION));
        properties.put(S3Properties.ACCESS_KEY, credential.getAccessKey());
        properties.put(S3Properties.SECRET_KEY, credential.getSecretKey());
        if (properties.containsKey(S3Properties.Env.TOKEN)) {
            properties.put(S3Properties.SESSION_TOKEN, credential.getSessionToken());
        }
        if (properties.containsKey(S3Properties.Env.MAX_CONNECTIONS)) {
            properties.put(S3Properties.MAX_CONNECTIONS, properties.get(S3Properties.Env.MAX_CONNECTIONS));
        }
        if (properties.containsKey(S3Properties.Env.REQUEST_TIMEOUT_MS)) {
            properties.put(S3Properties.REQUEST_TIMEOUT_MS, properties.get(S3Properties.Env.REQUEST_TIMEOUT_MS));
        }
        if (properties.containsKey(S3Properties.Env.CONNECTION_TIMEOUT_MS)) {
            properties.put(S3Properties.REQUEST_TIMEOUT_MS, properties.get(S3Properties.Env.CONNECTION_TIMEOUT_MS));
        }

        if (properties.containsKey(S3Properties.Env.ROLE_ARN)) {
            properties.put(S3Properties.ROLE_ARN, properties.get(S3Properties.Env.ROLE_ARN));
        }

        if (properties.containsKey(S3Properties.Env.EXTERNAL_ID)) {
            properties.put(S3Properties.EXTERNAL_ID, properties.get(S3Properties.Env.EXTERNAL_ID));
        }

        if (isMeta) {
            return properties;
        }
        return convertToS3Properties(properties, credential);
    }

    private static Map<String, String> convertToS3Properties(Map<String, String> properties,
            CloudCredential credential) {
        // s3 property in paimon is personalized
        String type = properties.get(CatalogMgr.CATALOG_TYPE_PROP);
        if (type != null && type.equalsIgnoreCase(Type.PAIMON.toString())) {
            return PaimonProperties.convertToS3Properties(properties, credential);
        }
        Map<String, String> s3Properties = Maps.newHashMap();
        String endpoint = properties.get(S3Properties.ENDPOINT);
        s3Properties.put(Constants.ENDPOINT, endpoint);
        s3Properties.put(Constants.AWS_REGION,
                    checkRegion(endpoint, properties.get(S3Properties.REGION), S3Properties.REGION));
        if (properties.containsKey(S3Properties.MAX_CONNECTIONS)) {
            s3Properties.put(Constants.MAXIMUM_CONNECTIONS, properties.get(S3Properties.MAX_CONNECTIONS));
        }
        if (properties.containsKey(S3Properties.REQUEST_TIMEOUT_MS)) {
            s3Properties.put(Constants.REQUEST_TIMEOUT, properties.get(S3Properties.REQUEST_TIMEOUT_MS));
        }
        if (properties.containsKey(S3Properties.CONNECTION_TIMEOUT_MS)) {
            s3Properties.put(Constants.SOCKET_TIMEOUT, properties.get(S3Properties.CONNECTION_TIMEOUT_MS));
        }

        setS3FsAccess(s3Properties, properties, credential);
        s3Properties.putAll(properties);
        // remove extra meta properties
        S3Properties.FS_KEYS.forEach(s3Properties::remove);

        if (LOG.isDebugEnabled()) {
            LOG.debug("s3Properties:{}\nproperties:{}", s3Properties, properties);
        }

        return s3Properties;
    }

    public static String checkRegion(String endpoint, String region, String regionKey) {
        if (Strings.isNullOrEmpty(region)) {
            region = S3Properties.getRegionOfEndpoint(endpoint);
        }
        if (Strings.isNullOrEmpty(region)) {
            String errorMsg = String.format("No '%s' info found, using SDK default region: us-east-1", regionKey);
            LOG.warn(errorMsg);
            return "us-east-1";
        }
        return region;
    }

    private static void setS3FsAccess(Map<String, String> s3Properties, Map<String, String> properties,
                                      CloudCredential credential) {
        s3Properties.put(Constants.MAX_ERROR_RETRIES, "2");
        s3Properties.putIfAbsent("fs.s3.impl", S3AFileSystem.class.getName());
        String credentialsProviders = getAWSCredentialsProviders(properties);
        s3Properties.put(Constants.AWS_CREDENTIALS_PROVIDER, credentialsProviders);
        if (credential.isWhole()) {
            s3Properties.put(Constants.ACCESS_KEY, credential.getAccessKey());
            s3Properties.put(Constants.SECRET_KEY, credential.getSecretKey());
        }
        if (credential.isTemporary()) {
            s3Properties.put(Constants.SESSION_TOKEN, credential.getSessionToken());
            s3Properties.put(Constants.AWS_CREDENTIALS_PROVIDER, TemporaryAWSCredentialsProvider.class.getName());
        }
        s3Properties.put(Constants.PATH_STYLE_ACCESS, properties.getOrDefault(USE_PATH_STYLE, "false"));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(S3Properties.S3_FS_PREFIX)) {
                s3Properties.put(entry.getKey(), entry.getValue());
            }
        }

        if (properties.containsKey(S3Properties.ROLE_ARN)
                && !Strings.isNullOrEmpty(properties.get(S3Properties.ROLE_ARN))) {
            // refer to https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/assumed_roles.html
            //          https://issues.apache.org/jira/browse/HADOOP-19201
            s3Properties.put(Constants.AWS_CREDENTIALS_PROVIDER, AssumedRoleCredentialProvider.class.getName());
            s3Properties.put(Constants.ASSUMED_ROLE_ARN, properties.get(S3Properties.ROLE_ARN));
            s3Properties.put(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER,
                    InstanceProfileCredentialsProvider.class.getName());

            if (properties.containsKey(S3Properties.EXTERNAL_ID)
                    && !Strings.isNullOrEmpty(properties.get(S3Properties.EXTERNAL_ID))) {
                LOG.warn("External ID is not supported for assumed role credential provider");
            }
        }
    }

    public static String getAWSCredentialsProviders(Map<String, String> properties) {
        String credentialsProviders;
        String hadoopCredProviders = properties.get(Constants.AWS_CREDENTIALS_PROVIDER);
        if (hadoopCredProviders != null) {
            credentialsProviders = hadoopCredProviders;
        } else {
            String defaultProviderList = String.join(",", S3Properties.AWS_CREDENTIALS_PROVIDERS);
            credentialsProviders = properties.getOrDefault(S3Properties.CREDENTIALS_PROVIDER, defaultProviderList);
        }
        return credentialsProviders;
    }

    private static Map<String, String> convertToGCSProperties(Map<String, String> props, CloudCredential credential) {
        // Now we use s3 client to access
        return convertToS3Properties(S3Properties.prefixToS3(props), credential);
    }

    private static Map<String, String> convertToOSSProperties(Map<String, String> props, CloudCredential credential) {
        Map<String, String> ossProperties = Maps.newHashMap();
        String endpoint = props.get(OssProperties.ENDPOINT);
        if (endpoint.startsWith(OssProperties.OSS_PREFIX)) {
            // may use oss.oss-cn-beijing.aliyuncs.com
            endpoint = endpoint.replace(OssProperties.OSS_PREFIX, "");
        }
        ossProperties.put(org.apache.hadoop.fs.aliyun.oss.Constants.ENDPOINT_KEY, endpoint);
        boolean hdfsEnabled = Boolean.parseBoolean(props.getOrDefault(OssProperties.OSS_HDFS_ENABLED, "false"));
        if (LocationPath.isHdfsOnOssEndpoint(endpoint) || hdfsEnabled) {
            // use endpoint or enable hdfs
            rewriteHdfsOnOssProperties(ossProperties, endpoint);
        } else {
            ossProperties.put("fs.oss.impl", getHadoopFSImplByScheme("oss"));
        }
        if (credential.isWhole()) {
            ossProperties.put(org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_ID, credential.getAccessKey());
            ossProperties.put(org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_SECRET, credential.getSecretKey());
        }
        if (credential.isTemporary()) {
            ossProperties.put(org.apache.hadoop.fs.aliyun.oss.Constants.SECURITY_TOKEN, credential.getSessionToken());
        }
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (entry.getKey().startsWith(OssProperties.OSS_FS_PREFIX)) {
                ossProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return ossProperties;
    }

    private static void rewriteHdfsOnOssProperties(Map<String, String> ossProperties, String endpoint) {
        if (!LocationPath.isHdfsOnOssEndpoint(endpoint)) {
            // just for robustness here, avoid wrong endpoint when oss-hdfs is enabled.
            // convert "oss-cn-beijing.aliyuncs.com" to "cn-beijing.oss-dls.aliyuncs.com"
            // reference link: https://www.alibabacloud.com/help/en/e-mapreduce/latest/oss-kusisurumen
            String[] endpointSplit = endpoint.split("\\.");
            if (endpointSplit.length > 0) {
                String region = endpointSplit[0].replace("oss-", "").replace("-internal", "");
                ossProperties.put(org.apache.hadoop.fs.aliyun.oss.Constants.ENDPOINT_KEY,
                        region + ".oss-dls.aliyuncs.com");
            }
        }
        ossProperties.put("fs.oss.impl", "com.aliyun.jindodata.oss.JindoOssFileSystem");
        ossProperties.put("fs.AbstractFileSystem.oss.impl", "com.aliyun.jindodata.oss.OSS");
    }

    private static Map<String, String> convertToCOSProperties(Map<String, String> props, CloudCredential credential) {
        Map<String, String> cosProperties = Maps.newHashMap();
        cosProperties.put(CosNConfigKeys.COSN_ENDPOINT_SUFFIX_KEY, props.get(CosProperties.ENDPOINT));
        cosProperties.put("fs.cosn.impl", getHadoopFSImplByScheme("cosn"));
        cosProperties.put("fs.lakefs.impl", getHadoopFSImplByScheme("lakefs"));
        if (credential.isWhole()) {
            cosProperties.put(CosNConfigKeys.COSN_USERINFO_SECRET_ID_KEY, credential.getAccessKey());
            cosProperties.put(CosNConfigKeys.COSN_USERINFO_SECRET_KEY_KEY, credential.getSecretKey());
        }
        // session token is unsupported
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (entry.getKey().startsWith(CosProperties.COS_FS_PREFIX)) {
                cosProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return cosProperties;
    }

    private static Map<String, String> convertToMinioProperties(Map<String, String> props, CloudCredential credential) {
        if (!props.containsKey(MinioProperties.REGION)) {
            props.put(MinioProperties.REGION, MinioProperties.DEFAULT_REGION);
        }
        return convertToS3Properties(S3Properties.prefixToS3(props), credential);
    }
}
