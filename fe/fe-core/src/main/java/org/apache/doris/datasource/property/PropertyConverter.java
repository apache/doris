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

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InitCatalogLog.Type;
import org.apache.doris.datasource.credentials.CloudCredential;
import org.apache.doris.datasource.credentials.CloudCredentialWithEndpoint;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.property.constants.CosProperties;
import org.apache.doris.datasource.property.constants.DLFProperties;
import org.apache.doris.datasource.property.constants.GCSProperties;
import org.apache.doris.datasource.property.constants.GlueProperties;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.datasource.property.constants.MinioProperties;
import org.apache.doris.datasource.property.constants.ObsProperties;
import org.apache.doris.datasource.property.constants.OssProperties;
import org.apache.doris.datasource.property.constants.PaimonProperties;
import org.apache.doris.datasource.property.constants.S3Properties;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.amazonaws.glue.catalog.util.AWSGlueConfig;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.cosn.CosNConfigKeys;
import org.apache.hadoop.fs.cosn.CosNFileSystem;
import org.apache.hadoop.fs.obs.OBSConstants;
import org.apache.hadoop.fs.obs.OBSFileSystem;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class PropertyConverter {

    private static final Logger LOG = LogManager.getLogger(PropertyConverter.class);
    public static final String USE_PATH_STYLE = "use_path_style";

    /**
     * Convert properties defined at doris to metadata properties on Cloud
     *       Step 1: convert and set cloud metadata properties and s3 properties
     *          example:
     *                 glue.endpoint -> aws.glue.endpoint for Glue
     *                               -> s3.endpoint for S3
     *                 glue.access_key -> aws.glue.access-key for Glue
     *                                  > s3.access_key for S3
     *       Step 2: convert props to BE properties, put them all to metaProperties
     *           example:
     *                 s3.endpoint -> AWS_ENDPOINT
     *                 s3.access_key -> AWS_ACCESS_KEY
     * These properties will be used for catalog/resource, and persisted to catalog/resource properties.
     * Some properties like AWS_XXX will be hidden, can find from HIDDEN_KEY in PrintableMap
     * @see org.apache.doris.common.util.PrintableMap
     */
    public static Map<String, String> convertToMetaProperties(Map<String, String> props) {
        Map<String, String> metaProperties = new HashMap<>();
        if (props.containsKey(GlueProperties.ENDPOINT)
                || props.containsKey(AWSGlueConfig.AWS_GLUE_ENDPOINT)) {
            CloudCredential credential = GlueProperties.getCredential(props);
            if (!credential.isWhole()) {
                credential = GlueProperties.getCompatibleCredential(props);
            }
            metaProperties = convertToGlueProperties(props, credential);
        } else if (props.containsKey(DLFProperties.ENDPOINT)
                || props.containsKey(DataLakeConfig.CATALOG_ENDPOINT)) {
            metaProperties = convertToDLFProperties(props, DLFProperties.getCredential(props));
        } else if (props.containsKey(S3Properties.Env.ENDPOINT)) {
            if (!hasS3Properties(props)) {
                // checkout env in the end
                // if meet AWS_XXX properties, convert to s3 properties
                return convertToS3EnvProperties(props, S3Properties.getEnvironmentCredentialWithEndpoint(props), true);
            }
        }
        metaProperties.putAll(props);
        metaProperties.putAll(S3ClientBEProperties.getBeFSProperties(props));
        return metaProperties;
    }

    private static boolean hasS3Properties(Map<String, String> props) {
        return props.containsKey(ObsProperties.ENDPOINT)
                || props.containsKey(GCSProperties.ENDPOINT)
                || props.containsKey(OssProperties.ENDPOINT)
                || props.containsKey(CosProperties.ENDPOINT)
                || props.containsKey(MinioProperties.ENDPOINT);
    }

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
        obsProperties.put(ObsProperties.FS.IMPL_DISABLE_CACHE, "true");
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
        } else if (fsScheme.equalsIgnoreCase("oss")) {
            return AliyunOSSFileSystem.class.getName();
        } else if (fsScheme.equalsIgnoreCase("cosn")) {
            return CosNFileSystem.class.getName();
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
        return s3Properties;
    }

    private static String checkRegion(String endpoint, String region, String regionKey) {
        if (Strings.isNullOrEmpty(region)) {
            region = S3Properties.getRegionOfEndpoint(endpoint);
        }
        if (Strings.isNullOrEmpty(region)) {
            String errorMsg = String.format("Required property '%s' when region is not in endpoint.", regionKey);
            Util.logAndThrowRuntimeException(LOG, errorMsg, new IllegalArgumentException(errorMsg));
        }
        return region;
    }

    private static void setS3FsAccess(Map<String, String> s3Properties, Map<String, String> properties,
                                      CloudCredential credential) {
        s3Properties.put(Constants.MAX_ERROR_RETRIES, "2");
        s3Properties.put("fs.s3.impl.disable.cache", "true");
        s3Properties.putIfAbsent("fs.s3.impl", S3AFileSystem.class.getName());
        String defaultProviderList = String.join(",", S3Properties.AWS_CREDENTIALS_PROVIDERS);
        String credentialsProviders = s3Properties
                .getOrDefault(S3Properties.CREDENTIALS_PROVIDER, defaultProviderList);
        s3Properties.put(Constants.AWS_CREDENTIALS_PROVIDER, credentialsProviders);
        if (credential.isWhole()) {
            s3Properties.put(Constants.ACCESS_KEY, credential.getAccessKey());
            s3Properties.put(Constants.SECRET_KEY, credential.getSecretKey());
        }
        if (credential.isTemporary()) {
            s3Properties.put(Constants.SESSION_TOKEN, credential.getSessionToken());
            s3Properties.put(Constants.AWS_CREDENTIALS_PROVIDER, TemporaryAWSCredentialsProvider.class.getName());
            s3Properties.put("fs.s3.impl.disable.cache", "true");
            s3Properties.put("fs.s3a.impl.disable.cache", "true");
        }
        s3Properties.put(Constants.PATH_STYLE_ACCESS, properties.getOrDefault(USE_PATH_STYLE, "false"));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(S3Properties.S3_FS_PREFIX)) {
                s3Properties.put(entry.getKey(), entry.getValue());
            }
        }
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
        ossProperties.put("fs.oss.impl.disable.cache", "true");
        ossProperties.put("fs.oss.impl", getHadoopFSImplByScheme("oss"));
        boolean hdfsEnabled = Boolean.parseBoolean(props.getOrDefault(OssProperties.OSS_HDFS_ENABLED, "false"));
        if (LocationPath.isHdfsOnOssEndpoint(endpoint) || hdfsEnabled) {
            // use endpoint or enable hdfs
            rewriteHdfsOnOssProperties(ossProperties, endpoint);
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
        cosProperties.put("fs.cosn.impl.disable.cache", "true");
        cosProperties.put("fs.cosn.impl", getHadoopFSImplByScheme("cosn"));
        if (credential.isWhole()) {
            cosProperties.put(CosNConfigKeys.COSN_SECRET_ID_KEY, credential.getAccessKey());
            cosProperties.put(CosNConfigKeys.COSN_SECRET_KEY_KEY, credential.getSecretKey());
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

    private static Map<String, String> convertToDLFProperties(Map<String, String> props, CloudCredential credential) {
        getPropertiesFromDLFConf(props);
        // if configure DLF properties in catalog properties, use them to override config in hive-site.xml
        getPropertiesFromDLFProps(props, credential);
        props.put(DataLakeConfig.CATALOG_CREATE_DEFAULT_DB, "false");
        return props;
    }

    public static void getPropertiesFromDLFConf(Map<String, String> props) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get properties from hive-site.xml");
        }
        // read properties from hive-site.xml.
        HiveConf hiveConf = new HiveConf();
        String metastoreType = hiveConf.get(HMSProperties.HIVE_METASTORE_TYPE);
        if (!HMSProperties.DLF_TYPE.equalsIgnoreCase(metastoreType)) {
            return;
        }
        String uid = props.get(DataLakeConfig.CATALOG_USER_ID);
        if (Strings.isNullOrEmpty(uid)) {
            throw new IllegalArgumentException("Required dlf property: " + DataLakeConfig.CATALOG_USER_ID);
        }
        getOSSPropertiesFromDLFConf(props, hiveConf);
    }

    private static void getOSSPropertiesFromDLFConf(Map<String, String> props, HiveConf hiveConf) {
        // get following properties from hive-site.xml
        // 1. region and endpoint. eg: cn-beijing
        String region = hiveConf.get(DataLakeConfig.CATALOG_REGION_ID);
        if (!Strings.isNullOrEmpty(region)) {
            // See: https://help.aliyun.com/document_detail/31837.html
            // And add "-internal" to access oss within vpc
            props.put(OssProperties.REGION, "oss-" + region);
            String publicAccess = hiveConf.get("dlf.catalog.accessPublic", "false");
            props.put(OssProperties.ENDPOINT, getOssEndpoint(region, Boolean.parseBoolean(publicAccess)));
        }
        // 2. ak and sk
        String ak = hiveConf.get(DataLakeConfig.CATALOG_ACCESS_KEY_ID);
        String sk = hiveConf.get(DataLakeConfig.CATALOG_ACCESS_KEY_SECRET);
        if (!Strings.isNullOrEmpty(ak)) {
            props.put(OssProperties.ACCESS_KEY, ak);
        }
        if (!Strings.isNullOrEmpty(sk)) {
            props.put(OssProperties.SECRET_KEY, sk);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get properties for oss in hive-site.xml: {}", props);
        }
    }

    private static void getPropertiesFromDLFProps(Map<String, String> props,
                                                  CloudCredential credential) {
        String metastoreType = props.get(HMSProperties.HIVE_METASTORE_TYPE);
        if (!HMSProperties.DLF_TYPE.equalsIgnoreCase(metastoreType)) {
            return;
        }
        // convert to dlf client properties. not convert if origin key found.
        if (!props.containsKey(DataLakeConfig.CATALOG_USER_ID)) {
            props.put(DataLakeConfig.CATALOG_USER_ID, props.get(DLFProperties.UID));
            String uid = props.get(DLFProperties.UID);
            if (Strings.isNullOrEmpty(uid)) {
                throw new IllegalArgumentException("Required dlf property: " + DLFProperties.UID);
            }
            String endpoint = props.get(DLFProperties.ENDPOINT);
            props.put(DataLakeConfig.CATALOG_ENDPOINT, endpoint);
            props.put(DataLakeConfig.CATALOG_REGION_ID, props.getOrDefault(DLFProperties.REGION,
                    S3Properties.getRegionOfEndpoint(endpoint)));
            props.put(DataLakeConfig.CATALOG_PROXY_MODE, props.getOrDefault(DLFProperties.PROXY_MODE, "DLF_ONLY"));
            props.put(DataLakeConfig.CATALOG_ACCESS_KEY_ID, credential.getAccessKey());
            props.put(DataLakeConfig.CATALOG_ACCESS_KEY_SECRET, credential.getSecretKey());
            props.put(DLFProperties.Site.ACCESS_PUBLIC, props.getOrDefault(DLFProperties.ACCESS_PUBLIC, "false"));
        }
        String uid = props.get(DataLakeConfig.CATALOG_USER_ID);
        if (Strings.isNullOrEmpty(uid)) {
            throw new IllegalArgumentException("Required dlf property: " + DataLakeConfig.CATALOG_USER_ID);
        }
        if (!props.containsKey(DLFProperties.ENDPOINT)) {
            // just display DLFProperties in catalog, and hide DataLakeConfig properties
            putNewPropertiesForCompatibility(props, credential);
        }
        // convert to oss property
        if (credential.isWhole()) {
            props.put(OssProperties.ACCESS_KEY, credential.getAccessKey());
            props.put(OssProperties.SECRET_KEY, credential.getSecretKey());
        }
        if (credential.isTemporary()) {
            props.put(OssProperties.SESSION_TOKEN, credential.getSessionToken());
        }
        String publicAccess = props.getOrDefault(DLFProperties.Site.ACCESS_PUBLIC, "false");
        String region = props.getOrDefault(DataLakeConfig.CATALOG_REGION_ID, props.get(DLFProperties.REGION));
        if (!Strings.isNullOrEmpty(region)) {
            boolean hdfsEnabled = Boolean.parseBoolean(props.getOrDefault(OssProperties.OSS_HDFS_ENABLED, "false"));
            if (hdfsEnabled) {
                props.putIfAbsent("fs.oss.impl", "com.aliyun.jindodata.oss.JindoOssFileSystem");
                props.put("fs.AbstractFileSystem.oss.impl", "com.aliyun.jindodata.oss.OSS");
                props.putIfAbsent(OssProperties.REGION, region);
                // example: cn-shanghai.oss-dls.aliyuncs.com
                // from https://www.alibabacloud.com/help/en/e-mapreduce/latest/oss-kusisurumen
                props.putIfAbsent(OssProperties.ENDPOINT, region + ".oss-dls.aliyuncs.com");
            } else {
                props.putIfAbsent(OssProperties.REGION, "oss-" + region);
                props.putIfAbsent(OssProperties.ENDPOINT, getOssEndpoint(region, Boolean.parseBoolean(publicAccess)));
            }
        }
    }

    private static void putNewPropertiesForCompatibility(Map<String, String> props, CloudCredential credential) {
        props.put(DLFProperties.UID, props.get(DataLakeConfig.CATALOG_USER_ID));
        String endpoint = props.get(DataLakeConfig.CATALOG_ENDPOINT);
        props.put(DLFProperties.ENDPOINT, endpoint);
        props.put(DLFProperties.REGION, props.getOrDefault(DataLakeConfig.CATALOG_REGION_ID,
                S3Properties.getRegionOfEndpoint(endpoint)));
        props.put(DLFProperties.PROXY_MODE, props.getOrDefault(DataLakeConfig.CATALOG_PROXY_MODE, "DLF_ONLY"));
        props.put(DLFProperties.ACCESS_KEY, credential.getAccessKey());
        props.put(DLFProperties.SECRET_KEY, credential.getSecretKey());
        props.put(DLFProperties.ACCESS_PUBLIC, props.getOrDefault(DLFProperties.Site.ACCESS_PUBLIC, "false"));
    }

    private static String getOssEndpoint(String region, boolean publicAccess) {
        String prefix = "http://oss-";
        String suffix = ".aliyuncs.com";
        if (!publicAccess) {
            suffix = "-internal" + suffix;
        }
        return prefix + region + suffix;
    }

    private static Map<String, String> convertToGlueProperties(Map<String, String> props, CloudCredential credential) {
        // convert doris glue property to glue properties, s3 client property and BE property
        String metastoreType = props.get(HMSProperties.HIVE_METASTORE_TYPE);
        String icebergType = props.get(IcebergExternalCatalog.ICEBERG_CATALOG_TYPE);
        boolean isGlueIceberg = IcebergExternalCatalog.ICEBERG_GLUE.equals(icebergType);
        if (!HMSProperties.GLUE_TYPE.equalsIgnoreCase(metastoreType) && !isGlueIceberg) {
            return props;
        }
        if (isGlueIceberg) {
            // glue ak sk for iceberg
            props.putIfAbsent(GlueProperties.ACCESS_KEY, credential.getAccessKey());
            props.putIfAbsent(GlueProperties.SECRET_KEY, credential.getSecretKey());
        }
        // set glue client metadata
        if (props.containsKey(GlueProperties.ENDPOINT)) {
            String endpoint = props.get(GlueProperties.ENDPOINT);
            props.put(AWSGlueConfig.AWS_GLUE_ENDPOINT, endpoint);
            String region = S3Properties.getRegionOfEndpoint(endpoint);
            props.put(AWSGlueConfig.AWS_REGION, region);
            if (credential.isWhole()) {
                props.put(AWSGlueConfig.AWS_GLUE_ACCESS_KEY, credential.getAccessKey());
                props.put(AWSGlueConfig.AWS_GLUE_SECRET_KEY, credential.getSecretKey());
            }
            if (credential.isTemporary()) {
                props.put(AWSGlueConfig.AWS_GLUE_SESSION_TOKEN, credential.getSessionToken());
            }
        } else {
            // compatible with old version, deprecated in the future version
            // put GlueProperties to map if origin key found.
            if (props.containsKey(AWSGlueConfig.AWS_GLUE_ENDPOINT)) {
                String endpoint = props.get(AWSGlueConfig.AWS_GLUE_ENDPOINT);
                props.put(GlueProperties.ENDPOINT, endpoint);
                if (props.containsKey(AWSGlueConfig.AWS_GLUE_ACCESS_KEY)) {
                    props.put(GlueProperties.ACCESS_KEY, props.get(AWSGlueConfig.AWS_GLUE_ACCESS_KEY));
                }
                if (props.containsKey(AWSGlueConfig.AWS_GLUE_SECRET_KEY)) {
                    props.put(GlueProperties.SECRET_KEY, props.get(AWSGlueConfig.AWS_GLUE_SECRET_KEY));
                }
                if (props.containsKey(AWSGlueConfig.AWS_GLUE_SESSION_TOKEN)) {
                    props.put(GlueProperties.SESSION_TOKEN, props.get(AWSGlueConfig.AWS_GLUE_SESSION_TOKEN));
                }
            }
        }
        // set s3 client credential
        // https://docs.aws.amazon.com/general/latest/gr/s3.html
        // Convert:
        // (
        //  "glue.region" = "us-east-1",
        //  "glue.access_key" = "xx",
        //  "glue.secret_key" = "yy"
        // )
        // To:
        // (
        //  "s3.region" = "us-east-1",
        //  "s3.endpoint" = "s3.us-east-1.amazonaws.com"
        //  "s3.access_key" = "xx",
        //  "s3.secret_key" = "yy"
        // )
        String endpoint = props.get(GlueProperties.ENDPOINT);
        String region = S3Properties.getRegionOfEndpoint(endpoint);
        if (!Strings.isNullOrEmpty(region)) {
            props.put(S3Properties.REGION, region);
            String suffix = ".amazonaws.com";
            if (endpoint.endsWith(".amazonaws.com.cn")) {
                suffix = ".amazonaws.com.cn";
            }
            String s3Endpoint = "s3." + region + suffix;
            if (isGlueIceberg) {
                s3Endpoint = "https://" + s3Endpoint;
            }
            props.put(S3Properties.ENDPOINT, s3Endpoint);
        }
        if (credential.isWhole()) {
            props.put(S3Properties.ACCESS_KEY, credential.getAccessKey());
            props.put(S3Properties.SECRET_KEY, credential.getSecretKey());
        }
        if (credential.isTemporary()) {
            props.put(S3Properties.SESSION_TOKEN, credential.getSessionToken());
        }
        return props;
    }
}

