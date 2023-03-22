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

import org.apache.doris.datasource.credentials.CloudCredential;
import org.apache.doris.datasource.property.constants.CosProperties;
import org.apache.doris.datasource.property.constants.DLFProperties;
import org.apache.doris.datasource.property.constants.GlueProperties;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.datasource.property.constants.ObsProperties;
import org.apache.doris.datasource.property.constants.OssProperties;
import org.apache.doris.datasource.property.constants.S3Properties;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.amazonaws.glue.catalog.util.AWSGlueConfig;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.obs.OBSConstants;
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
     * Convert properties defined at doris to FE S3 client properties and BE S3 client properties
     */
    public static Map<String, String> convert(Map<String, String> props) {
        if (props.containsKey(S3Properties.Environment.ENDPOINT)) {
            return convertToS3Properties(props, S3Properties.getCompatibleCredential(props));
        } else if (props.containsKey(S3Properties.ENDPOINT)) {
            return convertToS3Properties(props, S3Properties.getCredential(props));
        } else if (props.containsKey(ObsProperties.ENDPOINT)) {
            return convertToOBSProperties(props, ObsProperties.getCredential(props));
        } else if (props.containsKey(OssProperties.ENDPOINT)) {
            return convertToOSSProperties(props, OssProperties.getCredential(props));
        } else if (props.containsKey(CosProperties.ENDPOINT)) {
            return convertToCOSProperties(props, CosProperties.getCredential(props));
        } else if (props.containsKey(GlueProperties.ENDPOINT)) {
            return convertToGlueProperties(props, GlueProperties.getCredential(props));
        } else if (props.containsKey(DLFProperties.ENDPOINT)) {
            return convertToDLFProperties(props, DLFProperties.getCredential(props));
        }
        return props;
    }


    private static Map<String, String> convertToOBSProperties(Map<String, String> props,
                                                              CloudCredential credential) {
        Map<String, String> obsProperties = Maps.newHashMap();
        obsProperties.put(OBSConstants.ENDPOINT, props.get(ObsProperties.ENDPOINT));
        obsProperties.put("fs.obs.impl.disable.cache", "true");
        if (credential.isWhole()) {
            obsProperties.put(OBSConstants.ACCESS_KEY, credential.getAccessKey());
            obsProperties.put(OBSConstants.SECRET_KEY, credential.getSecretKey());
        }
        if (credential.isTemporary()) {
            obsProperties.put("fs.obs.session.token", credential.getSessionToken());
        }
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (entry.getKey().startsWith(ObsProperties.OBS_FS_PREFIX)) {
                obsProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return obsProperties;
    }

    private static Map<String, String> convertToS3Properties(Map<String, String> properties,
                                                             CloudCredential credential) {
        Map<String, String> s3Properties = Maps.newHashMap();
        s3Properties.put(Constants.ENDPOINT, properties.get(S3Properties.ENDPOINT));
        if (properties.containsKey(S3Properties.REGION)) {
            s3Properties.put(Constants.AWS_REGION, properties.get(S3Properties.REGION));
        }
        if (properties.containsKey(S3Properties.MAX_CONNECTIONS)) {
            s3Properties.put(Constants.MAXIMUM_CONNECTIONS, properties.get(S3Properties.MAX_CONNECTIONS));
        }
        if (properties.containsKey(S3Properties.REQUEST_TIMEOUT_MS)) {
            s3Properties.put(Constants.REQUEST_TIMEOUT, properties.get(S3Properties.REQUEST_TIMEOUT_MS));
        }
        if (properties.containsKey(S3Properties.CONNECTION_TIMEOUT_MS)) {
            s3Properties.put(Constants.SOCKET_TIMEOUT, properties.get(S3Properties.CONNECTION_TIMEOUT_MS));
        }
        s3Properties.put(Constants.MAX_ERROR_RETRIES, "2");
        s3Properties.put("fs.s3.impl.disable.cache", "true");
        s3Properties.put("fs.s3.impl", S3AFileSystem.class.getName());

        String defaultProviderList = String.join(",", S3Properties.AWS_CREDENTIALS_PROVIDERS);
        String credentialsProviders = s3Properties
                .getOrDefault(Constants.AWS_CREDENTIALS_PROVIDER, defaultProviderList);
        s3Properties.put(Constants.AWS_CREDENTIALS_PROVIDER, credentialsProviders);
        if (credential.isWhole()) {
            s3Properties.put(Constants.ACCESS_KEY, credential.getAccessKey());
            s3Properties.put(Constants.SECRET_KEY, properties.get(credential.getSecretKey()));
        }
        if (credential.isTemporary()) {
            s3Properties.put(Constants.SESSION_TOKEN, properties.get(S3Properties.SESSION_TOKEN));
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
        return s3Properties;
    }

    private static Map<String, String> convertToOSSProperties(Map<String, String> props, CloudCredential credential) {
        // Now we use s3 client to access
        return convertToS3Properties(props, credential);
    }

    private static Map<String, String> convertToCOSProperties(Map<String, String> props, CloudCredential credential) {
        // Now we use s3 client to access
        return convertToS3Properties(props, credential);
    }

    private static Map<String, String> convertToDLFProperties(Map<String, String> props, CloudCredential credential) {
        props = getPropertiesFromDLFSite(props, credential);
        props = getPropertiesFromDLFProps(props, credential);
        // add remain properties
        return convertToS3Properties(props, credential);
    }


    public static Map<String, String> getPropertiesFromDLFSite(Map<String, String> props, CloudCredential credential) {

        Map<String, String> res = new HashMap<>();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get properties from hive-site.xml");
        }
        // read properties from hive-site.xml.
        HiveConf hiveConf = new HiveConf();
        String metastoreType = hiveConf.get(HMSProperties.HIVE_METASTORE_TYPE);
        String propsMetastoreType = props.get(HMSProperties.HIVE_METASTORE_TYPE);
        if (!HMSProperties.DLF_TYPE.equalsIgnoreCase(metastoreType)
                && !HMSProperties.DLF_TYPE.equalsIgnoreCase(propsMetastoreType)) {
            return props;
        }
        // get following properties from hive-site.xml
        // 1. region and endpoint. eg: cn-beijing
        String region = hiveConf.get("dlf.catalog.region");
        if (!Strings.isNullOrEmpty(region)) {
            // See: https://help.aliyun.com/document_detail/31837.html
            // And add "-internal" to access oss within vpc
            res.put(S3Properties.REGION, "oss-" + region);
            String publicAccess = hiveConf.get("dlf.catalog.accessPublic", "false");
            res.put(S3Properties.ENDPOINT, getDLFEndpont(region, Boolean.parseBoolean(publicAccess)));
        }

        // 2. ak and sk
        String ak = hiveConf.get("dlf.catalog.accessKeyId");
        String sk = hiveConf.get("dlf.catalog.accessKeySecret");
        if (!Strings.isNullOrEmpty(ak)) {
            res.put(S3Properties.ACCESS_KEY, ak);
        }
        if (!Strings.isNullOrEmpty(sk)) {
            res.put(S3Properties.SECRET_KEY, sk);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get properties for oss in hive-site.xml: {}", res);
        }
        return res;
    }

    private static Map<String, String> getPropertiesFromDLFProps(Map<String, String> props,
                                                                 CloudCredential credential) {
        // convert doris dlf property to dlf metastore properties, s3 client property and BE property
        String ak = props.getOrDefault(DataLakeConfig.CATALOG_ACCESS_KEY_ID, credential.getAccessKey());
        String sk = props.getOrDefault(DataLakeConfig.CATALOG_ACCESS_KEY_SECRET, credential.getSecretKey());
        if (credential.isWhole()) {
            props.put(S3Properties.ACCESS_KEY, ak);
            props.put(S3Properties.SECRET_KEY, sk);
        }
        props.put(DataLakeConfig.CATALOG_USER_ID, props.getOrDefault(DataLakeConfig.CATALOG_USER_ID,
                props.get(DLFProperties.UID)));
        props.put(DataLakeConfig.CATALOG_PROXY_MODE, props.getOrDefault(DataLakeConfig.CATALOG_PROXY_MODE,
                props.getOrDefault(DLFProperties.PROXY_MODE, "DLF_ONLY")));

        String ifSetPublic = props.getOrDefault(DLFProperties.Site.ACCESS_PUBLIC,
                props.getOrDefault(DLFProperties.ACCESS_PUBLIC, "false"));
        String publicAccess = props.getOrDefault(DLFProperties.ACCESS_PUBLIC, ifSetPublic);
        String region = props.get(DataLakeConfig.CATALOG_REGION_ID);
        String endpoint = props.getOrDefault(DataLakeConfig.CATALOG_ENDPOINT,
                getDLFEndpont(region, Boolean.parseBoolean(publicAccess)));
        if (!Strings.isNullOrEmpty(region)) {
            props.put(Constants.AWS_REGION, "oss-" + region);
            props.put(Constants.ENDPOINT, endpoint);
        }
        return props;
    }

    private static String getDLFEndpont(String region, boolean publicAccess) {
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
        if (!HMSProperties.GLUE_TYPE.equalsIgnoreCase(metastoreType)) {
            return props;
        }
        // https://docs.aws.amazon.com/general/latest/gr/s3.html
        // Convert:
        // (
        //  "aws.region" = "us-east-1",
        //  "aws.glue.access-key" = "xx",
        //  "aws.glue.secret-key" = "yy"
        // )
        // To:
        // (
        //  "AWS_REGION" = "us-east-1",
        //  "AWS_ENDPOINT" = "s3.us-east-1.amazonaws.com"
        //  "AWS_ACCESS_KEY" = "xx",
        //  "AWS_SCRETE_KEY" = "yy"
        // )
        String region = props.get(AWSGlueConfig.AWS_REGION);
        if (!Strings.isNullOrEmpty(region)) {
            props.put(S3Properties.REGION, region);
            props.put(S3Properties.ENDPOINT, "s3." + region + ".amazonaws.com");
        }

        String ak = props.get(AWSGlueConfig.AWS_GLUE_ACCESS_KEY);
        String sk = props.get(AWSGlueConfig.AWS_GLUE_SECRET_KEY);
        if (!Strings.isNullOrEmpty(ak) && !Strings.isNullOrEmpty(sk)) {
            props.put(S3Properties.ACCESS_KEY, ak);
            props.put(S3Properties.SECRET_KEY, sk);
        }
        return convertToS3Properties(props, credential);
    }
}
