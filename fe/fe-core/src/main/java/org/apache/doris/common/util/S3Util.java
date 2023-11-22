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

package org.apache.doris.common.util;

import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.credentials.CloudCredential;
import org.apache.doris.datasource.property.constants.CosProperties;
import org.apache.doris.datasource.property.constants.ObsProperties;
import org.apache.doris.datasource.property.constants.OssProperties;
import org.apache.doris.datasource.property.constants.S3Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

public class S3Util {
    private static final Logger LOG = LogManager.getLogger(S3Util.class);

    public static boolean isObjStorage(String location) {
        return isObjStorageUseS3Client(location)
            // if treat cosn(tencent hadoop-cos) as a s3 file system, may bring incompatible issues
            || (location.startsWith(FeConstants.FS_PREFIX_COS) && !location.startsWith(FeConstants.FS_PREFIX_COSN))
            || location.startsWith(FeConstants.FS_PREFIX_OSS)
            || location.startsWith(FeConstants.FS_PREFIX_OBS);
    }

    private static boolean isObjStorageUseS3Client(String location) {
        return location.startsWith(FeConstants.FS_PREFIX_S3)
                || location.startsWith(FeConstants.FS_PREFIX_S3A)
                || location.startsWith(FeConstants.FS_PREFIX_S3N)
                || location.startsWith(FeConstants.FS_PREFIX_GCS)
                || location.startsWith(FeConstants.FS_PREFIX_BOS);
    }

    private static boolean isS3EndPoint(String location, Map<String, String> props) {
        if (props.containsKey(ObsProperties.ENDPOINT)
                || props.containsKey(OssProperties.ENDPOINT)
                || props.containsKey(CosProperties.ENDPOINT)) {
            return false;
        }
        // wide check range for the compatibility of s3 properties
        return (props.containsKey(S3Properties.ENDPOINT) || props.containsKey(S3Properties.Env.ENDPOINT))
                    && isObjStorage(location);
    }

    /**
     * The converted path is used for FE to get metadata
     * @param location origin location
     * @return metadata location path. just convert when storage is compatible with s3 client.
     */
    public static String convertToS3IfNecessary(String location, Map<String, String> props) {
        LOG.debug("try convert location to s3 prefix: " + location);
        // include the check for multi locations and in a table, such as both s3 and hdfs are in a table.
        if (isS3EndPoint(location, props) || isObjStorageUseS3Client(location)) {
            int pos = location.indexOf("://");
            if (pos == -1) {
                throw new RuntimeException("No '://' found in location: " + location);
            }
            return "s3" + location.substring(pos);
        }
        return normalizedLocation(location, props);
    }

    private static String normalizedLocation(String location, Map<String, String> props) {
        try {
            if (location.startsWith(HdfsResource.HDFS_PREFIX)) {
                return normalizedHdfsPath(location, props);
            }
            return location;
        } catch (URISyntaxException | UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static String normalizedHdfsPath(String location, Map<String, String> props)
            throws URISyntaxException, UnsupportedEncodingException {
        // Hive partition may contain special characters such as ' ', '<', '>' and so on.
        // Need to encode these characters before creating URI.
        // But doesn't encode '/' and ':' so that we can get the correct uri host.
        location = URLEncoder.encode(location, StandardCharsets.UTF_8.name()).replace("%2F", "/").replace("%3A", ":");
        URI normalizedUri = new URI(location);
        // compatible with 'hdfs:///' or 'hdfs:/'
        if (StringUtils.isEmpty(normalizedUri.getHost())) {
            location = URLDecoder.decode(location, StandardCharsets.UTF_8.name());
            String normalizedPrefix = HdfsResource.HDFS_PREFIX + "//";
            String brokenPrefix = HdfsResource.HDFS_PREFIX + "/";
            if (location.startsWith(brokenPrefix) && !location.startsWith(normalizedPrefix)) {
                location = location.replace(brokenPrefix, normalizedPrefix);
            }
            // Need add hdfs host to location
            String host = props.get(HdfsResource.DSF_NAMESERVICES);
            if (StringUtils.isNotEmpty(host)) {
                // Replace 'hdfs://key/' to 'hdfs://name_service/key/'
                // Or hdfs:///abc to hdfs://name_service/abc
                return location.replace(normalizedPrefix, normalizedPrefix + host + "/");
            } else {
                // 'hdfs://null/' equals the 'hdfs:///'
                if (location.startsWith(HdfsResource.HDFS_PREFIX + "///")) {
                    // Do not support hdfs:///location
                    throw new RuntimeException("Invalid location with empty host: " + location);
                } else {
                    // Replace 'hdfs://key/' to '/key/', try access local NameNode on BE.
                    return location.replace(normalizedPrefix, "/");
                }
            }
        }
        return URLDecoder.decode(location, StandardCharsets.UTF_8.name());
    }

    /**
     * The converted path is used for BE
     * @param location origin split path
     * @return BE scan range path
     */
    public static Path toScanRangeLocation(String location, Map<String, String> props) {
        // All storage will use s3 client on BE.
        if (isObjStorage(location)) {
            int pos = location.indexOf("://");
            if (pos == -1) {
                throw new RuntimeException("No '://' found in location: " + location);
            }
            if (isHdfsOnOssEndpoint(location)) {
                // if hdfs service is enabled on oss, use oss location
                // example: oss://examplebucket.cn-shanghai.oss-dls.aliyuncs.com/dir/file/0000.orc
                location = "oss" + location.substring(pos);
            } else {
                location = "s3" + location.substring(pos);
            }
        }
        return new Path(normalizedLocation(location, props));
    }

    public static boolean isHdfsOnOssEndpoint(String location) {
        // example: cn-shanghai.oss-dls.aliyuncs.com contains the "oss-dls.aliyuncs".
        // https://www.alibabacloud.com/help/en/e-mapreduce/latest/oss-kusisurumen
        return location.contains("oss-dls.aliyuncs");
    }

    public static S3Client buildS3Client(URI endpoint, String region, CloudCredential credential) {
        StaticCredentialsProvider scp;
        AwsCredentials awsCredential;
        if (!credential.isTemporary()) {
            awsCredential = AwsBasicCredentials.create(credential.getAccessKey(), credential.getSecretKey());
        } else {
            awsCredential = AwsSessionCredentials.create(credential.getAccessKey(), credential.getSecretKey(),
                        credential.getSessionToken());
        }
        scp = StaticCredentialsProvider.create(awsCredential);
        EqualJitterBackoffStrategy backoffStrategy = EqualJitterBackoffStrategy
                .builder()
                .baseDelay(Duration.ofSeconds(1))
                .maxBackoffTime(Duration.ofMinutes(1))
                .build();
        // retry 3 time with Equal backoff
        RetryPolicy retryPolicy = RetryPolicy
                .builder()
                .numRetries(3)
                .backoffStrategy(backoffStrategy)
                .build();
        ClientOverrideConfiguration clientConf = ClientOverrideConfiguration
                .builder()
                // set retry policy
                .retryPolicy(retryPolicy)
                // using AwsS3V4Signer
                .putAdvancedOption(SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create())
                .build();
        return S3Client.builder()
                .httpClient(UrlConnectionHttpClient.create())
                .endpointOverride(endpoint)
                .credentialsProvider(scp)
                .region(Region.of(region))
                .overrideConfiguration(clientConf)
                // disable chunkedEncoding because of bos not supported
                // use virtual hosted-style access
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(false)
                        .build())
                .build();
    }
}
