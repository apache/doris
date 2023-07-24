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

import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.credentials.CloudCredential;

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

import java.net.URI;
import java.time.Duration;

public class S3Util {
    private static final Logger LOG = LogManager.getLogger(S3Util.class);

    public static boolean isObjStorage(String location) {
        return isObjStorageUseS3Client(location)
                || location.startsWith(FeConstants.FS_PREFIX_COS)
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

    /**
     * The converted path is used for FE to get metadata
     * @param location origin location
     * @return metadata location path. just convert when storage is compatible with s3 client.
     */
    public static String convertToS3IfNecessary(String location) {
        LOG.debug("try convert location to s3 prefix: " + location);
        if (isObjStorageUseS3Client(location)) {
            int pos = location.indexOf("://");
            if (pos == -1) {
                throw new RuntimeException("No '://' found in location: " + location);
            }
            return "s3" + location.substring(pos);
        }
        return location;
    }

    /**
     * The converted path is used for BE
     * @param location origin split path
     * @return BE scan range path
     */
    public static Path toScanRangeLocation(String location) {
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
        return new Path(location);
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
