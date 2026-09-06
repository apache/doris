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

package org.apache.doris.connector.iceberg.dlf;

import org.apache.doris.connector.iceberg.dlf.client.DLFCachedClientPool;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
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
import java.util.Map;

/** Aliyun DLF Iceberg catalog backed by the DLF Hive-compatible metastore and OSS. */
public class DLFCatalog extends HiveCompatibleCatalog {

    private final S3CompatibleFileSystemProperties ossStorage;

    public DLFCatalog(S3CompatibleFileSystemProperties ossStorage) {
        this.ossStorage = ossStorage;
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
        super.initialize(name, initializeFileIO(properties, conf),
                new DLFCachedClientPool(conf, properties), properties);
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
        return new DLFTableOperations(conf, clients, fileIO, catalogName,
                tableIdentifier.namespace().level(0), tableIdentifier.name());
    }

    @Override
    protected FileIO initializeFileIO(Map<String, String> properties, Configuration hadoopConf) {
        String region = ossStorage.getRegion();
        boolean usePathStyle = Boolean.parseBoolean(ossStorage.getUsePathStyle());
        URI endpoint = URI.create(toS3CompatibleEndpoint(ossStorage.getEndpoint(), region));
        AwsCredentialsProvider credentials = buildCredentials(ossStorage);
        FileIO io = new S3FileIO(() -> buildOssS3Client(endpoint, region, credentials, usePathStyle));
        io.initialize(properties);
        return io;
    }

    public static String toS3CompatibleEndpoint(String endpoint, String region) {
        String endpointWithScheme = endpoint.contains("://") ? endpoint : "http://" + endpoint;
        URI endpointUri = URI.create(endpointWithScheme);
        String host = endpointUri.getHost();
        String publicHost = "oss-" + region + ".aliyuncs.com";
        String internalHost = "oss-" + region + "-internal.aliyuncs.com";
        // Only an unprefixed OSS host is rewritten so repeated normalization cannot produce s3.s3.
        if (publicHost.equalsIgnoreCase(host) || internalHost.equalsIgnoreCase(host)) {
            return endpointWithScheme.replace(host, "s3." + host);
        }
        return endpointWithScheme;
    }

    private static AwsCredentialsProvider buildCredentials(S3CompatibleFileSystemProperties oss) {
        if (!oss.hasStaticCredentials()) {
            return DefaultCredentialsProvider.create();
        }
        if (StringUtils.isBlank(oss.getSessionToken())) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(oss.getAccessKey(), oss.getSecretKey()));
        }
        return StaticCredentialsProvider.create(
                AwsSessionCredentials.create(oss.getAccessKey(), oss.getSecretKey(), oss.getSessionToken()));
    }

    private static S3Client buildOssS3Client(URI endpoint, String region, AwsCredentialsProvider credentials,
            boolean usePathStyle) {
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .numRetries(3)
                .backoffStrategy(EqualJitterBackoffStrategy.builder()
                        .baseDelay(Duration.ofSeconds(1))
                        .maxBackoffTime(Duration.ofMinutes(1))
                        .build())
                .build();
        ClientOverrideConfiguration clientConf = ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .putAdvancedOption(SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create())
                .build();
        return S3Client.builder()
                .httpClient(UrlConnectionHttpClient.builder()
                        .socketTimeout(Duration.ofSeconds(30))
                        .connectionTimeout(Duration.ofSeconds(30))
                        .build())
                .endpointOverride(endpoint)
                .credentialsProvider(credentials)
                .region(Region.of(region))
                .overrideConfiguration(clientConf)
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(usePathStyle)
                        .build())
                .build();
    }
}
