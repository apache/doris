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

/**
 * Aliyun DLF Iceberg catalog (hive-compatible). Ported from the legacy fe-core
 * {@code org.apache.doris.datasource.iceberg.dlf.DLFCatalog} into the plugin connector (P6-T07).
 *
 * <p>The three fe-core dependencies of the legacy {@code initializeFileIO} ({@code OSSProperties} /
 * {@code CloudCredential} / {@code S3Util}) are severed: the OSS endpoint/region/credentials are read from the
 * typed fe-filesystem {@link S3CompatibleFileSystemProperties} (D-061, injected by the connector), and the
 * S3-compatible client is built inline by {@link #buildOssS3Client} — a faithful replica of
 * {@code S3Util.buildS3Client(endpoint, region, credential, isUsePathStyle)}.
 */
public class DLFCatalog extends HiveCompatibleCatalog {

    private final S3CompatibleFileSystemProperties ossStorage;

    public DLFCatalog(S3CompatibleFileSystemProperties ossStorage) {
        this.ossStorage = ossStorage;
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
        super.initialize(name, initializeFileIO(properties, conf), new DLFCachedClientPool(this.conf, properties));
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
        String dbName = tableIdentifier.namespace().level(0);
        String tableName = tableIdentifier.name();
        return new DLFTableOperations(this.conf, this.clients, this.fileIO, this.catalogName, dbName, tableName);
    }

    @Override
    protected FileIO initializeFileIO(Map<String, String> properties, Configuration hadoopConf) {
        // OSS object storage backs the DLF catalog's data files. Read its endpoint/region/credentials from the
        // typed fe-filesystem storage (D-061) instead of the legacy OSSProperties/CloudCredential/S3Util.
        String region = ossStorage.getRegion();
        boolean isUsePathStyle = Boolean.parseBoolean(ossStorage.getUsePathStyle());
        URI endpointUri = URI.create(toS3CompatibleEndpoint(ossStorage.getEndpoint(), region));
        AwsCredentialsProvider credentials = buildCredentials(ossStorage);
        // s3 file io just supports s3-like endpoint
        FileIO io = new S3FileIO(() -> buildOssS3Client(endpointUri, region, credentials, isUsePathStyle));
        io.initialize(properties);
        return io;
    }

    /**
     * Rewrites a native OSS endpoint ({@code oss-<region>.*}) to its S3-compatible form ({@code s3.oss-<region>.*})
     * and ensures a scheme, mirroring the legacy {@code DLFCatalog.initializeFileIO} endpoint munging. PURE —
     * package-private for unit testing.
     */
    static String toS3CompatibleEndpoint(String endpoint, String region) {
        String s3Endpoint = endpoint.replace("oss-" + region, "s3.oss-" + region);
        if (!s3Endpoint.contains("://")) {
            s3Endpoint = "http://" + s3Endpoint;
        }
        return s3Endpoint;
    }

    private static AwsCredentialsProvider buildCredentials(S3CompatibleFileSystemProperties oss) {
        if (oss.hasStaticCredentials()) {
            if (StringUtils.isBlank(oss.getSessionToken())) {
                return StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(oss.getAccessKey(), oss.getSecretKey()));
            }
            return StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(oss.getAccessKey(), oss.getSecretKey(), oss.getSessionToken()));
        }
        // Legacy fell back to an explicit provider chain when AK/SK were absent; the SDK default chain is the
        // standard equivalent. DLF requires OSS credentials, so this branch is effectively unreachable; same
        // family as the documented T06 PROVIDER_CHAIN deviation (UT-invisible, P6.6 docker gate).
        return DefaultCredentialsProvider.create();
    }

    /**
     * Replicates legacy {@code S3Util.buildS3Client(endpoint, region, credential, isUsePathStyle)}: a
     * UrlConnection HTTP client (30s socket/connection timeouts), the endpoint override, the supplied
     * credentials, the region, a 3-retry equal-jitter policy plus the {@code AwsS3V4} signer, and chunked
     * encoding DISABLED (required by OSS/bos) with the configured path-style addressing.
     */
    private static S3Client buildOssS3Client(URI endpoint, String region, AwsCredentialsProvider credentials,
            boolean isUsePathStyle) {
        EqualJitterBackoffStrategy backoffStrategy = EqualJitterBackoffStrategy.builder()
                .baseDelay(Duration.ofSeconds(1))
                .maxBackoffTime(Duration.ofMinutes(1))
                .build();
        // retry 3 time with Equal backoff
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .numRetries(3)
                .backoffStrategy(backoffStrategy)
                .build();
        ClientOverrideConfiguration clientConf = ClientOverrideConfiguration.builder()
                // set retry policy
                .retryPolicy(retryPolicy)
                // using AwsS3V4Signer
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
                // disable chunkedEncoding because of bos not supported
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(isUsePathStyle)
                        .build())
                .build();
    }
}
