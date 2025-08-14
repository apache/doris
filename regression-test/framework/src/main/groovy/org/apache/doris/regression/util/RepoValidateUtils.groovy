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

package org.apache.doris.regression.util

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsRequest
import org.apache.doris.regression.Config;
import org.apache.doris.regression.suite.Suite;

import com.amazonaws.services.s3.AmazonS3Client;
import org.slf4j.Logger
import org.slf4j.LoggerFactory;

enum ValidateVersion {
    V2_1("2_1_X"),
    V3_0("3_0_X")

    public final String version;

    ValidateVersion(String version) {
        this.version = version
    }
}

abstract class RepoValidate {
    final public Suite suite
    final public Config config
    final public String version
    RepoValidate(Suite suite, String version, Config config) {
        this.suite = suite
        this.config = config
        this.version = ValidateVersion.valueOf("V${version.replace('.', '_')}").version
    }
    abstract String findMatchingRepoName(String suiteName);
};

class S3RepoValidate extends RepoValidate {
    final Logger logger = LoggerFactory.getLogger(S3RepoValidate.class)
    
    public AmazonS3Client s3Client;

    S3RepoValidate(Suite suite, String version, Config config) {
        super(suite, version, config)
        this.s3Client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(suite.getS3Endpoint(), suite.getS3Region())
                )
                .withCredentials(
                        new AWSStaticCredentialsProvider(
                                new BasicAWSCredentials(suite.getS3AK(), suite.getS3SK())
                        )
                )
                .build() as AmazonS3Client
    }

    String findMatchingRepoName(String suiteName) {
        String prefix = "${config.validateBackupPrefix}/${version}".trim()
        String normalizedPrefix = prefix.endsWith("/") ? prefix : prefix + "/"
        def bucketPath = "${suite.getS3BucketName()}/${prefix}"
        try {
            def request = new ListObjectsRequest()
                            .withBucketName(suite.getS3BucketName())
                            .withDelimiter("/")
                            .withPrefix(normalizedPrefix)

            def allCommonPrefixes = new ArrayList<String>()
            def totalDirs = 0

            while (true) {
                def result = s3Client.listObjects(request)
                def commonPrefixes = result.getCommonPrefixes()
                totalDirs += commonPrefixes.size()
                allCommonPrefixes.addAll(commonPrefixes)

                if (!result.isTruncated()) {
                    break
                }

                request.setMarker(result.getNextMarker())
            }

            logger.info("Found ${totalDirs} dir in path: ${bucketPath}")
            if (totalDirs == 0) {
                logger.error("No directories found in path: ${bucketPath}")
                null
            }

            def matchingKey = allCommonPrefixes.find { it.startsWith("${prefix}/${suiteName}_repo_") }
                    ?.with { key ->
                        key.substring(normalizedPrefix.length()).replaceAll('/+$', '')
                    }
            matchingKey // null if not found
        } catch (Exception e) {
            logger.error("Failed to list directories in path: ${bucketPath}", e)
            null
        }
    }
}
