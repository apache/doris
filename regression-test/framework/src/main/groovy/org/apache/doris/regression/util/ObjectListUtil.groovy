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

import org.apache.doris.regression.suite.Suite

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.model.ObjectListing

interface ListObjectsFileNames {
    public boolean isEmpty(String tableName, String tableId);
    public Set<String> listObjects(String userName, String userId);
};

class AwsListObjectsFileNames implements ListObjectsFileNames {
    private String ak;
    private String sk;
    private String endpoint;
    private String region;
    private String prefix;
    private String bucket;
    private Suite suite;
    private AmazonS3Client s3Client;
    public AwsListObjectsFileNames(String ak, String sk, String endpoint, String region, String prefix, String bucket, Suite suite) {
        this.ak = ak;
        this.sk = sk;
        this.endpoint = endpoint;
        this.region = region;
        this.prefix = prefix;
        this.bucket = bucket;
        this.suite = suite;
        def credentials = new BasicAWSCredentials(ak, sk)
        def endpointConfiguration = new EndpointConfiguration(endpoint, region)
        this.s3Client = AmazonS3ClientBuilder.standard().withEndpointConfiguration(endpointConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials)).build()
    }

    public boolean isEmpty(String tableName, String tableId) {
        def objectListing = s3Client.listObjects(
            new ListObjectsRequest().withMaxKeys(1).withBucketName(bucket).withPrefix("${prefix}/data/${tabletId}/"))

        suite.getLogger().info("tableName: ${tableName}, tabletId:${tabletId}, objectListing:${objectListing.getObjectSummaries()}".toString())
        return objectListing.getObjectSummaries().isEmpty();
    }

    public boolean isEmpty(String userName, String userId, String fileName) {
        def objectListing = s3Client.listObjects(
            new ListObjectsRequest().withMaxKeys(1)
                .withBucketName(bucket)
                .withPrefix("${prefix}/stage/${userName}/${userId}/${fileName}"))

        suite.getLogger().info("${prefix}/stage/${userName}/${userId}/${fileName}, objectListing:${objectListing.getObjectSummaries()}".toString())
        return objectListing.getObjectSummaries().isEmpty();
    }

    public Set<String> listObjects(String userName, String userId) {
        def objectListing = s3Client.listObjects(
            new ListObjectsRequest()
                    .withBucketName(bucket)
                    .withPrefix("${prefix}/stage/${userName}/${userId}/"))

        suite.getLogger().info("${prefix}/stage/${userName}/${userId}/, objectListing:${objectListing.getObjectSummaries()}".toString())
        Set<String> fileNames = new HashSet<>()
        for (def os: objectListing.getObjectSummaries()) {
            def split = os.key.split("/")
            if (split.length <= 0 ) {
                continue
            }
            fileNames.add(split[split.length-1])
        }
        return fileNames
    }
}