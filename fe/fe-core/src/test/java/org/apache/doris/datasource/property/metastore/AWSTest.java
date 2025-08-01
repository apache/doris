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

package org.apache.doris.datasource.property.metastore;

import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Disabled("Only run manually")
public class AWSTest {
    private static final String AWS_ACCESS_KEY_ID = "YOUR_ACCESS_KEY_ID"; // Replace with actual access key
    private static final String AWS_SECRET_ACCESS_KEY = "YOUR_SECRET_ACCESS_KEY"; // Replace with actual secret key
    private static final String AWS_REGION = "ap-northeast-1"; // Replace with actual region
    private static final String S3_BUCKET_NAME = "test"; // Replace with actual bucket name
    private static final String GLUE_CATALOG_NAME = "test"; // Replace with actual catalog name
    private static final String S3A_PATH = "s3a://aws-glue-assets-123-ap-southeast-1/"; // Replace with actual S3A path

    @BeforeEach
    public void setUp() {
        // Set AWS credentials and region using system properties
        System.setProperty("aws.accessKeyId", AWS_ACCESS_KEY_ID);
        System.setProperty("aws.secretKey", AWS_SECRET_ACCESS_KEY);
        System.setProperty("aws.region", AWS_REGION);
    }

    @Test
    public void testAWSS3() throws IOException {
        // Create S3 client
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(AWS_REGION) // Set the region
                .build();

        // List S3 buckets
        s3Client.listBuckets().forEach(bucket -> {
            System.out.println("Bucket Name: " + bucket.getName());
        });

        // List objects in the specified S3 bucket
        ObjectListing list = s3Client.listObjects(S3_BUCKET_NAME, "");
        list.getObjectSummaries().forEach(objectSummary -> {
            System.out.println("Object Key: " + objectSummary.getKey());
        });
    }

    @Test
    public void testGlueCatalog() throws IOException {
        // Initialize Glue catalog with properties
        Map<String, String> catalogProps = new HashMap<>();
        GlueCatalog glueCatalog = new GlueCatalog();
        glueCatalog.initialize(GLUE_CATALOG_NAME, catalogProps);

        // List namespaces in the Glue catalog
        glueCatalog.listNamespaces(Namespace.empty()).forEach(namespace -> {
            System.out.println("Namespace: " + namespace);
        });

        // Configure Hadoop FileSystem to use S3A with SystemPropertiesCredentialsProvider
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", SystemPropertiesCredentialsProvider.class.getName()); // Use SystemPropertiesCredentialsProvider
        conf.set("fs.defaultFS", S3A_PATH);
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        // Get the FileSystem and list files in the specified S3A path
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> a = fs.listFiles(new Path(S3A_PATH), true);
        while (a.hasNext()) {
            LocatedFileStatus next = a.next();
            System.out.println(next.getPath());
        }
    }
}
