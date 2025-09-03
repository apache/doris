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

package org.apache.doris.datasource.property.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Integration test for Google Cloud Storage (GCS) using S3AFileSystem.
 *
 * <p>
 * This test is configurable via system properties for endpoint, region,
 * access key, secret key, and GCS URI. Defaults are provided for convenience.
 * </p>
 *
 * <p>
 * Example usage:
 * <pre>
 * mvn test -Dgcs.endpoint=https://storage.googleapis.com \
 *          -Dgcs.accessKey=ccc \
 *          -Dgcs.secretKey=xxxx \
 *          -Dgcs.uri=gs://wd-test123/sales_data/2025-04-08/part-000000000000.parquet
 * </pre>
 * </p>
 */
@Disabled("Disabled by default. Enable and configure to run against GCS.")
public class GCSIT {

    // Configurable parameters
    private String endpoint;
    private String region;
    private String accessKey;
    private String secretKey;
    private String gcsUri;

    private Configuration hadoopConfig;

    /**
     * Setup method to initialize Hadoop configuration before each test.
     * Values can be overridden via system properties.
     */
    @BeforeEach
    public void setUp() {
        // Load configuration from system properties or use default values
        endpoint = System.getProperty("gcs.endpoint", "https://storage.googleapis.com");
        region = System.getProperty("gcs.region", "us-east1");
        accessKey = System.getProperty("gcs.accessKey", "your-access-key");
        secretKey = System.getProperty("gcs.secretKey", "your-secret-key");
        gcsUri = System.getProperty("gcs.uri", "gs://your-bucket/path/to/file.parquet");

        // Hadoop configuration for S3AFileSystem
        hadoopConfig = new Configuration();
        hadoopConfig.set("fs.gs.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConfig.set("fs.s3a.endpoint", endpoint);
        hadoopConfig.set("fs.s3a.endpoint.region", region);
        hadoopConfig.set("fs.s3a.access.key", accessKey);
        hadoopConfig.set("fs.s3a.secret.key", secretKey);
        hadoopConfig.set("fs.defaultFS", gcsUri);
    }

    /**
     * Test to verify if a GCS file exists and print filesystem scheme.
     *
     * @throws URISyntaxException if the GCS URI is invalid
     * @throws IOException if an I/O error occurs
     */
    @Disabled
    @Test
    public void testGCSFileExists() throws URISyntaxException, IOException {
        FileSystem fs = FileSystem.get(new URI(gcsUri), hadoopConfig);

        System.out.println("FileSystem scheme: " + fs.getScheme());
        System.out.println("File exists: " + fs.exists(new Path(gcsUri)));

        // Assert that the file exists
        Assertions.assertTrue(fs.exists(new Path(gcsUri)), "File should exist in GCS");
    }
}
