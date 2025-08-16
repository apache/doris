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

package org.apache.doris.common.plugin;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class S3PluginDownloaderTest {

    @Test
    public void testS3ConfigCreation() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "http://s3.amazonaws.com", "us-west-2", "test-bucket", "access-key", "secret-key");

        Assertions.assertEquals("http://s3.amazonaws.com", config.endpoint);
        Assertions.assertEquals("us-west-2", config.region);
        Assertions.assertEquals("test-bucket", config.bucket);
        Assertions.assertEquals("access-key", config.accessKey);
        Assertions.assertEquals("secret-key", config.secretKey);
    }

    @Test
    public void testS3ConfigToString() {
        S3PluginDownloader.S3Config config = new S3PluginDownloader.S3Config(
                "http://s3.amazonaws.com", "us-west-2", "test-bucket", "access-key", "secret-key");

        String configStr = config.toString();

        // Should contain basic info but mask secret key
        Assertions.assertTrue(configStr.contains("s3.amazonaws.com"));
        Assertions.assertTrue(configStr.contains("us-west-2"));
        Assertions.assertTrue(configStr.contains("test-bucket"));
        Assertions.assertTrue(configStr.contains("***")); // Access key should be masked
        Assertions.assertFalse(configStr.contains("access-key")); // Actual key should not appear
    }
}
