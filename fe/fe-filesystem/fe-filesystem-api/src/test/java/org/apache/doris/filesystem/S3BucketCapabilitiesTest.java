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

package org.apache.doris.filesystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class S3BucketCapabilitiesTest {

    @Test
    void resolvesOfficialDirectoryBucket() {
        S3BucketCapabilities capabilities = S3BucketCapabilities.resolve(
                "analytics--use1-az4--x-s3", "https://s3.us-east-1.amazonaws.com");

        Assertions.assertTrue(capabilities.isDirectoryBucket());
        Assertions.assertTrue(capabilities.officialAwsService());
        Assertions.assertEquals(S3BucketCapabilities.EndpointMode.AWS_SDK_RULES,
                capabilities.endpointMode());
        Assertions.assertEquals(S3BucketCapabilities.ChecksumPolicy.CRC32C,
                capabilities.checksumPolicy());
        Assertions.assertFalse(capabilities.supportsStartAfter());
        Assertions.assertFalse(capabilities.listIsLexicographic());
        Assertions.assertFalse(capabilities.supportsVersioning());
        Assertions.assertFalse(capabilities.supportsPresign());
    }

    @Test
    void keepsCustomEndpointGeneralPurpose() {
        S3BucketCapabilities capabilities = S3BucketCapabilities.resolve(
                "analytics--use1-az4--x-s3", "https://minio.example.com");

        Assertions.assertFalse(capabilities.isDirectoryBucket());
        Assertions.assertFalse(capabilities.officialAwsService());
        Assertions.assertEquals(S3BucketCapabilities.ChecksumPolicy.CONTENT_MD5,
                capabilities.checksumPolicy());
    }

    @Test
    void rejectsUnsupportedDirectoryConfiguration() {
        S3BucketCapabilities capabilities = S3BucketCapabilities.resolve(
                "analytics--use1-az4--x-s3", "https://s3.dualstack.us-east-1.amazonaws.com");

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> capabilities.validateDirectoryConfiguration(
                        "https://s3.dualstack.us-east-1.amazonaws.com", "us-east-1", false));
    }

    @Test
    void validatesRegionAndZone() {
        S3BucketCapabilities capabilities = S3BucketCapabilities.resolve(
                "analytics--use1-az4--x-s3",
                "https://analytics--use1-az4--x-s3.s3express-use1-az4.us-east-1.amazonaws.com");

        Assertions.assertDoesNotThrow(() -> capabilities.validateDirectoryConfiguration(
                "https://analytics--use1-az4--x-s3.s3express-use1-az4.us-east-1.amazonaws.com",
                "us-east-1", false));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> capabilities.validateDirectoryConfiguration(
                        "https://analytics--use1-az4--x-s3.s3express-use1-az4.us-east-1.amazonaws.com",
                        "us-west-2", false));

        S3BucketCapabilities wrongZone = S3BucketCapabilities.resolve(
                "analytics--use1-az4--x-s3",
                "https://analytics--use1-az4--x-s3.s3express-use1-az5.us-east-1.amazonaws.com");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> wrongZone.validateDirectoryConfiguration(
                        "https://analytics--use1-az4--x-s3.s3express-use1-az5.us-east-1.amazonaws.com",
                        "us-east-1", false));
    }

    @Test
    void recognizesSdkRoutedDirectoryBucketUri() {
        Assertions.assertTrue(S3BucketCapabilities.isDirectoryBucketUri(
                "s3a://analytics--use1-az4--x-s3/warehouse", ""));
        Assertions.assertFalse(S3BucketCapabilities.isDirectoryBucketUri(
                "s3a://analytics--use1-az4--x-s3/warehouse", "https://minio.example.com"));
    }
}
