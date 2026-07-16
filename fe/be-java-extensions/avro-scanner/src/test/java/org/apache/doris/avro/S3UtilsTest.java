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

package org.apache.doris.avro;

import org.junit.Assert;
import org.junit.Test;

public class S3UtilsTest {

    @Test
    public void testDirectoryBucketClassification() {
        String bucket = "analytics--use1-az4--x-s3";
        Assert.assertTrue(S3Utils.isAwsDirectoryBucket(
                bucket, "https://s3.us-east-1.amazonaws.com"));
        Assert.assertTrue(S3Utils.isAwsDirectoryBucket(bucket, ""));
        Assert.assertFalse(S3Utils.isAwsDirectoryBucket(
                bucket, "https://minio.example.com"));
        Assert.assertFalse(S3Utils.isAwsDirectoryBucket(
                "analytics--x-s3", "https://s3.us-east-1.amazonaws.com"));
    }
}
