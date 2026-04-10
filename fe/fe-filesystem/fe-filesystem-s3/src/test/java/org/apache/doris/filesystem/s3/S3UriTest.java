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

package org.apache.doris.filesystem.s3;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class S3UriTest {

    @Test
    void parseS3VirtualHostedStyle() {
        S3Uri uri = S3Uri.parse("s3://my-bucket/path/to/file.csv", false);
        Assertions.assertEquals("my-bucket", uri.bucket());
        Assertions.assertEquals("path/to/file.csv", uri.key());
    }

    @Test
    void parseS3aScheme() {
        S3Uri uri = S3Uri.parse("s3a://my-bucket/key", false);
        Assertions.assertEquals("my-bucket", uri.bucket());
        Assertions.assertEquals("key", uri.key());
    }

    @Test
    void parseS3nScheme() {
        S3Uri uri = S3Uri.parse("s3n://my-bucket/dir/file", false);
        Assertions.assertEquals("my-bucket", uri.bucket());
        Assertions.assertEquals("dir/file", uri.key());
    }

    @Test
    void parseBucketOnly() {
        S3Uri uri = S3Uri.parse("s3://my-bucket", false);
        Assertions.assertEquals("my-bucket", uri.bucket());
        Assertions.assertEquals("", uri.key());
    }

    @Test
    void parseNormalizesDoubleSlashInKey() {
        S3Uri uri = S3Uri.parse("s3://bucket//path", false);
        Assertions.assertEquals("bucket", uri.bucket());
        Assertions.assertEquals("path", uri.key());
    }

    @Test
    void parseHttpsEndpoint() {
        S3Uri uri = S3Uri.parse("https://s3.amazonaws.com/my-bucket/key", false);
        Assertions.assertEquals("s3.amazonaws.com", uri.bucket());
        Assertions.assertEquals("my-bucket/key", uri.key());
    }

    @Test
    void nullPathThrows() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> S3Uri.parse(null, false));
    }

    @Test
    void noSchemeThrows() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> S3Uri.parse("bucket/key", false));
    }
}
