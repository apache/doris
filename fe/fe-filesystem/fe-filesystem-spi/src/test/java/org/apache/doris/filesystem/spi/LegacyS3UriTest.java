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

package org.apache.doris.filesystem.spi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

class LegacyS3UriTest {

    @ParameterizedTest
    @ValueSource(strings = {"dir//file.csv", "dir/./file.csv", "dir/sub/../file.csv",
            "./file.csv", "dir/file.csv/", "dir/.../file.csv", "dir/.", "dir/.."})
    void testPreservesObjectKeyPathSegments(String key) {
        for (LegacyS3Uri uri : new LegacyS3Uri[] {
                LegacyS3Uri.create("s3://bucket/" + key, false, false),
                LegacyS3Uri.create("https://bucket.s3.us-west-1.amazonaws.com/" + key, false, false),
                LegacyS3Uri.create("https://s3.us-west-1.amazonaws.com/bucket/" + key, true, false)}) {
            Assertions.assertEquals("bucket", uri.getBucket());
            Assertions.assertEquals(key, uri.getKey());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"dir/..", "."})
    void testEndpointDerivationForLiteralDotKey(String key) {
        Assertions.assertEquals("s3.us-west-1.amazonaws.com", LegacyS3Uri.deriveEndpointQuietly(
                Map.of("uri", "https://bucket.s3.us-west-1.amazonaws.com/" + key), "false", "false"));
        Assertions.assertEquals("s3.us-west-1.amazonaws.com", LegacyS3Uri.deriveEndpointQuietly(
                Map.of("uri", "https://s3.us-west-1.amazonaws.com/bucket/" + key), "true", "false"));
    }

    @Test
    void testMissingKeyStillFails() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LegacyS3Uri.create("s3://bucket/", false, false));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LegacyS3Uri.create("https://bucket.s3.us-west-1.amazonaws.com/", false, false));
        Assertions.assertNull(LegacyS3Uri.deriveEndpointQuietly(
                Map.of("uri", "https://bucket.s3.us-west-1.amazonaws.com/"), "false", "false"));
    }
}
