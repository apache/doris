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

import org.junit.jupiter.api.Test
import static org.junit.jupiter.api.Assertions.*

class S3TestStorageTest {
    @Test
    void legacyKeysRemainUnchangedWithoutAPrefix() {
        assertEquals("outfile/part_", S3TestStorage.writeKey([:], "outfile/part_"))
    }

    @Test
    void generatedObjectsAreScopedWithoutChangingPublicFixtureConfiguration() {
        Map config = [s3BucketName: "public-fixtures", s3WriteBucketName: "private-results", s3WritePrefix: "runs/123/"]
        assertEquals("runs/123/outfile/part_", S3TestStorage.writeKey(config, "outfile/part_"))
        assertEquals("runs/123/outfile/", S3TestStorage.writeKey(config, "outfile/"))
        assertEquals("public-fixtures", config.s3BucketName)
        assertEquals("private-results", config.s3WriteBucketName)
    }

    @Test
    void malformedOrEscapingPathsFailBeforeAnObjectOperation() {
        for (String key : ["/absolute", "../other-build", "a/../other", "a/./b", ""]) {
            assertThrows(IllegalArgumentException) { S3TestStorage.writeKey([s3WritePrefix: "runs/123"], key) }
        }
        for (String prefix : ["/absolute", "../other", "runs/../other", "/"]) {
            assertThrows(IllegalArgumentException) { S3TestStorage.writeKey([s3WritePrefix: prefix], "outfile/part_") }
        }
    }
}
