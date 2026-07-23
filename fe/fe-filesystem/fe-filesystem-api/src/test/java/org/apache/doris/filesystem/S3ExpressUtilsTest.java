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

class S3ExpressUtilsTest {

    @Test
    void parsesCompleteDirectoryBucketName() {
        Assertions.assertEquals("usw2-az1", S3ExpressUtils.directoryBucketZoneId(
                "analytics--usw2-az1--x-s3").orElseThrow());
        Assertions.assertFalse(S3ExpressUtils.isDirectoryBucket("analytics--usw2-azx--x-s3"));
        Assertions.assertFalse(S3ExpressUtils.isDirectoryBucket("analytics--x-s3"));
        Assertions.assertFalse(S3ExpressUtils.isDirectoryBucket("--usw2-az1--x-s3"));
    }

    @Test
    void convertsObjectPrefixToContainingDirectory() {
        Assertions.assertEquals("data/", S3ExpressUtils.directoryPrefix("data/file.csv"));
        Assertions.assertEquals("data/", S3ExpressUtils.directoryPrefix("data/"));
        Assertions.assertEquals("", S3ExpressUtils.directoryPrefix("file.csv"));
        Assertions.assertNull(S3ExpressUtils.directoryPrefix(null));
    }
}
