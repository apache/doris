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

import java.util.Optional;

/**
 * The endpoint host is a DNS name, so the region derived from it must not depend on the case the
 * user wrote ({@code oss-cn-beijing.aliyuncs.com} == {@code OSS-CN-BEIJING.ALIYUNCS.COM}).
 */
class LegacyS3UriTest {

    @Test
    void ossRegionIsCaseInsensitive() {
        LegacyS3Uri lower = LegacyS3Uri.create(
                "https://my-bucket.oss-cn-bejing.aliyuncs.com/resources/doc.txt", false, false);
        LegacyS3Uri upper = LegacyS3Uri.create(
                "https://my-bucket.OSS-CN-BEJING.ALIYUNCS.COM/resources/doc.txt", false, false);

        Assertions.assertEquals(Optional.of("oss-cn-bejing"), lower.getRegion());
        Assertions.assertEquals(Optional.of("oss-cn-bejing"), upper.getRegion());
    }

    @Test
    void plainRegionIsCaseInsensitive() {
        LegacyS3Uri lower = LegacyS3Uri.create(
                "https://my-bucket.s3.us-west-1.amazonaws.com/resources/doc.txt", false, false);
        LegacyS3Uri upper = LegacyS3Uri.create(
                "https://my-bucket.S3.US-WEST-1.AMAZONAWS.COM/resources/doc.txt", false, false);

        Assertions.assertEquals(Optional.of("us-west-1"), lower.getRegion());
        Assertions.assertEquals(Optional.of("us-west-1"), upper.getRegion());
    }
}
