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

package org.apache.doris.datasource.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * {@code getRegionOfEndpoint} derives the region from an endpoint host, so the result must not
 * depend on the case the user wrote for the host or the scheme.
 */
public class S3ResourceCompatTest {

    @Test
    public void testOssRegionIsCaseInsensitive() {
        Assertions.assertEquals("oss-cn-beijing",
                S3ResourceCompat.getRegionOfEndpoint("oss-cn-beijing.aliyuncs.com"));
        Assertions.assertEquals("oss-cn-beijing",
                S3ResourceCompat.getRegionOfEndpoint("OSS-CN-BEIJING.ALIYUNCS.COM"));
        Assertions.assertEquals("oss-cn-beijing",
                S3ResourceCompat.getRegionOfEndpoint("HTTPS://OSS-CN-BEIJING.ALIYUNCS.COM"));
    }

    @Test
    public void testPlainRegionIsCaseInsensitive() {
        Assertions.assertEquals("us-east-1",
                S3ResourceCompat.getRegionOfEndpoint("s3.us-east-1.amazonaws.com"));
        Assertions.assertEquals("us-east-1",
                S3ResourceCompat.getRegionOfEndpoint("HTTPS://S3.US-EAST-1.AMAZONAWS.COM"));
    }

    @Test
    public void testIpEndpointHasNoRegion() {
        Assertions.assertNull(S3ResourceCompat.getRegionOfEndpoint("192.168.0.1:8999"));
    }
}
