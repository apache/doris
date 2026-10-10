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

package org.apache.doris.filesystem.obs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * The Huawei OBS endpoint is matched by DNS hostname, so classification must be case-insensitive
 * (same class of defect as DORIS-29438 / OSS).
 */
class ObsFileSystemProviderTest {

    private final ObsFileSystemProvider provider = new ObsFileSystemProvider();

    @Test
    void supports_isCaseInsensitiveForObsEndpoint() {
        Map<String, String> lower = new HashMap<>();
        lower.put("obs.endpoint", "obs.cn-north-4.myhuaweicloud.com");
        Map<String, String> upper = new HashMap<>();
        upper.put("obs.endpoint", "OBS.CN-NORTH-4.MYHUAWEICLOUD.COM");

        Assertions.assertTrue(provider.supports(lower));
        Assertions.assertTrue(provider.supports(upper), upper.toString());
    }

    @Test
    void supportsGuess_isCaseInsensitiveForObsEndpointAndUri() {
        Map<String, String> upperEndpoint = new HashMap<>();
        upperEndpoint.put("s3.endpoint", "OBS.CN-NORTH-4.MYHUAWEICLOUD.COM");
        Assertions.assertTrue(provider.supportsGuess(upperEndpoint), upperEndpoint.toString());

        Map<String, String> upperUri = new HashMap<>();
        upperUri.put("uri", "obs://my-bucket.OBS.CN-NORTH-4.MYHUAWEICLOUD.COM/test_obs/000000_0");
        Assertions.assertTrue(provider.supportsGuess(upperUri), upperUri.toString());
    }

    @Test
    void supportsGuess_rejectsForeignEndpointRegardlessOfCase() {
        Map<String, String> aws = new HashMap<>();
        aws.put("s3.endpoint", "HTTPS://S3.US-EAST-1.AMAZONAWS.COM");

        Assertions.assertFalse(provider.supportsGuess(aws));
        Assertions.assertFalse(provider.supports(aws));
    }
}
