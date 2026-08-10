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

package org.apache.doris.connector.hudi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Statement-scoped scan reuse key construction for Hudi (offline; no table environment needed). */
class HudiScanReuseKeyTest {

    private static HudiTableHandle handle() {
        return new HudiTableHandle.Builder("db", "t", "/warehouse/t", "COPY_ON_WRITE")
                .inputFormat("org.apache.hudi.hadoop.HoodieParquetInputFormat")
                .partitionKeyNames(Arrays.asList("year", "month"))
                .prunedPartitionPaths(Arrays.asList("year=2025/month=01", "year=2025/month=02"))
                .queryInstant("20250429000000000")
                .build();
    }

    private static HudiScanPlanProvider.HudiScanReuseKey key(HudiTableHandle handle) {
        return HudiScanPlanProvider.hudiScanReuseKey(handle);
    }

    @Test
    void sameScanYieldsSameKey() {
        Assertions.assertEquals(key(handle()), key(handle()),
                "two identical handles must produce the same reuse key");
    }

    @Test
    void differentQueryInstantYieldsDifferentKey() {
        HudiTableHandle other = handle().toBuilder()
                .queryInstant("20250430000000000")
                .build();
        Assertions.assertNotEquals(key(handle()), key(other),
                "a different snapshot instant must not reuse the cached ranges");
    }

    @Test
    void incrementalWindowYieldsDifferentKey() {
        HudiTableHandle incremental = handle().toBuilder()
                .beginInstant("20250420000000000")
                .endInstant("20250430000000000")
                .build();
        Assertions.assertNotEquals(key(handle()), key(incremental),
                "an incremental window must not reuse a snapshot scan's ranges");

        HudiTableHandle otherWindow = incremental.toBuilder()
                .endInstant("20250425000000000")
                .build();
        Assertions.assertNotEquals(key(incremental), key(otherWindow),
                "a different incremental window must not reuse the cached ranges");
    }

    @Test
    void incrementalParamsYieldsDifferentKey() {
        HudiTableHandle incrementalBase = handle().toBuilder()
                .beginInstant("20250420000000000")
                .endInstant("20250430000000000")
                .build();
        Map<String, String> params = new HashMap<>();
        params.put("hoodie.datasource.read.incr.path.glob", "/warehouse/t/*/*");
        HudiTableHandle withParams = incrementalBase.toBuilder()
                .incrementalParams(params)
                .build();
        Assertions.assertNotEquals(key(incrementalBase), key(withParams),
                "incremental options must participate in the reuse key");
    }

    @Test
    void differentPrunedPartitionsYieldDifferentKey() {
        HudiTableHandle other = handle().toBuilder()
                .prunedPartitionPaths(Collections.singletonList("year=2025/month=01"))
                .build();
        Assertions.assertNotEquals(key(handle()), key(other),
                "a different pruned partition set must not reuse the cached ranges");
    }
}
