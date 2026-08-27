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

package org.apache.doris.connector.hms;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class HmsClientConfigBatchTest {

    @Test
    public void usesBoundedDefaults() {
        HmsClientConfig config = new HmsClientConfig(Collections.emptyMap(), 0);
        Assertions.assertEquals(5000, config.getPartitionBatchSize());
        Assertions.assertEquals(30_000L, config.getPartitionBatchFallbackTimeoutMillis());
    }

    @Test
    public void parsesAndValidatesBatchProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(HmsClientConfig.PARTITION_BATCH_SIZE_KEY, "321");
        properties.put(HmsClientConfig.PARTITION_BATCH_FALLBACK_TIMEOUT_MS_KEY, "4567");
        HmsClientConfig config = new HmsClientConfig(properties, 1);
        Assertions.assertEquals(321, config.getPartitionBatchSize());
        Assertions.assertEquals(4567L, config.getPartitionBatchFallbackTimeoutMillis());

        properties.put(HmsClientConfig.PARTITION_BATCH_SIZE_KEY, "0");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HmsClientConfig(properties, 1));
        properties.put(HmsClientConfig.PARTITION_BATCH_SIZE_KEY, "not-a-number");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HmsClientConfig(properties, 1));
    }
}
