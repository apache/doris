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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.cache.ConnectorTableKey;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PaimonPartitionViewSizeEstimatorTest {

    @Test
    public void weightIsPrecomputedAndGrowsWithOwnedPartitions() {
        ConnectorTableKey key = new ConnectorTableKey("db", "table", 10L, -1L);
        PaimonPartitionView one = new PaimonPartitionView(key,
                Collections.singletonList(partition(1)));
        PaimonPartitionView two = new PaimonPartitionView(key,
                Arrays.asList(partition(1), partition(2)));

        Assertions.assertEquals(one.getEstimatedBytes(),
                PaimonPartitionViewSizeEstimator.estimateEntry(key, one));
        Assertions.assertTrue(two.getEstimatedBytes() > one.getEstimatedBytes());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> one.add(partition(3)));
    }

    private static ConnectorPartitionInfo partition(int bucket) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("dt", "2026-08-07");
        values.put("bucket", Integer.toString(bucket));
        List<String> orderedValues = new ArrayList<>(values.values());
        return new ConnectorPartitionInfo(
                "dt=2026-08-07/bucket=" + bucket,
                values,
                Collections.emptyMap(),
                10_000L + bucket,
                128L * 1024L * 1024L,
                1_786_048_000_000L + bucket,
                4L,
                orderedValues,
                Arrays.asList(false, false));
    }
}
