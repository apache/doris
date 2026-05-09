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

package org.apache.doris.catalog.stream;

import org.apache.doris.transaction.TransactionCommitFailedException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OlapTableStreamUpdateTest {

    @Test
    public void testCheckPartitionOffsetForHistoricalConsumeOk() throws Exception {
        Map<Long, Long> historicalPartitionOffset = Collections.singletonMap(1L, 100L);
        Map<Long, Long> partitionOffset = Collections.emptyMap();

        Map<Long, Long> next = Collections.singletonMap(1L, 100L);
        Map<Long, Long> prev = Collections.emptyMap();
        OlapTableStreamUpdate update = new OlapTableStreamUpdate(next, prev);

        update.checkPartitionOffset("test_db", "s1", historicalPartitionOffset, partitionOffset);
    }

    @Test
    public void testCheckPartitionOffsetForHistoricalConsumeConflict() {
        Map<Long, Long> historicalPartitionOffset = Collections.singletonMap(1L, 100L);
        Map<Long, Long> partitionOffset = Collections.emptyMap();

        Map<Long, Long> next = Collections.singletonMap(1L, 101L);
        Map<Long, Long> prev = new HashMap<>();
        OlapTableStreamUpdate update = new OlapTableStreamUpdate(prev, next);

        TransactionCommitFailedException exception = Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> update.checkPartitionOffset("test_db", "s1", historicalPartitionOffset, partitionOffset));
        Assertions.assertTrue(exception.getMessage().contains("history offset already consumed"));
    }
}
