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

package org.apache.doris.datasource;

import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class SplitInitialDeliveryTest {
    @Test
    void initialResponseHasIndependentBoundAndBackendMetadataDoesNotChangeChannelIdentity() throws Exception {
        Backend original = new Backend(1, "localhost", 0);
        Backend refreshed = new Backend(1, "new-host", 9000);
        FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
        Mockito.when(policy.computeScanRangeAssignment(Mockito.anyList())).thenAnswer(call -> {
            Multimap<Backend, Split> assigned = ArrayListMultimap.create();
            for (Split split : call.<List<Split>>getArgument(0)) {
                assigned.put(original, split);
            }
            return assigned;
        });
        SplitAssignment assignment = new SplitAssignment(policy, new SplitGenerator() { },
                (be, properties, split, keys, admission) -> new TScanRangeLocations(),
                Collections.emptyMap(), Collections.emptyList(), true, 128, 200);
        List<Split> splits = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            splits.add(Mockito.mock(Split.class));
        }
        assignment.addToQueue(splits);
        assignment.finishSchedule();
        SplitSource source = new SplitSource(refreshed, assignment, 1000, 64);
        Assertions.assertEquals(64, source.getNextBatch(100).size());
        Assertions.assertEquals(100, source.getNextBatch(100).size());
        Assertions.assertEquals(36, source.getNextBatch(100).size());
        Assertions.assertTrue(source.getNextBatch(100).isEmpty());
    }
}
