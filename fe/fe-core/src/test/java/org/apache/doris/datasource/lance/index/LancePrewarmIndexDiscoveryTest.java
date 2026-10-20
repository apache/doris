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

package org.apache.doris.datasource.lance.index;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.Dataset;
import org.lance.index.Index;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class LancePrewarmIndexDiscoveryTest {
    private Index index(String name, List<Integer> fields) {
        return Index.builder().name(name).fields(fields).build();
    }

    @Test
    void mergesPhysicalSegmentsWithoutRequiringIndexDetails() {
        Dataset dataset = Mockito.mock(Dataset.class);
        Mockito.when(dataset.getIndexes()).thenReturn(Arrays.asList(
                index("legacy", Collections.singletonList(3)),
                index("legacy", Arrays.asList(3, 8)),
                index("__lance_frag_reuse", Collections.emptyList()),
                index("__lance_mem_wal", Collections.emptyList())));
        Assertions.assertEquals(Collections.singletonMap("legacy", Arrays.asList(3, 8)),
                LanceDatasetIndexDiscovery.readPrewarmIndexFields(dataset));
        Mockito.verify(dataset, Mockito.never()).describeIndices(Mockito.any());
    }

    @Test
    void rejectsMalformedUserMetadata() {
        Dataset dataset = Mockito.mock(Dataset.class);
        Mockito.when(dataset.getIndexes()).thenReturn(null);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceDatasetIndexDiscovery.readPrewarmIndexFields(dataset));
        for (Index malformed : Arrays.asList(null, index("", Collections.singletonList(3)),
                index("legacy", Collections.emptyList()), index("legacy", Collections.singletonList(-1)),
                index("legacy", Arrays.asList(3, null)))) {
            Mockito.when(dataset.getIndexes()).thenReturn(Collections.singletonList(malformed));
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> LanceDatasetIndexDiscovery.readPrewarmIndexFields(dataset));
        }
    }

    @Test
    void retainsPhysicalAndLogicalDiscoveryBounds() {
        Dataset dataset = Mockito.mock(Dataset.class);
        Mockito.when(dataset.getIndexes()).thenReturn(Collections.nCopies(
                LanceDatasetIndexDiscovery.MAX_PHYSICAL_INDEX_ENTRIES + 1,
                index("legacy", Collections.singletonList(3))));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceDatasetIndexDiscovery.readPrewarmIndexFields(dataset));
        Mockito.when(dataset.getIndexes()).thenReturn(IntStream.rangeClosed(
                0, LanceDatasetIndexDiscovery.MAX_LOGICAL_INDEXES)
                .mapToObj(i -> index("index_" + i, Collections.singletonList(3))).collect(Collectors.toList()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceDatasetIndexDiscovery.readPrewarmIndexFields(dataset));
    }
}
