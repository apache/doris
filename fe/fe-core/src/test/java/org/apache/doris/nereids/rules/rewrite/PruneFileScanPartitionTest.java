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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Focused checks for the partition-range fallback in {@link PruneFileScanPartition}. */
public class PruneFileScanPartitionTest {
    @Test
    public void testFallbackRangesComeOnlyFromTheRecordedPartitionView() throws AnalysisException {
        Map<String, PartitionItem> recordedView = new LinkedHashMap<>();
        recordedView.put("p=1", listItem(1));
        recordedView.put("p=2", listItem(2));

        Optional<SortedPartitionRanges<String>> ranges = PruneFileScanPartition.fallbackSortedPartitionRanges(
                SelectedPartitions.NOT_PRUNED, recordedView);

        Assertions.assertTrue(ranges.isPresent());
        List<String> rangeNames = ranges.get().sortedPartitions.stream()
                .map(partition -> partition.id)
                .collect(Collectors.toList());
        Assertions.assertEquals(List.of("p=1", "p=2"), rangeNames);
    }

    private static ListPartitionItem listItem(int value) throws AnalysisException {
        PartitionKey key = PartitionKey.createPartitionKey(
                List.of(new PartitionValue(String.valueOf(value))), List.of(new Column("p", Type.INT)));
        return new ListPartitionItem(List.of(key));
    }
}
