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
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
        ExternalTable externalTable = Mockito.mock(ExternalTable.class);
        LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);

        Optional<SortedPartitionRanges<String>> ranges = PruneFileScanPartition.fallbackSortedPartitionRanges(
                externalTable, scan, SelectedPartitions.NOT_PRUNED, recordedView);

        Assertions.assertTrue(ranges.isPresent());
        List<String> rangeNames = rangeNames(ranges);
        Assertions.assertEquals(List.of("p=1", "p=2"), rangeNames);
        // A view whose generation is not a materialized connector snapshot must not consult the cross-query
        // range cache: those ranges could have been built from another enumeration of the same table.
        Mockito.verify(externalTable, Mockito.never()).getSortedPartitionRanges(Mockito.any());
    }

    @Test
    public void testMaterializedConnectorViewReusesCachedRanges() throws AnalysisException {
        Map<String, PartitionItem> recordedView = new LinkedHashMap<>();
        recordedView.put("p=1", listItem(1));
        recordedView.put("p=2", listItem(2));
        ExternalTable externalTable = Mockito.mock(ExternalTable.class);
        LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
        // The ranges of an immutable MVCC pin: same version token, so the cross-query cache entry describes
        // exactly the partition set the recorded map came from.
        SortedPartitionRanges<String> cached = SortedPartitionRanges.build(recordedView);
        Mockito.when(externalTable.getSortedPartitionRanges(scan)).thenReturn(Optional.of(cached));
        SelectedPartitions materializedPin = new SelectedPartitions(2, recordedView, true);

        Optional<SortedPartitionRanges<String>> ranges = PruneFileScanPartition.fallbackSortedPartitionRanges(
                externalTable, scan, materializedPin, recordedView);

        Assertions.assertTrue(ranges.isPresent());
        Assertions.assertSame(cached, ranges.get());
        Assertions.assertEquals(List.of("p=1", "p=2"), rangeNames(ranges));
    }

    @Test
    public void testMaterializedConnectorViewWithoutCacheEntryRebuildsRanges() throws AnalysisException {
        Map<String, PartitionItem> recordedView = new LinkedHashMap<>();
        recordedView.put("p=1", listItem(1));
        ExternalTable externalTable = Mockito.mock(ExternalTable.class);
        LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
        Mockito.when(externalTable.getSortedPartitionRanges(scan)).thenReturn(Optional.empty());
        SelectedPartitions materializedPin = new SelectedPartitions(1, recordedView, true);

        Optional<SortedPartitionRanges<String>> ranges = PruneFileScanPartition.fallbackSortedPartitionRanges(
                externalTable, scan, materializedPin, recordedView);

        Assertions.assertTrue(ranges.isPresent());
        Assertions.assertEquals(List.of("p=1"), rangeNames(ranges));
    }

    @Test
    public void testRecordedRangesWinOverTheCacheLookup() throws AnalysisException {
        Map<String, PartitionItem> recordedView = new LinkedHashMap<>();
        recordedView.put("p=1", listItem(1));
        ExternalTable externalTable = Mockito.mock(ExternalTable.class);
        LogicalFileScan scan = Mockito.mock(LogicalFileScan.class);
        SortedPartitionRanges<String> recorded = SortedPartitionRanges.build(recordedView);
        SelectedPartitions withRecordedRanges = new SelectedPartitions(1, recordedView, true, false,
                Optional.of(recorded));

        Optional<SortedPartitionRanges<String>> ranges = PruneFileScanPartition.fallbackSortedPartitionRanges(
                externalTable, scan, withRecordedRanges, recordedView);

        Assertions.assertTrue(ranges.isPresent());
        Assertions.assertSame(recorded, ranges.get());
        Mockito.verify(externalTable, Mockito.never()).getSortedPartitionRanges(Mockito.any());
    }

    private static List<String> rangeNames(Optional<SortedPartitionRanges<String>> ranges) {
        return ranges.get().sortedPartitions.stream()
                .map(partition -> partition.id)
                .collect(Collectors.toList());
    }

    private static ListPartitionItem listItem(int value) throws AnalysisException {
        PartitionKey key = PartitionKey.createPartitionKey(
                List.of(new PartitionValue(String.valueOf(value))), List.of(new Column("p", Type.INT)));
        return new ListPartitionItem(List.of(key));
    }
}
