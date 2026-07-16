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

import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Guards the predicate-driven scan capability: paimon must opt OUT of the FE prune-to-zero short-circuit
 * ({@link ConnectorScanPlanProvider#ignorePartitionPruneShortCircuit()} == true) so that, with master-parity
 * {@code isNull=false} genuine-null partitions, a {@code col IS NULL} query (which prunes every partition away
 * at FE) is NOT short-circuited to zero rows but re-planned from the pushed predicate — restoring the
 * genuine-null row (regression test_paimon_runtime_filter_partition_pruning qt_null_partition_4). The SPI
 * default stays {@code false} so partition-restricting connectors (e.g. MaxCompute) keep the short-circuit.
 */
public class PaimonScanPlanProviderCapabilityTest {

    @Test
    public void paimonOptsOutOfPruneToZeroShortCircuit() {
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), null);
        Assertions.assertTrue(provider.ignorePartitionPruneShortCircuit(),
                "paimon is predicate-driven and must opt out of the prune-to-zero short-circuit");
    }

    @Test
    public void spiDefaultKeepsShortCircuit() {
        // A connector that does not override the capability keeps the short-circuit (MaxCompute parity).
        ConnectorScanPlanProvider defaultProvider = (session, handle, columns, filter) -> Collections.emptyList();
        Assertions.assertFalse(defaultProvider.ignorePartitionPruneShortCircuit(),
                "the SPI default must keep the prune-to-zero short-circuit");
    }

    private static Map<String, String> part(String key, String value) {
        Map<String, String> m = new HashMap<>();
        m.put(key, value);
        return m;
    }

    private static PaimonScanRange rangeWithPartition(String path, Map<String, String> partitionValues) {
        return new PaimonScanRange.Builder()
                .path(path)
                .partitionValues(partitionValues)
                .build();
    }

    @Test
    public void scannedPartitionCountReturnsDistinctPartitions() {
        // FIX-L12 THE load-bearing RED assertion: paimon restores legacy selectedPartitionNum =
        // distinct dataSplit.partition() (== distinct rendered getPartitionValues() maps). Three ranges
        // over TWO partitions (two ranges of dt=1 + one of dt=2) must count 2, not 3. A mutation that
        // drops the override (default OptionalLong.empty()) makes this red.
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), null);
        List<ConnectorScanRange> ranges = Arrays.asList(
                rangeWithPartition("/t/dt=1/a.parquet", part("dt", "1")),
                rangeWithPartition("/t/dt=1/b.parquet", part("dt", "1")),
                rangeWithPartition("/t/dt=2/c.parquet", part("dt", "2")));
        Assertions.assertEquals(OptionalLong.of(2L), provider.scannedPartitionCount(ranges));
    }

    @Test
    public void scannedPartitionCountEmptyForUnpartitionedTable() {
        // Every range's partition map is empty (unpartitioned table) -> report nothing so the engine keeps
        // its own count. (Same value as the un-overridden default; documents the fall-through, not RED-able.)
        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), null);
        List<ConnectorScanRange> ranges = Arrays.asList(
                rangeWithPartition("/t/a.parquet", Collections.emptyMap()),
                rangeWithPartition("/t/b.parquet", Collections.emptyMap()));
        Assertions.assertEquals(OptionalLong.empty(), provider.scannedPartitionCount(ranges));
    }

    @Test
    public void scannedPartitionCountDefaultsToEmpty() {
        // The SPI default (non-overriding connector, e.g. hive/MaxCompute) reports nothing.
        ConnectorScanPlanProvider defaultProvider = (session, handle, columns, filter) -> Collections.emptyList();
        Assertions.assertEquals(OptionalLong.empty(),
                defaultProvider.scannedPartitionCount(Collections.emptyList()));
    }
}
