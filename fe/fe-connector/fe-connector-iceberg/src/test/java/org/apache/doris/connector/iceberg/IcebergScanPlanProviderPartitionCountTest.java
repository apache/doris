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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.scan.ConnectorScanRange;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

/**
 * FIX-L12 — guards {@link IcebergScanPlanProvider#scannedPartitionCount} and the
 * {@link IcebergScanRange#getScannedPartitionKey()} identity it counts, which restore legacy
 * {@code IcebergScanNode}'s {@code selectedPartitionNum = partitionMapInfos.size()} (keyed by
 * {@code (PartitionData) file().partition()}).
 *
 * <p><b>Why this matters:</b> for a hidden/transform-partitioned iceberg table ({@code days(ts)},
 * {@code bucket(n,id)}) the engine's Nereids pruning only sees the declared source columns and cannot map
 * a predicate on {@code ts} to the {@code days(ts)} partition, so it over-reports the scanned-partition
 * count (feeding EXPLAIN {@code partition=N/M} and the {@code sql_block_rule} {@code partition_num} guard).
 * The connector, which resolves the real partitions via manifest/residual evaluation, reports the faithful
 * distinct count. The key is {@code specId|partitionDataJson} so two files are counted equal iff they share
 * the same spec and partition tuple — disambiguating cross-spec value collisions the value-only json merges.</p>
 */
public class IcebergScanPlanProviderPartitionCountTest {

    private static IcebergScanRange range(String path, Integer specId, String partitionDataJson) {
        IcebergScanRange.Builder builder = new IcebergScanRange.Builder().path(path);
        if (specId != null) {
            builder.partitionSpecId(specId);
        }
        if (partitionDataJson != null) {
            builder.partitionDataJson(partitionDataJson);
        }
        return builder.build();
    }

    @Test
    public void scannedPartitionKeyCombinesSpecAndData() {
        // The identity is specId|partitionDataJson; null when the file carries no PartitionData (unpartitioned).
        Assertions.assertEquals("0|[\"1\"]", range("/t/f.parquet", 0, "[\"1\"]").getScannedPartitionKey());
        Assertions.assertNull(range("/t/f.parquet", null, null).getScannedPartitionKey());
    }

    @Test
    public void scannedPartitionKeyDisambiguatesCrossSpecCollision() {
        // identity(id)=2 (spec 0) vs bucket(id)=2 (spec 1) both render ["2"] but are DISTINCT partitions;
        // including the spec id keeps them apart, matching legacy's PartitionData-object de-dup.
        Assertions.assertNotEquals(
                range("/t/a.parquet", 0, "[\"2\"]").getScannedPartitionKey(),
                range("/t/b.parquet", 1, "[\"2\"]").getScannedPartitionKey());
    }

    @Test
    public void scannedPartitionCountReturnsDistinctPartitions() {
        // FIX-L12 THE load-bearing RED assertion: three files over TWO distinct partitions (two files in
        // partition [1] + one in [2]) count 2, not 3. A mutation dropping the override (default
        // OptionalLong.empty()) makes this red.
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), null);
        List<ConnectorScanRange> ranges = Arrays.asList(
                range("/t/a.parquet", 0, "[\"1\"]"),
                range("/t/b.parquet", 0, "[\"1\"]"),
                range("/t/c.parquet", 0, "[\"2\"]"));
        Assertions.assertEquals(OptionalLong.of(2L), provider.scannedPartitionCount(ranges));
    }

    @Test
    public void scannedPartitionCountEmptyForUnpartitionedTable() {
        // No range carries a partition key (unpartitioned) -> report nothing so the engine keeps its count.
        // (Same value as the un-overridden default; documents the fall-through, not RED-able.)
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), null);
        List<ConnectorScanRange> ranges = Arrays.asList(
                range("/t/a.parquet", null, null),
                range("/t/b.parquet", null, null));
        Assertions.assertEquals(OptionalLong.empty(), provider.scannedPartitionCount(ranges));
    }
}
