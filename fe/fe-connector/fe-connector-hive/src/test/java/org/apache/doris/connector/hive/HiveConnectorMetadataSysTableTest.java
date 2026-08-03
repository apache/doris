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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

/**
 * Tests the hive connector's system-table exposure ({@code t$partitions}), the FIX-R3 restoration of
 * legacy {@code HMSExternalTable.getSupportedSysTables()} for {@code dlaType==HIVE}.
 *
 * <p>WHY these assertions matter:</p>
 * <ul>
 *   <li><b>{@code partitions} is advertised, TVF-backed.</b> A flipped hive table is a
 *       {@code PluginDrivenExternalTable}; fe-core discovers its sys tables from
 *       {@code listSupportedSysTables} and asks {@code isPartitionValuesSysTable} whether each is served
 *       by the generic {@code partition_values} TVF (fe-core {@code PartitionsSysTable}) vs a native
 *       scan. Reporting nothing here left {@code t$partitions} resolving to "Unknown sys table".</li>
 *   <li><b>Exposed UNCONDITIONALLY (partitioned or not).</b> Mirrors legacy: a {@code $partitions}
 *       query on a NON-partitioned table must reach the TVF and throw "… is not a partitioned table",
 *       not "Unknown sys table" — so a non-partitioned handle must still advertise {@code partitions}.</li>
 *   <li><b>No native handle.</b> Because {@code partitions} is TVF-backed, {@code getSysTableHandle}
 *       stays empty — the native handle path must never be consulted for it.</li>
 * </ul>
 *
 * <p>The hive-handle path never touches the metastore client, so the client is {@code null}
 * (mirroring {@code HiveConnectorMetadataSiblingDelegationTest}); a foreign-handle divert is covered
 * there.</p>
 */
public class HiveConnectorMetadataSysTableTest {

    private HiveConnectorMetadata metadata() {
        return new HiveConnectorMetadata(null, Collections.emptyMap(), new FakeConnectorContext());
    }

    private HiveTableHandle partitionedHandle() {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(Collections.singletonList("p"))
                .build();
    }

    private HiveTableHandle unpartitionedHandle() {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE).build();
    }

    @Test
    public void listsOnlyPartitionsForAHiveHandle() {
        Assertions.assertEquals(Collections.singletonList("partitions"),
                metadata().listSupportedSysTables(null, partitionedHandle()),
                "hive exposes the partitions sys table (t$partitions), served by the partition_values TVF");
    }

    @Test
    public void exposesPartitionsUnconditionallyEvenWhenUnpartitioned() {
        // Load-bearing: a $partitions query on a non-partitioned table must reach the TVF (which throws
        // "is not a partitioned table"), not short-circuit to "Unknown sys table". So it must still advertise.
        Assertions.assertEquals(Collections.singletonList("partitions"),
                metadata().listSupportedSysTables(null, unpartitionedHandle()),
                "partitions must be advertised for a non-partitioned hive table too (legacy parity)");
    }

    @Test
    public void partitionsIsPartitionValuesTvfBacked() {
        Assertions.assertTrue(metadata().isPartitionValuesSysTable(null, partitionedHandle(), "partitions"),
                "hive's partitions sys table is served by the generic partition_values TVF");
    }

    @Test
    public void onlyPartitionsIsTvfBacked() {
        HiveConnectorMetadata md = metadata();
        HiveTableHandle h = partitionedHandle();
        Assertions.assertFalse(md.isPartitionValuesSysTable(null, h, "snapshots"),
                "hive exposes no sys table other than partitions");
        Assertions.assertFalse(md.isPartitionValuesSysTable(null, h, "PARTITIONS"),
                "the sys-table name is case-sensitive lowercase (findSysTable is exact-match)");
        Assertions.assertFalse(md.isPartitionValuesSysTable(null, h, null),
                "a null sys name is simply not exposed (no NPE)");
    }

    @Test
    public void tvfBackedSysTableHasNoNativeHandle() {
        Optional<ConnectorTableHandle> handle =
                metadata().getSysTableHandle(null, partitionedHandle(), "partitions");
        Assertions.assertFalse(handle.isPresent(),
                "a TVF-backed sys table is served without a native connector handle");
    }
}
