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
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Skeleton tests for {@link IcebergScanPlanProvider} (P6.2-T01), mirroring the paimon connector's
 * {@code PaimonScanPlanProviderTest}. They pin the two capability constants and that {@code planScan}
 * resolves the table through the {@link IcebergCatalogOps} seam inside the auth context. The real split
 * planning (predicate pushdown, delete files, field-id history dict, COUNT/batch, vended credentials)
 * lands in P6.2-T02..T09 — these tests are extended then.
 *
 * <p>The seam fully covers the remote {@code Catalog} call, so the provider is built with a
 * {@link RecordingIcebergCatalogOps} fake and a {@code null} real catalog — entirely offline, no Mockito.
 */
public class IcebergScanPlanProviderTest {

    /** A recording seam whose {@code loadTable} returns a metadata-only fake iceberg table. */
    private static RecordingIcebergCatalogOps opsWithTable() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = new FakeIcebergTable("t1",
                new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(), "s3://bucket/db/t1", Collections.emptyMap());
        return ops;
    }

    @Test
    public void getScanRangeTypeIsFileScan() {
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), new RecordingIcebergCatalogOps());
        // WHY: iceberg is file-based, so BE must build a TFileScanRange. MUTATION: JDBC_SCAN / CUSTOM -> red.
        Assertions.assertEquals(ConnectorScanRangeType.FILE_SCAN, provider.getScanRangeType());
    }

    @Test
    public void ignorePartitionPruneShortCircuitIsTrue() {
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), new RecordingIcebergCatalogOps());
        // WHY: iceberg is predicate-driven — it re-plans through its own SDK from the pushed predicate and
        // never consults requiredPartitions (same as the legacy IcebergScanNode / paimon). So a GENUINE FE
        // prune-to-zero must scan-all rather than short-circuit to zero rows; otherwise once T02 wires the
        // predicate path, `WHERE col IS NULL` on a genuine-null partition rendered as a non-null sentinel
        // would drop rows. MUTATION: returning the default false -> red.
        Assertions.assertTrue(provider.ignorePartitionPruneShortCircuit());
    }

    @Test
    public void planScanResolvesTableViaSeamAndReturnsNoSplits() {
        // Skeleton: planScan resolves the table through the catalog-ops seam (the entry point T02..T09 build
        // on) and returns no ranges yet. Proves the catalogOps wiring without the not-yet-ported split logic.
        RecordingIcebergCatalogOps ops = opsWithTable();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(), ops);

        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        Assertions.assertTrue(ranges.isEmpty());
        // Proves the (db, table) coordinates were threaded through the seam to loadTable.
        Assertions.assertEquals("db1", ops.lastLoadDb);
        Assertions.assertEquals("t1", ops.lastLoadTable);
    }

    @Test
    public void planScanResolvesTableInsideAuthContext() {
        // The remote loadTable must sit INSIDE context.executeAuthenticated so the FE-injected Kerberos UGI
        // applies (mirrors IcebergConnectorMetadata + paimon's PaimonScanPlanProvider.resolveTable).
        RecordingIcebergCatalogOps ops = opsWithTable();
        RecordingConnectorContext context = new RecordingConnectorContext();
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), ops, context);

        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        Assertions.assertTrue(ranges.isEmpty());
        // MUTATION: resolving the table OUTSIDE the auth wrap (calling catalogOps.loadTable directly) ->
        // authCount stays 0 -> red.
        Assertions.assertEquals(1, context.authCount);
        Assertions.assertEquals("db1", ops.lastLoadDb);
    }
}
