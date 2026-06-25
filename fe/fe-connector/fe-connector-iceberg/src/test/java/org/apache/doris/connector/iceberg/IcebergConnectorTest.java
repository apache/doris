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

import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Connector-level tests for {@link IcebergConnector} that can run offline (no live catalog / no AWS call). The
 * live s3tables catalog construction (hand-built {@code S3TablesClient} + {@code S3TablesCatalog.initialize}) is
 * exercised only at the P6.6 docker plugin-zip gate; here we lock the FAIL-LOUD routing invariants that guard it.
 * No Mockito — the {@link RecordingConnectorContext} fail-loud fake is used.
 */
public class IcebergConnectorTest {

    @Test
    public void s3TablesWithoutStorageFailsLoud() {
        // WHY: legacy IcebergS3TablesMetaStoreProperties always derives from S3Properties.of(origProps); the
        // connector needs a bound S3-compatible storage to derive the region + credentials for the control-plane
        // S3TablesClient. Missing storage must fail loud (a clear DorisConnectorException), NOT silently route
        // s3tables through the generic CatalogUtil path or fall back to an anonymous client. MUTATION: dropping
        // the chosenS3 presence check -> a NullPointer / wrong-path error instead of this message -> red.
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergConnector connector = new IcebergConnector(
                Map.of("iceberg.catalog.type", "s3tables",
                        "warehouse", "arn:aws:s3tables:us-east-1:1:bucket/b"),
                ctx);
        DorisConnectorException ex =
                Assertions.assertThrows(DorisConnectorException.class, () -> connector.getMetadata(null));
        Assertions.assertTrue(ex.getMessage().contains("S3-compatible storage"),
                "expected a fail-loud message naming the missing S3-compatible storage, got: " + ex.getMessage());
    }

    @Test
    public void s3TablesWithoutRegionFailsLoud() {
        // WHY: Region.of("") would yield an invalid AWS region that only blows up deep in the SDK; the connector
        // must reject a region-less s3tables storage up front (legacy getRegion() is validated non-blank at
        // property-binding time). MUTATION: passing a blank region straight to Region.of -> a cryptic SDK error
        // instead of this message -> red.
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        List<StorageProperties> storages = Collections.singletonList(
                new FakeS3CompatibleStorageProperties("S3").accessKey("AK").secretKey("SK"));
        ctx.storageProperties = storages;
        IcebergConnector connector = new IcebergConnector(
                Map.of("iceberg.catalog.type", "s3tables",
                        "warehouse", "arn:aws:s3tables:us-east-1:1:bucket/b"),
                ctx);
        DorisConnectorException ex =
                Assertions.assertThrows(DorisConnectorException.class, () -> connector.getMetadata(null));
        Assertions.assertTrue(ex.getMessage().contains("region"),
                "expected a fail-loud message naming the missing region, got: " + ex.getMessage());
    }

    @Test
    public void dlfWithoutStorageFailsLoud() {
        // WHY: legacy IcebergAliyunDLFMetaStoreProperties always selected an OSS StorageProperties; the connector
        // needs a bound OSS storage to back the DLFCatalog's S3FileIO. A missing one must fail loud BEFORE any
        // metastore call, NOT route dlf through the generic CatalogUtil path (which would ClassNotFound on the
        // dlf impl). MUTATION: dropping the chosenS3 presence check -> a different/cryptic error -> red.
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergConnector connector = new IcebergConnector(
                Map.of("iceberg.catalog.type", "dlf", "warehouse", "oss://b/wh"), ctx);
        DorisConnectorException ex =
                Assertions.assertThrows(DorisConnectorException.class, () -> connector.getMetadata(null));
        Assertions.assertTrue(ex.getMessage().contains("OSS storage"),
                "expected a fail-loud message naming the missing OSS storage, got: " + ex.getMessage());
    }

    @Test
    public void declaresMvccAndTimeTravelCapabilities() {
        // WHY: SUPPORTS_MVCC_SNAPSHOT is the gate PluginDrivenExternalDatabase checks to build the MVCC/MTMV
        // table subclass (so beginQuerySnapshot/resolveTimeTravel/applySnapshot fire); SUPPORTS_TIME_TRAVEL
        // mirrors paimon. MUTATION: leaving the default empty capability set -> iceberg tables build as plain
        // non-MVCC tables, time-travel silently reads latest -> red. (Inert pre-cutover; iceberg is not yet in
        // SPI_READY_TYPES, so getCapabilities does not touch the catalog and needs no live connection.)
        IcebergConnector connector = new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Set<ConnectorCapability> caps = connector.getCapabilities();
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT));
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_TIME_TRAVEL));
    }

    @Test
    public void declaresFullSchemaWriteOrderCapability() {
        // WHY (T06): legacy bindIcebergTableSink ALWAYS projects the write child to table.getFullSchema()
        // order (BindSink:797-803), regardless of the INSERT column list. The generic
        // bindConnectorTableSink reproduces that full-schema projection ONLY when the connector declares
        // SINK_REQUIRE_FULL_SCHEMA_ORDER (BindSink:892-905); otherwise it keeps user-column order, which
        // (a) writes values into the wrong remote columns and (b) makes the write-sort columnIndex
        // (a full-schema position) resolve against a misaligned sink output. MUTATION: omitting the
        // capability -> `INSERT INTO t (name, id) ...` orders/writes by the wrong column -> red.
        IcebergConnector connector = new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Assertions.assertTrue(connector.getCapabilities()
                .contains(ConnectorCapability.SINK_REQUIRE_FULL_SCHEMA_ORDER));
    }

    @Test
    public void declaresParallelWriteCapability() {
        // WHY (C3b ④b): legacy iceberg INSERT distributes via PhysicalIcebergTableSink, whose partition-hash
        // branch is DEAD (it reads getPartitionNames(), which IcebergExternalTable never overrides -> empty),
        // so at RUNTIME every iceberg INSERT (partitioned or not) returns SINK_RANDOM_PARTITIONED (parallel
        // writers). Post-cutover the generic PhysicalConnectorTableSink reproduces SINK_RANDOM_PARTITIONED
        // ONLY when the connector declares SUPPORTS_PARALLEL_WRITE (else it falls through to GATHER, a single
        // writer = a parallelism regression vs legacy). MUTATION: omitting the capability -> iceberg INSERT
        // degrades to GATHER post-flip -> red.
        IcebergConnector connector = new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Assertions.assertTrue(connector.getCapabilities()
                .contains(ConnectorCapability.SUPPORTS_PARALLEL_WRITE),
                "iceberg must declare SUPPORTS_PARALLEL_WRITE so post-flip INSERT keeps legacy random "
                        + "(parallel) distribution instead of degrading to GATHER");
    }
}
