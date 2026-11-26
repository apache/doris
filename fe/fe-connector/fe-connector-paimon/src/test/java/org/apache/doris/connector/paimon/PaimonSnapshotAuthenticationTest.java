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

import org.apache.doris.connector.cache.ConnectorMetadataCache;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Stream;

public class PaimonSnapshotAuthenticationTest {
    private final RecordingConnectorContext context = new RecordingConnectorContext();
    private final RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();

    private PaimonTableHandle handle() {
        FakePaimonTable table = new FakePaimonTable("t",
                RowType.builder().field("region", DataTypes.STRING()).build(),
                Collections.singletonList("region"), Collections.emptyList());
        ops.table = table;
        ops.latestSnapshotId = OptionalLong.of(7L);
        ops.snapshotReadAuthentication = () -> context.authDepth > 0;
        PaimonTableHandle handle = new PaimonTableHandle("db", "t",
                Collections.singletonList("region"), Collections.emptyList());
        handle.setPaimonTable(table);
        return handle;
    }

    private PaimonConnectorMetadata metadata(PaimonLatestSnapshotCache cache) {
        return new PaimonConnectorMetadata(ops, PaimonCatalogProperties.of(Collections.emptyMap()), context,
                new PaimonSchemaAtMemo(10), cache,
                new ConnectorMetadataCache<>("paimon", "partition_view", Collections.emptyMap()));
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, 60L})
    public void snapshotCacheLoadsInsideAuthentication(long ttl) {
        PaimonTableHandle handle = handle();
        PaimonLatestSnapshotCache cache = new PaimonLatestSnapshotCache(ttl, 10);
        Assertions.assertEquals(7L, metadata(cache).beginQuerySnapshot(null, handle).get().getSnapshotId());
        ops.latestSnapshotId = OptionalLong.of(8L);
        Assertions.assertEquals(ttl == 0 ? 8L : 7L,
                metadata(cache).beginQuerySnapshot(null, handle).get().getSnapshotId());
        cache.invalidate(Identifier.create("db", "t"));
        Assertions.assertEquals(8L, metadata(cache).beginQuerySnapshot(null, handle).get().getSnapshotId());
        Assertions.assertEquals(0, context.authDepth);
    }

    @Test
    public void partitionCacheKeyLoadsSnapshotInsideAuthentication() {
        PaimonTableHandle handle = handle();
        Assertions.assertTrue(metadata(new PaimonLatestSnapshotCache(0, 10))
                .listPartitionNames(null, handle).isEmpty());
        Assertions.assertTrue(ops.log.contains("latestSnapshotId"));
        Assertions.assertEquals(0, context.authDepth);
    }

    @Test
    public void emptySnapshotRemainsAnEmptyPin() {
        PaimonTableHandle handle = handle();
        ops.latestSnapshotId = OptionalLong.empty();
        Assertions.assertEquals(-1L, metadata(new PaimonLatestSnapshotCache(0, 10))
                .beginQuerySnapshot(null, handle).get().getSnapshotId());
    }

    @Test
    public void authenticationFailureDoesNotCacheAnEmptySnapshot() {
        PaimonTableHandle handle = handle();
        PaimonLatestSnapshotCache cache = new PaimonLatestSnapshotCache(60, 10);
        context.failAuth = true;
        Assertions.assertThrows(RuntimeException.class, () -> metadata(cache).beginQuerySnapshot(null, handle));
        Assertions.assertFalse(ops.log.contains("latestSnapshotId"));
        context.failAuth = false;
        Assertions.assertEquals(7L, metadata(cache).beginQuerySnapshot(null, handle).get().getSnapshotId());
        Assertions.assertEquals(0, context.authDepth);
    }

    static Stream<ConnectorTimeTravelSpec> timeTravelSpecs() {
        return Stream.of(ConnectorTimeTravelSpec.snapshotId("7"),
                ConnectorTimeTravelSpec.timestamp("1700000000000", true),
                ConnectorTimeTravelSpec.tag("release"),
                ConnectorTimeTravelSpec.versionRef("release"),
                ConnectorTimeTravelSpec.branch("branch"),
                ConnectorTimeTravelSpec.incremental(Map.of("startSnapshotId", "1", "endSnapshotId", "7")),
                ConnectorTimeTravelSpec.options(Collections.emptyMap()));
    }

    @ParameterizedTest
    @MethodSource("timeTravelSpecs")
    public void timeTravelReadsInsideAuthentication(ConnectorTimeTravelSpec spec) {
        PaimonTableHandle handle = handle();
        ops.snapshotExists = true;
        ops.branchExists = true;
        ops.branchTable = ops.table;
        ops.tagSnapshot = new PaimonCatalogOps.TagSnapshot(7L, 1L);
        ops.snapshotIdAtOrBefore = OptionalLong.of(7L);
        ops.snapshotSchemaId = OptionalLong.of(1L);
        Assertions.assertEquals(7L, metadata(new PaimonLatestSnapshotCache(0, 10))
                .resolveTimeTravel(null, handle, spec).get().getSnapshotId());
        Assertions.assertEquals(0, context.authDepth);
    }
}
